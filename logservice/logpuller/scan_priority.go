// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logpuller

import (
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// scanPrioritySources records the independent reasons that require a scan task
// to run at high priority. It is a bitmask so priority decisions do not allocate.
type scanPrioritySources uint8

const (
	scanPrioritySourceInitialLowLag scanPrioritySources = 1 << iota
	scanPrioritySourceInherited
	scanPrioritySourceSpanEverCaughtUp
	scanPrioritySourceRegionLowLag
)

func (s scanPrioritySources) String() string {
	if s == 0 {
		return "none"
	}
	sources := make([]string, 0, 4)
	if s&scanPrioritySourceInitialLowLag != 0 {
		sources = append(sources, "initial-low-lag")
	}
	if s&scanPrioritySourceInherited != 0 {
		sources = append(sources, "inherited")
	}
	if s&scanPrioritySourceSpanEverCaughtUp != 0 {
		sources = append(sources, "span-ever-caught-up")
	}
	if s&scanPrioritySourceRegionLowLag != 0 {
		sources = append(sources, "region-low-lag")
	}
	return strings.Join(sources, ",")
}

// scanPriorityIntent carries the minimum priority that must be preserved while
// a range is resolved into regions or a failed request is retried.
type scanPriorityIntent struct {
	priorityFloor TaskType
	sources       scanPrioritySources
}

// scanPriorityFacts is an immutable snapshot used to make one region priority
// decision at the region scheduling boundary.
type scanPriorityFacts struct {
	intent           scanPriorityIntent
	spanEverCaughtUp bool
	regionResolvedTs uint64
}

type scanPriorityDecision struct {
	priority TaskType
	sources  scanPrioritySources
}

// scanPriorityResolver owns all rules that determine the scan priority sent to
// TiKV/CSE. Rules contribute a priority floor, and the highest floor wins.
type scanPriorityResolver struct {
	pdClock pdutil.Clock
}

func newScanPriorityResolver(pdClock pdutil.Clock) *scanPriorityResolver {
	return &scanPriorityResolver{pdClock: pdClock}
}

func (r *scanPriorityResolver) initialScanIntent(startTs uint64) scanPriorityIntent {
	intent := scanPriorityIntent{priorityFloor: TaskLowPrior}
	if r.isTsCloseToCurrent(startTs) {
		intent.priorityFloor = TaskHighPrior
		intent.sources = scanPrioritySourceInitialLowLag
	}
	return intent
}

// retryScanIntent preserves the effective priority of the failed request. The
// resolver will combine this floor with the latest span and region facts.
func retryScanIntent(region regionInfo) scanPriorityIntent {
	priority := taskTypeFromScanPriority(region.scanPriority)
	sources := region.scanPrioritySources
	if priority == TaskHighPrior {
		sources |= scanPrioritySourceInherited
	}
	return scanPriorityIntent{priorityFloor: priority, sources: sources}
}

func (r *scanPriorityResolver) resolve(facts scanPriorityFacts) scanPriorityDecision {
	decision := scanPriorityDecision{priority: TaskLowPrior}

	// Inherited and span-level floors are cheap to evaluate and can avoid a
	// pdClock read for the common sticky-high path.
	if facts.intent.priorityFloor == TaskHighPrior {
		decision.requireHigh(facts.intent.sources)
	}
	if facts.spanEverCaughtUp {
		decision.requireHigh(scanPrioritySourceSpanEverCaughtUp)
	}
	if decision.priority == TaskHighPrior {
		return decision
	}

	if r.isTsCloseToCurrent(facts.regionResolvedTs) {
		decision.requireHigh(scanPrioritySourceRegionLowLag)
	}
	return decision
}

func (d *scanPriorityDecision) requireHigh(source scanPrioritySources) {
	d.priority = TaskHighPrior
	d.sources |= source
}

// observeSpanResolved maintains the sticky span-level fact used by the
// resolver. Once a span has caught up, all its future region scans have a high
// priority floor even if the span later falls behind.
func (r *scanPriorityResolver) observeSpanResolved(span *subscribedSpan, resolvedTs uint64) {
	if span.everCaughtUp.Load() || !r.isTsCloseToCurrent(resolvedTs) {
		return
	}
	if span.everCaughtUp.CompareAndSwap(false, true) {
		log.Info("subscription catches up for the first time",
			zap.Uint64("subscriptionID", uint64(span.subID)),
			zap.Uint64("resolvedTs", resolvedTs))
	}
}

func (r *scanPriorityResolver) isTsCloseToCurrent(ts uint64) bool {
	if ts == 0 {
		return false
	}
	threshold := time.Duration(config.GetGlobalServerConfig().Debug.Puller.OldStartTsScanLowPriorityThreshold)
	return r.pdClock.CurrentTime().Sub(oracle.GetTimeFromTS(ts)) <= threshold
}
