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
type scanPrioritySources uint64

const (
	scanPrioritySourceSpanEverCaughtUp scanPrioritySources = 1 << iota
	scanPrioritySourceRegionLowLag
)

func (s scanPrioritySources) String() string {
	if s == 0 {
		return "none"
	}
	sources := make([]string, 0, 2)
	if s&scanPrioritySourceSpanEverCaughtUp != 0 {
		sources = append(sources, "span-ever-caught-up")
	}
	if s&scanPrioritySourceRegionLowLag != 0 {
		sources = append(sources, "region-low-lag")
	}
	return strings.Join(sources, ",")
}

// scanPriorityFacts is an immutable snapshot used to make one region priority
// decision at the region scheduling boundary.
type scanPriorityFacts struct {
	inherited        scanPriorityDecision
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

func defaultScanPriorityDecision() scanPriorityDecision {
	return scanPriorityDecision{priority: TaskLowPrior}
}

// scanPriorityDecisionFromRegion preserves the effective decision of a failed
// request while the range is resolved again. Inheritance is transport, not a
// separate reason for the priority.
func scanPriorityDecisionFromRegion(region regionInfo) scanPriorityDecision {
	return scanPriorityDecision{
		priority: taskTypeFromScanPriority(region.scanPriority),
		sources:  region.scanPrioritySources,
	}
}

func (r *scanPriorityResolver) resolve(facts scanPriorityFacts) scanPriorityDecision {
	decision := facts.inherited
	if facts.spanEverCaughtUp {
		decision.requireAtLeast(TaskHighPrior, scanPrioritySourceSpanEverCaughtUp)
	}
	if r.isTsCloseToCurrent(facts.regionResolvedTs) {
		decision.requireAtLeast(TaskHighPrior, scanPrioritySourceRegionLowLag)
	}
	return decision
}

// requireAtLeast applies one priority floor. TaskType values with a smaller
// numeric value have a higher priority.
func (d *scanPriorityDecision) requireAtLeast(priority TaskType, source scanPrioritySources) {
	if priority < d.priority {
		d.priority = priority
	}
	if priority == d.priority {
		d.sources |= source
	}
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
