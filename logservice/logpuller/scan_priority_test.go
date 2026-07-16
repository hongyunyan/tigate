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
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestScanPriorityResolver(t *testing.T) {
	setScanPriorityLagThresholdForTest(t, 30*time.Minute)

	now := time.Date(2026, time.July, 16, 12, 0, 0, 0, time.UTC)
	pdClock := pdutil.NewClock4Test()
	pdClock.(*pdutil.Clock4Test).SetTS(oracle.GoTimeToTS(now))
	resolver := newScanPriorityResolver(pdClock)

	tests := []struct {
		name             string
		facts            scanPriorityFacts
		expectedPriority TaskType
		expectedSources  scanPrioritySources
	}{
		{
			name: "all rules remain low",
			facts: scanPriorityFacts{
				inherited:        defaultScanPriorityDecision(),
				regionResolvedTs: oracle.GoTimeToTS(now.Add(-31 * time.Minute)),
			},
			expectedPriority: TaskLowPrior,
		},
		{
			name: "inherited decision is preserved",
			facts: scanPriorityFacts{
				inherited: scanPriorityDecision{
					priority: TaskHighPrior,
					sources:  scanPrioritySourceRegionLowLag,
				},
				regionResolvedTs: oracle.GoTimeToTS(now.Add(-31 * time.Minute)),
			},
			expectedPriority: TaskHighPrior,
			expectedSources:  scanPrioritySourceRegionLowLag,
		},
		{
			name: "span ever caught up floor",
			facts: scanPriorityFacts{
				inherited:        defaultScanPriorityDecision(),
				spanEverCaughtUp: true,
				regionResolvedTs: oracle.GoTimeToTS(now.Add(-31 * time.Minute)),
			},
			expectedPriority: TaskHighPrior,
			expectedSources:  scanPrioritySourceSpanEverCaughtUp,
		},
		{
			name: "region low lag floor",
			facts: scanPriorityFacts{
				inherited:        defaultScanPriorityDecision(),
				regionResolvedTs: oracle.GoTimeToTS(now.Add(-time.Minute)),
			},
			expectedPriority: TaskHighPrior,
			expectedSources:  scanPrioritySourceRegionLowLag,
		},
		{
			name: "independent high floors are combined",
			facts: scanPriorityFacts{
				inherited: scanPriorityDecision{
					priority: TaskHighPrior,
					sources:  scanPrioritySourceRegionLowLag,
				},
				spanEverCaughtUp: true,
				regionResolvedTs: oracle.GoTimeToTS(now.Add(-time.Minute)),
			},
			expectedPriority: TaskHighPrior,
			expectedSources: scanPrioritySourceRegionLowLag |
				scanPrioritySourceSpanEverCaughtUp,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := resolver.resolve(test.facts)
			require.Equal(t, test.expectedPriority, decision.priority)
			require.Equal(t, test.expectedSources, decision.sources)
		})
	}
}

func TestScanPriorityDecisionFromRegion(t *testing.T) {
	region := regionInfo{
		scanPriority:        cdcpb.ScanPriority_SCAN_PRIORITY_HIGH,
		scanPrioritySources: scanPrioritySourceRegionLowLag,
	}

	decision := scanPriorityDecisionFromRegion(region)
	require.Equal(t, TaskHighPrior, decision.priority)
	require.Equal(t, scanPrioritySourceRegionLowLag, decision.sources)
}

var benchmarkScanPriorityDecision scanPriorityDecision

func BenchmarkScanPriorityResolver(b *testing.B) {
	now := time.Date(2026, time.July, 16, 12, 0, 0, 0, time.UTC)
	pdClock := pdutil.NewClock4Test()
	pdClock.(*pdutil.Clock4Test).SetTS(oracle.GoTimeToTS(now))
	resolver := newScanPriorityResolver(pdClock)

	b.Run("span sticky high", func(b *testing.B) {
		facts := scanPriorityFacts{
			inherited:        defaultScanPriorityDecision(),
			spanEverCaughtUp: true,
		}
		b.ReportAllocs()
		for b.Loop() {
			benchmarkScanPriorityDecision = resolver.resolve(facts)
		}
	})

	b.Run("region lag", func(b *testing.B) {
		facts := scanPriorityFacts{
			inherited:        defaultScanPriorityDecision(),
			regionResolvedTs: oracle.GoTimeToTS(now),
		}
		b.ReportAllocs()
		for b.Loop() {
			benchmarkScanPriorityDecision = resolver.resolve(facts)
		}
	})
}
