// Copyright 2026 Google LLC
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

package agentstats

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
)

// cgroupStats builds an agent reading: usage/max bytes, the memory.stat entries
// the guest reported, and cumulative CPU nanoseconds.
func cgroupStats(usage, maxUsage uint64, stats map[string]uint64, cpuNanos uint64) *agentpb.CgroupStats {
	return &agentpb.CgroupStats{
		MemoryStats: &agentpb.MemoryStats{
			Usage: &agentpb.MemoryData{Usage: usage, MaxUsage: maxUsage},
			Stats: stats,
		},
		CpuStats: &agentpb.CpuStats{
			CpuUsage: &agentpb.CpuUsage{TotalUsage: cpuNanos},
		},
	}
}

func TestFromCgroupStats(t *testing.T) {
	for _, tc := range []struct {
		name string
		cs   *agentpb.CgroupStats
		want Sample
	}{
		{
			name: "cgroup v2 guest",
			cs:   cgroupStats(157286400, 209715200, map[string]uint64{"inactive_file": 20971520}, 1234567000),
			want: Sample{
				MemoryCurrentBytes:    157286400,
				MemoryPeakBytes:       209715200,
				MemoryWorkingSetBytes: 136314880,
				CPUUsageUsec:          1234567,
			},
		},
		{
			// A v1 guest names the hierarchical figure differently. Dropping to
			// the working-set-equals-usage fallback here would over-report every
			// sample from such a guest rather than fail visibly, so it is worth
			// pinning that the alternate key is understood.
			name: "cgroup v1 guest reports total_inactive_file",
			cs:   cgroupStats(1000, 2000, map[string]uint64{"total_inactive_file": 400}, 0),
			want: Sample{MemoryCurrentBytes: 1000, MemoryPeakBytes: 2000, MemoryWorkingSetBytes: 600},
		},
		{
			// v1 reports both: a per-cgroup figure and the hierarchical total.
			// The total is the one that means what v2's inactive_file means.
			name: "both keys present prefers the v2 name",
			cs:   cgroupStats(1000, 0, map[string]uint64{"inactive_file": 100, "total_inactive_file": 400}, 0),
			want: Sample{MemoryCurrentBytes: 1000, MemoryWorkingSetBytes: 900},
		},
		{
			// The two figures are not a consistent snapshot, so this is
			// reachable; on uint64 the naive subtraction would report ~1.8e19.
			name: "reclaimable cache above usage floors at zero",
			cs:   cgroupStats(1000, 0, map[string]uint64{"inactive_file": 4000}, 0),
			want: Sample{MemoryCurrentBytes: 1000, MemoryWorkingSetBytes: 0},
		},
		{
			name: "equal usage and reclaimable cache floors at zero",
			cs:   cgroupStats(1000, 0, map[string]uint64{"inactive_file": 1000}, 0),
			want: Sample{MemoryCurrentBytes: 1000, MemoryWorkingSetBytes: 0},
		},
		{
			// Nothing to subtract, so the working set collapses to usage: an
			// over-report, which is the safe direction.
			name: "no memory.stat entries",
			cs:   cgroupStats(1000, 2000, nil, 0),
			want: Sample{MemoryCurrentBytes: 1000, MemoryPeakBytes: 2000, MemoryWorkingSetBytes: 1000},
		},
		{
			name: "memory.stat without a reclaimable-cache entry",
			cs:   cgroupStats(1000, 0, map[string]uint64{"anon": 900}, 0),
			want: Sample{MemoryCurrentBytes: 1000, MemoryWorkingSetBytes: 1000},
		},
		{
			// Guests below Linux 5.19 have no cgroup v2 high-water mark. The rest
			// of the sample must survive that.
			name: "no peak reported",
			cs:   cgroupStats(1000, 0, map[string]uint64{"inactive_file": 100}, 5000),
			want: Sample{MemoryCurrentBytes: 1000, MemoryWorkingSetBytes: 900, CPUUsageUsec: 5},
		},
		{
			// The agent reports nanoseconds and the proto wants microseconds.
			// A sub-microsecond total truncates to zero rather than rounding up.
			name: "sub-microsecond cpu time truncates",
			cs:   cgroupStats(0, 0, nil, 999),
			want: Sample{},
		},
		{
			name: "cpu time rounds down to whole microseconds",
			cs:   cgroupStats(0, 0, nil, 1999),
			want: Sample{CPUUsageUsec: 1},
		},
		{
			name: "memory reported without cpu",
			cs:   &agentpb.CgroupStats{MemoryStats: &agentpb.MemoryStats{Usage: &agentpb.MemoryData{Usage: 4096}}},
			want: Sample{MemoryCurrentBytes: 4096, MemoryWorkingSetBytes: 4096},
		},
		{
			name: "cpu reported without memory",
			cs:   &agentpb.CgroupStats{CpuStats: &agentpb.CpuStats{CpuUsage: &agentpb.CpuUsage{TotalUsage: 2000}}},
			want: Sample{CPUUsageUsec: 2},
		},
		{
			// What the agent answers for a container it has no accounting for —
			// one that has exited, most often. Must be a zero sample, not a
			// panic: this is parsed on a timer for the life of every workload.
			name: "nil cgroup stats",
			cs:   nil,
			want: Sample{},
		},
		{
			name: "empty cgroup stats",
			cs:   &agentpb.CgroupStats{},
			want: Sample{},
		},
		{
			// The reclaimable figure is there but the usage it subtracts from is
			// not, so the working set floors rather than wrapping.
			name: "reclaimable cache without a usage message",
			cs:   &agentpb.CgroupStats{MemoryStats: &agentpb.MemoryStats{Stats: map[string]uint64{"inactive_file": 100}}},
			want: Sample{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, FromCgroupStats(tc.cs)); diff != "" {
				t.Errorf("FromCgroupStats() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSamplePlus(t *testing.T) {
	maxUint64 := ^uint64(0)

	for _, tc := range []struct {
		name string
		a, b Sample
		want Sample
	}{
		{
			name: "adds every field",
			a:    Sample{MemoryCurrentBytes: 100, MemoryPeakBytes: 200, MemoryWorkingSetBytes: 90, CPUUsageUsec: 10},
			b:    Sample{MemoryCurrentBytes: 1, MemoryPeakBytes: 2, MemoryWorkingSetBytes: 3, CPUUsageUsec: 4},
			want: Sample{MemoryCurrentBytes: 101, MemoryPeakBytes: 202, MemoryWorkingSetBytes: 93, CPUUsageUsec: 14},
		},
		{
			// The accumulator starts here, so a zero left operand must be the
			// identity or every actor's first container would be dropped.
			name: "zero is the identity",
			a:    Sample{},
			b:    Sample{MemoryCurrentBytes: 7, MemoryPeakBytes: 8, MemoryWorkingSetBytes: 9, CPUUsageUsec: 10},
			want: Sample{MemoryCurrentBytes: 7, MemoryPeakBytes: 8, MemoryWorkingSetBytes: 9, CPUUsageUsec: 10},
		},
		{
			// A wrapped total would read as a nearly idle actor, which is the one
			// wrong answer that looks plausible.
			name: "saturates instead of wrapping",
			a:    Sample{MemoryCurrentBytes: maxUint64, MemoryPeakBytes: maxUint64, MemoryWorkingSetBytes: maxUint64, CPUUsageUsec: maxUint64},
			b:    Sample{MemoryCurrentBytes: 1, MemoryPeakBytes: 2, MemoryWorkingSetBytes: 3, CPUUsageUsec: 4},
			want: Sample{MemoryCurrentBytes: maxUint64, MemoryPeakBytes: maxUint64, MemoryWorkingSetBytes: maxUint64, CPUUsageUsec: maxUint64},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, tc.a.Plus(tc.b)); diff != "" {
				t.Errorf("Sample.Plus() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
