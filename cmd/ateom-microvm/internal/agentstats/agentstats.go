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

// Package agentstats turns the kata guest agent's per-container cgroup
// accounting into the resource-usage sample ateom reports.
//
// The micro-VM ateom uses it to answer ateompb.Ateom/GetWorkloadStats. The host
// cgroup is the wrong place to look on this runtime: the guest's RAM is a fixed
// allocation cloud-hypervisor takes at boot, so the host cgroup reads roughly
// the same whether the actor is idle or saturated. The numbers that move with
// the workload are the ones the guest kernel keeps, and the agent is what can
// read them.
//
// This package is deliberately pure — it converts an already-fetched
// agentpb.CgroupStats and never talks to a guest — which keeps it testable
// without a live micro-VM and, unlike the rest of the micro-VM ateom, without
// the linux build tag.
package agentstats

import (
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
)

// Sample is a point-in-time reading for one container, or the sum of several.
// It carries the same four numbers as the gVisor ateom's cgroupstats.Sample,
// because both feed the same four fields of GetWorkloadStatsResponse.
//
// Anything the guest did not report reads as zero rather than failing the whole
// sample, for the same reason as there: a partial reading is more useful than
// none. FromCgroupStats says which fields can do that and why.
type Sample struct {
	// MemoryCurrentBytes is what is currently charged to the container's guest
	// cgroup, page cache included.
	MemoryCurrentBytes uint64

	// MemoryPeakBytes is the high-water mark of MemoryCurrentBytes. Zero when
	// the guest kernel does not expose one: the agent fills it from the cgroup's
	// max-usage file, which cgroup v2 only grew in Linux 5.19.
	//
	// For a single container this is exact; summed across containers it is an
	// upper bound, not an observed maximum — see Plus for why.
	MemoryPeakBytes uint64

	// MemoryWorkingSetBytes is MemoryCurrentBytes less the reclaimable page
	// cache, floored at zero — the figure to compare against a memory limit,
	// since MemoryCurrentBytes drifts upward with cache the kernel would drop
	// for free under pressure.
	MemoryWorkingSetBytes uint64

	// CPUUsageUsec is cumulative CPU time consumed by the container, as seen by
	// the guest kernel.
	CPUUsageUsec uint64
}

// Keys of the memory.stat entry holding reclaimable file-backed pages. The
// agent passes the guest's memory.stat through verbatim, so which one is
// present depends on the cgroup version the guest kernel gave the container:
// v2 names it inactive_file, v1 has a per-cgroup inactive_file and the
// hierarchical total_inactive_file, and the total is the one that matches what
// v2's figure means.
const (
	inactiveFileV2 = "inactive_file"
	inactiveFileV1 = "total_inactive_file"
)

// FromCgroupStats converts one container's guest cgroup accounting.
//
// It never fails. Every field the agent left out reads as zero, and cs itself
// may be nil — the agent answers without cgroup stats for a container it has no
// accounting for, which is a normal state for one that has exited rather than
// an error. The caller decides what an all-zero container means; see the
// summing in the micro-VM ateom's GetWorkloadStats.
func FromCgroupStats(cs *agentpb.CgroupStats) Sample {
	mem := cs.GetMemoryStats().GetUsage()
	current := mem.GetUsage()

	// Saturating rather than wrapping. The guest reads usage and memory.stat a
	// moment apart, so the reclaimable figure can legitimately exceed the usage
	// read beside it; on uint64 the naive subtraction gives an absurd number
	// instead of the near-zero the reading means.
	workingSet := current
	if inactiveFile, ok := inactiveFileBytes(cs); ok {
		workingSet = 0
		if inactiveFile < current {
			workingSet = current - inactiveFile
		}
	}

	return Sample{
		MemoryCurrentBytes: current,
		MemoryPeakBytes:    mem.GetMaxUsage(),

		MemoryWorkingSetBytes: workingSet,

		// The agent reports CPU time in nanoseconds, matching the runc stats
		// struct its own is modeled on; the proto wants microseconds. Truncating
		// division loses at most a microsecond per sample, and the field is
		// cumulative, so the error does not accumulate across samples.
		CPUUsageUsec: cs.GetCpuStats().GetCpuUsage().GetTotalUsage() / 1000,
	}
}

// inactiveFileBytes returns the reclaimable page cache the guest reported, and
// whether it reported one at all. Absent, the working set collapses to usage,
// which over-reports by however much reclaimable cache the container holds —
// the safe direction, since it never claims the workload is using less than it
// is.
func inactiveFileBytes(cs *agentpb.CgroupStats) (uint64, bool) {
	stats := cs.GetMemoryStats().GetStats()
	for _, key := range []string{inactiveFileV2, inactiveFileV1} {
		if v, ok := stats[key]; ok {
			return v, true
		}
	}
	return 0, false
}

// Plus returns the sum of two samples, for accumulating an actor's containers
// into the one figure the proto reports.
//
// Summing the peaks is an upper bound on the peak of the sum, not the peak of
// the sum itself: two containers that peaked at different moments add up to a
// total the actor never actually reached. The bound is the honest
// approximation, and for the single-container actors this runtime mostly serves
// it is exact.
//
// The guest kernel does track the true figure: every container cgroup sits
// under the shared /ateomchv parent (see StartOverlayWorkload and the
// CgroupsPath defaults in internal/kata), so with hierarchical accounting the
// parent's memory.peak is the actor-level maximum this sum approximates. The
// kata-agent just has no RPC that reads a cgroup by path — StatsContainer is
// per-container only. If the agent ever grows a sandbox-level stats read, that
// replaces this summing outright.
//
// Saturating on overflow, so a nonsensical reading from one container cannot
// wrap the total to a small number and read as healthy.
func (s Sample) Plus(o Sample) Sample {
	return Sample{
		MemoryCurrentBytes:    addSaturating(s.MemoryCurrentBytes, o.MemoryCurrentBytes),
		MemoryPeakBytes:       addSaturating(s.MemoryPeakBytes, o.MemoryPeakBytes),
		MemoryWorkingSetBytes: addSaturating(s.MemoryWorkingSetBytes, o.MemoryWorkingSetBytes),
		CPUUsageUsec:          addSaturating(s.CPUUsageUsec, o.CPUUsageUsec),
	}
}

// addSaturating returns a+b, or the maximum uint64 if that would wrap.
func addSaturating(a, b uint64) uint64 {
	if sum := a + b; sum >= a {
		return sum
	}
	const maxUint64 = ^uint64(0)
	return maxUint64
}
