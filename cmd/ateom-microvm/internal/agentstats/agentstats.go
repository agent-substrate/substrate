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

	// MemoryAnonBytes is the container's anonymous memory: what it allocated
	// that no file backs, and that therefore cannot be reclaimed by dropping
	// it. Zero when the guest's memory.stat names it something anonKeys does
	// not know.
	//
	// Not reported on the wire, and not one of the four fields above. It exists
	// for the guest-wide breakdown in internal/guestmem, which needs a
	// per-container figure that does NOT overlap the guest's global page cache.
	// MemoryWorkingSetBytes cannot be that figure: it only removes the INACTIVE
	// file pages, so a container's active file pages remain in it while also
	// being counted in the guest's Cached, and a breakdown built on it would add
	// up to more memory than the guest has.
	MemoryAnonBytes uint64

	// CPUUsageUsec is cumulative CPU time consumed by the container, as seen by
	// the guest kernel.
	CPUUsageUsec uint64
}

// The agent passes the guest's memory.stat through verbatim — rustjail's
// get_memory_stats sets stats = memory.stat.raw — so which keys are present
// depends on the cgroup version the guest kernel gave the container, and each
// list below carries both spellings.
//
// On this runtime it is always the FIRST key that hits, and that is settled at
// boot rather than observed: kata 4.0.0 ships configuration-clh.toml with
// kernel_params = "cgroup_no_v1=all systemd.unified_cgroup_hierarchy=1"
// (src/runtime/arch/{arm64,amd64}-options.mk, substituted into the toml at
// build time), ParseConfig reads that field and buildVMConfig appends it to the
// cloud-hypervisor cmdline verbatim. So the guest kernel will not mount a v1
// controller at all, systemd mounts cgroup2 on /sys/fs/cgroup, and the agent's
// cgroups-rs hierarchies::auto() — which picks by the magic number of that
// mount — resolves to v2 for every container.
//
// The v1 spellings stay anyway. None of that chain is this package's to
// guarantee: a kata bump, an arch options file or an operator's kernel_params
// could each move the guest back to v1, and the failure mode would be a
// silently zeroed slice rather than an error. Two extra map lookups on a path
// that runs once per collection is the right price for that.

// Keys of the memory.stat entry holding reclaimable file-backed pages: v2 names
// it inactive_file, v1 has a per-cgroup inactive_file and the hierarchical
// total_inactive_file, and the total is the one that matches what v2's figure
// means.
var inactiveFileKeys = []string{"inactive_file", "total_inactive_file"}

// Keys of the memory.stat entry holding anonymous pages, in the order to try
// them. Same version split as the reclaimable keys above: v2 calls it anon, v1
// calls it rss and also publishes the hierarchical total_rss, which is the one
// that matches what v2's figure means — so it is tried first.
//
// v2's anon excludes tmpfs, which it charges to shmem (and counts again in
// file). That is what keeps this figure disjoint from the breakdown's Tmpfs
// slice, which reads the guest-wide Shmem: a page a container writes to a tmpfs
// moves shmem and leaves anon alone, so the two slices cannot claim the same
// page however much a workload writes.
var anonKeys = []string{"anon", "total_rss", "rss"}

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
	if inactiveFile, ok := statByKey(cs, inactiveFileKeys); ok {
		workingSet = 0
		if inactiveFile < current {
			workingSet = current - inactiveFile
		}
	}

	anon, _ := statByKey(cs, anonKeys)

	return Sample{
		MemoryCurrentBytes: current,
		MemoryPeakBytes:    mem.GetMaxUsage(),

		MemoryWorkingSetBytes: workingSet,
		MemoryAnonBytes:       anon,

		// The agent reports CPU time in nanoseconds, matching the runc stats
		// struct its own is modeled on; the proto wants microseconds. Truncating
		// division loses at most a microsecond per sample, and the field is
		// cumulative, so the error does not accumulate across samples.
		CPUUsageUsec: cs.GetCpuStats().GetCpuUsage().GetTotalUsage() / 1000,
	}
}

// statByKey returns the first memory.stat entry among keys that the guest
// reported, and whether it reported any of them.
//
// Both callers treat "reported none" as a miss rather than a zero, but they
// want different things from it. The working set collapses to usage, which
// over-reports by however much reclaimable cache the container holds — the safe
// direction, since it never claims the workload is using less than it is. The
// anon figure has no such fallback and stays zero, which pushes the weight into
// the breakdown's residual rather than into the container slice.
func statByKey(cs *agentpb.CgroupStats, keys []string) (uint64, bool) {
	stats := cs.GetMemoryStats().GetStats()
	for _, key := range keys {
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
// under the shared /ateomchv parent (see StartRootfsContainer and the
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
		MemoryAnonBytes:       addSaturating(s.MemoryAnonBytes, o.MemoryAnonBytes),
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
