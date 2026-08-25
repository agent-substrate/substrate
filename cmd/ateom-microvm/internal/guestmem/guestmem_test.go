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

package guestmem

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// labelledScrape is the shape where the agent publishes /proc/meminfo as one
// family keyed by an item label. Values in kibibytes, as /proc/meminfo has
// them, for a guest given 2048 MiB. Trimmed to the entries this package reads
// plus a few it must ignore.
const labelledScrape = `# HELP kata_guest_meminfo Statistics about memory usage on the system.
# TYPE kata_guest_meminfo gauge
kata_guest_meminfo{item="mem_total"} 2013204
kata_guest_meminfo{item="mem_free"} 1310720
kata_guest_meminfo{item="mem_available"} 1800000
kata_guest_meminfo{item="buffers"} 8192
kata_guest_meminfo{item="cached"} 409600
kata_guest_meminfo{item="s_reclaimable"} 32768
kata_guest_meminfo{item="shmem"} 4096
kata_guest_meminfo{item="slab"} 51200
# HELP kata_guest_load Load average.
# TYPE kata_guest_load gauge
kata_guest_load{item="load1"} 0.08
kata_guest_stat{cpu="cpu0",item="user"} 1234
# TYPE kata_agent_total_rss gauge
kata_agent_total_rss 15728640
`

// flattenedScrape is the other shape: the entry folded into the metric name,
// CamelCase kept from /proc/meminfo, and values already converted to bytes.
// Same guest as labelledScrape, so both must parse to the same Guest.
const flattenedScrape = `# TYPE kata_guest_meminfo_MemTotal gauge
kata_guest_meminfo_MemTotal 2.061520896e+09
kata_guest_meminfo_MemFree 1.34217728e+09
kata_guest_meminfo_Buffers 8388608
kata_guest_meminfo_Cached 419430400
kata_guest_meminfo_SReclaimable 33554432
kata_guest_meminfo_Shmem 4194304
kata_agent_total_rss 1.572864e+07
`

// wantGuest is the guest both scrapes above describe, in bytes.
var wantGuest = Guest{
	Total:        2013204 * 1024,
	Free:         1310720 * 1024,
	Buffers:      8192 * 1024,
	Cached:       409600 * 1024,
	SReclaimable: 32768 * 1024,
	Shmem:        4096 * 1024,
	AgentRSS:     15728640,
}

// agent400Scrape reproduces what kata-agent 4.0.0 actually publishes — the
// release hack/microvm-assets/assemble.sh pins — rather than a shape this
// package hoped for. Transcribed from src/agent/src/metrics.rs: the family
// names come from NAMESPACE_KATA_GUEST/NAMESPACE_KATA_AGENT, the item labels
// from set_gauge_vec_meminfo, and the values are bytes because the agent reads
// /proc/meminfo through the procfs crate, which scales kB away.
//
// Every family the agent emits and this package ignores is kept, in the order
// the registry gathers them, because "ignores the rest of a real scrape" is
// most of what Parse has to get right. Trimmed only in the number of meminfo
// entries and CPUs, which are long and uninteresting.
//
// Not captured from a live guest — this is still transcription, and a real
// capture should replace it. It is pinned to a version, which the hand-built
// fixtures above are not.
const agent400Scrape = `# HELP kata_agent_scrape_count Metrics scrape count
# TYPE kata_agent_scrape_count counter
kata_agent_scrape_count 41
# HELP kata_agent_threads Agent process threads
# TYPE kata_agent_threads gauge
kata_agent_threads 12
# HELP kata_agent_total_rss Agent process total RSS size
# TYPE kata_agent_total_rss gauge
kata_agent_total_rss 15728640
# HELP kata_agent_total_time Agent process total time
# TYPE kata_agent_total_time gauge
kata_agent_total_time 0.34
# HELP kata_agent_total_vm Agent process total VM size
# TYPE kata_agent_total_vm gauge
kata_agent_total_vm 1250037760
# HELP kata_guest_load Guest system load.
# TYPE kata_guest_load gauge
kata_guest_load{item="load1"} 0.08
# HELP kata_guest_meminfo Statistics about memory usage in the system.
# TYPE kata_guest_meminfo gauge
kata_guest_meminfo{item="active"} 209715200
kata_guest_meminfo{item="anon_pages"} 178257920
kata_guest_meminfo{item="buffers"} 8388608
kata_guest_meminfo{item="cached"} 419430400
kata_guest_meminfo{item="dirty"} 131072
kata_guest_meminfo{item="inactive"} 314572800
kata_guest_meminfo{item="inactive_file"} 289406976
kata_guest_meminfo{item="mapped"} 41943040
kata_guest_meminfo{item="mem_available"} 1843200000
kata_guest_meminfo{item="mem_free"} 1342177280
kata_guest_meminfo{item="mem_total"} 2061520896
kata_guest_meminfo{item="page_tables"} 2097152
kata_guest_meminfo{item="s_reclaimable"} 33554432
kata_guest_meminfo{item="s_unreclaim"} 19922944
kata_guest_meminfo{item="shmem"} 4194304
kata_guest_meminfo{item="slab"} 53477376
kata_guest_meminfo{item="swap_free"} 0
kata_guest_meminfo{item="swap_total"} 0
# HELP kata_guest_vm_stat Guest virtual memory statistics.
# TYPE kata_guest_vm_stat gauge
kata_guest_vm_stat{item="pgfault"} 92341
`

// The one assumption in this package nothing else can check: that the family
// names, the item spellings and the units are what the pinned agent emits. A
// wrong guess here does not fail loudly — an unmatched optional entry reads as
// zero and its bytes drift into the residual — so it has to be asserted.
func TestParseReadsARealAgentScrape(t *testing.T) {
	got, err := Parse(agent400Scrape)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	want := Guest{
		Total:        2061520896,
		Free:         1342177280,
		Buffers:      8388608,
		Cached:       419430400,
		SReclaimable: 33554432,
		Shmem:        4194304,
		AgentRSS:     15728640,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Parse mismatch (-want +got):\n%s", diff)
	}
	// The agent reports bytes, so the kibibyte heuristic must stay out of it.
	// Scaling a real scrape by 1024 would claim a 2 TiB guest.
	if got.Total != 2061520896 {
		t.Errorf("Total = %d: a byte-valued scrape was rescaled", got.Total)
	}
}

// The two spellings are the thing this parser exists to be indifferent to: the
// kata-agent's metric naming is not a contract ateom controls, and a bump that
// changes it must not silently zero a slice.
func TestParseAcceptsBothMetricShapes(t *testing.T) {
	for name, scrape := range map[string]string{
		"labelled":  labelledScrape,
		"flattened": flattenedScrape,
	} {
		t.Run(name, func(t *testing.T) {
			got, err := Parse(scrape)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if diff := cmp.Diff(wantGuest, got); diff != "" {
				t.Errorf("Parse mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// The unit heuristic is the other half of that indifference, and the half that
// fails silently rather than loudly if it is wrong: a scrape misread by 1024x
// still parses.
func TestParseInfersUnits(t *testing.T) {
	tests := map[string]struct {
		memTotal  string
		wantTotal uint64
	}{
		"kibibytes are scaled": {"2013204", 2013204 * 1024},
		"bytes are left alone": {"2061520896", 2061520896},
		// The floor sits at 64 MiB, well under any guest this runtime boots and
		// well over any value that could only be kibibytes.
		"just under the floor is kibibytes": {"67108863", 67108863 * 1024},
		"just over the floor is bytes":      {"67108865", 67108865},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			g, err := Parse("kata_guest_meminfo{item=\"mem_total\"} " + tc.memTotal + "\n")
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if g.Total != tc.wantTotal {
				t.Errorf("Total = %d, want %d", g.Total, tc.wantTotal)
			}
		})
	}
}

// One scale factor for the whole scrape, not one per entry: a nearly-idle
// guest's Shmem is small enough to look like kibibytes whichever unit it is in,
// so deciding per entry would corrupt the small slices of a large guest.
func TestParseScalesEveryEntryTogether(t *testing.T) {
	g, err := Parse(`kata_guest_meminfo{item="mem_total"} 2013204
kata_guest_meminfo{item="shmem"} 64
`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if g.Shmem != 64*1024 {
		t.Errorf("Shmem = %d, want %d (the small entry must take MemTotal's scale)", g.Shmem, 64*1024)
	}
}

func TestParseNeedsMemTotal(t *testing.T) {
	for name, scrape := range map[string]string{
		"empty":          "",
		"comments only":  "# HELP kata_guest_meminfo x\n# TYPE kata_guest_meminfo gauge\n",
		"other entries":  "kata_guest_meminfo{item=\"mem_free\"} 1310720\n",
		"zero mem_total": "kata_guest_meminfo{item=\"mem_total\"} 0\n",
		"unparseable":    "kata_guest_meminfo{item=\"mem_total\"} nonsense\n",
		"negative":       "kata_guest_meminfo{item=\"mem_total\"} -1\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse(scrape); err == nil {
				t.Error("Parse must fail without a usable MemTotal rather than report a zero-sized guest")
			}
		})
	}
}

// The failure this error reports has one likely cause — the agent's metric
// naming moved — and the fix is to teach the package the new name. So the error
// has to carry the new name, or the reader has to go reproduce a guest and dump
// the scrape by hand to learn it.
func TestParseErrorNamesWhatTheAgentDidPublish(t *testing.T) {
	_, err := Parse(`# HELP some_other_meminfo x
some_other_meminfo{item="mem_total"} 2013204
kata_guest_vmstat{item="pgfault"} 17
some_other_meminfo{item="mem_free"} 1310720
`)
	if err == nil {
		t.Fatal("Parse must fail when it cannot find MemTotal")
	}
	for _, want := range []string{"some_other_meminfo", "kata_guest_vmstat"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not name the family %q, so it does not say how to fix itself: %v", want, err)
		}
	}
	// Named once, not once per sample.
	if n := strings.Count(err.Error(), "some_other_meminfo"); n != 1 {
		t.Errorf("family named %d times, want 1: %v", n, err)
	}
}

// The scrape is remote input and this text goes into a log line.
func TestParseErrorBoundsTheFamilyList(t *testing.T) {
	var b strings.Builder
	for i := range maxReportedFamilies * 3 {
		fmt.Fprintf(&b, "family_%03d 1\n", i)
	}
	_, err := Parse(b.String())
	if err == nil {
		t.Fatal("Parse must fail when it cannot find MemTotal")
	}
	if n := strings.Count(err.Error(), "family_"); n > maxReportedFamilies {
		t.Errorf("error names %d families, want at most %d", n, maxReportedFamilies)
	}
	if !strings.Contains(err.Error(), "...") {
		t.Error("a truncated list must say it was truncated")
	}
}

// Every entry but MemTotal is optional: a missing one shifts weight between
// slices, which the residual absorbs, and is not worth losing the whole
// breakdown over.
func TestParseToleratesMissingEntries(t *testing.T) {
	g, err := Parse("kata_guest_meminfo{item=\"mem_total\"} 2013204\n")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if g.Free != 0 || g.Cached != 0 || g.AgentRSS != 0 {
		t.Errorf("absent entries must read as zero, got %+v", g)
	}
}

// A kata-agent that grows a metric shape this package has never seen must not
// take the ones it does know down with it.
func TestParseIgnoresWhatItCannotRead(t *testing.T) {
	g, err := Parse(`# HELP kata_guest_meminfo x
kata_guest_meminfo{item="mem_total"} 2013204
this_line_has_no_value
kata_guest_netdev_stat{interface="eth0",item="recv_bytes"} 4096
kata_guest_meminfo{unexpected_label="mem_free"} 999
malformed{ 12
kata_guest_meminfo{item="mem_free"} 1310720
`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if g.Total != 2013204*1024 || g.Free != 1310720*1024 {
		t.Errorf("surrounding junk changed the parse: %+v", g)
	}
}

// sumOf adds every slice including the signed residual. Kept as a helper so a
// slice added later has to be added here too, and the tiling tests below fail
// loudly if it is not.
func sumOf(b Breakdown) int64 {
	return int64(b.Free) + int64(b.PageCache) + int64(b.Containers) +
		int64(b.Tmpfs) + int64(b.Agent) + b.KernelAndOther
}

func TestComputeTilesTotal(t *testing.T) {
	// 512 MiB of anonymous memory across the actor's containers.
	const containersAnon = 512 << 20
	b := Compute(wantGuest, containersAnon)

	if sum := sumOf(b); sum != int64(b.Total) {
		t.Errorf("components sum to %d, want Total %d — the slices must tile the guest by construction", sum, b.Total)
	}
	if b.Containers != containersAnon {
		t.Errorf("Containers = %d, want %d", b.Containers, containersAnon)
	}

	// Shmem is in Cached and is not reclaimable, so it must not be in PageCache.
	wantCache := wantGuest.Buffers + (wantGuest.Cached - wantGuest.Shmem) + wantGuest.SReclaimable
	if b.PageCache != wantCache {
		t.Errorf("PageCache = %d, want %d (Cached must have Shmem taken out of it)", b.PageCache, wantCache)
	}
	if b.Tmpfs != wantGuest.Shmem {
		t.Errorf("Tmpfs = %d, want %d — what PageCache gives up, Tmpfs must claim", b.Tmpfs, wantGuest.Shmem)
	}
}

// The case the Tmpfs slice exists for: a workload writing to a guest tmpfs
// allocates guest RAM under an API that looks like disk. The current runtime
// keeps rootfs and volume writes on the host (see rootfsupper.go), so this is
// the shape a snapshot from the retired guest-tmpfs-upper era has, and the
// shape any future move of write storage back inside the guest would take.
// Those pages land in Shmem: not in the container's cgroup anon, and taken back
// out of Cached because they cannot be reclaimed. Without a slice of their own
// they are invisible — they fall through into the residual and read as the
// guest kernel having grown.
func TestComputeAttributesAFileWrittenToTmpfs(t *testing.T) {
	const write = 256 << 20

	before := Compute(wantGuest, 0)

	after := wantGuest
	after.Free -= write
	after.Shmem += write
	after.Cached += write // the kernel counts tmpfs pages in Cached too
	got := Compute(after, 0)

	if delta := got.Tmpfs - before.Tmpfs; delta != write {
		t.Errorf("Tmpfs moved by %d, want %d", delta, write)
	}
	if got.PageCache != before.PageCache {
		t.Errorf("PageCache moved from %d to %d; tmpfs pages are not reclaimable and must not read as cache",
			before.PageCache, got.PageCache)
	}
	if got.Containers != before.Containers {
		t.Errorf("Containers moved from %d to %d; the cgroup charges tmpfs to shmem, not anon",
			before.Containers, got.Containers)
	}
	if got.KernelAndOther != before.KernelAndOther {
		t.Errorf("KernelAndOther moved from %d to %d; a container's file write is not the guest kernel growing",
			before.KernelAndOther, got.KernelAndOther)
	}
	if sum := sumOf(got); sum != int64(got.Total) {
		t.Errorf("components sum to %d, want Total %d", sum, got.Total)
	}
}

// The comparison the slice is meant to support: the same bytes written to a
// durable-dir volume are host-backed, so the guest holds them as ordinary
// reclaimable page cache instead. Same workload, same byte count, different
// slice — and only one of the two costs memory-snapshot bytes.
func TestComputeSeparatesTmpfsFromRealPageCache(t *testing.T) {
	const write = 256 << 20

	toTmpfs := wantGuest
	toTmpfs.Free -= write
	toTmpfs.Shmem += write
	toTmpfs.Cached += write

	toDurableDir := wantGuest
	toDurableDir.Free -= write
	toDurableDir.Cached += write // a file page with a backing store: no Shmem

	tmpfs, durable := Compute(toTmpfs, 0), Compute(toDurableDir, 0)

	if tmpfs.Tmpfs-durable.Tmpfs != write {
		t.Errorf("Tmpfs: %d vs %d, want a %d difference", tmpfs.Tmpfs, durable.Tmpfs, write)
	}
	if durable.PageCache-tmpfs.PageCache != write {
		t.Errorf("PageCache: %d vs %d, want a %d difference", durable.PageCache, tmpfs.PageCache, write)
	}
	if tmpfs.Free != durable.Free {
		t.Errorf("Free differs (%d vs %d); both writes cost the guest the same RAM", tmpfs.Free, durable.Free)
	}
}

// The residual is the only place an overlap between the measured slices can
// show up, so it must be allowed to go negative. Clamping it at zero would make
// the one failure mode this decomposition has invisible.
func TestComputeResidualGoesNegativeOnOverlap(t *testing.T) {
	g := Guest{Total: 1 << 30, Free: 1 << 29, Cached: 1 << 29}
	b := Compute(g, 1<<29) // containers claiming half the guest on top of that

	if b.KernelAndOther >= 0 {
		t.Fatalf("KernelAndOther = %d, want negative: the slices claim more than the guest has", b.KernelAndOther)
	}
	if b.KernelAndOther != -(1 << 29) {
		t.Errorf("KernelAndOther = %d, want %d", b.KernelAndOther, -(1 << 29))
	}
}

// The guest assembles /proc/meminfo without a lock, so Shmem can legitimately
// read a hair above the Cached line next to it. On uint64 the naive subtraction
// gives an absurd number instead of the near-zero the reading means.
func TestComputeSaturatesShmemAboveCached(t *testing.T) {
	g := Guest{Total: 1 << 30, Cached: 1000, Shmem: 2000, Buffers: 4096}
	b := Compute(g, 0)
	if b.PageCache != 4096 {
		t.Errorf("PageCache = %d, want 4096: Cached-Shmem must floor at zero, not wrap", b.PageCache)
	}
	// The floor changes what PageCache gives up, not what Tmpfs claims: Shmem
	// is reported as read, so the residual absorbs the inconsistency instead of
	// a slice silently shrinking to hide it.
	if b.Tmpfs != 2000 {
		t.Errorf("Tmpfs = %d, want 2000", b.Tmpfs)
	}
}

func TestNormalizeFoldsSpellings(t *testing.T) {
	want := normalize("MemTotal")
	for _, spelling := range []string{"mem_total", "memtotal", "MEM_TOTAL", "Mem-Total"} {
		if got := normalize(spelling); got != want {
			t.Errorf("normalize(%q) = %q, want %q", spelling, got, want)
		}
	}
	if strings.Contains(want, "_") {
		t.Errorf("normalize left a separator in %q", want)
	}
}
