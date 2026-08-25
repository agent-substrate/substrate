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

// Package guestmem answers "where did the micro-VM's guest RAM actually go".
//
// GetWorkloadStats reports the workload's containers, which is the right answer
// to "how much is this actor using" and no answer at all to "why is a 2048 MiB
// guest full". The containers are one slice of the guest; the guest kernel, the
// kata-agent, the page cache charged to no container, the tmpfs mounts the
// guest carries, and whatever is still free are the rest, and none of them
// appear in a per-container cgroup read.
//
// The split is also what connects guest RAM to snapshot cost. A Full checkpoint
// writes the guest's touched pages to a sparse memory-ranges file, so how big a
// snapshot is — and therefore how long a suspend and a resume take — is decided
// by the slices below. They are not equally hard to shrink: page cache could be
// dropped before a checkpoint without changing what the guest computes, tmpfs
// pages could not — nothing backs them, so the kernel has nowhere to drop them
// to — and container anon is the workload itself. A single "the guest is using
// 1.2 GiB" tells a reader none of that.
//
// The missing slices come from the kata-agent's own Prometheus registry
// (grpc.AgentService/GetMetrics), which carries the guest's /proc/meminfo. This
// package parses that scrape and subtracts the pieces down to a residual, so
// the result adds up to the guest's own MemTotal by construction — see
// Breakdown.
//
// Deliberately pure, like agentstats: it converts an already-fetched scrape and
// never talks to a guest, so it is testable without a live micro-VM and needs
// no linux build tag.
package guestmem

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Names of the two metric families this package reads out of the agent's
// scrape. Everything else in it (vmstat, loadavg, per-CPU /proc/stat, diskstat,
// netdev) is ignored.
// Both verified against kata-agent 4.0.0 (src/agent/src/metrics.rs), which is
// the kata-static release hack/microvm-assets/assemble.sh pins. The agent hand
// rolls its registry rather than using the Prometheus process collector, so
// neither name follows the collector's conventions and neither can be guessed.
const (
	// meminfoFamily is the guest kernel's /proc/meminfo, which the agent
	// republishes as one gauge labelled by entry: kata_guest_meminfo{item=...}.
	meminfoFamily = "kata_guest_meminfo"

	// agentRSSMetric is the agent process's own resident set. Bytes: the agent
	// reads stat.rss, which the kernel reports in pages, and multiplies by the
	// page size before setting the gauge. The name carries no unit suffix and
	// no "process" namespace, so it looks nothing like what the standard
	// collector would publish.
	agentRSSMetric = "kata_agent_total_rss"
)

// meminfoUnitFloor is the MemTotal below which the meminfo entries are read as
// kibibytes rather than bytes.
//
// The units are genuinely ambiguous at the wire. /proc/meminfo is kB, and
// whether the agent passes that through or converts depends on how it reads the
// file — the Rust procfs crate normalizes to bytes, a hand-rolled parse
// typically does not. Neither is visible in the scrape: the family name carries
// no unit suffix.
//
// So infer it from magnitude. A value under this floor cannot be a plausible
// guest's RAM in bytes, and reading it as kibibytes is the only interpretation
// that is not absurd. The rule only misfires on a guest assigned less than 64
// MiB, which this runtime never boots — the kata config default is 2048 MiB and
// right-sizing moves it up, not down.
const meminfoUnitFloor = 64 << 20

// Guest is the subset of the guest's own accounting the breakdown needs, in
// bytes. Zero means "the agent did not report it", which for every field except
// Total is a survivable gap rather than an error.
type Guest struct {
	// Total is MemTotal: RAM the guest kernel can hand out. Strictly less than
	// the RAM the VMM assigned the VM — firmware and the kernel's own early
	// reservations come off the top before the kernel counts what is left — so
	// it is the right denominator for a breakdown of what the guest can see,
	// and the wrong one for a breakdown of what the host paid for.
	Total uint64

	// Free is MemFree: never handed out. Not MemAvailable, which adds an
	// estimate of the reclaimable cache and would double-count against
	// PageCache below.
	Free uint64

	// Buffers, Cached, SReclaimable and Shmem are the page-cache constituents.
	// Cached includes Shmem (tmpfs pages), which is not reclaimable, so Compute
	// subtracts it back out of PageCache and reports it as its own slice.
	Buffers      uint64
	Cached       uint64
	SReclaimable uint64
	Shmem        uint64

	// AgentRSS is the kata-agent process's resident set. Small in absolute
	// terms and interesting anyway: it is pure overhead, it is per-VM, and at
	// this density it is the kind of number that only looks negligible until it
	// is multiplied by the actors on a node.
	AgentRSS uint64
}

// Breakdown is one guest's RAM split into slices that do not overlap and, by
// construction, add up to Total.
//
// "By construction" is doing real work here: KernelAndOther is a RESIDUAL, not
// a measurement. Everything the five measured slices fail to account for lands
// in it, so the slices always tile Total exactly and the sum is not a
// consistency check. What the residual is good for is the opposite — its SIGN
// and its MAGNITUDE are the check. See the field.
type Breakdown struct {
	// Total is Guest.Total, repeated so a consumer holding only a Breakdown can
	// still compute proportions.
	Total uint64

	// Free is memory the guest kernel has not handed to anybody.
	Free uint64

	// PageCache is file-backed memory the kernel would give back under
	// pressure: Buffers + (Cached - Shmem) + SReclaimable. On a guest that has
	// just booted a container image this is usually the largest slice after
	// Free, and it is the one most often mistaken for a leak.
	PageCache uint64

	// Containers is the sum of the workload containers' ANONYMOUS memory, not
	// their cgroup usage. Anonymous is what makes this slice disjoint from
	// PageCache: a container's file pages are already counted there, and
	// counting its cgroup usage here would count them twice.
	//
	// The flip side is that this slice under-reports what a container "costs" —
	// its kernel memory is real and lands in KernelAndOther instead, and the
	// files it wrote land in Tmpfs or PageCache. For "who is using the guest's
	// RAM" that is the wrong bias; for "do these slices tile the guest" it is
	// the only one that works. Read this against GetWorkloadStats' working-set
	// figure, which answers the first question and does not have to tile
	// anything.
	Containers uint64

	// Tmpfs is Shmem: pages of a filesystem that lives in RAM, plus shared
	// memory segments. Whatever puts them there, they are:
	//
	//   - not reclaimable, unlike PageCache — there is no backing store to
	//     write them out to, so the kernel cannot drop them under pressure;
	//   - not visible in Containers, because the cgroup charges them to shmem
	//     rather than anon;
	//   - captured by a Full checkpoint, because they are guest RAM, so they
	//     cost snapshot bytes and therefore suspend and resume latency.
	//
	// Folding them into PageCache would say they are reclaimable, which is
	// wrong in the expensive direction: a reader would conclude the guest has
	// headroom it does not have. Leaving them in KernelAndOther, which is what
	// this package did before the slice existed, attributes them to the guest
	// kernel, which is wrong about who to go ask.
	//
	// On the CURRENT runtime a workload's file writes do NOT land here. Each
	// container's overlay upper is a host directory served over virtio-fs (see
	// cmd/ateom-microvm/rootfsupper.go), so rootfs writes cost host disk, and
	// durable-dir volumes are host directories on the same share. What is left
	// is the small fixed set the runtime mounts — /dev, /dev/shm and /run, 64
	// MiB each at most (see cmd/ateom-microvm/spec.go) — plus anything the
	// workload mounts itself.
	//
	// It still earns a slice of its own, for two reasons that outlive the
	// current arrangement. Snapshots from the retired guest-tmpfs-upper era
	// still restore, and their upper does ride in guest RAM (see
	// cmd/ateom-microvm/restore.go). And this is the slice that moves if write
	// storage ever goes back inside the guest: a design that trades virtio-fs
	// for guest-side storage shows up here as RAM, and therefore as snapshot
	// bytes, which is precisely the regression worth being able to see before
	// it is shipped rather than after.
	//
	// Guest-wide, not per container: Shmem comes from /proc/meminfo, so it
	// covers every tmpfs the guest mounts at once. Separating them would mean a
	// per-cgroup shmem read that only the container slice could use.
	Tmpfs uint64

	// Agent is the kata-agent's resident set.
	Agent uint64

	// KernelAndOther is what is left: Total minus the five above. Genuinely a
	// mix — the guest kernel's non-reclaimable slab, page tables, kernel
	// stacks, vmalloc, plus any process in the guest that is not a workload
	// container, plus the container kernel memory that Containers declines to
	// claim.
	//
	// SIGNED, and negative is meaningful rather than a bug to clamp away. It
	// means the measured slices overlapped — the same pages counted twice —
	// which is the failure mode this decomposition can actually have, and the
	// only way it becomes visible. Flooring it at zero would hide exactly the
	// case worth seeing. A healthy guest sits at a modest positive value; a
	// large negative one says the container figure and the page-cache figure
	// are fighting over the same pages and the split needs revisiting.
	KernelAndOther int64
}

// Parse extracts the guest accounting from one kata-agent GetMetrics scrape.
//
// It fails only when MemTotal is missing or unparseable, because without a
// denominator there is no breakdown to compute. Every other entry it wants is
// optional and absent reads as zero: a missing Shmem shifts a little weight
// between two slices, a missing MemTotal makes the whole thing meaningless.
//
// The lookups are deliberately loose about spelling. The agent may publish
// meminfo either as one family labelled by entry
// (kata_guest_meminfo{item="mem_total"}) or flattened into the metric name
// (kata_guest_meminfo_mem_total), and the entry names themselves may keep
// /proc/meminfo's CamelCase or be snake_cased on the way through. All of those
// normalize to the same key, so a kata-agent version bump that changes the
// spelling does not silently zero a slice.
func Parse(scrape string) (Guest, error) {
	s := parseScrape(scrape)

	total, ok := s.meminfo("MemTotal")
	if !ok {
		// Name what the agent DID publish, not just what was wanted. This error
		// has one likely cause — a kata-agent whose metric naming has moved —
		// and the fix is to teach this package the new name, so the error is
		// only useful if it carries the new name. Without that a reader is left
		// to reproduce a guest and dump the scrape by hand.
		return Guest{}, fmt.Errorf("no %s MemTotal entry in the agent scrape (%d metrics parsed); the families it did publish are: %s",
			meminfoFamily, len(s), strings.Join(familyNames(scrape), ", "))
	}
	if total == 0 {
		return Guest{}, errors.New("the agent scrape reports a guest with zero MemTotal")
	}

	// One scale factor for every meminfo entry, decided by the only entry whose
	// plausible range is known. Deciding per entry would be worse: a nearly-idle
	// guest's Shmem is small enough to look like kibibytes whichever it is.
	scale := uint64(1)
	if total < meminfoUnitFloor {
		scale = 1024
	}
	get := func(name string) uint64 {
		v, _ := s.meminfo(name)
		return v * scale
	}

	agentRSS, _ := s.value(agentRSSMetric)

	return Guest{
		Total:        total * scale,
		Free:         get("MemFree"),
		Buffers:      get("Buffers"),
		Cached:       get("Cached"),
		SReclaimable: get("SReclaimable"),
		Shmem:        get("Shmem"),
		AgentRSS:     agentRSS,
	}, nil
}

// Compute slices g up, charging containersAnon to the workload.
//
// containersAnon is the sum of the actor's containers' Sample.MemoryAnonBytes;
// see Breakdown.Containers for why it is the anonymous figure and not the
// cgroup usage.
func Compute(g Guest, containersAnon uint64) Breakdown {
	// Cached counts tmpfs pages, which are not cache in the sense that matters
	// here — nothing reclaims them by dropping them. Take them out and report
	// them as Tmpfs instead. Saturating, because the kernel assembles
	// /proc/meminfo without a lock and Shmem can read a hair above the Cached
	// line beside it.
	cached := g.Cached
	if g.Shmem < cached {
		cached -= g.Shmem
	} else {
		cached = 0
	}
	pageCache := g.Buffers + cached + g.SReclaimable

	measured := g.Free + pageCache + containersAnon + g.Shmem + g.AgentRSS

	return Breakdown{
		Total:      g.Total,
		Free:       g.Free,
		PageCache:  pageCache,
		Containers: containersAnon,
		Tmpfs:      g.Shmem,
		Agent:      g.AgentRSS,
		// int64 across the whole guest range: a Total that would overflow it is
		// an 8-exbibyte VM. The subtraction is the point — see the field.
		KernelAndOther: int64(g.Total) - int64(measured),
	}
}

// scrape is a parsed Prometheus text exposition, keyed by normalized metric
// identity: the normalized family name, plus "/" and the normalized "item"
// label when the sample carries one. Both halves go through normalize so that
// the labelled and flattened spellings of one /proc entry land on keys the
// lookups below can derive from the same input.
type scrape map[string]uint64

// parseScrape reads the exposition format loosely enough for a scrape produced
// by another project's registry.
//
// It ignores what it does not understand rather than failing: HELP/TYPE
// comments, samples with labels other than item, values it cannot read as a
// number. A kata-agent that grows a metric this package has never heard of must
// not break the parse of the ones it has.
func parseScrape(text string) scrape {
	out := make(scrape)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// name[{labels}] value [timestamp]. The name part cannot contain a
		// space, and a label value could, so split at the LAST brace rather than
		// on whitespace.
		nameEnd := strings.LastIndex(line, "}")
		if nameEnd < 0 {
			nameEnd = strings.IndexAny(line, " \t")
			if nameEnd < 0 {
				continue
			}
		} else {
			nameEnd++
		}
		key, ok := metricKey(line[:nameEnd])
		if !ok {
			continue
		}
		fields := strings.Fields(line[nameEnd:])
		if len(fields) == 0 {
			continue
		}
		// Prometheus values are floats even when the source is an integer
		// counter, so "2013204" and "2013204.0" both have to read.
		f, err := strconv.ParseFloat(fields[0], 64)
		if err != nil || f < 0 {
			continue
		}
		out[key] = uint64(f)
	}
	return out
}

// maxReportedFamilies caps how many names an unparseable scrape names back. A
// kata-agent publishes on the order of a dozen families and this only runs on
// the error path, but the scrape is remote input and the result goes in a log
// line, so it is bounded rather than trusted to be small.
const maxReportedFamilies = 24

// familyNames lists the distinct metric families a scrape carries, in the
// spelling the agent used rather than the normalized one, for the error above.
//
// A second walk of the text rather than something the parse kept: the parse
// runs on every collection and this runs when one has already failed, so the
// cost is in the right place and the hot path stays a plain map.
func familyNames(text string) []string {
	seen := make(map[string]struct{})
	var names []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name := line
		if i := strings.IndexAny(line, "{ \t"); i >= 0 {
			name = line[:i]
		}
		if name == "" {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
		if len(names) == maxReportedFamilies {
			names = append(names, "...")
			break
		}
	}
	if len(names) == 0 {
		return []string{"(none: the scrape held no metric lines at all)"}
	}
	sort.Strings(names)
	return names
}

// metricKey turns a sample's name-and-labels into the map key, or reports that
// the sample is not one this package can index (a labelled sample whose labels
// do not include item).
func metricKey(nameAndLabels string) (string, bool) {
	open := strings.Index(nameAndLabels, "{")
	if open < 0 {
		return normalize(nameAndLabels), true
	}
	name := nameAndLabels[:open]
	labels := strings.TrimSuffix(nameAndLabels[open+1:], "}")
	item, ok := itemLabel(labels)
	if !ok {
		return "", false
	}
	return normalize(name) + "/" + normalize(item), true
}

// itemLabel pulls the item label out of a sample's label set, which is what
// the agent keys its /proc-derived gauges by.
//
// Hand-rolled rather than a real label parser: the values here are /proc field
// names, so they contain no commas, quotes or escapes, and a full parse would
// buy nothing. A value that did contain one simply fails to match and its slice
// reads zero.
func itemLabel(labels string) (string, bool) {
	for _, pair := range strings.Split(labels, ",") {
		name, value, found := strings.Cut(strings.TrimSpace(pair), "=")
		if !found || strings.TrimSpace(name) != "item" {
			continue
		}
		return strings.Trim(strings.TrimSpace(value), `"`), true
	}
	return "", false
}

// meminfo looks up one /proc/meminfo entry, trying both shapes the agent might
// have published it in: labelled by entry, then flattened into the name.
func (s scrape) meminfo(entry string) (uint64, bool) {
	if v, ok := s[normalize(meminfoFamily)+"/"+normalize(entry)]; ok {
		return v, true
	}
	return s.value(meminfoFamily + "_" + entry)
}

// value looks up an unlabelled metric by name, normalized the same way the
// stored keys are so the two agree.
func (s scrape) value(name string) (uint64, bool) {
	v, ok := s[normalize(name)]
	return v, ok
}

// normalize folds the spellings of one /proc entry onto a single key:
// "MemTotal", "mem_total" and "memtotal" are the same entry.
func normalize(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
		}
	}
	return b.String()
}
