// Copyright 2026 The Agent Substrate Authors
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

// Package hardware probes host hardware compatibility attributes and evaluates
// sandbox runtime compatibility between workers and snapshots.
package hardware

import (
	"runtime"
	"strings"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

const (
	// SchemaVersionV1 is the initial compatibility schema version.
	SchemaVersionV1 = "v1"

	// AttrArchitecture is the CPU architecture ("amd64", "arm64", etc.).
	AttrArchitecture = "architecture"

	// AttrSandboxBundleHash identifies the sandbox runtime bundle: a
	// sha256 over a SandboxConfig version's asset digests, computed by atelet.
	AttrSandboxBundleHash = "sandbox_bundle_hash"

	// AttrCPUFeatures is the set of CPU features `runsc cpu-features` reports
	// for a gVisor runtime, encoded by EncodeCPUFeatures as a hex bitmask over
	// the architecture's feature table. A snapshot's set must be a subset of
	// the worker's.
	AttrCPUFeatures = "cpu_features"

	// AttrCPUVendor identifies the host CPU vendor (e.g. "GenuineIntel",
	// "AuthenticAMD", or arm64 implementer "0x41") reported by microVM
	// runtimes.
	AttrCPUVendor = "cpu_vendor"

	// AttrCPUModel identifies the host CPU family/model (e.g. "6:143" on amd64
	// or "0x0:0xd40:1" on arm64) reported by microVM runtimes.
	AttrCPUModel = "cpu_model"
)

// ProbeHost returns a minimal v1 VersionedSandboxCompat with the current
// host's architecture in atelet wire format, for synthetic workers in tests
// and benchmarks. Production workers report the attributes atelet writes to
// sandbox-runtimes.json instead.
func ProbeHost() *ateletpb.VersionedSandboxCompat {
	return &ateletpb.VersionedSandboxCompat{
		SchemaVersion: SchemaVersionV1,
		Attributes: []*ateletpb.AttributeEntry{
			{
				Key:   AttrArchitecture,
				Value: runtime.GOARCH,
			},
		},
	}
}

// WorkerKeyPrefix starts every attribute key a worker owns. atelet owns the
// host keys above and never writes a key with this prefix.
const WorkerKeyPrefix = "worker."

// IsHostKey reports whether key is one of the host keys atelet writes to a
// worker's sandbox-runtimes.json.
func IsHostKey(key string) bool {
	switch key {
	case AttrArchitecture, AttrCPUFeatures, AttrCPUVendor, AttrCPUModel, AttrSandboxBundleHash:
		return true
	}
	return false
}

// IsWorkerKey reports whether key is one a worker owns.
func IsWorkerKey(key string) bool {
	return strings.HasPrefix(key, WorkerKeyPrefix)
}

// Requirement is what a worker must run to restore a snapshot: the
// ActorSandboxRuntime the snapshot was stamped with and the most the restore
// may recover, prepared for matching against many workers. The scheduler asks
// it once per candidate, so the snapshot's AttrCPUFeatures is parsed here,
// once, rather than on every comparison. The zero Requirement, like
// NewRequirement(nil, ...), requires nothing.
type Requirement struct {
	snap   *ateapi.ActorSandboxRuntime
	compat *ateapi.VersionedSandboxCompat // snap's version
	// max is the most the restore may recover. min is the least a worker
	// must restore at to match, max unless AtLeast lowered it.
	max, min ateapi.CompatibleFidelity
	// cpuFeatures is compat's AttrCPUFeatures parsed, when it has one.
	// badCPUFeatures records one that is not a canonical mask, which no
	// worker satisfies.
	cpuFeatures    featureMask
	badCPUFeatures bool
}

// NewRequirement prepares snap for matching. fidelity is the most the restore
// may recover: the snapshot's fidelity, or less when the caller restores less
// of it. A nil snap requires nothing.
func NewRequirement(snap *ateapi.ActorSandboxRuntime, fidelity ateapi.SnapshotFidelity) Requirement {
	if snap == nil {
		return Requirement{}
	}
	r := Requirement{snap: snap, compat: snap.GetVersion(), max: compatibleFidelity(fidelity)}
	r.min = r.max
	for _, a := range r.compat.GetAttributes() {
		if a.GetKey() == AttrCPUFeatures {
			var ok bool
			r.cpuFeatures, ok = parseFeatureMask(a.GetValue())
			r.badCPUFeatures = !ok
			break
		}
	}
	return r
}

// compatibleFidelity is f as a CompatibleFidelity. A fidelity outside the
// enum caps nothing: the restore request carries it unchanged, and atelet
// rejects it.
func compatibleFidelity(f ateapi.SnapshotFidelity) ateapi.CompatibleFidelity {
	switch f {
	case ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		return ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_VOLUMES
	case ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS:
		return ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_ROOTFS
	default:
		return ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_MEMORY
	}
}

// Runtime returns the ActorSandboxRuntime the requirement was prepared from, or
// nil when it requires nothing.
func (r Requirement) Runtime() *ateapi.ActorSandboxRuntime {
	return r.snap
}

// Max returns the most the restore may recover. Only for a requirement of
// something.
func (r Requirement) Max() ateapi.CompatibleFidelity {
	return r.max
}

// AtLeast returns r matching any worker that restores the snapshot at f or
// higher, rather than only at Max.
func (r Requirement) AtLeast(f ateapi.CompatibleFidelity) Requirement {
	r.min = f
	return r
}

// RestoreFidelity returns the most rt, the runtime an assignment records,
// restores of the snapshot: COMPATIBLE_FIDELITY_NONE for another sandbox
// class. Only for a requirement of something.
func (r Requirement) RestoreFidelity(rt *ateapi.ActorSandboxRuntime) ateapi.CompatibleFidelity {
	if rt.GetSandboxClass() != r.snap.GetSandboxClass() {
		return ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_NONE
	}
	return r.restoreFidelity(rt.GetVersion())
}

// restoreFidelity returns the most a worker runtime with compat worker
// restores of the snapshot. A runtime under another schema version, or one
// without a version, restores it without memory, as a runtime with a
// different value and no rule would. Otherwise each key either side has
// limits it:
//   - An equal value limits nothing.
//   - AttrCPUFeatures limits nothing when the snapshot's set is a subset of
//     the worker's, and caps at ROOTFS otherwise.
//   - A different value, or a key on one side only, caps at the rule the
//     worker gives for the snapshot's value and the rule the snapshot gives
//     for the worker's, the lower of the two, or at ROOTFS when neither side
//     has one. A missing key has value "" and no rules.
func (r Requirement) restoreFidelity(worker *ateapi.VersionedSandboxCompat) ateapi.CompatibleFidelity {
	fidelity := r.max
	if worker == nil || r.compat == nil || worker.GetSchemaVersion() == "" || worker.GetSchemaVersion() != r.compat.GetSchemaVersion() {
		return min(fidelity, ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_ROOTFS)
	}
	wAttrs := worker.GetAttributes()
	sAttrs := r.compat.GetAttributes()
	// API validation bounds attributes to at most 32 entries with unique keys,
	// so the nested scans are small and avoid map allocation on the hot path.
	for _, s := range sAttrs {
		fidelity = min(fidelity, r.keyFidelity(s.GetKey(), find(wAttrs, s.GetKey()), s))
	}
	for _, w := range wAttrs {
		if find(sAttrs, w.GetKey()) == nil {
			fidelity = min(fidelity, r.keyFidelity(w.GetKey(), w, nil))
		}
	}
	return fidelity
}

// keyFidelity returns the most a restore recovers by key, which the worker
// has as w and the snapshot as s. A nil entry is a missing key.
func (r Requirement) keyFidelity(key string, w, s *ateapi.AttributeEntry) ateapi.CompatibleFidelity {
	if key == AttrCPUFeatures {
		if w != nil && s != nil && !r.badCPUFeatures {
			if mask, ok := parseFeatureMask(w.GetValue()); ok && r.cpuFeatures.subsetOf(mask) {
				return ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_MEMORY
			}
		}
		return ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_ROOTFS
	}
	if w.GetValue() == s.GetValue() {
		return ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_MEMORY
	}
	fidelity := ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_MEMORY
	wRule, wOK := rule(w, s.GetValue())
	sRule, sOK := rule(s, w.GetValue())
	if wOK {
		fidelity = min(fidelity, wRule)
	}
	if sOK {
		fidelity = min(fidelity, sRule)
	}
	if !wOK && !sOK {
		fidelity = ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_ROOTFS
	}
	return fidelity
}

// find returns the entry of attrs for key, or nil.
func find(attrs []*ateapi.AttributeEntry, key string) *ateapi.AttributeEntry {
	for _, a := range attrs {
		if a.GetKey() == key {
			return a
		}
	}
	return nil
}

// rule returns the fidelity a gives for the other side's value, and whether
// it gives one. A nil a gives none.
func rule(a *ateapi.AttributeEntry, other string) (ateapi.CompatibleFidelity, bool) {
	for _, c := range a.GetCompatibleValues() {
		if c.GetValue() == other {
			return c.GetFidelity(), true
		}
	}
	return 0, false
}

// classOf returns the entry of status for class, or nil when the worker does
// not run that class.
func classOf(status *ateapi.WorkerStatus, class string) *ateapi.WorkerSandboxClass {
	for _, c := range status.GetSandboxClasses() {
		if c.GetSandboxClass() == class {
			return c
		}
	}
	return nil
}

// runtimeOf is the ActorSandboxRuntime an assignment or snapshot is stamped with
// for compat under class.
func runtimeOf(class string, compat *ateapi.VersionedSandboxCompat) *ateapi.ActorSandboxRuntime {
	return &ateapi.ActorSandboxRuntime{SandboxClass: class, Version: compat}
}

// bestRuntime returns the runtime from status that restores the most of the
// snapshot, and that fidelity: in the worker's entry for the snapshot's
// class, DefaultRuntime first, then RestorableRuntimes in order, the first
// that reaches Max. nil and COMPATIBLE_FIDELITY_NONE if the worker does not
// run that class. Only for a requirement of something.
func (r Requirement) bestRuntime(status *ateapi.WorkerStatus) (*ateapi.WorkerSandboxRuntime, ateapi.CompatibleFidelity) {
	c := classOf(status, r.snap.GetSandboxClass())
	if c == nil {
		return nil, ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_NONE
	}
	best, bestFidelity := c.GetDefaultRuntime(), r.restoreFidelity(c.GetDefaultRuntime().GetVersion())
	for _, rt := range c.GetRestorableRuntimes() {
		if bestFidelity == r.max {
			break
		}
		if f := r.restoreFidelity(rt.GetVersion()); f > bestFidelity {
			best, bestFidelity = rt, f
		}
	}
	return best, bestFidelity
}

// matchingRuntime returns the runtime from status that restores all of the
// snapshot Max allows, or nil. Only for a requirement of something.
func (r Requirement) matchingRuntime(status *ateapi.WorkerStatus) *ateapi.WorkerSandboxRuntime {
	if rt, f := r.bestRuntime(status); f == r.max {
		return rt
	}
	return nil
}

// MatchingRuntime returns the runtime from status that restores all of the
// snapshot Max allows, as a stamp: the snapshot's class with the version of
// the runtime matchingRuntime finds, or nil when it finds none. A requirement
// of nothing returns ColdBootRuntime.
func (r Requirement) MatchingRuntime(status *ateapi.WorkerStatus) *ateapi.ActorSandboxRuntime {
	if r.snap == nil {
		return ColdBootRuntime(status)
	}
	if rt := r.matchingRuntime(status); rt != nil {
		return runtimeOf(r.snap.GetSandboxClass(), rt.GetVersion())
	}
	return nil
}

// WorkerMatches reports whether a worker with status restores the snapshot at
// the requirement's least fidelity: Max, unless AtLeast lowered it. A
// requirement of nothing is met by any worker.
func (r Requirement) WorkerMatches(status *ateapi.WorkerStatus) bool {
	if r.snap == nil {
		return true
	}
	_, f := r.bestRuntime(status)
	return f >= r.min
}

// WorkerRestores reports whether a worker with status restores the snapshot
// at all, at any fidelity above COMPATIBLE_FIDELITY_NONE. A requirement of
// nothing is met by any worker.
func (r Requirement) WorkerRestores(status *ateapi.WorkerStatus) bool {
	if r.snap == nil {
		return true
	}
	_, f := r.bestRuntime(status)
	return f > ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_NONE
}

// WorkerRuntimeFor returns the worker's runtime that RuntimeFor stamps, as the
// worker reported it, or nil before the worker has registered.
func (r Requirement) WorkerRuntimeFor(status *ateapi.WorkerStatus) *ateapi.WorkerSandboxRuntime {
	if r.snap != nil {
		if rt := r.matchingRuntime(status); rt != nil {
			return rt
		}
	}
	classes := status.GetSandboxClasses()
	if len(classes) == 0 {
		return nil
	}
	return classes[0].GetDefaultRuntime()
}

// RuntimeFor returns the runtime a worker with status runs an actor under
// whose snapshot is stamped with the required runtime: the matching runtime
// when it restores all of the snapshot Max allows, else the ColdBootRuntime
// the guest boots fresh under. This is what an assignment records, and what
// RestoreFidelity is asked of for the restore.
func (r Requirement) RuntimeFor(status *ateapi.WorkerStatus) *ateapi.ActorSandboxRuntime {
	if rt := r.MatchingRuntime(status); rt != nil {
		return rt
	}
	return ColdBootRuntime(status)
}

// ColdBootRuntime returns the runtime a fresh actor starts under on a worker
// with status: the DefaultRuntime of its class, as a stamp, or nil before the
// worker has registered.
func ColdBootRuntime(status *ateapi.WorkerStatus) *ateapi.ActorSandboxRuntime {
	classes := status.GetSandboxClasses()
	if len(classes) == 0 {
		return nil
	}
	return runtimeOf(classes[0].GetSandboxClass(), classes[0].GetDefaultRuntime().GetVersion())
}

// SandboxClass returns the sandbox class a worker with status registered, or
// "" before it has.
func SandboxClass(status *ateapi.WorkerStatus) string {
	classes := status.GetSandboxClasses()
	if len(classes) == 0 {
		return ""
	}
	return classes[0].GetSandboxClass()
}
