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

// Package hardware probes host hardware compatibility attributes and evaluates
// sandbox runtime compatibility between workers and snapshots.
package hardware

import (
	"runtime"

	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
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

// Requirement is what a worker must run to restore a snapshot: the
// SandboxRuntime the snapshot was stamped with, prepared for matching against
// many workers. The scheduler asks it once per candidate, so the snapshot's
// AttrCPUFeatures is parsed here, once, rather than on every comparison. The
// zero Requirement, like NewRequirement(nil), requires nothing.
type Requirement struct {
	snap   *ateapipb.SandboxRuntime
	compat *ateapipb.VersionedSandboxCompat // snap's version
	// cpuFeatures is compat's AttrCPUFeatures parsed, when it has one.
	// badCPUFeatures records one that is not a canonical mask, which no
	// worker satisfies.
	cpuFeatures    featureMask
	badCPUFeatures bool
}

// NewRequirement prepares snap for matching. A nil snap requires nothing.
func NewRequirement(snap *ateapipb.SandboxRuntime) Requirement {
	r := compatRequirement(snap.GetVersion())
	r.snap = snap
	return r
}

func compatRequirement(compat *ateapipb.VersionedSandboxCompat) Requirement {
	r := Requirement{compat: compat}
	for _, a := range compat.GetAttributes() {
		if a.GetKey() == AttrCPUFeatures {
			var ok bool
			r.cpuFeatures, ok = parseFeatureMask(a.GetValue())
			r.badCPUFeatures = !ok
			break
		}
	}
	return r
}

// Runtime returns the SandboxRuntime the requirement was prepared from, or
// nil when it requires nothing.
func (r Requirement) Runtime() *ateapipb.SandboxRuntime {
	return r.snap
}

// Matches reports whether a worker's SandboxRuntime can restore the
// snapshot. A requirement of nothing is met by any worker. Otherwise the
// sandbox classes must be equal and the compat must match.
func (r Requirement) Matches(worker *ateapipb.SandboxRuntime) bool {
	if r.snap == nil {
		return true
	}
	if worker == nil || worker.GetSandboxClass() != r.snap.GetSandboxClass() {
		return false
	}
	return r.matchesCompat(worker.GetVersion())
}

// matchesCompat reports whether worker can restore the snapshot: the same
// non-empty schema version, and every attribute on the snapshot satisfied by
// worker. AttrCPUFeatures is satisfied when the snapshot's feature bitmask is
// a subset of the worker's; every other attribute must be present on worker
// with an equal value. Keys only the worker has are ignored, so a worker can
// start reporting a new key under the same schema version without
// invalidating the snapshots taken before it; removing or recomputing a key
// is a new schema version, under which older snapshots no longer match.
func (r Requirement) matchesCompat(worker *ateapipb.VersionedSandboxCompat) bool {
	if worker == nil || r.compat == nil {
		return false
	}
	if worker.GetSchemaVersion() == "" || worker.GetSchemaVersion() != r.compat.GetSchemaVersion() {
		return false
	}
	if r.badCPUFeatures {
		return false
	}
	wAttrs := worker.GetAttributes()
	// API validation bounds attributes to at most 32 entries with unique keys,
	// so the nested scan is small and avoids map allocation on the hot path.
	for _, sa := range r.compat.GetAttributes() {
		found := false
		for _, wa := range wAttrs {
			if wa.GetKey() != sa.GetKey() {
				continue
			}
			if !r.attributeSatisfied(sa.GetKey(), wa.GetValue(), sa.GetValue()) {
				return false
			}
			found = true
			break
		}
		if !found {
			return false
		}
	}
	return true
}

// attributeSatisfied reports whether a worker's value for key satisfies the
// snapshot's. AttrCPUFeatures is a set the worker's must include, compared
// against the mask parsed by NewRequirement; every other attribute must be
// equal.
func (r Requirement) attributeSatisfied(key, workerVal, snapVal string) bool {
	if key == AttrCPUFeatures {
		w, ok := parseFeatureMask(workerVal)
		return ok && r.cpuFeatures.subsetOf(w)
	}
	return workerVal == snapVal
}

// MatchingRuntime returns the SandboxRuntime from status that can restore
// the snapshot (checking DefaultRuntime first, then RestorableRuntimes in
// order), or nil if none matches. A requirement of nothing returns
// DefaultRuntime.
func (r Requirement) MatchingRuntime(status *ateapipb.WorkerStatus) *ateapipb.SandboxRuntime {
	if def := status.GetDefaultRuntime(); r.snap == nil || r.Matches(def) {
		return def
	}
	for _, rt := range status.GetRestorableRuntimes() {
		if r.Matches(rt) {
			return rt
		}
	}
	return nil
}

// WorkerMatches reports whether a worker with status can restore the
// snapshot. A requirement of nothing is met by any worker.
func (r Requirement) WorkerMatches(status *ateapipb.WorkerStatus) bool {
	return r.snap == nil || r.MatchingRuntime(status) != nil
}

// RuntimeFor returns the runtime a worker with status runs an actor under
// whose snapshot is stamped with the required runtime: the matching runtime
// when it can restore the snapshot, else the DefaultRuntime the guest boots
// fresh under. This is what an assignment records and what the actor's next
// checkpoint is stamped with.
func (r Requirement) RuntimeFor(status *ateapipb.WorkerStatus) *ateapipb.SandboxRuntime {
	if rt := r.MatchingRuntime(status); rt != nil {
		return rt
	}
	return status.GetDefaultRuntime()
}

// Matches reports whether a worker's SandboxRuntime can restore a snapshot
// stamped with snap, for one comparison; a caller holding one snap against
// many workers should prepare a Requirement once. A nil snap imposes no
// constraint.
func Matches(worker, snap *ateapipb.SandboxRuntime) bool {
	return NewRequirement(snap).Matches(worker)
}

// MatchesCompat reports whether worker can restore a snapshot stamped with
// snap, for one comparison. Unlike Matches, a nil snap is a stamp missing its
// identity and matches nothing.
func MatchesCompat(worker, snap *ateapipb.VersionedSandboxCompat) bool {
	return compatRequirement(snap).matchesCompat(worker)
}

// MatchingRuntime is NewRequirement(snap).MatchingRuntime(status).
func MatchingRuntime(status *ateapipb.WorkerStatus, snap *ateapipb.SandboxRuntime) *ateapipb.SandboxRuntime {
	return NewRequirement(snap).MatchingRuntime(status)
}

// WorkerMatches is NewRequirement(snap).WorkerMatches(status).
func WorkerMatches(status *ateapipb.WorkerStatus, snap *ateapipb.SandboxRuntime) bool {
	return NewRequirement(snap).WorkerMatches(status)
}

// RuntimeFor is NewRequirement(snap).RuntimeFor(status).
func RuntimeFor(status *ateapipb.WorkerStatus, snap *ateapipb.SandboxRuntime) *ateapipb.SandboxRuntime {
	return NewRequirement(snap).RuntimeFor(status)
}
