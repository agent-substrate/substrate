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

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	// SchemaVersionV1 is the initial compatibility schema version.
	SchemaVersionV1 = "v1"

	// AttrArchitecture is the CPU architecture ("amd64", "arm64", etc.).
	AttrArchitecture = "architecture"

	// AttrSandboxRuntimeHash identifies the sandbox runtime binaries: a
	// sha256 over a SandboxConfig version's asset digests and pause image
	// digest, computed by atelet.
	AttrSandboxRuntimeHash = "sandbox_runtime_hash"

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

// Matches reports whether a worker's SandboxRuntime can restore a snapshot
// stamped with snap. A nil snapshot SandboxRuntime imposes no constraint.
// Otherwise the sandbox classes must be equal and MatchesCompat must hold.
func Matches(worker, snap *ateapipb.SandboxRuntime) bool {
	if snap == nil {
		return true
	}
	if worker == nil || worker.GetSandboxClass() != snap.GetSandboxClass() {
		return false
	}
	return MatchesCompat(worker.GetVersion(), snap.GetVersion())
}

// MatchesCompat reports whether worker can restore a snapshot stamped with
// snap: the same non-empty schema version, and every attribute on snap
// satisfied by worker. AttrCPUFeatures is satisfied when the snapshot's
// feature bitmask is a subset of the worker's; every other attribute must be
// present on worker with an equal value. Keys only the worker has are
// ignored, so a worker can start reporting a new key under the same schema
// version without invalidating the snapshots taken before it; removing or
// recomputing a key is a new schema version, under which older snapshots no
// longer match.
func MatchesCompat(worker, snap *ateapipb.VersionedSandboxCompat) bool {
	if worker == nil || snap == nil {
		return false
	}
	if worker.GetSchemaVersion() == "" || worker.GetSchemaVersion() != snap.GetSchemaVersion() {
		return false
	}
	wAttrs := worker.GetAttributes()
	sAttrs := snap.GetAttributes()
	// API validation bounds attributes to at most 32 entries with unique keys,
	// so the nested scan is small and avoids map allocation on the hot path.
	for _, sa := range sAttrs {
		found := false
		for _, wa := range wAttrs {
			if wa.GetKey() != sa.GetKey() {
				continue
			}
			if !attributeSatisfied(sa.GetKey(), wa.GetValue(), sa.GetValue()) {
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

// attributeSatisfied reports whether a worker's value for key satisfies a
// snapshot's. AttrCPUFeatures is a set the worker's must include; every
// other attribute must be equal.
func attributeSatisfied(key, workerVal, snapVal string) bool {
	if key == AttrCPUFeatures {
		return matchesCPUFeatures(workerVal, snapVal)
	}
	return workerVal == snapVal
}

// MatchingRuntime returns the SandboxRuntime from status that can restore a
// snapshot stamped with snap (checking DefaultRuntime first, then
// RestorableRuntimes in order), or nil if none matches. When snap is nil,
// DefaultRuntime is returned.
func MatchingRuntime(status *ateapipb.WorkerStatus, snap *ateapipb.SandboxRuntime) *ateapipb.SandboxRuntime {
	if snap == nil {
		return status.GetDefaultRuntime()
	}
	if def := status.GetDefaultRuntime(); Matches(def, snap) {
		return def
	}
	for _, rt := range status.GetRestorableRuntimes() {
		if Matches(rt, snap) {
			return rt
		}
	}
	return nil
}

// WorkerMatches reports whether a worker with status can restore a snapshot
// stamped with snap. A nil snapshot SandboxRuntime imposes no constraint.
func WorkerMatches(status *ateapipb.WorkerStatus, snap *ateapipb.SandboxRuntime) bool {
	if snap == nil {
		return true
	}
	return MatchingRuntime(status, snap) != nil
}

// RuntimeFor returns the runtime a worker with status runs an actor under
// whose snapshot is stamped with snap: the matching runtime when it can
// restore the snapshot, else the DefaultRuntime the guest boots fresh under.
// This is what an assignment records and what the actor's next checkpoint
// is stamped with.
func RuntimeFor(status *ateapipb.WorkerStatus, snap *ateapipb.SandboxRuntime) *ateapipb.SandboxRuntime {
	if rt := MatchingRuntime(status, snap); rt != nil {
		return rt
	}
	return status.GetDefaultRuntime()
}
