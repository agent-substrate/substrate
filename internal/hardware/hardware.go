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

	// TODO: Add AttrCPUFeatures ("cpu_features") and other compatibility
	// attributes as snapshot compatibility expands.
)

// ProbeHost inspects the current host and returns its v1 VersionedSandboxCompat
// in atelet wire format.
//
// TODO: Probe and populate cpu_features and other host hardware attributes
// (e.g. via CPUID on amd64 and MIDR_EL1 on arm64).
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
			if wa.GetKey() == sa.GetKey() {
				if wa.GetValue() != sa.GetValue() {
					return false
				}
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
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
