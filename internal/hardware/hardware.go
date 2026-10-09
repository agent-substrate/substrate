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
	"cmp"
	"runtime"
	"slices"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	// SchemaVersionV1 is the initial compatibility schema version.
	SchemaVersionV1 = "v1"

	// AttrArchitecture is the CPU architecture ("amd64", "arm64", etc.).
	AttrArchitecture = "architecture"

	// TODO: Add AttrCPUFeatures ("cpu_features"), AttrGVisorAssetHash
	// ("gvisor_asset_hash"), and other compatibility attributes as snapshot
	// compatibility expands.
)

// ProbeHost inspects the current host and returns its v1 VersionedSandboxCompat
// in atelet wire format.
//
// Attributes are emitted sorted by key so that repeated registrations of the
// same runtime compare equal under proto.Equal without triggering spurious
// store writes.
//
// TODO: Probe and populate cpu_features and other host hardware attributes
// (e.g. via CPUID on amd64 and MIDR_EL1 on arm64).
func ProbeHost() *ateletpb.VersionedSandboxCompat {
	attrs := []*ateletpb.AttributeEntry{
		{
			Key:   AttrArchitecture,
			Value: runtime.GOARCH,
		},
	}
	slices.SortFunc(attrs, func(a, b *ateletpb.AttributeEntry) int {
		return cmp.Compare(a.GetKey(), b.GetKey())
	})
	return &ateletpb.VersionedSandboxCompat{
		SchemaVersion: SchemaVersionV1,
		Attributes:    attrs,
	}
}

// Matches reports whether a worker's SandboxRuntime satisfies the SandboxRuntime
// recorded on snap. A nil snapshot SandboxRuntime imposes no constraint.
// Otherwise, sandbox_class, compat_version.schema_version, and all
// compat_version.attributes must match 1-to-1.
func Matches(worker, snap *ateapipb.SandboxRuntime) bool {
	if snap == nil {
		return true
	}
	if worker == nil || worker.GetSandboxClass() != snap.GetSandboxClass() {
		return false
	}
	return MatchesCompat(worker.GetCompatVersion(), snap.GetCompatVersion())
}

// MatchesCompat reports whether two VersionedSandboxCompat values have the same
// non-empty schema version and an exact 1-to-1 match on all attributes.
func MatchesCompat(worker, snap *ateapipb.VersionedSandboxCompat) bool {
	if worker == nil || snap == nil {
		return false
	}
	if worker.GetSchemaVersion() == "" || worker.GetSchemaVersion() != snap.GetSchemaVersion() {
		return false
	}
	wAttrs := worker.GetAttributes()
	sAttrs := snap.GetAttributes()
	if len(wAttrs) != len(sAttrs) {
		return false
	}
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
