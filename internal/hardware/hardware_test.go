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

package hardware

import (
	"runtime"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestProbeHost(t *testing.T) {
	compat := ProbeHost()
	if got := compat.GetSchemaVersion(); got != SchemaVersionV1 {
		t.Errorf("ProbeHost() schema_version = %q, want %q", got, SchemaVersionV1)
	}
	attrs := compat.GetAttributes()
	if len(attrs) != 1 || attrs[0].GetKey() != AttrArchitecture || attrs[0].GetValue() != runtime.GOARCH {
		t.Errorf("ProbeHost() attributes = %v, want [{%s: %s}]", attrs, AttrArchitecture, runtime.GOARCH)
	}
}

func TestMatches(t *testing.T) {
	runtimeWith := func(class, ver string, attrs ...*ateapipb.AttributeEntry) *ateapipb.SandboxRuntime {
		return &ateapipb.SandboxRuntime{
			SandboxClass: class,
			Version: &ateapipb.VersionedSandboxCompat{
				SchemaVersion: ver,
				Attributes:    attrs,
			},
		}
	}
	amd64GVisor := runtimeWith("gvisor", "v1",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
	)
	amd64GVisorReordered := runtimeWith("gvisor", "v1",
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
	)
	arm64GVisor := runtimeWith("gvisor", "v1",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "arm64"},
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
	)
	amd64MicroVM := runtimeWith("microvm", "v1",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
	)
	amd64GVisorV2 := runtimeWith("gvisor", "v2",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
	)
	amd64GVisorExtraAttr := runtimeWith("gvisor", "v1",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
		&ateapipb.AttributeEntry{Key: "gvisor_asset_hash", Value: "abc"},
	)

	tests := []struct {
		name   string
		worker *ateapipb.SandboxRuntime
		snap   *ateapipb.SandboxRuntime
		want   bool
	}{
		{"nil snapshot imposes no constraint", amd64GVisor, nil, true},
		{"exact match", amd64GVisor, amd64GVisor, true},
		{"attribute order ignored", amd64GVisor, amd64GVisorReordered, true},
		{"different attribute value fails", arm64GVisor, amd64GVisor, false},
		{"different sandbox class fails", amd64MicroVM, amd64GVisor, false},
		{"different schema version fails", amd64GVisorV2, amd64GVisor, false},
		{"attribute the snapshot predates is not checked", amd64GVisorExtraAttr, amd64GVisor, true},
		{"attribute missing on worker fails", amd64GVisor, amd64GVisorExtraAttr, false},
		{"nil worker fails when snapshot is stamped", nil, amd64GVisor, false},
		{"nil version fails", &ateapipb.SandboxRuntime{SandboxClass: "gvisor"}, amd64GVisor, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Matches(tc.worker, tc.snap); got != tc.want {
				t.Errorf("Matches() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMatchesCompat(t *testing.T) {
	compat := func(ver string, attrs ...*ateapipb.AttributeEntry) *ateapipb.VersionedSandboxCompat {
		return &ateapipb.VersionedSandboxCompat{SchemaVersion: ver, Attributes: attrs}
	}
	amd64 := &ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"}
	arm64 := &ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "arm64"}
	cpu := &ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"}

	tests := []struct {
		name         string
		worker, snap *ateapipb.VersionedSandboxCompat
		want         bool
	}{
		{"equal", compat("v1", amd64), compat("v1", amd64), true},
		{"key only the worker has is ignored", compat("v1", amd64, cpu), compat("v1", amd64), true},
		{"key only the snapshot has fails", compat("v1", amd64), compat("v1", amd64, cpu), false},
		{"value differs", compat("v1", arm64), compat("v1", amd64), false},
		{"schema version differs", compat("v2", amd64), compat("v1", amd64), false},
		{"both schema versions empty", compat("", amd64), compat("", amd64), false},
		{"no attributes on either", compat("v1"), compat("v1"), true},
		// Unlike Matches, a nil snapshot identity here is a stamped snapshot
		// missing its identity, not the absence of a constraint.
		{"nil snapshot identity", compat("v1", amd64), nil, false},
		{"nil worker identity", nil, compat("v1", amd64), false},
		{"both nil", nil, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := MatchesCompat(tc.worker, tc.snap); got != tc.want {
				t.Errorf("MatchesCompat() = %v, want %v", got, tc.want)
			}
		})
	}
}
