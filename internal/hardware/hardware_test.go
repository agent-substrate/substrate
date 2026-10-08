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
	if got := compat.GetVersion(); got != SchemaVersionV1 {
		t.Errorf("ProbeHost() version = %q, want %q", got, SchemaVersionV1)
	}
	if len(compat.GetAttributes()) != 1 || compat.GetAttributes()[0].GetKey() != AttrArchitecture || compat.GetAttributes()[0].GetValue() != runtime.GOARCH {
		t.Errorf("ProbeHost() attributes = %v, want [{%s: %s}]", compat.GetAttributes(), AttrArchitecture, runtime.GOARCH)
	}
}

func TestMatches(t *testing.T) {
	runtimeWith := func(class, name, ver string, attrs ...*ateapipb.AttributeEntry) *ateapipb.SandboxRuntime {
		return &ateapipb.SandboxRuntime{
			SandboxClass: class,
			Name:         name,
			Version: &ateapipb.VersionedSandboxCompat{
				Version:    ver,
				Attributes: attrs,
			},
		}
	}
	amd64GVisor := runtimeWith("gvisor", "gvisor-2", "v1",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
	)
	amd64GVisorReordered := runtimeWith("gvisor", "gvisor-other-name", "v1",
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
	)
	arm64GVisor := runtimeWith("gvisor", "gvisor-2", "v1",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "arm64"},
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
	)
	amd64MicroVM := runtimeWith("microvm", "microvm-1", "v1",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
	)
	amd64GVisorV2 := runtimeWith("gvisor", "gvisor-2", "v2",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
	)
	amd64GVisorExtraAttr := runtimeWith("gvisor", "gvisor-2", "v1",
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
		{"informational name and attribute order ignored", amd64GVisor, amd64GVisorReordered, true},
		{"different attribute value fails", arm64GVisor, amd64GVisor, false},
		{"different sandbox class fails", amd64MicroVM, amd64GVisor, false},
		{"different schema version fails", amd64GVisorV2, amd64GVisor, false},
		{"extra attribute on worker fails 1-to-1 match", amd64GVisorExtraAttr, amd64GVisor, false},
		{"extra attribute on snapshot fails 1-to-1 match", amd64GVisor, amd64GVisorExtraAttr, false},
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
