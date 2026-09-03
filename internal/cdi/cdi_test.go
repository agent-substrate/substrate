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

package cdi

import "testing"

// A spec shaped like nvidia-ctk's: an "all" device carrying both nodes, a device
// per index, and a UUID alias for the same hardware.
const twoGPUSpec = `{
  "cdiVersion": "0.6.0",
  "kind": "nvidia.com/gpu",
  "containerEdits": {"env": ["COMMON=1"], "deviceNodes": [{"path": "/dev/nvidiactl"}]},
  "devices": [
    {"name": "all", "containerEdits": {"deviceNodes": [{"path": "/dev/nvidia0"}, {"path": "/dev/nvidia1"}]}},
    {"name": "0", "containerEdits": {"deviceNodes": [{"path": "/dev/nvidia0"}]}},
    {"name": "1", "containerEdits": {"deviceNodes": [{"path": "/dev/nvidia1"}]}},
    {"name": "GPU-abcdef", "containerEdits": {"deviceNodes": [{"path": "/dev/nvidia0"}]}}
  ]
}`

func mustParse(t *testing.T) *Spec {
	t.Helper()
	spec, err := Parse([]byte(twoGPUSpec))
	if err != nil {
		t.Fatalf("Parse() failed: %v", err)
	}
	return spec
}

func nodePaths(e Edits) []string {
	var paths []string
	for _, n := range e.DeviceNodes {
		paths = append(paths, n.Path)
	}
	return paths
}

func TestParseRejectsGarbage(t *testing.T) {
	if _, err := Parse([]byte("not json")); err == nil {
		t.Fatal("Parse() = nil error, want one")
	}
}

func TestEditsFor(t *testing.T) {
	tests := []struct {
		name    string
		devices []string
		want    []string
		wantErr bool
	}{{
		// Spec-level edits apply whether or not a device was named.
		name:    "no devices",
		devices: nil,
		want:    []string{"/dev/nvidiactl"},
	}, {
		name:    "one device",
		devices: []string{"1"},
		want:    []string{"/dev/nvidiactl", "/dev/nvidia1"},
	}, {
		name:    "several devices",
		devices: []string{"0", "1"},
		want:    []string{"/dev/nvidiactl", "/dev/nvidia0", "/dev/nvidia1"},
	}, {
		// Generators name the same hardware several ways; naming two of them is
		// the caller's error to make, and the duplicate must be visible rather
		// than quietly collapsed.
		name:    "aliases of one device repeat its nodes",
		devices: []string{"0", "GPU-abcdef"},
		want:    []string{"/dev/nvidiactl", "/dev/nvidia0", "/dev/nvidia0"},
	}, {
		name:    "unknown device",
		devices: []string{"7"},
		wantErr: true,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mustParse(t).EditsFor(tc.devices)
			if tc.wantErr {
				if err == nil {
					t.Fatal("EditsFor() = nil error, want one")
				}
				return
			}
			if err != nil {
				t.Fatalf("EditsFor() failed: %v", err)
			}
			paths := nodePaths(got)
			if len(paths) != len(tc.want) {
				t.Fatalf("device nodes = %v, want %v", paths, tc.want)
			}
			for i := range tc.want {
				if paths[i] != tc.want[i] {
					t.Fatalf("device nodes = %v, want %v", paths, tc.want)
				}
			}
		})
	}
}

// EditsFor must not mutate the spec: the spec-level edits are the starting point
// for every call, so appending to them in place would leak one call's devices
// into the next.
func TestEditsForDoesNotAccumulate(t *testing.T) {
	spec := mustParse(t)
	if _, err := spec.EditsFor([]string{"0"}); err != nil {
		t.Fatalf("EditsFor() failed: %v", err)
	}
	got, err := spec.EditsFor([]string{"1"})
	if err != nil {
		t.Fatalf("EditsFor() failed: %v", err)
	}
	want := []string{"/dev/nvidiactl", "/dev/nvidia1"}
	paths := nodePaths(got)
	if len(paths) != len(want) {
		t.Fatalf("second call = %v, want %v", paths, want)
	}
	for i := range want {
		if paths[i] != want[i] {
			t.Fatalf("second call = %v, want %v", paths, want)
		}
	}
}
