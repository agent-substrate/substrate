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

package v1alpha1

import (
	"testing"

	"k8s.io/utils/ptr"
)

func TestDefaultVersionConfig(t *testing.T) {
	version := func(name string, state *SandboxVersionState) SandboxVersionConfig {
		return SandboxVersionConfig{Name: name, State: state}
	}

	tests := []struct {
		name     string
		spec     SandboxConfigSpec
		wantName string // "" means no default is expected
	}{{
		name: "default with nil state is returned",
		spec: SandboxConfigSpec{
			DefaultVersion: "v1",
			Versions:       []SandboxVersionConfig{version("v1", nil)},
		},
		wantName: "v1",
	}, {
		name: "default explicitly Enabled is returned",
		spec: SandboxConfigSpec{
			DefaultVersion: "v1",
			Versions:       []SandboxVersionConfig{version("v1", ptr.To(SandboxVersionStateEnabled))},
		},
		wantName: "v1",
	}, {
		name: "default picked out of several versions",
		spec: SandboxConfigSpec{
			DefaultVersion: "v2",
			Versions: []SandboxVersionConfig{
				version("v1", ptr.To(SandboxVersionStateDisabled)),
				version("v2", nil),
				version("v3", nil),
			},
		},
		wantName: "v2",
	}, {
		name: "Disabled default is treated as missing",
		spec: SandboxConfigSpec{
			DefaultVersion: "v1",
			Versions: []SandboxVersionConfig{
				version("v1", ptr.To(SandboxVersionStateDisabled)),
				version("v2", nil),
			},
		},
	}, {
		name: "defaultVersion names no entry",
		spec: SandboxConfigSpec{
			DefaultVersion: "v9",
			Versions:       []SandboxVersionConfig{version("v1", nil)},
		},
	}, {
		name: "no versions",
		spec: SandboxConfigSpec{DefaultVersion: "v1"},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.spec.DefaultVersionConfig()
			if tt.wantName == "" {
				if ok || got != nil {
					t.Fatalf("DefaultVersionConfig() = %+v, %v; want nil, false", got, ok)
				}
				return
			}
			if !ok || got == nil {
				t.Fatalf("DefaultVersionConfig() = %+v, %v; want %q, true", got, ok, tt.wantName)
			}
			if got.Name != tt.wantName {
				t.Fatalf("DefaultVersionConfig().Name = %q; want %q", got.Name, tt.wantName)
			}
			// The pointer must alias the spec so callers see the real entry,
			// not a copy.
			if got != &tt.spec.Versions[indexOf(tt.spec.Versions, tt.wantName)] {
				t.Fatalf("DefaultVersionConfig() returned a copy, want pointer into spec.Versions")
			}
		})
	}
}

func indexOf(versions []SandboxVersionConfig, name string) int {
	for i := range versions {
		if versions[i].Name == name {
			return i
		}
	}
	return -1
}
