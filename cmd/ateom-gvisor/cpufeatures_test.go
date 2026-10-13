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

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/hardware"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// fakeCPUFeaturesRunsc writes an executable that prints features when invoked as
// `runsc cpu-features` and fails otherwise, so a probe passes only if it asks
// for cpu-features.
func fakeCPUFeaturesRunsc(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runsc")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n[ \"$*\" = cpu-features ] || { echo \"unexpected args: $*\" >&2; exit 2; }\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunscCPUFeaturesProbe(t *testing.T) {
	ctx := context.Background()
	want, err := cpuFeaturesHash("avx,fpu,sse")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		paths   func(t *testing.T) map[string]string
		wantErr string
	}{{
		name: "release tarball's runsc",
		paths: func(t *testing.T) map[string]string {
			return map[string]string{"gvisor": fakeCPUFeaturesRunsc(t, "echo fpu,sse,avx\n")}
		},
	}, {
		name: "bare runsc asset",
		paths: func(t *testing.T) map[string]string {
			return map[string]string{"runsc": fakeCPUFeaturesRunsc(t, "echo sse,avx,fpu\n")}
		},
	}, {
		name: "prefers the release tarball",
		paths: func(t *testing.T) map[string]string {
			return map[string]string{"gvisor": fakeCPUFeaturesRunsc(t, "echo fpu,sse,avx\n"), "runsc": fakeCPUFeaturesRunsc(t, "exit 1\n")}
		},
	}, {
		name: "runsc fails",
		paths: func(t *testing.T) map[string]string {
			return map[string]string{"gvisor": fakeCPUFeaturesRunsc(t, "echo no cpuid >&2\nexit 1\n")}
		},
		wantErr: "no cpuid",
	}, {
		name: "runsc reports nothing",
		paths: func(t *testing.T) map[string]string {
			return map[string]string{"gvisor": fakeCPUFeaturesRunsc(t, "echo\n")}
		},
		wantErr: "reported no CPU features",
	}, {
		name:    "no runsc asset",
		paths:   func(*testing.T) map[string]string { return map[string]string{"kata-kernel": "/static/kernel"} },
		wantErr: "has no runsc asset",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runscCPUFeatures{}.Probe(ctx, &ateletpb.SandboxVersion{Name: "v1", AssetPaths: tt.paths(t)})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Probe() error = %v, want one containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Probe() failed: %v", err)
			}
			if len(got) != 1 || got[0].GetKey() != hardware.AttrCPUFeatures || got[0].GetValue() != want {
				t.Errorf("Probe() = %v, want one %s attribute %s", got, hardware.AttrCPUFeatures, want)
			}
		})
	}
}

func TestCPUFeaturesHash(t *testing.T) {
	want, err := cpuFeaturesHash("avx,fpu,sse")
	if err != nil {
		t.Fatal(err)
	}
	// The same set, however runsc prints it.
	for _, out := range []string{"sse,fpu,avx\n", " fpu , avx,sse,,", "avx,fpu,sse,fpu"} {
		if got, err := cpuFeaturesHash(out); err != nil || got != want {
			t.Errorf("cpuFeaturesHash(%q) = %q, %v, want %q", out, got, err, want)
		}
	}
	if got, _ := cpuFeaturesHash("avx,fpu"); got == want {
		t.Error("cpuFeaturesHash ignored a missing feature")
	}
	if _, err := cpuFeaturesHash(" \n"); err == nil {
		t.Error("cpuFeaturesHash of empty output succeeded")
	}
}
