//go:build linux

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
)

func TestMaybeInjectGPU_NoGPUIsNoop(t *testing.T) {
	dir := t.TempDir()
	old := gpuDeviceGlob
	gpuDeviceGlob = filepath.Join(dir, "nvidia[0-9]*") // matches nothing
	defer func() { gpuDeviceGlob = old }()
	if err := maybeInjectGPU(context.Background(), "actor_uid", "c1"); err != nil {
		t.Fatalf("expected no-op nil when no GPU is present, got %v", err)
	}
}

// TestGPUPresent checks detection matches any GPU index, not just nvidia0.
func TestGPUPresent(t *testing.T) {
	dir := t.TempDir()
	old := gpuDeviceGlob
	gpuDeviceGlob = filepath.Join(dir, "nvidia[0-9]*")
	defer func() { gpuDeviceGlob = old }()

	if gpuPresent() {
		t.Fatal("expected absent before creation")
	}
	// A worker sharing a multi-GPU node can be assigned nvidia2, not nvidia0.
	if err := os.WriteFile(filepath.Join(dir, "nvidia2"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if !gpuPresent() {
		t.Fatal("expected present after creating nvidia2")
	}
}

func TestGenerateCDISpec_InvokesCtk(t *testing.T) {
	dir := t.TempDir()
	oldT := toolkitDir
	toolkitDir = dir
	defer func() { toolkitDir = oldT }()

	// Fake nvidia-ctk (run directly by the glibc ateom) that writes a minimal JSON
	// spec to the --output= path.
	const script = `#!/bin/sh
out=""
for a in "$@"; do
	case "$a" in --output=*) out="${a#--output=}" ;; esac
done
printf '{"cdiVersion":"0.6.0","kind":"nvidia.com/gpu","devices":[{"name":"all","containerEdits":{"deviceNodes":[{"path":"/dev/nvidia0","type":"c","major":195,"minor":0}]}}]}' > "$out"
`
	os.WriteFile(filepath.Join(dir, "nvidia-ctk"), []byte(script), 0o755)

	out := filepath.Join(dir, "cdi")
	if err := generateCDISpec(context.Background(), out); err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(out, "nvidia.json"))
	if err != nil || !strings.Contains(string(data), "nvidia.com/gpu") {
		t.Fatalf("spec not written correctly: %q err=%v", data, err)
	}
}

func TestGenerateCDISpec_NonZeroFails(t *testing.T) {
	dir := t.TempDir()
	oldT := toolkitDir
	toolkitDir = dir
	defer func() { toolkitDir = oldT }()
	os.WriteFile(filepath.Join(dir, "nvidia-ctk"), []byte("#!/bin/sh\nexit 3\n"), 0o755)
	if err := generateCDISpec(context.Background(), filepath.Join(dir, "cdi")); err == nil {
		t.Fatal("expected error on non-zero exit")
	}
}
