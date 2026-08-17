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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
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

func TestInjectGPUIntoBundle(t *testing.T) {
	dir := t.TempDir()

	// Minimal CDI spec: one device node with major/minor OMITTED (as nvidia-ctk
	// emits) plus an env var. injectGPUIntoBundle must resolve major/minor from the
	// host — /dev/null is a char device present everywhere as 1,3 — so we assert on it.
	// The hooks cover all three cases: create-symlinks is allowlisted and kept,
	// update-ldcache is excluded, and an unrecognized hook (as a newer host toolkit
	// could emit) is dropped rather than run unreviewed.
	specJSON := `{
  "cdiVersion": "0.6.0",
  "kind": "nvidia.com/gpu",
  "devices": [
    {
      "name": "all",
      "containerEdits": {
        "deviceNodes": [{"path": "/dev/null"}],
        "env": ["NVIDIA_TEST=1"],
        "hooks": [
          {"hookName": "createContainer", "path": "/x/nvidia-cdi-hook",
           "args": ["nvidia-cdi-hook", "create-symlinks", "--link", "a::b"]},
          {"hookName": "createContainer", "path": "/x/nvidia-cdi-hook",
           "args": ["nvidia-cdi-hook", "update-ldcache", "--folder", "/usr/local/nvidia/lib64"]},
          {"hookName": "createContainer", "path": "/x/nvidia-cdi-hook",
           "args": ["nvidia-cdi-hook", "some-future-hook", "--flag", "v"]}
        ]
      }
    }
  ]
}`
	specDir := filepath.Join(dir, "cdi")
	os.MkdirAll(specDir, 0o755)
	os.WriteFile(filepath.Join(specDir, "nvidia.json"), []byte(specJSON), 0o644)

	bundle := filepath.Join(dir, "bundle")
	// The rootfs is already mounted by SetupBundleRootfs before injection runs.
	os.MkdirAll(filepath.Join(bundle, "rootfs"), 0o755)
	// The CUDA base image sets NVIDIA_VISIBLE_DEVICES; injection must strip it.
	base := &specs.Spec{Version: "1.0.0", Process: &specs.Process{
		Args: []string{"true"},
		Env:  []string{"NVIDIA_VISIBLE_DEVICES=all"},
	}}
	data, _ := json.Marshal(base)
	os.WriteFile(filepath.Join(bundle, "config.json"), data, 0o644)

	if err := injectGPUIntoBundle(context.Background(), bundle, specDir); err != nil {
		t.Fatalf("inject: %v", err)
	}

	out, _ := os.ReadFile(filepath.Join(bundle, "config.json"))
	var got specs.Spec
	json.Unmarshal(out, &got)

	var dev *specs.LinuxDevice
	for i := range got.Linux.Devices {
		if got.Linux.Devices[i].Path == "/dev/null" {
			dev = &got.Linux.Devices[i]
		}
	}
	if dev == nil {
		t.Fatalf("expected /dev/null device injected, spec=%s", out)
	}
	// The library resolved major/minor and type from the host (1,3,"c").
	if dev.Type != "c" || dev.Major != 1 || dev.Minor != 3 {
		t.Fatalf("device not resolved from host: type=%q major=%d minor=%d", dev.Type, dev.Major, dev.Minor)
	}
	var hasEnv, hasVisibleDevices bool
	for _, e := range got.Process.Env {
		if e == "NVIDIA_TEST=1" {
			hasEnv = true
		}
		if strings.HasPrefix(e, "NVIDIA_VISIBLE_DEVICES=") {
			hasVisibleDevices = true
		}
	}
	if !hasEnv {
		t.Fatalf("expected NVIDIA_TEST env injected, spec=%s", out)
	}
	// NVIDIA_VISIBLE_DEVICES must be stripped so runsc's nvproxy does not invoke
	// nvidia-container-cli (we set up the GPU via CDI instead).
	if hasVisibleDevices {
		t.Fatalf("expected NVIDIA_VISIBLE_DEVICES stripped, spec=%s", out)
	}
	// Only allowlisted hooks run. update-ldcache is excluded because its ldconfig
	// needs a private /proc mount (its SONAME symlinks are staged directly instead),
	// and an unrecognized hook is dropped rather than run unreviewed.
	var kept []string
	if got.Hooks != nil {
		for _, h := range got.Hooks.CreateContainer {
			if len(h.Args) > 1 {
				kept = append(kept, h.Args[1])
			}
			if !strings.HasPrefix(h.Path, toolkitDir) {
				t.Fatalf("hook path %q should point at the mounted toolkit", h.Path)
			}
		}
	}
	if len(kept) != 1 || kept[0] != "create-symlinks" {
		t.Fatalf("hooks = %v, want only [create-symlinks]", kept)
	}
}

// TestPrependLibraryPath covers the two cases that decide whether a CUDA program
// can load libcuda.so.1: an image that sets no LD_LIBRARY_PATH (must get one) and
// an NVIDIA image that already lists the driver directory (must be left alone).
func TestPrependLibraryPath(t *testing.T) {
	dirs := []string{"/usr/local/nvidia/lib64"}
	for _, tc := range []struct {
		name string
		env  []string
		want string
	}{
		{"no existing value", []string{"PATH=/bin"}, "LD_LIBRARY_PATH=/usr/local/nvidia/lib64"},
		{"template value is kept after the driver dir", []string{"LD_LIBRARY_PATH=/opt/app/lib"},
			"LD_LIBRARY_PATH=/usr/local/nvidia/lib64:/opt/app/lib"},
		{"already present is left alone", []string{"LD_LIBRARY_PATH=/usr/local/nvidia/lib64:/x"},
			"LD_LIBRARY_PATH=/usr/local/nvidia/lib64:/x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := prependLibraryPath(tc.env, dirs)
			var found string
			for _, e := range got {
				if strings.HasPrefix(e, "LD_LIBRARY_PATH=") {
					found = e
				}
			}
			if found != tc.want {
				t.Fatalf("got %q, want %q", found, tc.want)
			}
			if n := strings.Count(strings.Join(got, " "), "LD_LIBRARY_PATH="); n != 1 {
				t.Fatalf("want exactly one LD_LIBRARY_PATH entry, got %d in %v", n, got)
			}
		})
	}
}

// TestInjectGPUIntoBundle_Idempotent guards the invariant that atelet re-unpacks the
// bundle before each Run/Restore. The edits are appends against config.json on disk,
// so without the guard a second injection doubles every device, mount, env entry and
// hook — silently, since nothing errors.
func TestInjectGPUIntoBundle_Idempotent(t *testing.T) {
	dir := t.TempDir()
	// Explicit major/minor so the device need not exist on the test host.
	specJSON := `{
  "cdiVersion": "0.6.0",
  "kind": "nvidia.com/gpu",
  "devices": [
    {
      "name": "all",
      "containerEdits": {
        "deviceNodes": [{"path": "/dev/nvidia0", "major": 195, "minor": 0, "type": "c"}],
        "env": ["NVIDIA_TEST=1"],
        "hooks": [
          {"hookName": "createContainer", "path": "/x/nvidia-cdi-hook",
           "args": ["nvidia-cdi-hook", "create-symlinks", "--link", "a::b"]}
        ]
      }
    }
  ]
}`
	specDir := filepath.Join(dir, "cdi")
	os.MkdirAll(specDir, 0o755)
	os.WriteFile(filepath.Join(specDir, "nvidia.json"), []byte(specJSON), 0o644)

	bundle := filepath.Join(dir, "bundle")
	os.MkdirAll(filepath.Join(bundle, "rootfs"), 0o755)
	data, _ := json.Marshal(&specs.Spec{Version: "1.0.0", Process: &specs.Process{Args: []string{"true"}}})
	os.WriteFile(filepath.Join(bundle, "config.json"), data, 0o644)

	// devices, mounts, env, hooks.
	shape := func() [4]int {
		out, _ := os.ReadFile(filepath.Join(bundle, "config.json"))
		var s specs.Spec
		if err := json.Unmarshal(out, &s); err != nil {
			t.Fatal(err)
		}
		hooks := 0
		if s.Hooks != nil {
			hooks = len(s.Hooks.CreateContainer)
		}
		return [4]int{len(s.Linux.Devices), len(s.Mounts), len(s.Process.Env), hooks}
	}

	if err := injectGPUIntoBundle(context.Background(), bundle, specDir); err != nil {
		t.Fatalf("first inject: %v", err)
	}
	first := shape()
	if first[0] == 0 {
		t.Fatalf("first inject added no devices: %v", first)
	}
	if err := injectGPUIntoBundle(context.Background(), bundle, specDir); err != nil {
		t.Fatalf("second inject: %v", err)
	}
	if second := shape(); second != first {
		t.Fatalf("second injection changed the bundle: %v -> %v (devices, mounts, env, hooks)", first, second)
	}
}

// TestStageSonameSymlinks_ConfinedToRootfs covers the two ways the staging writes
// could escape into ateom's mount namespace, where the shared image cache and other
// actors' bundles live: a directory the image redirects out of the rootfs, and a
// SONAME read out of a library that is not a bare filename.
func TestStageSonameSymlinks_ConfinedToRootfs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		soname string
		// dest is the CDI mount destination inside the actor.
		dest string
		// escape, when set, is planted in the rootfs as a symlink to the outside dir.
		escape string
	}{
		{
			name:   "image redirects the driver dir out of the rootfs",
			soname: "libcuda.so.1",
			dest:   "/usr/lib/gpu/libcuda.so.580.65.06",
			escape: "usr/lib/gpu",
		},
		{
			name:   "SONAME climbs out with ../",
			soname: "../../../../escaped.so.1",
			dest:   "/usr/lib/gpu/libcuda.so.580.65.06",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			rootfs := filepath.Join(dir, "rootfs")
			outside := filepath.Join(dir, "outside")
			if err := os.MkdirAll(filepath.Join(rootfs, "usr/lib"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(outside, 0o755); err != nil {
				t.Fatal(err)
			}
			victim := filepath.Join(outside, "libcuda.so.1")
			if err := os.WriteFile(victim, []byte("do not delete"), 0o644); err != nil {
				t.Fatal(err)
			}
			if tc.escape != "" {
				if err := os.Symlink(outside, filepath.Join(rootfs, tc.escape)); err != nil {
					t.Fatal(err)
				}
			}

			old := elfSonameFn
			elfSonameFn = func(string) (string, error) { return tc.soname, nil }
			defer func() { elfSonameFn = old }()

			// Refusing outright and skipping are both acceptable; writing outside is not.
			_ = stageSonameSymlinks(context.Background(), rootfs, []specs.Mount{{
				Source:      "/host/libcuda.so.580.65.06",
				Destination: tc.dest,
			}})

			if b, err := os.ReadFile(victim); err != nil || string(b) != "do not delete" {
				t.Fatalf("a file outside the rootfs was modified: err=%v content=%q", err, b)
			}
			if entries, err := os.ReadDir(outside); err == nil && len(entries) != 1 {
				t.Fatalf("something was created outside the rootfs: %v", entries)
			}
		})
	}
}

func TestInjectGPUIntoBundle_MissingSpecFails(t *testing.T) {
	dir := t.TempDir()
	bundle := filepath.Join(dir, "bundle")
	os.MkdirAll(bundle, 0o755)
	base := &specs.Spec{Version: "1.0.0", Process: &specs.Process{}}
	data, _ := json.Marshal(base)
	os.WriteFile(filepath.Join(bundle, "config.json"), data, 0o644)

	// An empty spec dir has no nvidia.json, so injection fails to read the spec.
	emptyDir := filepath.Join(dir, "cdi-empty")
	os.MkdirAll(emptyDir, 0o755)
	if err := injectGPUIntoBundle(context.Background(), bundle, emptyDir); err == nil {
		t.Fatal("expected error when the CDI spec is missing")
	}
}
