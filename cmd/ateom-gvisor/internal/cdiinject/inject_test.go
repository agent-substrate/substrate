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

package cdiinject

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/internal/cdi"
)

// statDev returns a device node's numbers, so the expectation matches whatever
// host the test runs on.
func statDev(t *testing.T, path string) (int64, int64) {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	rdev := uint64(st.Rdev)
	return int64(unix.Major(rdev)), int64(unix.Minor(rdev))
}

// testHookBinary stands in for the trusted hook binary a caller supplies.
const testHookBinary = "/opt/toolkit/cdi-hook"

// injectFixture reads a spec from disk and applies the "all" device with the
// options the gVisor ateom uses, so these tests exercise the real combination.
func injectFixture(t *testing.T, bundleDir, specDir string) error {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(specDir, "nvidia.json"))
	if err != nil {
		return err
	}
	spec, err := cdi.Parse(data)
	if err != nil {
		return err
	}
	return IntoBundle(context.Background(), bundleDir, spec, Options{
		Devices:      []string{"all"},
		AllowedHooks: map[string]bool{"create-symlinks": true, "enable-cuda-compat": true},
		HookBinary:   testHookBinary,
		LibraryDirs:  []string{"/usr/local/nvidia/lib64"},
		DropEnv:      []string{"NVIDIA_VISIBLE_DEVICES"},
	})
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

	if err := injectFixture(t, bundle, specDir); err != nil {
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
	// The spec omitted major/minor, so they must come from stat'ing the host.
	wantMajor, wantMinor := statDev(t, "/dev/null")
	if dev.Type != "c" || dev.Major != wantMajor || dev.Minor != wantMinor {
		t.Fatalf("device not resolved from host: type=%q major=%d minor=%d, want c %d %d",
			dev.Type, dev.Major, dev.Minor, wantMajor, wantMinor)
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
			if !strings.HasPrefix(h.Path, testHookBinary) {
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

	if err := injectFixture(t, bundle, specDir); err != nil {
		t.Fatalf("first inject: %v", err)
	}
	first := shape()
	if first[0] == 0 {
		t.Fatalf("first inject added no devices: %v", first)
	}
	if err := injectFixture(t, bundle, specDir); err != nil {
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
			_ = StageSonameSymlinks(context.Background(), rootfs, []specs.Mount{{
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
	if err := injectFixture(t, bundle, emptyDir); err == nil {
		t.Fatal("expected error when the CDI spec is missing")
	}
}
