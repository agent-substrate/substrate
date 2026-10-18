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

package ocispec

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/roottest"
	"golang.org/x/sys/unix"
)

func TestOpenSubPath(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "existing"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("existing", filepath.Join(root, "internal")); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		ro, fail bool
	}{
		{"existing", true, false}, {"existing", false, false}, {"created", false, false},
		{"missing-ro", true, true}, {"missing-parent/child", false, true},
		{"escape", false, true}, {"escape/child", false, true}, {"internal", true, true},
		{"../outside", false, true}, {"/absolute", false, true}, {"a//b", false, true}, {".", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := openSubPath(root, tc.name, tc.ro)
			if f != nil {
				f.Close()
			}
			if (err != nil) != tc.fail {
				t.Fatalf("open = %v, want failure %v", err, tc.fail)
			}
		})
	}
	for _, name := range []string{"missing-ro", "missing-parent"} {
		if _, err := os.Stat(filepath.Join(root, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unexpected creation of %s: %v", name, err)
		}
	}
}

func TestStageSubPath(t *testing.T) {
	roottest.Require(t, "bind mounts")
	root, mounts := t.TempDir(), t.TempDir()
	for _, dir := range []string{"selected", "sibling"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "selected", "file"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, ro := range []bool{false, true} {
		name := "rw"
		if ro {
			name = "ro"
		}
		target := filepath.Join(mounts, name)
		if err := stageSubPath(root, "selected", target, ro); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := unix.Unmount(target, unix.MNT_DETACH); err != nil {
				t.Error(err)
			}
		})
		if b, err := os.ReadFile(filepath.Join(target, "file")); err != nil || string(b) != "data" {
			t.Fatalf("selected root: %q %v", b, err)
		}
		if _, err := os.Stat(filepath.Join(target, "sibling")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("sibling visible: %v", err)
		}
		err := os.WriteFile(filepath.Join(target, "write"), []byte("ok"), 0o600)
		if ro && !errors.Is(err, unix.EROFS) {
			t.Fatalf("read-only write: %v", err)
		}
		if !ro && err != nil {
			t.Fatal(err)
		}
	}
	// Replacing the host pathname after mounting cannot redirect the pinned bind.
	if err := os.Rename(filepath.Join(root, "selected"), filepath.Join(root, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "sibling"), filepath.Join(root, "selected")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(mounts, "rw", "file")); err != nil {
		t.Fatal(err)
	}
	if err := stageSubPath(root, "selected", filepath.Join(mounts, "escape"), false); err == nil {
		t.Fatal("accepted symlink")
	}
}

func TestPrepareSubPathsEmpty(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "not-created")
	o := Options{ActorDirs: &ateworkerpb.ActorDirs{ExternalVolumeMountsDir: filepath.Join(root, "missing")}, Container: &ateworkerpb.Container{ExternalVolumeMounts: []*ateworkerpb.VolumeMount{{Name: "data", MountPath: "/data"}}}}
	if err := PrepareSubPaths(o, target); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty sub-path created staging directory: %v", err)
	}
}
