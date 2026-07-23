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

package imagecache

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestOverlaySpecRoundTrip(t *testing.T) {
	bundle := t.TempDir()
	in := &OverlaySpec{
		Layers:    []string{"/cache/layers/sha256/aaa", "/cache/layers/sha256/bbb"},
		ExtraDirs: []string{"/run/ate"},
	}
	if err := WriteSpec(bundle, in); err != nil {
		t.Fatalf("WriteSpec: %v", err)
	}
	out, err := ReadSpec(bundle)
	if err != nil {
		t.Fatalf("ReadSpec: %v", err)
	}
	if out == nil {
		t.Fatalf("ReadSpec returned nil for a bundle with a spec")
	}
	if !slices.Equal(out.Layers, in.Layers) {
		t.Errorf("Layers = %v, want %v", out.Layers, in.Layers)
	}
	if !slices.Equal(out.ExtraDirs, in.ExtraDirs) {
		t.Errorf("ExtraDirs = %v, want %v", out.ExtraDirs, in.ExtraDirs)
	}
}

// A bundle without a spec (e.g. prepared by a pre-imagecache atelet) reads
// as nil-and-no-error: the consumer must leave its rootfs untouched.
func TestReadSpec_Absent(t *testing.T) {
	spec, err := ReadSpec(t.TempDir())
	if err != nil {
		t.Fatalf("ReadSpec: %v", err)
	}
	if spec != nil {
		t.Errorf("ReadSpec = %+v, want nil for a bundle without a spec", spec)
	}
}

func TestReadSpec_UnknownVersion(t *testing.T) {
	bundle := t.TempDir()
	if err := os.WriteFile(filepath.Join(bundle, OverlaySpecFileName), []byte(`{"version":99}`), 0o600); err != nil {
		t.Fatalf("writing spec: %v", err)
	}
	if _, err := ReadSpec(bundle); err == nil {
		t.Errorf("ReadSpec accepted an unknown spec version")
	}
}

func TestOverlayLowerDirs(t *testing.T) {
	t.Run("reverses to top-first", func(t *testing.T) {
		got := overlayLowerDirs([]string{"/c/base", "/c/top"})
		want := []string{"/c/top/fs", "/c/base/fs"}
		if !slices.Equal(got, want) {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	// Images can list the same layer at several positions (repeated
	// identical build steps produce identical diffids). overlayfs rejects a
	// repeated lowerdir with ELOOP, so duplicates must collapse to the
	// topmost occurrence. Regression test for the "too many levels of
	// symbolic links" mount failure.
	t.Run("deduplicates repeated layers keeping topmost", func(t *testing.T) {
		got := overlayLowerDirs([]string{"/c/base", "/c/dup", "/c/dup", "/c/top"})
		want := []string{"/c/top/fs", "/c/dup/fs", "/c/base/fs"}
		if !slices.Equal(got, want) {
			t.Errorf("got %q, want %q", got, want)
		}
	})
}

func TestCreateExtraDirs(t *testing.T) {
	t.Run("creates target inside rootfs", func(t *testing.T) {
		root := t.TempDir()
		if err := createExtraDirs(root, []string{"/run/ate"}); err != nil {
			t.Fatalf("createExtraDirs: %v", err)
		}
		info, err := os.Stat(filepath.Join(root, "run", "ate"))
		if err != nil {
			t.Fatalf("extra dir not created in rootfs: %v", err)
		}
		if !info.IsDir() {
			t.Errorf("extra dir must be a directory to host the identity bind mount")
		}
	})

	t.Run("refuses symlink escaping the rootfs", func(t *testing.T) {
		root := t.TempDir()
		outside := t.TempDir()
		// A malicious image could ship /run as a symlink pointing out of the
		// rootfs; os.Root must refuse to follow it.
		if err := os.Symlink(outside, filepath.Join(root, "run")); err != nil {
			t.Fatalf("planting symlink: %v", err)
		}
		if err := createExtraDirs(root, []string{"/run/ate"}); err == nil {
			t.Errorf("expected error when /run escapes the rootfs, got nil")
		}
		// Nothing may be created through the escaping symlink.
		if entries, err := os.ReadDir(outside); err != nil {
			t.Errorf("reading outside dir: %v", err)
		} else if len(entries) != 0 {
			t.Errorf("write escaped the rootfs: %s is not empty (%d entries)", outside, len(entries))
		}
	})

	t.Run("refuses parent traversal", func(t *testing.T) {
		root := t.TempDir()
		if err := createExtraDirs(root, []string{"/../escape"}); err == nil {
			t.Errorf("expected error for a parent-traversal extra dir, got nil")
		}
	})
}
