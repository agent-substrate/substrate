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
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
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
		return // unreachable; makes the non-nil-ness explicit to static analysis
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

func TestLegacyOverlayMountOptions(t *testing.T) {
	t.Run("assembles top-first lowerdir with upper and work", func(t *testing.T) {
		got, err := legacyOverlayMountOptions([]string{"/c/top/fs", "/c/base/fs"}, "/b/upper", "/b/work")
		if err != nil {
			t.Fatalf("legacyOverlayMountOptions: %v", err)
		}
		want := "lowerdir=/c/top/fs:/c/base/fs,upperdir=/b/upper,workdir=/b/work"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	// Image volumes mount without a writable layer: an empty upper must
	// assemble lowerdir-only options, which overlayfs makes read-only.
	t.Run("empty upper assembles lowerdir-only options", func(t *testing.T) {
		got, err := legacyOverlayMountOptions([]string{"/c/top/fs", "/c/base/fs"}, "", "")
		if err != nil {
			t.Fatalf("legacyOverlayMountOptions: %v", err)
		}
		want := "lowerdir=/c/top/fs:/c/base/fs"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("refuses option separators in layer paths", func(t *testing.T) {
		if _, err := legacyOverlayMountOptions([]string{"/c/evil:layer/fs"}, "/b/upper", "/b/work"); err == nil {
			t.Errorf("expected error for ':' in a layer path")
		}
	})

	t.Run("refuses option separators in upper or work", func(t *testing.T) {
		if _, err := legacyOverlayMountOptions([]string{"/c/top/fs"}, "/b/evil:upper", "/b/work"); err == nil {
			t.Errorf("expected error for ':' in the upper path")
		}
		if _, err := legacyOverlayMountOptions([]string{"/c/top/fs"}, "/b/upper", "/b/evil,work"); err == nil {
			t.Errorf("expected error for ',' in the work path")
		}
	})

	// mount(2) copies its option string through a single kernel page; with
	// digest-derived layer paths (~114 bytes each) that is roughly 34 layers.
	// The preflight must refuse an over-long chain naming the kernel
	// requirement instead of letting mount(2) fail with a bare E2BIG.
	t.Run("refuses chains beyond the single-page option string", func(t *testing.T) {
		var lowers []string
		opts := "lowerdir="
		for len(opts) < os.Getpagesize() {
			lowers = append(lowers, "/pool/layers/sha256/"+strings.Repeat("ab", 32)+"/fs")
			opts = "lowerdir=" + strings.Join(lowers, ":")
		}
		_, err := legacyOverlayMountOptions(lowers, "/b/upper", "/b/work")
		if err == nil {
			t.Fatalf("expected error for a %d-byte option string on a %d-byte page", len(opts), os.Getpagesize())
		}
		if !strings.Contains(err.Error(), "kernel >= 6.5 needed for lowerdir+") {
			t.Errorf("error must name the kernel requirement, got %q", err)
		}
	})

	t.Run("chains within the page assemble", func(t *testing.T) {
		// Production-length paths at the reported cap: roughly 34
		// digest-derived layers must still assemble on the legacy path.
		var lowers []string
		for range 34 {
			lowers = append(lowers, "/var/lib/ate/imagecache/layers/sha256/"+strings.Repeat("ab", 32)+"/fs")
		}
		if _, err := legacyOverlayMountOptions(lowers, "/b/upper", "/b/work"); err != nil {
			t.Errorf("34 digest-derived layers should fit one page, got %v", err)
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

func TestSpecImageDigestCompat(t *testing.T) {
	dir := t.TempDir()
	// Round trip with the new field.
	if err := WriteSpec(dir, &OverlaySpec{ImageDigest: "sha256:" + strings.Repeat("ef", 32), Layers: []string{"/pool/layers/sha256/aa"}}); err != nil {
		t.Fatalf("WriteSpec: %v", err)
	}
	spec, err := ReadSpec(dir)
	if err != nil || spec == nil {
		t.Fatalf("ReadSpec: %v, %v", spec, err)
	}
	if want := "sha256:" + strings.Repeat("ef", 32); spec.ImageDigest != want {
		t.Errorf("ImageDigest = %q, want %q", spec.ImageDigest, want)
	}

	// A spec written by an older atelet (no imageDigest) still parses.
	old, err := json.Marshal(map[string]any{"version": 1, "layers": []string{"/pool/layers/sha256/bb"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, OverlaySpecFileName), old, 0o600); err != nil {
		t.Fatal(err)
	}
	spec, err = ReadSpec(dir)
	if err != nil || spec == nil {
		t.Fatalf("ReadSpec(old): %v, %v", spec, err)
	}
	if spec.ImageDigest != "" {
		t.Errorf("old spec ImageDigest = %q, want empty", spec.ImageDigest)
	}
}

func TestWriteSpecLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	if err := WriteSpec(dir, &OverlaySpec{Layers: []string{"/pool/layers/sha256/aa"}}); err != nil {
		t.Fatalf("WriteSpec: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != OverlaySpecFileName {
			t.Errorf("unexpected file left in bundle: %q", e.Name())
		}
	}
}
