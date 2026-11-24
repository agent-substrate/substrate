// Copyright 2026 The Agent Substrate Authors
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

	"github.com/google/go-cmp/cmp"
)

func TestOverlaySpecRoundTrip(t *testing.T) {
	for name, in := range map[string]*OverlaySpec{
		"store images, version 1": {
			LowerDirs:   []string{"/cache/layers/sha256/aaa/fs", "/cache/layers/sha256/bbb/fs"},
			StoreLayers: []string{"/cache/layers/sha256/aaa", "/cache/layers/sha256/bbb"},
			ExtraDirs:   []string{"/run/ate"},
			ImageVolumes: []ImageVolumeOverlay{{
				Name:        "models",
				LowerDirs:   []string{"/cache/layers/sha256/ccc/fs"},
				StoreLayers: []string{"/cache/layers/sha256/ccc"},
			}},
		},
		"provider image, version 2": {
			LowerDirs: []string{"/var/lib/ate/image-provider/nydus/app"},
			ExtraDirs: []string{"/run/ate"},
			ImageVolumes: []ImageVolumeOverlay{{
				Name:        "models",
				LowerDirs:   []string{"/cache/layers/sha256/ccc/fs"},
				StoreLayers: []string{"/cache/layers/sha256/ccc"},
			}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			bundle := t.TempDir()
			if err := WriteSpec(bundle, in); err != nil {
				t.Fatalf("WriteSpec: %v", err)
			}
			out, err := ReadSpec(bundle)
			if err != nil {
				t.Fatalf("ReadSpec: %v", err)
			}
			if diff := cmp.Diff(in, out); diff != "" {
				t.Errorf("ReadSpec mismatch (-want +got):\n%s", diff)
			}
		})
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

func TestReadSpec_Version(t *testing.T) {
	for _, tc := range []struct {
		name, spec    string
		wantLowerDirs []string
		wantErr       bool
	}{
		{name: "v1 derives lowerdirs", spec: `{"version":1,"layers":["/l"]}`, wantLowerDirs: []string{"/l/fs"}},
		{name: "v2 lowerdirs as they are", spec: `{"version":2,"lowerDirs":["/p"]}`, wantLowerDirs: []string{"/p"}},
		{name: "v2 derives Store lowerdirs", spec: `{"version":2,"layers":["/l"],"imageVolumes":[{"name":"v","lowerDirs":["/p"]}]}`, wantLowerDirs: []string{"/l/fs"}},
		{name: "both Store layers and lowerdirs", spec: `{"version":2,"lowerDirs":["/l/fs"],"layers":["/l"]}`, wantErr: true},
		{name: "v1 with lowerdirs", spec: `{"version":1,"lowerDirs":["/p"]}`, wantErr: true},
		{name: "unknown version", spec: `{"version":99}`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := t.TempDir()
			if err := os.WriteFile(filepath.Join(bundle, OverlaySpecFileName), []byte(tc.spec), 0o600); err != nil {
				t.Fatalf("writing spec: %v", err)
			}
			spec, err := ReadSpec(bundle)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ReadSpec error = %v, want error %t", err, tc.wantErr)
			}
			if err == nil && !slices.Equal(spec.LowerDirs, tc.wantLowerDirs) {
				t.Errorf("LowerDirs = %q, want %q", spec.LowerDirs, tc.wantLowerDirs)
			}
		})
	}
}

// WriteSpec writes version 1, which workers that predate lowerDirs read,
// unless an image has lowerdirs without Store layers.
func TestWriteSpec_Version(t *testing.T) {
	store := []string{"/l"}
	storeLowers := []string{"/l/fs"}
	for _, tc := range []struct {
		name string
		spec OverlaySpec
		want int
	}{
		{name: "store images", spec: OverlaySpec{LowerDirs: storeLowers, StoreLayers: store}, want: 1},
		{name: "zero-layer image", spec: OverlaySpec{}, want: 1},
		{name: "provider rootfs", spec: OverlaySpec{LowerDirs: []string{"/p"}}, want: 2},
		{name: "provider volume", spec: OverlaySpec{LowerDirs: storeLowers, StoreLayers: store, ImageVolumes: []ImageVolumeOverlay{{Name: "v", LowerDirs: []string{"/p"}}}}, want: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := t.TempDir()
			if err := WriteSpec(bundle, &tc.spec); err != nil {
				t.Fatalf("WriteSpec: %v", err)
			}
			b, err := os.ReadFile(filepath.Join(bundle, OverlaySpecFileName))
			if err != nil {
				t.Fatal(err)
			}
			var got struct {
				Version   int      `json:"version"`
				LowerDirs []string `json:"lowerDirs"`
			}
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatal(err)
			}
			if got.Version != tc.want {
				t.Errorf("version = %d, want %d", got.Version, tc.want)
			}
			if got.Version == 1 && got.LowerDirs != nil {
				t.Errorf("version 1 spec has lowerDirs %q", got.LowerDirs)
			}
		})
	}
}

func TestOverlayLowerDirs(t *testing.T) {
	t.Run("reverses to top-first", func(t *testing.T) {
		got := overlayLowerDirs([]string{"/c/base", "/c/top"})
		want := []string{"/c/top", "/c/base"}
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
		want := []string{"/c/top", "/c/dup", "/c/base"}
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

func TestSpecImageDigestCompat(t *testing.T) {
	dir := t.TempDir()
	// Round trip with the new field.
	if err := WriteSpec(dir, &OverlaySpec{ImageDigest: "sha256:" + strings.Repeat("ef", 32), StoreLayers: []string{"/pool/layers/sha256/aa"}}); err != nil {
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
	if err := WriteSpec(dir, &OverlaySpec{StoreLayers: []string{"/pool/layers/sha256/aa"}}); err != nil {
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

// WriteSpec stores a Store image's lowerdirs as its layers, so lowerdirs that
// are not those of the layers fail instead of being dropped.
func TestWriteSpecLowerDirsOfStoreLayers(t *testing.T) {
	layers := []string{"/cache/layers/sha256/aaa", "/cache/layers/sha256/bbb"}
	for _, tc := range []struct {
		name    string
		spec    *OverlaySpec
		wantErr bool
	}{{
		name: "layers only",
		spec: &OverlaySpec{StoreLayers: layers},
	}, {
		name: "lowerdirs of the layers",
		spec: &OverlaySpec{StoreLayers: layers, LowerDirs: storeLowerDirs(layers)},
	}, {
		name:    "other lowerdirs",
		spec:    &OverlaySpec{StoreLayers: layers, LowerDirs: []string{"/var/lib/ate/image-provider/nydus/app"}},
		wantErr: true,
	}, {
		name: "volume with other lowerdirs",
		spec: &OverlaySpec{StoreLayers: layers, ImageVolumes: []ImageVolumeOverlay{{
			Name: "models", StoreLayers: layers, LowerDirs: layers,
		}}},
		wantErr: true,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			err := WriteSpec(t.TempDir(), tc.spec)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Errorf("WriteSpec error = %v, want error %v", err, tc.wantErr)
			}
		})
	}
}
