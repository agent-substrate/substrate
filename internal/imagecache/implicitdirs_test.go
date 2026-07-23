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
	"testing"
)

// makeLayer materializes a fake pool layer: dirs maps rel path -> mode for
// real directory entries; implicit lists the ImplicitDirs metadata.
func makeLayer(t *testing.T, dirs map[string]os.FileMode, implicit []string) string {
	t.Helper()
	layer := t.TempDir()
	fs := filepath.Join(layer, layerFSDirName)
	if err := os.Mkdir(fs, 0o755); err != nil {
		t.Fatal(err)
	}
	for rel, mode := range dirs {
		p := filepath.Join(fs, rel)
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
	}
	if implicit != nil {
		b, err := json.Marshal(&whiteoutSet{Version: 1, ImplicitDirs: implicit})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(layer, layerWhiteoutsFileName), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return layer
}

// The canonical shadowing case: the base declares tmp as 1777, an upper
// layer created it implicitly (0755) — the merged view must be repaired to
// the base's attrs.
func TestResolveAndApplyImplicitDirFixups(t *testing.T) {
	base := makeLayer(t, map[string]os.FileMode{"tmp": 0o777 | os.ModeSticky}, nil)
	upper := makeLayer(t, map[string]os.FileMode{"tmp": 0o755}, []string{"tmp"})

	fixups, err := resolveImplicitDirFixups([]string{base, upper})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(fixups) != 1 || fixups[0].Path != "tmp" {
		t.Fatalf("fixups = %+v, want exactly one for \"tmp\"", fixups)
	}
	if fixups[0].Mode != 0o777|os.ModeSticky {
		t.Errorf("fixup mode = %v, want 1777 (sticky preserved)", fixups[0].Mode)
	}
	if !fixups[0].HasOwner {
		t.Errorf("fixup carries no owner info")
	}

	// Apply against a stand-in for the merged rootfs (a plain dir here; the
	// overlay copy-up behavior is covered by the root-gated test).
	rootfs := t.TempDir()
	if err := os.Mkdir(filepath.Join(rootfs, "tmp"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := applyDirFixups(rootfs, fixups); err != nil {
		t.Fatalf("apply: %v", err)
	}
	fi, err := os.Lstat(filepath.Join(rootfs, "tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm() | fi.Mode()&os.ModeSticky; got != 0o777|os.ModeSticky {
		t.Errorf("merged tmp mode = %v, want 1777", got)
	}
}

func TestResolveImplicitDirFixups_NoRepairCases(t *testing.T) {
	t.Run("topmost provider declared the dir", func(t *testing.T) {
		// Lower created it implicitly, but the top layer declares it: the
		// merged attrs already come from a real declaration.
		lower := makeLayer(t, map[string]os.FileMode{"d": 0o755}, []string{"d"})
		top := makeLayer(t, map[string]os.FileMode{"d": 0o700}, nil)
		fixups, err := resolveImplicitDirFixups([]string{lower, top})
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if len(fixups) != 0 {
			t.Errorf("fixups = %+v, want none", fixups)
		}
	})

	t.Run("implicit in every layer", func(t *testing.T) {
		a := makeLayer(t, map[string]os.FileMode{"d": 0o755}, []string{"d"})
		b := makeLayer(t, map[string]os.FileMode{"d": 0o755}, []string{"d"})
		fixups, err := resolveImplicitDirFixups([]string{a, b})
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if len(fixups) != 0 {
			t.Errorf("fixups = %+v, want none (no authoritative attrs exist)", fixups)
		}
	})

	t.Run("no metadata at all (pre-recording layers)", func(t *testing.T) {
		a := makeLayer(t, map[string]os.FileMode{"d": 0o700}, nil)
		b := makeLayer(t, map[string]os.FileMode{"d": 0o755}, nil)
		fixups, err := resolveImplicitDirFixups([]string{a, b})
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if len(fixups) != 0 {
			t.Errorf("fixups = %+v, want none", fixups)
		}
	})

	t.Run("escaping metadata path is rejected", func(t *testing.T) {
		a := makeLayer(t, nil, []string{"../escape"})
		if _, err := resolveImplicitDirFixups([]string{a}); err == nil {
			t.Errorf("resolve accepted an escaping implicit dir path")
		}
	})
}

// applyDirFixups must skip paths that are hidden or shadowed in the merged
// view rather than failing or following the shadow.
func TestApplyDirFixups_SkipsShadowedPaths(t *testing.T) {
	rootfs := t.TempDir()
	if err := os.WriteFile(filepath.Join(rootfs, "afile"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	fixups := []dirFixup{
		{Path: "missing", Mode: 0o700},
		{Path: "afile", Mode: 0o700},
	}
	if err := applyDirFixups(rootfs, fixups); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if fi, _ := os.Lstat(filepath.Join(rootfs, "afile")); fi.Mode().Perm() != 0o644 {
		t.Errorf("non-directory was chmodded: %v", fi.Mode())
	}
}
