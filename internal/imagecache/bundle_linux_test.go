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

package imagecache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// writeLayer builds a layer dir (fs/ tree + whiteouts.json) as the store's
// unpack would.
func writeLayer(t *testing.T, dir string, files map[string]string, wh *whiteoutSet) {
	t.Helper()
	fs := filepath.Join(dir, layerFSDirName)
	if err := os.MkdirAll(fs, 0o755); err != nil {
		t.Fatalf("mkdir fs: %v", err)
	}
	for name, body := range files {
		p := filepath.Join(fs, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	if wh != nil {
		wh.Version = 1
		b, err := json.Marshal(wh)
		if err != nil {
			t.Fatalf("marshal whiteouts: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, layerWhiteoutsFileName), b, 0o600); err != nil {
			t.Fatalf("write whiteouts.json: %v", err)
		}
	}
}

// FinalizeLayer materializes whiteout devices (mknod, CAP_MKNOD) and opaque
// xattrs (trusted.*, CAP_SYS_ADMIN); only root has those in a plain test
// environment. Runs in privileged CI / root shells, skips elsewhere.
func TestFinalizeLayer_MaterializesWhiteouts(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root (CAP_MKNOD + CAP_SYS_ADMIN for trusted.* xattrs)")
	}
	dir := t.TempDir()
	writeLayer(t, dir,
		map[string]string{"kept.txt": "kept"},
		&whiteoutSet{
			Whiteouts: []string{"removed.txt", "sub/dir/removed-deep.txt"},
			Opaques:   []string{"opaque-dir"},
		})

	if err := FinalizeLayer(dir); err != nil {
		t.Fatalf("FinalizeLayer: %v", err)
	}

	for _, p := range []string{"removed.txt", "sub/dir/removed-deep.txt"} {
		fi, err := os.Lstat(filepath.Join(dir, layerFSDirName, p))
		if err != nil {
			t.Fatalf("whiteout %q not created: %v", p, err)
		}
		if fi.Mode()&os.ModeCharDevice == 0 {
			t.Errorf("whiteout %q mode = %v, want char device", p, fi.Mode())
		}
		var st unix.Stat_t
		if err := unix.Stat(filepath.Join(dir, layerFSDirName, p), &st); err == nil && st.Rdev != 0 {
			t.Errorf("whiteout %q rdev = %d, want 0:0", p, st.Rdev)
		}
	}

	var val [8]byte
	n, err := unix.Getxattr(filepath.Join(dir, layerFSDirName, "opaque-dir"), "trusted.overlay.opaque", val[:])
	if err != nil || string(val[:n]) != "y" {
		t.Errorf("opaque xattr = %q (err=%v), want \"y\"", val[:n], err)
	}

	if _, err := os.Stat(filepath.Join(dir, layerFinalizedMarkerName)); err != nil {
		t.Errorf("finalized marker missing: %v", err)
	}

	// Idempotent: second call is a marker-hit no-op.
	if err := FinalizeLayer(dir); err != nil {
		t.Errorf("FinalizeLayer (second call): %v", err)
	}
}

// Escape rejection needs no privileges: a crafted whiteouts.json must fail
// validation before any mknod/setxattr is attempted.
func TestFinalizeLayer_RejectsEscapingPaths(t *testing.T) {
	t.Run("whiteout escape", func(t *testing.T) {
		dir := t.TempDir()
		writeLayer(t, dir, nil, &whiteoutSet{Whiteouts: []string{"../escape"}})
		if err := FinalizeLayer(dir); err == nil {
			t.Errorf("FinalizeLayer accepted an escaping whiteout path")
		}
	})
	t.Run("opaque escape", func(t *testing.T) {
		dir := t.TempDir()
		writeLayer(t, dir, nil, &whiteoutSet{Opaques: []string{"a/../../escape"}})
		if err := FinalizeLayer(dir); err == nil {
			t.Errorf("FinalizeLayer accepted an escaping opaque path")
		}
	})
}

// A layer with no whiteouts.json finalizes to just the marker; needs no
// privileges.
func TestFinalizeLayer_NoWhiteouts(t *testing.T) {
	dir := t.TempDir()
	writeLayer(t, dir, map[string]string{"f": "x"}, nil)
	if err := FinalizeLayer(dir); err != nil {
		t.Fatalf("FinalizeLayer: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, layerFinalizedMarkerName)); err != nil {
		t.Errorf("finalized marker missing: %v", err)
	}
}

// A bundle without an overlay spec must be left untouched.
func TestSetupBundleRootfs_NoSpecIsNoop(t *testing.T) {
	bundle := t.TempDir()
	if err := SetupBundleRootfs(bundle); err != nil {
		t.Fatalf("SetupBundleRootfs: %v", err)
	}
	if entries, _ := os.ReadDir(bundle); len(entries) != 0 {
		t.Errorf("no-spec bundle was modified: %v", entries)
	}
}

// A zero-layer spec composes without any mount: empty rootfs plus ExtraDirs.
// Needs no privileges (the stale-unmount attempt's failure is ignored).
func TestSetupBundleRootfs_ZeroLayers(t *testing.T) {
	bundle := t.TempDir()
	if err := WriteSpec(bundle, &OverlaySpec{Layers: nil, ExtraDirs: []string{"/run/ate"}}); err != nil {
		t.Fatalf("WriteSpec: %v", err)
	}
	if err := SetupBundleRootfs(bundle); err != nil {
		t.Fatalf("SetupBundleRootfs: %v", err)
	}
	fi, err := os.Stat(filepath.Join(bundle, "rootfs", "run", "ate"))
	if err != nil || !fi.IsDir() {
		t.Errorf("ExtraDir not created in rootfs: fi=%v err=%v", fi, err)
	}
	for _, d := range []string{"upper", "work"} {
		if fi, err := os.Stat(filepath.Join(bundle, d)); err != nil || !fi.IsDir() {
			t.Errorf("bundle dir %q missing: %v", d, err)
		}
	}
}

// Implicit-parent metadata repair through a real overlay: the base declares
// a 0700 dir, the top layer created it implicitly (0755 in its tree), and
// after compose the merged view must show 0700 — copied up into the
// bundle's upper, with the shared layer trees untouched. Needs root.
func TestSetupBundleRootfs_ImplicitDirMetadataRepair(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root (mount/unmount)")
	}
	base := t.TempDir()
	writeLayer(t, base, map[string]string{"secret/keep.txt": "k"}, nil)
	if err := os.Chmod(filepath.Join(base, layerFSDirName, "secret"), 0o700); err != nil {
		t.Fatal(err)
	}
	top := t.TempDir()
	writeLayer(t, top, map[string]string{"secret/new.txt": "n"}, &whiteoutSet{ImplicitDirs: []string{"secret"}})

	bundle := t.TempDir()
	if err := WriteSpec(bundle, &OverlaySpec{Layers: []string{base, top}}); err != nil {
		t.Fatalf("WriteSpec: %v", err)
	}
	if err := SetupBundleRootfs(bundle); err != nil {
		t.Fatalf("SetupBundleRootfs: %v", err)
	}
	t.Cleanup(func() { _ = UnmountAllUnder(bundle) })

	fi, err := os.Lstat(filepath.Join(bundle, "rootfs", "secret"))
	if err != nil {
		t.Fatalf("stat merged dir: %v", err)
	}
	if fi.Mode().Perm() != 0o700 {
		t.Errorf("merged secret mode = %v, want 0700 from the declaring base layer", fi.Mode().Perm())
	}
	// The repair must land in the bundle upper, not the shared pool.
	if fi, err := os.Lstat(filepath.Join(top, layerFSDirName, "secret")); err != nil || fi.Mode().Perm() != 0o755 {
		t.Errorf("shared top layer tree was modified: %v %v", fi, err)
	}
	if _, err := os.Lstat(filepath.Join(bundle, "upper", "secret")); err != nil {
		t.Errorf("repair did not copy up into the bundle upper: %v", err)
	}
}

// Full overlay mount + UnmountAllUnder round trip; needs CAP_SYS_ADMIN.
func TestSetupBundleRootfs_MountAndUnmount(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root (mount/unmount)")
	}
	layer := t.TempDir()
	writeLayer(t, layer, map[string]string{"from-layer.txt": "hello"}, nil)

	bundle := t.TempDir()
	if err := WriteSpec(bundle, &OverlaySpec{Layers: []string{layer}, ExtraDirs: []string{"/run/ate"}}); err != nil {
		t.Fatalf("WriteSpec: %v", err)
	}
	if err := SetupBundleRootfs(bundle); err != nil {
		t.Fatalf("SetupBundleRootfs: %v", err)
	}
	t.Cleanup(func() { _ = UnmountAllUnder(bundle) })

	if got, err := os.ReadFile(filepath.Join(bundle, "rootfs", "from-layer.txt")); err != nil || string(got) != "hello" {
		t.Errorf("layer content not visible through overlay: %q (%v)", got, err)
	}
	if fi, err := os.Stat(filepath.Join(bundle, "rootfs", "run", "ate")); err != nil || !fi.IsDir() {
		t.Errorf("ExtraDir missing in overlay: %v", err)
	}
	// A write through the mount lands in the bundle's upper, not the layer.
	if err := os.WriteFile(filepath.Join(bundle, "rootfs", "written.txt"), []byte("w"), 0o644); err != nil {
		t.Fatalf("write through overlay: %v", err)
	}
	if _, err := os.Stat(filepath.Join(bundle, "upper", "written.txt")); err != nil {
		t.Errorf("write did not land in upper: %v", err)
	}
	if _, err := os.Stat(filepath.Join(layer, layerFSDirName, "written.txt")); err == nil {
		t.Errorf("write leaked into the shared layer")
	}

	if err := UnmountAllUnder(bundle); err != nil {
		t.Fatalf("UnmountAllUnder: %v", err)
	}
	if _, err := os.Stat(filepath.Join(bundle, "rootfs", "from-layer.txt")); err == nil {
		t.Errorf("rootfs still shows layer content after unmount")
	}
}

// Regression test for the mount(2) single-page option-string cap: a lowerdir
// chain whose joined paths exceed one page (~34 digest-derived layers) used
// to fail with a bare EINVAL. The fsconfig lowerdir+ path has no aggregate
// limit. Needs CAP_SYS_ADMIN.
func TestSetupBundleRootfs_ManyLayers(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root (mount/unmount)")
	}
	// Digest-length dir names so each path matches production length (~114
	// bytes); 64 of them comfortably exceed the page that motivated this.
	pool := filepath.Join(t.TempDir(), "sha256")
	const n = 64
	layers := make([]string, n)
	joined := 0
	for i := range layers {
		layers[i] = filepath.Join(pool, fmt.Sprintf("%064d", i))
		writeLayer(t, layers[i], map[string]string{fmt.Sprintf("from-layer-%02d.txt", i): "x"}, nil)
		joined += len(layers[i]) + len("/fs") + 1
	}
	if pageSize := os.Getpagesize(); joined <= pageSize {
		t.Fatalf("test layers join to %d bytes, not exceeding the %d-byte page this test guards against", joined, pageSize)
	}

	bundle := t.TempDir()
	if err := WriteSpec(bundle, &OverlaySpec{Layers: layers}); err != nil {
		t.Fatalf("WriteSpec: %v", err)
	}
	if err := SetupBundleRootfs(bundle); err != nil {
		t.Fatalf("SetupBundleRootfs with %d layers: %v", n, err)
	}
	t.Cleanup(func() { _ = UnmountAllUnder(bundle) })

	// Bottom-most and top-most layers are both visible in the merged view.
	for _, i := range []int{0, n - 1} {
		p := filepath.Join(bundle, "rootfs", fmt.Sprintf("from-layer-%02d.txt", i))
		if _, err := os.Stat(p); err != nil {
			t.Errorf("layer %d content missing from merged rootfs: %v", i, err)
		}
	}

	if err := UnmountAllUnder(bundle); err != nil {
		t.Fatalf("UnmountAllUnder: %v", err)
	}
}
