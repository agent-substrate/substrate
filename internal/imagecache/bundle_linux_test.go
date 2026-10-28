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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/internal/roottest"
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
	roottest.Require(t, "CAP_MKNOD + CAP_SYS_ADMIN for trusted.* xattrs")
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
	roottest.Require(t, "mount/unmount")
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
	roottest.Require(t, "mount/unmount")
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
	roottest.Require(t, "mount/unmount")
	// Overlayfs caps a chain at 500 lower layers, and production-length
	// paths (~114 bytes) cannot outgrow a 64 KiB page within that cap. So
	// each digest-named layer dir is padded until its path fills the 255
	// bytes one fsconfig(2) string value carries, and the layer count
	// follows the page size: 17 layers on 4 KiB pages, 257 on 64 KiB pages.
	pool := filepath.Join(t.TempDir(), "sha256")
	const lowerLen = 255
	pad := lowerLen - len(filepath.Join(pool, strings.Repeat("0", 64), layerFSDirName))
	if pad < 0 {
		t.Fatalf("temp dir %q is too long to pad a lowerdir path to %d bytes", pool, lowerLen)
	}
	pageSize := os.Getpagesize()
	n := pageSize/(lowerLen+1) + 1
	layers := make([]string, n)
	joined := 0
	for i := range layers {
		layers[i] = filepath.Join(pool, fmt.Sprintf("%064d", i)+strings.Repeat("p", pad))
		writeLayer(t, layers[i], map[string]string{fmt.Sprintf("from-layer-%02d.txt", i): "x"}, nil)
		joined += len(layers[i]) + len("/fs") + 1
	}
	if joined <= pageSize {
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

// Image volumes reach identical content through both arms: one layer binds,
// several overlay.
func TestSetupBundleRootfs_ImageVolumes(t *testing.T) {
	roottest.Require(t, "mount/unmount")

	for _, tc := range []struct {
		name   string
		layers int
	}{
		{"one layer binds", 1},
		{"three layers overlay", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var layers []string
			for i := range tc.layers {
				dir := t.TempDir()
				writeLayer(t, dir, map[string]string{
					fmt.Sprintf("layer%d.txt", i): "content",
					"shadowed.txt":                fmt.Sprintf("from-layer-%d", i),
				}, nil)
				layers = append(layers, dir)
			}
			// A shared layer with a submount now and one mounted after setup. Only
			// the bind arm could carry either along.
			fsDir := filepath.Join(layers[tc.layers-1], layerFSDirName)
			for _, d := range []string{"sub", "later"} {
				if err := os.Mkdir(filepath.Join(fsDir, d), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			if err := unix.Mount(fsDir, fsDir, "", unix.MS_BIND, ""); err != nil {
				t.Fatalf("self-binding %q: %v", fsDir, err)
			}
			t.Cleanup(func() { _ = unix.Unmount(fsDir, unix.MNT_DETACH) })
			if err := unix.Mount("", fsDir, "", unix.MS_SHARED, ""); err != nil {
				t.Fatalf("making %q shared: %v", fsDir, err)
			}
			if err := unix.Mount("tmpfs", filepath.Join(fsDir, "sub"), "tmpfs", 0, ""); err != nil {
				t.Fatalf("mounting nested tmpfs: %v", err)
			}

			bundle := t.TempDir()
			if err := WriteSpec(bundle, &OverlaySpec{
				Layers:       []string{layers[0]},
				ImageVolumes: []ImageVolumeOverlay{{Name: "agent", Layers: layers}},
			}); err != nil {
				t.Fatalf("WriteSpec: %v", err)
			}
			if err := SetupBundleRootfs(bundle); err != nil {
				t.Fatalf("SetupBundleRootfs: %v", err)
			}
			t.Cleanup(func() { _ = UnmountAllUnder(bundle) })
			if err := unix.Mount("tmpfs", filepath.Join(fsDir, "later"), "tmpfs", 0, ""); err != nil {
				t.Fatalf("mounting tmpfs after setup: %v", err)
			}

			mnt := filepath.Join(bundle, "volumes", "agent")
			for i := range tc.layers {
				if _, err := os.Stat(filepath.Join(mnt, fmt.Sprintf("layer%d.txt", i))); err != nil {
					t.Errorf("layer %d not visible in the volume: %v", i, err)
				}
			}
			// Later layers win, same as the rootfs overlay.
			want := fmt.Sprintf("from-layer-%d", tc.layers-1)
			if got, err := os.ReadFile(filepath.Join(mnt, "shadowed.txt")); err != nil || string(got) != want {
				t.Errorf("shadowed.txt = %q (%v), want %q", got, err, want)
			}
			// No upper on either arm, so there is nowhere for a write to go.
			for _, name := range []string{"nope.txt", "sub/nope.txt", "later/nope.txt"} {
				if err := os.WriteFile(filepath.Join(mnt, name), []byte("x"), 0o644); err == nil {
					t.Errorf("write of %s succeeded through a read-only image volume", name)
				} else if !errors.Is(err, unix.EROFS) {
					t.Errorf("write of %s failed with %v, want EROFS", name, err)
				}
			}
			// The shared pool must never see the attempt.
			if _, err := os.Stat(filepath.Join(layers[tc.layers-1], layerFSDirName, "nope.txt")); err == nil {
				t.Error("write leaked into the shared layer pool")
			}

			if err := UnmountAllUnder(bundle); err != nil {
				t.Fatalf("UnmountAllUnder: %v", err)
			}
			if _, err := os.Stat(filepath.Join(mnt, "layer0.txt")); err == nil {
				t.Error("volume still shows content after unmount")
			}
		})
	}
}

func TestMountPointsUnderStatx_NonExistentAndMultiBundle(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	points, err := mountPointsUnderStatx(missing)
	if err != nil {
		t.Fatalf("mountPointsUnderStatx(missing) error = %v", err)
	}
	if len(points) != 0 {
		t.Fatalf("mountPointsUnderStatx(missing) = %v, want empty", points)
	}

	// Build a multi-container bundles directory matching actorDirs.GetOciBundleDir(),
	// including containers and image volumes named after bundle subdirectories
	// ("upper", "work", "rootfs", "volumes") so name-based pruning only
	// triggers inside a bundle directory.
	bundlesDir := filepath.Join(t.TempDir(), "bundles")
	containerNames := []string{"pause", "main", "upper", "work", "rootfs", "volumes"}
	for _, cname := range containerNames {
		for _, sub := range []string{
			filepath.Join(bundlesDir, cname, "rootfs", "etc"),
			filepath.Join(bundlesDir, cname, "upper", "var", "log"),
			filepath.Join(bundlesDir, cname, "work", "work"),
			filepath.Join(bundlesDir, cname, "volumes", "agent", "bin"),
			filepath.Join(bundlesDir, cname, "volumes", "upper", "bin"),
			filepath.Join(bundlesDir, cname, "volumes", "work", "lib"),
		} {
			if err := os.MkdirAll(sub, 0o755); err != nil {
				t.Fatalf("MkdirAll(%q): %v", sub, err)
			}
		}
		for _, file := range []string{
			filepath.Join(bundlesDir, cname, "config.json"),
			filepath.Join(bundlesDir, cname, "rootfs-overlay.json"),
			filepath.Join(bundlesDir, cname, "rootfs", "etc", "hosts"),
			filepath.Join(bundlesDir, cname, "upper", "var", "log", "app.log"),
			filepath.Join(bundlesDir, cname, "work", "work", "inprogress"),
			filepath.Join(bundlesDir, cname, "volumes", "agent", "bin", "agent"),
			filepath.Join(bundlesDir, cname, "volumes", "upper", "bin", "upper-bin"),
			filepath.Join(bundlesDir, cname, "volumes", "work", "lib", "work-lib"),
		} {
			if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
				t.Fatalf("WriteFile(%q): %v", file, err)
			}
		}
	}
	if err := os.Symlink("rootfs", filepath.Join(bundlesDir, "main", "rootfs-link")); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	// When running unprivileged, chmod 0000 on each bundle's upper/, work/,
	// unmounted rootfs/, and unmounted volume dirs so any attempt by
	// mountPointsUnderStatx to open and walk inside them fails with EACCES.
	if os.Geteuid() != 0 {
		var noReadDirs []string
		for _, cname := range containerNames {
			noReadDirs = append(noReadDirs,
				filepath.Join(bundlesDir, cname, "upper"),
				filepath.Join(bundlesDir, cname, "work"),
				filepath.Join(bundlesDir, cname, "rootfs"),
				filepath.Join(bundlesDir, cname, "volumes", "agent"),
				filepath.Join(bundlesDir, cname, "volumes", "upper"),
				filepath.Join(bundlesDir, cname, "volumes", "work"),
			)
		}
		for _, d := range noReadDirs {
			if err := os.Chmod(d, 0o000); err != nil {
				t.Fatalf("Chmod(%q, 0000): %v", d, err)
			}
		}
		t.Cleanup(func() {
			for _, d := range noReadDirs {
				_ = os.Chmod(d, 0o755)
			}
		})
	}

	points, err = mountPointsUnderStatx(bundlesDir)
	if err != nil {
		t.Fatalf("mountPointsUnderStatx(unmounted multi-bundle) error = %v", err)
	}
	if len(points) != 0 {
		t.Fatalf("mountPointsUnderStatx(unmounted multi-bundle) = %v, want empty", points)
	}

	// Passing a regular file directly to mountPointsUnderStatx must not fail
	// with ENOTDIR.
	points, err = mountPointsUnderStatx(filepath.Join(bundlesDir, "main", "config.json"))
	if err != nil {
		t.Fatalf("mountPointsUnderStatx(regular file) error = %v", err)
	}
	if len(points) != 0 {
		t.Fatalf("mountPointsUnderStatx(regular file) = %v, want empty", points)
	}

	if err := UnmountAllUnder(bundlesDir); err != nil {
		t.Fatalf("UnmountAllUnder: %v", err)
	}

	if os.Geteuid() != 0 {
		// Verify that containers named "upper", "work", "rootfs", and
		// "volumes" under bundlesDir are walked rather than skipped:
		// making a container's volumes/ directory unreadable causes
		// mountPointsUnderStatx to fail with EACCES when descending into
		// that container.
		for _, cname := range []string{"upper", "work", "rootfs", "volumes"} {
			volDir := filepath.Join(bundlesDir, cname, "volumes")
			if err := os.Chmod(volDir, 0o000); err != nil {
				t.Fatalf("Chmod(%q, 0000): %v", volDir, err)
			}
			_, walkErr := mountPointsUnderStatx(bundlesDir)
			if err := os.Chmod(volDir, 0o755); err != nil {
				t.Fatalf("Chmod(%q, 0755): %v", volDir, err)
			}
			if walkErr == nil {
				t.Fatalf("mountPointsUnderStatx skipped container %q under bundlesDir", cname)
			}
		}

		// Verify that image volumes named "upper" and "work" under
		// <bundle>/volumes/ are checked with statxAt rather than skipped:
		// a volumes/ directory with read-only mode 0400 allows Readdirnames
		// to list ["upper", "work"] but causes statxAt on those entries to
		// fail with EACCES.
		onlyUpperWorkVols := filepath.Join(t.TempDir(), "bundles", "app", "volumes")
		for _, vname := range []string{"upper", "work"} {
			if err := os.MkdirAll(filepath.Join(onlyUpperWorkVols, vname), 0o755); err != nil {
				t.Fatalf("MkdirAll: %v", err)
			}
		}
		appBundle := filepath.Dir(onlyUpperWorkVols)
		if err := os.WriteFile(filepath.Join(appBundle, "config.json"), []byte("{}"), 0o644); err != nil {
			t.Fatalf("WriteFile(config.json): %v", err)
		}
		if err := os.Chmod(onlyUpperWorkVols, 0o400); err != nil {
			t.Fatalf("Chmod(volumes, 0400): %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(onlyUpperWorkVols, 0o755) })
		if _, err := mountPointsUnderStatx(filepath.Dir(appBundle)); err == nil {
			t.Fatalf("mountPointsUnderStatx skipped volume entries named upper/work under volumes/")
		}
	}
}

func TestSetupBundleRootfs_MultiBundleUnmount(t *testing.T) {
	roottest.Require(t, "mount/unmount")

	pauseLayer := t.TempDir()
	writeLayer(t, pauseLayer, map[string]string{"pause.txt": "pause"}, nil)
	mainLayer := t.TempDir()
	writeLayer(t, mainLayer, map[string]string{"main.txt": "main"}, nil)
	volLayer1 := t.TempDir()
	writeLayer(t, volLayer1, map[string]string{"vol1.txt": "vol1"}, nil)
	volLayer2 := t.TempDir()
	writeLayer(t, volLayer2, map[string]string{"vol.txt": "vol"}, nil)

	bundlesDir := filepath.Join(t.TempDir(), "bundles")
	pauseBundle := filepath.Join(bundlesDir, "pause")
	mainBundle := filepath.Join(bundlesDir, "main")
	upperBundle := filepath.Join(bundlesDir, "upper")
	workBundle := filepath.Join(bundlesDir, "work")
	rootfsBundle := filepath.Join(bundlesDir, "rootfs")
	volumesBundle := filepath.Join(bundlesDir, "volumes")
	for _, b := range []string{pauseBundle, mainBundle, upperBundle, workBundle, rootfsBundle, volumesBundle} {
		if err := os.MkdirAll(b, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", b, err)
		}
	}

	for _, b := range []string{pauseBundle, upperBundle, workBundle, rootfsBundle, volumesBundle} {
		if err := WriteSpec(b, &OverlaySpec{Layers: []string{pauseLayer}}); err != nil {
			t.Fatalf("WriteSpec(%q): %v", b, err)
		}
	}
	if err := WriteSpec(mainBundle, &OverlaySpec{
		Layers: []string{mainLayer},
		ImageVolumes: []ImageVolumeOverlay{
			{Name: "agent", Layers: []string{volLayer1, volLayer2}},
			{Name: "upper", Layers: []string{volLayer1, volLayer2}},
			{Name: "work", Layers: []string{volLayer1, volLayer2}},
		},
	}); err != nil {
		t.Fatalf("WriteSpec(main): %v", err)
	}

	t.Cleanup(func() { _ = UnmountAllUnder(bundlesDir) })
	for _, b := range []string{pauseBundle, mainBundle, upperBundle, workBundle, rootfsBundle, volumesBundle} {
		if err := SetupBundleRootfs(b); err != nil {
			t.Fatalf("SetupBundleRootfs(%q): %v", b, err)
		}
	}

	pts, err := mountPointsUnderStatx(bundlesDir)
	if err != nil {
		t.Fatalf("mountPointsUnderStatx(bundlesDir): %v", err)
	}
	slices.Sort(pts)
	want := []string{
		filepath.Join(mainBundle, "rootfs"),
		filepath.Join(mainBundle, "volumes", "agent"),
		filepath.Join(mainBundle, "volumes", "upper"),
		filepath.Join(mainBundle, "volumes", "work"),
		filepath.Join(pauseBundle, "rootfs"),
		filepath.Join(rootfsBundle, "rootfs"),
		filepath.Join(upperBundle, "rootfs"),
		filepath.Join(volumesBundle, "rootfs"),
		filepath.Join(workBundle, "rootfs"),
	}
	slices.Sort(want)
	if !slices.Equal(pts, want) {
		t.Fatalf("mountPointsUnderStatx(bundlesDir) = %v, want %v", pts, want)
	}

	if err := UnmountAllUnder(bundlesDir); err != nil {
		t.Fatalf("UnmountAllUnder(bundlesDir): %v", err)
	}
	for _, check := range []string{
		filepath.Join(pauseBundle, "rootfs", "pause.txt"),
		filepath.Join(upperBundle, "rootfs", "pause.txt"),
		filepath.Join(workBundle, "rootfs", "pause.txt"),
		filepath.Join(rootfsBundle, "rootfs", "pause.txt"),
		filepath.Join(volumesBundle, "rootfs", "pause.txt"),
		filepath.Join(mainBundle, "rootfs", "main.txt"),
		filepath.Join(mainBundle, "volumes", "agent", "vol.txt"),
		filepath.Join(mainBundle, "volumes", "upper", "vol.txt"),
		filepath.Join(mainBundle, "volumes", "work", "vol.txt"),
	} {
		if _, err := os.Stat(check); err == nil {
			t.Errorf("%q still visible after UnmountAllUnder(bundlesDir)", check)
		}
	}
}

func TestMountPointsUnderStatx_RegularFileBindMount(t *testing.T) {
	roottest.Require(t, "mount/unmount")
	stateDir := t.TempDir()
	src := filepath.Join(t.TempDir(), "netns")
	target := filepath.Join(stateDir, "null-netns")
	for _, f := range []string{src, target} {
		if err := os.WriteFile(f, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := unix.Mount(src, target, "", unix.MS_BIND, ""); err != nil {
		t.Fatalf("bind mount: %v", err)
	}
	t.Cleanup(func() { _ = unix.Unmount(target, unix.MNT_DETACH) })

	pts, err := mountPointsUnderStatx(target)
	if err != nil {
		t.Fatalf("mountPointsUnderStatx(target file): %v", err)
	}
	if len(pts) != 1 || pts[0] != target {
		t.Fatalf("mountPointsUnderStatx(target file) = %v, want [%q]", pts, target)
	}
}

func BenchmarkUnmountAllUnder(b *testing.B) {
	bundlesDir := filepath.Join(b.TempDir(), "bundles")
	for _, sub := range []string{
		filepath.Join(bundlesDir, "pause", "rootfs", "bin"),
		filepath.Join(bundlesDir, "pause", "upper", "tmp"),
		filepath.Join(bundlesDir, "pause", "work", "work"),
		filepath.Join(bundlesDir, "main", "rootfs", "etc"),
		filepath.Join(bundlesDir, "main", "upper", "var", "log"),
		filepath.Join(bundlesDir, "main", "work", "work"),
		filepath.Join(bundlesDir, "main", "volumes", "vol0", "bin"),
		filepath.Join(bundlesDir, "main", "volumes", "vol1", "lib"),
	} {
		if err := os.MkdirAll(sub, 0o755); err != nil {
			b.Fatalf("MkdirAll(%q): %v", sub, err)
		}
	}
	for i := range 32 {
		p := filepath.Join(bundlesDir, "main", "upper", "var", "log", fmt.Sprintf("file-%02d.log", i))
		if err := os.WriteFile(p, []byte("log"), 0o644); err != nil {
			b.Fatalf("WriteFile(%q): %v", p, err)
		}
	}
	for _, f := range []string{
		filepath.Join(bundlesDir, "pause", "config.json"),
		filepath.Join(bundlesDir, "main", "config.json"),
		filepath.Join(bundlesDir, "main", "rootfs-overlay.json"),
	} {
		if err := os.WriteFile(f, []byte("{}"), 0o644); err != nil {
			b.Fatalf("WriteFile(%q): %v", f, err)
		}
	}

	// Construct a synthetic 5,000-line mountinfo table representative of a
	// dense node with hundreds of active sandboxes and long overlay options.
	var sb strings.Builder
	lowerChain := strings.Repeat("/var/lib/ate/image-cache/layers/sha256/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/fs:", 16)
	for i := range 5000 {
		fmt.Fprintf(&sb, "%d 100 0:250 / /var/lib/ate/actors/actor-%04d/bundles/main/rootfs rw,relatime shared:%d - overlay overlay rw,lowerdir=%s,upperdir=/var/lib/ate/actors/actor-%04d/bundles/main/upper,workdir=/var/lib/ate/actors/actor-%04d/bundles/main/work\n",
			1000+i, i, 500+i, lowerChain, i, i)
	}
	mountinfoFile := filepath.Join(b.TempDir(), "mountinfo")
	if err := os.WriteFile(mountinfoFile, []byte(sb.String()), 0o600); err != nil {
		b.Fatalf("WriteFile(mountinfo): %v", err)
	}

	b.Run("statx=true", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := mountPointsUnderStatx(bundlesDir); err != nil {
				b.Fatalf("mountPointsUnderStatx: %v", err)
			}
		}
	})

	b.Run("statx=false", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			f, err := os.Open(mountinfoFile)
			if err != nil {
				b.Fatalf("Open(mountinfo): %v", err)
			}
			_, err = mountPointsIn(f, bundlesDir)
			_ = f.Close()
			if err != nil {
				b.Fatalf("mountPointsIn: %v", err)
			}
		}
	})
}
