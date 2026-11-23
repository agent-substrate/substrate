//go:build linux

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
	"archive/tar"
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

// FinalizeLayer materializes 0:0 whiteout devices and opaque
// xattrs (trusted.*, CAP_SYS_ADMIN); only root has those in a plain test
// environment. Runs in privileged CI / root shells, skips elsewhere.
func TestFinalizeLayer_MaterializesWhiteouts(t *testing.T) {
	roottest.Require(t, "CAP_SYS_ADMIN for trusted.* xattrs")
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

// The merged rootfs root is the container's "/"; at 0700 a non-root container
// cannot search it and can exec nothing.
func TestSetupBundleRootfs_RootIsSearchableByNonRoot(t *testing.T) {
	assertSearchable := func(t *testing.T, rootfs string) {
		t.Helper()
		fi, err := os.Stat(rootfs)
		if err != nil {
			t.Fatalf("stat merged rootfs root: %v", err)
		}
		if perm := fi.Mode().Perm(); perm&0o005 != 0o005 {
			t.Errorf("merged rootfs root mode = %04o, want world read+search", perm)
		}
	}

	t.Run("zero layers", func(t *testing.T) {
		bundle := t.TempDir()
		if err := WriteSpec(bundle, &OverlaySpec{Layers: nil}); err != nil {
			t.Fatalf("WriteSpec: %v", err)
		}
		if err := SetupBundleRootfs(bundle); err != nil {
			t.Fatalf("SetupBundleRootfs: %v", err)
		}
		assertSearchable(t, filepath.Join(bundle, "rootfs"))
	})

	t.Run("overlay", func(t *testing.T) {
		roottest.Require(t, "mount/unmount")
		layer := t.TempDir()
		writeLayer(t, layer, map[string]string{"bin/app": "x"}, nil)

		bundle := t.TempDir()
		if err := WriteSpec(bundle, &OverlaySpec{Layers: []string{layer}}); err != nil {
			t.Fatalf("WriteSpec: %v", err)
		}
		if err := SetupBundleRootfs(bundle); err != nil {
			t.Fatalf("SetupBundleRootfs: %v", err)
		}
		t.Cleanup(func() { _ = UnmountAllUnder(bundle) })
		assertSearchable(t, filepath.Join(bundle, "rootfs"))
	})
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

// unpackTestLayer unpacks entries into a layer dir under dir the way the
// store's unpack does: the tree under fs/ plus its whiteouts.json.
func unpackTestLayer(t *testing.T, dir string, entries []tarEntry) {
	t.Helper()
	fsDir := filepath.Join(dir, layerFSDirName)
	if err := os.Mkdir(fsDir, 0o755); err != nil {
		t.Fatalf("mkdir fs: %v", err)
	}
	wh, err := unpackInto(t, fsDir, buildTar(t, entries))
	if err != nil {
		t.Fatalf("unpackLayer: %v", err)
	}
	b, err := json.Marshal(wh)
	if err != nil {
		t.Fatalf("marshal whiteouts: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, layerWhiteoutsFileName), b, 0o600); err != nil {
		t.Fatalf("write whiteouts.json: %v", err)
	}
}

// TestSetupBundleRootfs_PresentsLayerOwnership composes a rootfs from layers
// unpacked from tars and checks the merged view shows the owners the tars
// record: the view runsc's gofer and virtiofsd serve to the actor. The top
// layer adds files under the private home directory and the setgid team
// directory without declaring them, so the merged directories' owners and
// modes also have to survive the implicit-dir repair, which copies them from
// the base layer's unpacked tree. It runs as root and under the gVisor
// worker's capabilities, which have no CAP_FSETID.
func TestSetupBundleRootfs_PresentsLayerOwnership(t *testing.T) {
	roottest.Require(t, "chown and mount")
	t.Run("root", testSetupBundleRootfsPresentsLayerOwnership)
	runUnderCaps(t, "gvisor-worker-caps", gvisorWorkerCaps, testSetupBundleRootfsPresentsLayerOwnership)
}

func testSetupBundleRootfsPresentsLayerOwnership(t *testing.T) {
	base := t.TempDir()
	unpackTestLayer(t, base, ownershipEntries)
	top := t.TempDir()
	unpackTestLayer(t, top, []tarEntry{
		{name: "home/agent/.profile", typeflag: tar.TypeReg, mode: 0o600, body: "p", uid: 1000, gid: 1000},
		{name: "srv/team/notes", typeflag: tar.TypeReg, mode: 0o664, body: "n", uid: 1000, gid: 100},
	})

	bundle := t.TempDir()
	if err := WriteSpec(bundle, &OverlaySpec{Layers: []string{base, top}}); err != nil {
		t.Fatalf("WriteSpec: %v", err)
	}
	if err := SetupBundleRootfs(bundle); err != nil {
		t.Fatalf("SetupBundleRootfs: %v", err)
	}
	t.Cleanup(func() { _ = UnmountAllUnder(bundle) })

	rootfs := filepath.Join(bundle, "rootfs")
	assertOwner(t, filepath.Join(rootfs, "home/agent"), 1000, 1000, os.ModeDir|0o700)
	assertOwner(t, filepath.Join(rootfs, "home/agent/.bashrc"), 1000, 1000, 0o644)
	assertOwner(t, filepath.Join(rootfs, "home/agent/.profile"), 1000, 1000, 0o600)
	assertOwner(t, filepath.Join(rootfs, "home/agent/rc"), 1002, 1002, os.ModeSymlink|0o777)
	assertOwner(t, filepath.Join(rootfs, "srv/app.conf"), 1000, 2000, 0o640)
	assertOwner(t, filepath.Join(rootfs, "srv/team"), 1000, 100, os.ModeDir|os.ModeSetgid|0o775)
	assertOwner(t, filepath.Join(rootfs, "opt/app"), 1001, 1001, os.ModeDir|0o555)
	assertOwner(t, filepath.Join(rootfs, "home"), 0, 0, os.ModeDir|0o755)
}

// TestSetupBundleRootfs_RepairsWhiteoutParentUnderSetgidDir checks the repair
// of an implicit directory that FinalizeLayer creates for a whiteout. The top
// layer declares srv/shared setgid with group 1000 and whites out a file in
// srv/shared/docs without declaring docs, so docs starts as 0:1000 with
// setgid inherited. The repair must still give it the base layer's 02770, and
// in the gVisor worker root is not in group 1000 and has no CAP_FSETID.
func TestSetupBundleRootfs_RepairsWhiteoutParentUnderSetgidDir(t *testing.T) {
	roottest.Require(t, "chown, mknod and mount")
	t.Run("root", testSetupBundleRootfsRepairsWhiteoutParentUnderSetgidDir)
	runUnderCaps(t, "gvisor-worker-caps", gvisorWorkerCaps, testSetupBundleRootfsRepairsWhiteoutParentUnderSetgidDir)
}

func testSetupBundleRootfsRepairsWhiteoutParentUnderSetgidDir(t *testing.T) {
	base := t.TempDir()
	unpackTestLayer(t, base, []tarEntry{
		{name: "srv/shared/docs/", typeflag: tar.TypeDir, mode: 0o2770, uid: 1000, gid: 1000},
		{name: "srv/shared/docs/old", typeflag: tar.TypeReg, mode: 0o660, body: "o", uid: 1000, gid: 1000},
	})
	top := t.TempDir()
	unpackTestLayer(t, top, []tarEntry{
		{name: "srv/shared/", typeflag: tar.TypeDir, mode: 0o2775, gid: 1000},
		{name: "srv/shared/docs/.wh.old", typeflag: tar.TypeReg},
	})

	bundle := t.TempDir()
	if err := WriteSpec(bundle, &OverlaySpec{Layers: []string{base, top}}); err != nil {
		t.Fatalf("WriteSpec: %v", err)
	}
	if err := SetupBundleRootfs(bundle); err != nil {
		t.Fatalf("SetupBundleRootfs: %v", err)
	}
	t.Cleanup(func() { _ = UnmountAllUnder(bundle) })

	// FinalizeLayer created docs in the top layer under the setgid parent.
	assertOwner(t, filepath.Join(top, layerFSDirName, "srv/shared/docs"), 0, 1000, 0)
	rootfs := filepath.Join(bundle, "rootfs")
	assertOwner(t, filepath.Join(rootfs, "srv/shared/docs"), 1000, 1000, os.ModeDir|os.ModeSetgid|0o770)
	if _, err := os.Lstat(filepath.Join(rootfs, "srv/shared/docs/old")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("whited-out srv/shared/docs/old: Lstat error %v, want ErrNotExist", err)
	}
}

// mountPaths returns the sorted paths of points.
func mountPaths(points []mountPoint) []string {
	paths := make([]string, 0, len(points))
	for _, p := range points {
		paths = append(paths, p.path)
	}
	slices.Sort(paths)
	return paths
}

// fakeMounts fakes mounts on a real directory tree. Paths are absolute.
type fakeMounts struct {
	// mounted counts the mounts stacked at each path.
	mounted map[string]int
	// statErr is returned by statx for each path. It is cleared when the
	// last mount at the path is detached.
	statErr map[string]error
	// unmountErr is returned by unmount for each path.
	unmountErr map[string]error

	statted   []string
	unmounted []string
}

// sys returns a mountSys that runs the real statx and then applies f.
func (f *fakeMounts) sys(t *testing.T) mountSys {
	return mountSys{
		statx: func(dirfd int, path string) (unix.Statx_t, error) {
			if dirfd != unix.AT_FDCWD {
				dir, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", dirfd))
				if err != nil {
					t.Fatalf("Readlink(dirfd): %v", err)
				}
				path = filepath.Join(dir, path)
			}
			f.statted = append(f.statted, path)
			if err := f.statErr[path]; err != nil {
				return unix.Statx_t{}, err
			}
			sx, err := statxAt(unix.AT_FDCWD, path)
			if err == nil && f.mounted[path] > 0 {
				sx.Attributes |= unix.STATX_ATTR_MOUNT_ROOT
			}
			return sx, err
		},
		unmount: func(target string, flags int) error {
			f.unmounted = append(f.unmounted, target)
			if flags != unix.MNT_DETACH|unix.UMOUNT_NOFOLLOW {
				t.Errorf("unmount(%q) flags = %#x, want MNT_DETACH|UMOUNT_NOFOLLOW", target, flags)
			}
			if err := f.unmountErr[target]; err != nil {
				return err
			}
			if f.mounted[target] == 0 {
				return unix.EINVAL
			}
			f.mounted[target]--
			if f.mounted[target] == 0 {
				delete(f.statErr, target)
			}
			return nil
		},
	}
}

// fakeTree creates dirs and empty files under a new temp dir and returns the
// dir with symlinks resolved, to match the paths /proc/self/fd reports.
func fakeTree(t *testing.T, dirs, files []string) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(root, f), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// finders are the mount listings UnmountAllUnder and UnmountBundlesUnder use.
var finders = map[string]func(string, mountSys) ([]mountPoint, error){
	"UnmountAllUnder":     mountPointsUnder,
	"UnmountBundlesUnder": bundleMountPoints,
}

func TestUnmountAllUnder_DirArgument(t *testing.T) {
	for _, unmount := range []func(string) error{UnmountAllUnder, UnmountBundlesUnder} {
		for _, dir := range []string{"", ".", "bundles"} {
			if err := unmount(dir); err == nil {
				t.Errorf("unmount(%q) = nil, want error for a relative dir", dir)
			}
		}
		if err := unmount(filepath.Join(t.TempDir(), "missing")); err != nil {
			t.Errorf("unmount(missing dir) = %v, want nil", err)
		}
	}
}

func TestUnmountAllUnder_ContinuesPastBadEntry(t *testing.T) {
	for name, find := range finders {
		t.Run(name, func(t *testing.T) {
			root := fakeTree(t, []string{"a/rootfs", "a/volumes", "b/rootfs"}, []string{"a/volumes/bad"})
			bad := filepath.Join(root, "a/volumes/bad")
			mntA := filepath.Join(root, "a/rootfs")
			mntB := filepath.Join(root, "b/rootfs")
			f := &fakeMounts{
				mounted: map[string]int{mntA: 1, mntB: 1},
				statErr: map[string]error{bad: unix.EACCES},
			}
			err := unmountAllUnder(root, f.sys(t), find)
			if !errors.Is(err, unix.EACCES) || !strings.Contains(err.Error(), bad) {
				t.Errorf("unmountAllUnder = %v, want EACCES for %q", err, bad)
			}
			if f.mounted[mntA] != 0 || f.mounted[mntB] != 0 {
				t.Errorf("mounts left after unmountAllUnder: %v", f.mounted)
			}
		})
	}
}

func TestUnmountAllUnder_DeadMount(t *testing.T) {
	for name, find := range finders {
		t.Run(name, func(t *testing.T) {
			root := fakeTree(t, []string{"main/volumes/fuse", "other/rootfs"}, nil)
			fuse := filepath.Join(root, "main/volumes/fuse")
			broken := filepath.Join(root, "other/rootfs")
			f := &fakeMounts{
				mounted: map[string]int{fuse: 1},
				statErr: map[string]error{fuse: unix.ENOTCONN, broken: unix.EIO},
			}
			err := unmountAllUnder(root, f.sys(t), find)
			if f.mounted[fuse] != 0 {
				t.Errorf("dead mount %q not detached", fuse)
			}
			// broken is not a mount, so its statx error is returned.
			if !errors.Is(err, unix.EIO) || errors.Is(err, unix.ENOTCONN) {
				t.Errorf("unmountAllUnder = %v, want only the EIO for %q", err, broken)
			}
		})
	}
}

func TestUnmountAllUnder_GivesUpAfterMaxPasses(t *testing.T) {
	root := fakeTree(t, []string{"main/rootfs"}, nil)
	rootfs := filepath.Join(root, "main/rootfs")
	f := &fakeMounts{
		mounted:    map[string]int{rootfs: 1},
		unmountErr: map[string]error{rootfs: unix.EINVAL},
	}
	err := unmountAllUnder(root, f.sys(t), bundleMountPoints)
	if err == nil || !strings.Contains(err.Error(), "mounts remain") {
		t.Fatalf("unmountAllUnder = %v, want mounts remain error", err)
	}
	if len(f.unmounted) != maxUnmountPasses {
		t.Errorf("unmounted %d times, want %d", len(f.unmounted), maxUnmountPasses)
	}
}

func TestUnmountBundlesUnder_ChecksOnlyBundleMounts(t *testing.T) {
	root := fakeTree(t, []string{
		"main/rootfs/in-rootfs",
		"main/upper",
		"main/volumes/agent",
		"main/other",
		"upper/rootfs",
		"volumes/volumes/data",
	}, []string{"main/config.json", "file"})
	if err := os.Symlink("main", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	p := func(rel string) string { return filepath.Join(root, rel) }
	f := &fakeMounts{mounted: map[string]int{
		p("main/rootfs"):          1,
		p("main/volumes/agent"):   1,
		p("upper/rootfs"):         1,
		p("volumes/volumes/data"): 1,
		// SetupBundleRootfs never mounts these.
		p("main/upper"):            1,
		p("main/other"):            1,
		p("main/rootfs/in-rootfs"): 1,
	}}
	points, err := bundleMountPoints(root, f.sys(t))
	if err != nil {
		t.Fatalf("bundleMountPoints: %v", err)
	}
	want := []string{p("main/rootfs"), p("main/volumes/agent"), p("upper/rootfs"), p("volumes/volumes/data")}
	slices.Sort(want)
	if got := mountPaths(points); !slices.Equal(got, want) {
		t.Errorf("bundleMountPoints = %q, want %q", got, want)
	}
	for _, path := range f.statted {
		if strings.HasPrefix(path, p("main/rootfs")+"/") || strings.HasPrefix(path, p("link")+"/") {
			t.Errorf("checked %q", path)
		}
	}
}

func TestSetupBundleRootfs_MultiBundleUnmount(t *testing.T) {
	roottest.Require(t, "mount/unmount")

	rootLayer := t.TempDir()
	writeLayer(t, rootLayer, map[string]string{"root.txt": "root"}, nil)
	volLayer1 := t.TempDir()
	writeLayer(t, volLayer1, map[string]string{"vol1.txt": "vol1"}, nil)
	volLayer2 := t.TempDir()
	writeLayer(t, volLayer2, map[string]string{"vol.txt": "vol"}, nil)

	// Containers and volumes named after bundle subdirs must still be found.
	bundlesDir := filepath.Join(t.TempDir(), "bundles")
	t.Cleanup(func() { _ = UnmountBundlesUnder(bundlesDir) })
	var want []string
	for _, c := range []string{"main", "upper", "rootfs", "volumes"} {
		b := filepath.Join(bundlesDir, c)
		if err := os.MkdirAll(b, 0o755); err != nil {
			t.Fatal(err)
		}
		spec := &OverlaySpec{Layers: []string{rootLayer}}
		want = append(want, filepath.Join(b, "rootfs"))
		if c == "main" {
			for _, v := range []string{"agent", "upper"} {
				spec.ImageVolumes = append(spec.ImageVolumes, ImageVolumeOverlay{Name: v, Layers: []string{volLayer1, volLayer2}})
				want = append(want, ImageVolumeMountPath(b, v))
			}
		}
		if err := WriteSpec(b, spec); err != nil {
			t.Fatalf("WriteSpec(%q): %v", b, err)
		}
		if err := SetupBundleRootfs(b); err != nil {
			t.Fatalf("SetupBundleRootfs(%q): %v", b, err)
		}
	}
	slices.Sort(want)

	points, err := bundleMountPoints(bundlesDir, hostMountSys())
	if err != nil {
		t.Fatalf("bundleMountPoints(bundlesDir): %v", err)
	}
	if got := mountPaths(points); !slices.Equal(got, want) {
		t.Fatalf("bundleMountPoints(bundlesDir) = %q, want %q", got, want)
	}
	if err := UnmountBundlesUnder(bundlesDir); err != nil {
		t.Fatalf("UnmountBundlesUnder(bundlesDir): %v", err)
	}
	if points, err := mountPointsUnder(bundlesDir, hostMountSys()); err != nil || len(points) != 0 {
		t.Errorf("after UnmountBundlesUnder: mounts %q, err %v; want none", mountPaths(points), err)
	}
}

func TestUnmountBundlesUnder_StackedMounts(t *testing.T) {
	roottest.Require(t, "mount/unmount")
	bundlesDir := t.TempDir()
	rootfs := filepath.Join(bundlesDir, "main", "rootfs")
	if err := os.MkdirAll(rootfs, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for range 3 {
			_ = unix.Unmount(rootfs, unix.MNT_DETACH)
		}
	})
	for i := range 3 {
		if err := unix.Mount("tmpfs", rootfs, "tmpfs", 0, ""); err != nil {
			t.Fatalf("mount tmpfs %d: %v", i, err)
		}
	}
	if err := UnmountBundlesUnder(bundlesDir); err != nil {
		t.Fatalf("UnmountBundlesUnder: %v", err)
	}
	assertNotMountRoot(t, rootfs)
}

func TestUnmountAllUnder_ShadowedMount(t *testing.T) {
	roottest.Require(t, "mount/unmount")
	bundlesDir := t.TempDir()
	mainDir := filepath.Join(bundlesDir, "main")
	rootfs := filepath.Join(mainDir, "rootfs")
	if err := os.MkdirAll(rootfs, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = unix.Unmount(mainDir, unix.MNT_DETACH)
		_ = unix.Unmount(rootfs, unix.MNT_DETACH)
	})
	if err := unix.Mount("tmpfs", rootfs, "tmpfs", 0, ""); err != nil {
		t.Fatalf("mount tmpfs at rootfs: %v", err)
	}
	// A mount over main hides the rootfs mount from the first walk.
	if err := unix.Mount("tmpfs", mainDir, "tmpfs", 0, ""); err != nil {
		t.Fatalf("mount tmpfs at main: %v", err)
	}
	if err := UnmountAllUnder(bundlesDir); err != nil {
		t.Fatalf("UnmountAllUnder: %v", err)
	}
	assertNotMountRoot(t, mainDir)
	assertNotMountRoot(t, rootfs)
}

func TestUnmountAllUnder_RegularFileBindMount(t *testing.T) {
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
	if err := UnmountAllUnder(stateDir); err != nil {
		t.Fatalf("UnmountAllUnder: %v", err)
	}
	assertNotMountRoot(t, target)
}

// assertNotMountRoot fails t if path is a mount root.
func assertNotMountRoot(t *testing.T, path string) {
	t.Helper()
	sx, err := statxAt(unix.AT_FDCWD, path)
	if err != nil {
		t.Fatal(err)
	}
	if sx.Attributes&unix.STATX_ATTR_MOUNT_ROOT != 0 {
		t.Errorf("%q is still a mount point", path)
	}
}

func BenchmarkUnmountBundlesUnder(b *testing.B) {
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
	for _, f := range []string{
		filepath.Join(bundlesDir, "pause", "config.json"),
		filepath.Join(bundlesDir, "main", "config.json"),
		filepath.Join(bundlesDir, "main", "rootfs-overlay.json"),
	} {
		if err := os.WriteFile(f, []byte("{}"), 0o644); err != nil {
			b.Fatalf("WriteFile(%q): %v", f, err)
		}
	}

	b.ReportAllocs()
	for b.Loop() {
		if err := UnmountBundlesUnder(bundlesDir); err != nil {
			b.Fatalf("UnmountBundlesUnder: %v", err)
		}
	}
}
