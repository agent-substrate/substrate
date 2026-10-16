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
	"archive/tar"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/roottest"
)

// ownerOf returns path's numeric owner and permission bits without following a
// final-component symlink.
func ownerOf(t *testing.T, path string) (uid, gid uint32, mode os.FileMode) {
	t.Helper()
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("lstat %s: %v", path, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("lstat %s: no syscall.Stat_t", path)
	}
	return st.Uid, st.Gid, fi.Mode()
}

// assertOwner checks path's numeric owner and, unless want is 0, its mode
// (type and permission bits).
func assertOwner(t *testing.T, path string, wantUID, wantGID uint32, want os.FileMode) {
	t.Helper()
	uid, gid, mode := ownerOf(t, path)
	if uid != wantUID || gid != wantGID {
		t.Errorf("%s owner = %d:%d, want %d:%d", path, uid, gid, wantUID, wantGID)
	}
	if want != 0 && mode != want {
		t.Errorf("%s mode = %v, want %v", path, mode, want)
	}
}

// ownershipEntries is a layer giving image content to non-root users the way
// `useradd -m agent && chown -R agent:agent /home/agent` does, plus the entry
// kinds whose ownership needs its own handling.
var ownershipEntries = []tarEntry{
	{name: "home/", typeflag: tar.TypeDir},
	// A private home directory: only its owner may enter it.
	{name: "home/agent/", typeflag: tar.TypeDir, mode: 0o700, uid: 1000, gid: 1000},
	{name: "home/agent/.bashrc", typeflag: tar.TypeReg, mode: 0o644, body: "rc", uid: 1000, gid: 1000},
	// A setuid-free file owned by a non-root user and another group.
	{name: "srv/", typeflag: tar.TypeDir},
	{name: "srv/app.conf", typeflag: tar.TypeReg, mode: 0o640, body: "conf", uid: 1000, gid: 2000},
	// Directory setgid and sticky bits survive the chown that follows the
	// chmod: the kernel clears them only on non-directories.
	{name: "tmp/", typeflag: tar.TypeDir, mode: 0o1777},
	{name: "srv/team/", typeflag: tar.TypeDir, mode: 0o2775, uid: 1000, gid: 100},
	// A symlink's own owner differs from its target's: lchown, not chown.
	{name: "home/agent/rc", typeflag: tar.TypeSymlink, linkname: ".bashrc", uid: 1002, gid: 1002},
	// A hardlink to a symlink another user owns (moby writes a symlink with
	// several names this way): the shared inode takes the link header's owner.
	{name: "home/agent/ln", typeflag: tar.TypeSymlink, linkname: ".bashrc", uid: 1002, gid: 1002},
	{name: "home/agent/ln2", typeflag: tar.TypeLink, linkname: "home/agent/ln", uid: 1006, gid: 1006},
	// A read-only directory owned by another user still receives its child.
	{name: "opt/", typeflag: tar.TypeDir},
	{name: "opt/app/", typeflag: tar.TypeDir, mode: 0o555, uid: 1001, gid: 1001},
	{name: "opt/app/bin", typeflag: tar.TypeReg, mode: 0o755, body: "bin", uid: 1001, gid: 1001},
	// A hardlink to a file another user already owns.
	{name: "opt/app/bin2", typeflag: tar.TypeLink, linkname: "opt/app/bin", uid: 1001, gid: 1001},
	// A repeated directory entry: the later header's owner wins.
	{name: "data/", typeflag: tar.TypeDir, mode: 0o755, uid: 1003, gid: 1003},
	{name: "data/", typeflag: tar.TypeDir, mode: 0o750, uid: 1004, gid: 1004},
	// A directory replaced by a symlink keeps the symlink's owner, and its
	// target is not re-moded or re-owned through it.
	{name: "run/", typeflag: tar.TypeDir},
	{name: "var/", typeflag: tar.TypeDir},
	{name: "var/run/", typeflag: tar.TypeDir, mode: 0o700, uid: 1005, gid: 1005},
	{name: "var/run", typeflag: tar.TypeSymlink, linkname: "../run"},
}

// TestUnpackLayer_AppliesOwnership checks that every entry kind keeps the
// numeric owner its tar header records, as containerd and moby apply it.
// Before ownership was applied, home/agent here unpacked as root-owned 0700,
// so its own user could not enter it inside the actor.
func TestUnpackLayer_AppliesOwnership(t *testing.T) {
	roottest.Require(t, "giving files to other uids requires CAP_CHOWN")
	runAsRootAndAtelet(t, testUnpackLayerAppliesOwnership)
}

func testUnpackLayerAppliesOwnership(t *testing.T) {
	dir, _, err := runUnpack(t, ownershipEntries)
	if err != nil {
		t.Fatalf("unpackLayer: %v", err)
	}

	assertOwner(t, filepath.Join(dir, "home"), 0, 0, os.ModeDir|0o755)
	assertOwner(t, filepath.Join(dir, "home/agent"), 1000, 1000, os.ModeDir|0o700)
	assertOwner(t, filepath.Join(dir, "home/agent/.bashrc"), 1000, 1000, 0o644)
	assertOwner(t, filepath.Join(dir, "srv/app.conf"), 1000, 2000, 0o640)
	assertOwner(t, filepath.Join(dir, "tmp"), 0, 0, os.ModeDir|os.ModeSticky|0o777)
	assertOwner(t, filepath.Join(dir, "srv/team"), 1000, 100, os.ModeDir|os.ModeSetgid|0o775)
	assertOwner(t, filepath.Join(dir, "home/agent/rc"), 1002, 1002, os.ModeSymlink|0o777)
	assertOwner(t, filepath.Join(dir, "home/agent/ln"), 1006, 1006, os.ModeSymlink|0o777)
	assertOwner(t, filepath.Join(dir, "home/agent/ln2"), 1006, 1006, os.ModeSymlink|0o777)
	assertOwner(t, filepath.Join(dir, "opt/app"), 1001, 1001, os.ModeDir|0o555)
	assertOwner(t, filepath.Join(dir, "opt/app/bin"), 1001, 1001, 0o755)
	assertOwner(t, filepath.Join(dir, "opt/app/bin2"), 1001, 1001, 0o755)
	assertOwner(t, filepath.Join(dir, "data"), 1004, 1004, os.ModeDir|0o750)
	assertOwner(t, filepath.Join(dir, "var/run"), 0, 0, os.ModeSymlink|0o777)
	assertOwner(t, filepath.Join(dir, "run"), 0, 0, os.ModeDir|0o755)

	// Eviction must still be able to remove a read-only tree that belongs to
	// another user.
	if err := RemoveAllWritable(filepath.Join(dir, "opt")); err != nil {
		t.Errorf("RemoveAllWritable: %v", err)
	}
}

// TestUnpackLayer_UnprivilegedKeepsUnpackingUser checks the documented
// fallback for a process that cannot chown (unit tests, local tooling): the
// unpack succeeds and every entry belongs to the unpacking user.
func TestUnpackLayer_UnprivilegedKeepsUnpackingUser(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("covers an unprivileged unpack; TestUnpackLayer_AppliesOwnership covers root")
	}

	dir, _, err := runUnpack(t, ownershipEntries)
	if err != nil {
		t.Fatalf("unpackLayer: %v", err)
	}
	uid, gid := uint32(os.Geteuid()), uint32(os.Getegid())
	for _, name := range []string{"home/agent", "home/agent/.bashrc", "srv/app.conf", "opt/app/bin"} {
		got, _, _ := ownerOf(t, filepath.Join(dir, name))
		if got != uid {
			t.Errorf("%s uid = %d, want the unpacking user's %d", name, got, uid)
		}
	}
	// The group may come from the parent directory (BSD semantics on macOS),
	// so only the Linux default is pinned.
	if _, got, _ := ownerOf(t, filepath.Join(dir, "home/agent")); got != gid && runtime.GOOS == "linux" {
		t.Errorf("home/agent gid = %d, want %d", got, gid)
	}
	// Modes still apply even though the owners could not.
	if _, _, mode := ownerOf(t, filepath.Join(dir, "home/agent")); mode != os.ModeDir|0o700 {
		t.Errorf("home/agent mode = %v, want drwx------", mode)
	}
	if err := RemoveAllWritable(filepath.Join(dir, "opt")); err != nil {
		t.Errorf("RemoveAllWritable: %v", err)
	}
}

// TestUnpackLayer_AppliesModesWithOwners checks that files keep their tar
// mode once given to their owners: group write survives the umask, and
// setuid and setgid, which the kernel clears on chown, are put back where the
// process's capabilities allow. atelet holds only CAP_CHOWN and
// CAP_DAC_OVERRIDE; with them a setuid-root binary keeps its bit, while one
// another user owns, or one setgid to a group root is not in, loses it
// without failing the unpack.
func TestUnpackLayer_AppliesModesWithOwners(t *testing.T) {
	roottest.Require(t, "giving files to other uids requires CAP_CHOWN")
	runAsRootAndAtelet(t, testUnpackLayerAppliesModesWithOwners)
}

func testUnpackLayerAppliesModesWithOwners(t *testing.T) {
	fowner, fsetid := hasEffectiveCap(t, capFowner), hasEffectiveCap(t, capFsetid)

	old := syscall.Umask(0o022)
	t.Cleanup(func() { syscall.Umask(old) })

	entries := []tarEntry{
		{name: "srv/", typeflag: tar.TypeDir},
		{name: "srv/shared.txt", typeflag: tar.TypeReg, mode: 0o664, body: "s", uid: 1000, gid: 100},
		{name: "srv/run.sh", typeflag: tar.TypeReg, mode: 0o775, body: "r", uid: 1000, gid: 100},
		{name: "usr/", typeflag: tar.TypeDir},
		{name: "usr/bin/", typeflag: tar.TypeDir},
		{name: "usr/bin/sudo", typeflag: tar.TypeReg, mode: 0o4755, body: "sudo"},
		{name: "usr/bin/sudoedit", typeflag: tar.TypeLink, linkname: "usr/bin/sudo"},
		{name: "usr/bin/newgrp", typeflag: tar.TypeReg, mode: 0o6755, body: "newgrp"},
		{name: "usr/bin/crontab", typeflag: tar.TypeReg, mode: 0o2755, body: "crontab", gid: 54321},
		{name: "opt/", typeflag: tar.TypeDir},
		{name: "opt/tool", typeflag: tar.TypeReg, mode: 0o4755, body: "tool", uid: 1000, gid: 1000},
	}
	dir, _, err := runUnpack(t, entries)
	if err != nil {
		t.Fatalf("unpackLayer: %v", err)
	}

	ifCap := func(held bool, bit os.FileMode) os.FileMode {
		if held {
			return bit
		}
		return 0
	}
	assertOwner(t, filepath.Join(dir, "srv/shared.txt"), 1000, 100, 0o664)
	assertOwner(t, filepath.Join(dir, "srv/run.sh"), 1000, 100, 0o775)
	assertOwner(t, filepath.Join(dir, "usr/bin/sudo"), 0, 0, os.ModeSetuid|0o755)
	assertOwner(t, filepath.Join(dir, "usr/bin/sudoedit"), 0, 0, os.ModeSetuid|0o755)
	assertOwner(t, filepath.Join(dir, "usr/bin/newgrp"), 0, 0, os.ModeSetuid|os.ModeSetgid|0o755)
	assertOwner(t, filepath.Join(dir, "usr/bin/crontab"), 0, 54321, ifCap(fsetid, os.ModeSetgid)|0o755)
	assertOwner(t, filepath.Join(dir, "opt/tool"), 1000, 1000, ifCap(fowner, os.ModeSetuid)|0o755)
}

// TestUnpackLayer_DirectoryAliasesTakeLatestHeader checks that a directory a
// layer declares under two names, one through a symlink, takes the header
// written last, whichever name restores first. Restoring both names gave the
// second a chmod on a directory the first had already given to another user,
// which needs CAP_FOWNER, and an unprivileged unpack the wrong mode.
func TestUnpackLayer_DirectoryAliasesTakeLatestHeader(t *testing.T) {
	if os.Geteuid() != 0 {
		testUnpackLayerDirectoryAliasesTakeLatestHeader(t)
		return
	}
	runAsRootAndAtelet(t, testUnpackLayerDirectoryAliasesTakeLatestHeader)
}

func testUnpackLayerDirectoryAliasesTakeLatestHeader(t *testing.T) {
	early := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	entries := []tarEntry{
		{name: "cc/", typeflag: tar.TypeDir},
		// The later header is on the longer name for b ("cc/b" against
		// "a/b") and on the shorter name for d ("cc/d" against "alias/d"),
		// so neither name order picks it.
		{name: "a", typeflag: tar.TypeSymlink, linkname: "cc"},
		{name: "alias", typeflag: tar.TypeSymlink, linkname: "cc"},
		{name: "a/b/", typeflag: tar.TypeDir, mode: 0o750, uid: 1000, gid: 1000, modTime: early},
		{name: "cc/b/", typeflag: tar.TypeDir, mode: 0o705, uid: 1001, gid: 1001, modTime: late},
		{name: "alias/d/", typeflag: tar.TypeDir, mode: 0o750, uid: 1000, gid: 1000, modTime: early},
		{name: "cc/d/", typeflag: tar.TypeDir, mode: 0o1705, uid: 1001, gid: 1001, modTime: late},
	}
	dir, _, err := runUnpack(t, entries)
	if err != nil {
		t.Fatalf("unpackLayer: %v", err)
	}
	want := map[string]os.FileMode{
		"cc/b": os.ModeDir | 0o705,
		"cc/d": os.ModeDir | os.ModeSticky | 0o705,
	}
	for name, mode := range want {
		uid, gid, got := ownerOf(t, filepath.Join(dir, name))
		if got != mode {
			t.Errorf("%s mode = %v, want %v", name, got, mode)
		}
		// Without CAP_CHOWN every entry keeps the unpacking user.
		if os.Geteuid() == 0 && (uid != 1001 || gid != 1001) {
			t.Errorf("%s owner = %d:%d, want 1001:1001", name, uid, gid)
		}
		fi, err := os.Lstat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if !fi.ModTime().Equal(late) {
			t.Errorf("%s mtime = %v, want %v", name, fi.ModTime(), late)
		}
	}
}

// stickyDirEntries is a layer with sticky directories other users own, holding
// entries that belong to users other than root. Unlinking such an entry takes
// owning it or the directory, or CAP_FOWNER, which atelet does not hold;
// CAP_DAC_OVERRIDE does not cover it.
var stickyDirEntries = []tarEntry{
	// A shared drop directory, as `install -d -m 1777 -o app` creates.
	{name: "shared/", typeflag: tar.TypeDir, mode: 0o1777, uid: 1000, gid: 1000},
	{name: "shared/f", typeflag: tar.TypeReg, mode: 0o644, body: "f", uid: 1000, gid: 1000},
	{name: "shared/g", typeflag: tar.TypeReg, mode: 0o644, body: "g", uid: 1001, gid: 1001},
	// The same, read-only, inside a private directory, with a read-only
	// directory a third user owns below it.
	{name: "home/", typeflag: tar.TypeDir},
	{name: "home/app/", typeflag: tar.TypeDir, mode: 0o700, uid: 1000, gid: 1000},
	{name: "home/app/drop/", typeflag: tar.TypeDir, mode: 0o1555, uid: 1000, gid: 1000},
	{name: "home/app/drop/sub/", typeflag: tar.TypeDir, mode: 0o555, uid: 1001, gid: 1001},
	{name: "home/app/drop/sub/h", typeflag: tar.TypeReg, mode: 0o444, body: "h", uid: 1002, gid: 1002},
}

// unpackStickyDirs unpacks stickyDirEntries into dir and checks the tree has
// the owners and modes the removal tests rely on.
func unpackStickyDirs(t *testing.T, dir string) {
	t.Helper()
	if _, err := unpackInto(t, dir, buildTar(t, stickyDirEntries)); err != nil {
		t.Fatalf("unpackLayer: %v", err)
	}
	assertOwner(t, filepath.Join(dir, "shared"), 1000, 1000, os.ModeDir|os.ModeSticky|0o777)
	assertOwner(t, filepath.Join(dir, "shared/g"), 1001, 1001, 0o644)
	assertOwner(t, filepath.Join(dir, "home/app/drop"), 1000, 1000, os.ModeDir|os.ModeSticky|0o555)
	assertOwner(t, filepath.Join(dir, "home/app/drop/sub/h"), 1002, 1002, 0o444)
}

// TestRemoveAllWritable_OtherUsersStickyDirs checks that eviction can remove a
// layer whose sticky directories belong to other users.
func TestRemoveAllWritable_OtherUsersStickyDirs(t *testing.T) {
	roottest.Require(t, "giving files to other uids requires CAP_CHOWN")
	runAsRootAndAtelet(t, func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "layer")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		unpackStickyDirs(t, dir)
		if err := RemoveAllWritable(dir); err != nil {
			t.Fatalf("RemoveAllWritable: %v", err)
		}
		if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("layer dir survived RemoveAllWritable: %v", err)
		}
	})
}

// TestNew_SweepsRetiredLayerWithOtherUsersStickyDirs checks that a retired
// layer holding other users' sticky directories does not stop the store from
// opening: the startup sweep must remove it, or atelet fails to start.
func TestNew_SweepsRetiredLayerWithOtherUsersStickyDirs(t *testing.T) {
	roottest.Require(t, "giving files to other uids requires CAP_CHOWN")
	runAsRootAndAtelet(t, func(t *testing.T) {
		root := t.TempDir()
		s, err := New(root)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		retired := filepath.Join(s.layersDir(), retiredPrefix+"0123456789ab-1")
		fsDir := filepath.Join(retired, layerFSDirName)
		if err := os.MkdirAll(fsDir, 0o700); err != nil {
			t.Fatal(err)
		}
		unpackStickyDirs(t, fsDir)
		if _, err := New(root); err != nil {
			t.Fatalf("New after a retirement left the layer behind: %v", err)
		}
		if _, err := os.Lstat(retired); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("retired layer dir survived the startup sweep: %v", err)
		}
	})
}
