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
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/agent-substrate/substrate/internal/roottest"
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
	// A symlink's own owner differs from its target's: lchown, not chown.
	{name: "home/agent/rc", typeflag: tar.TypeSymlink, linkname: ".bashrc", uid: 1002, gid: 1002},
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

	dir, _, err := runUnpack(t, ownershipEntries)
	if err != nil {
		t.Fatalf("unpackLayer: %v", err)
	}

	assertOwner(t, filepath.Join(dir, "home"), 0, 0, os.ModeDir|0o755)
	assertOwner(t, filepath.Join(dir, "home/agent"), 1000, 1000, os.ModeDir|0o700)
	assertOwner(t, filepath.Join(dir, "home/agent/.bashrc"), 1000, 1000, 0o644)
	assertOwner(t, filepath.Join(dir, "srv/app.conf"), 1000, 2000, 0o640)
	assertOwner(t, filepath.Join(dir, "home/agent/rc"), 1002, 1002, os.ModeSymlink|0o777)
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
