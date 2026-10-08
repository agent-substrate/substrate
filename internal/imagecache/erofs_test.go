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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	erofs "github.com/erofs/go-erofs"
)

// buildEROFS runs buildLayerEROFS over tarBytes into a temp file and returns
// the file (positioned anywhere; callers use ReadAt) and its size.
func buildEROFS(t *testing.T, tarBytes []byte) (*os.File, int64, error) {
	t.Helper()
	f, size, _, err := buildEROFSDigest(t, tarBytes)
	return f, size, err
}

func buildEROFSDigest(t *testing.T, tarBytes []byte) (*os.File, int64, string, error) {
	t.Helper()
	dir := t.TempDir()
	out, err := os.Create(filepath.Join(dir, layerEROFSFileName))
	if err != nil {
		t.Fatalf("os.Create: %v", err)
	}
	t.Cleanup(func() { out.Close() })
	size, digest, _, err := buildLayerEROFS(bytes.NewReader(tarBytes), out, dir)
	if err != nil {
		return nil, 0, "", err
	}
	fi, err := out.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != size {
		t.Fatalf("reported size %d, file size %d", size, fi.Size())
	}
	return out, size, digest, nil
}

// Two builds of the same tar must be byte-identical: a restored guest reads
// the image on its new node at the offsets it cached on the old one.
func TestBuildLayerEROFS_Reproducible(t *testing.T) {
	// Several xattrs on one entry: Go iterates PAXRecords in random order,
	// and the image digest is part of the restore contract.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, hdr := range []*tar.Header{
		{Name: "bin/", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "bin/sh", Typeflag: tar.TypeReg, Mode: 0o755, Size: 5000, PAXRecords: map[string]string{
			paxXattrPrefix + "user.a": "1", paxXattrPrefix + "user.b": "2", paxXattrPrefix + "user.c": "3",
			paxXattrPrefix + "security.capability": "cap", paxXattrPrefix + "user.d": "4",
		}},
		{Name: "etc/hosts", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1},
	} {
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader: %v", err)
		}
		if _, err := tw.Write(bytes.Repeat([]byte("s"), int(hdr.Size))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar.Close: %v", err)
	}
	b := buf.Bytes()
	f1, _, d1, err := buildEROFSDigest(t, b)
	if err != nil {
		t.Fatal(err)
	}
	f2, _, d2, err := buildEROFSDigest(t, b)
	if err != nil {
		t.Fatal(err)
	}
	if d1 != d2 || !strings.HasPrefix(d1, "sha256:") {
		t.Fatalf("digests differ: %s vs %s", d1, d2)
	}
	// The superblock's build time (offset 1024 + 24, u64 seconds then u32
	// nanoseconds) is pinned to zero, which is what makes the image
	// reproducible across runs rather than equal only within one second.
	var sbTime [12]byte
	if _, err := f1.ReadAt(sbTime[:], 1024+24); err != nil {
		t.Fatal(err)
	}
	if sbTime != [12]byte{} {
		t.Fatalf("superblock build time = %x, want zero", sbTime)
	}
	b1, _ := io.ReadAll(io.NewSectionReader(f1, 0, 1<<30))
	b2, _ := io.ReadAll(io.NewSectionReader(f2, 0, 1<<30))
	if !bytes.Equal(b1, b2) {
		t.Fatal("images differ byte for byte")
	}
	h := sha256.Sum256(b1)
	if want := "sha256:" + hex.EncodeToString(h[:]); d1 != want {
		t.Fatalf("digest %s, want %s", d1, want)
	}
}

// openEROFS builds the image and opens it with the go-erofs reader, so the
// tests check what a kernel would see rather than the writer's bookkeeping.
func openEROFS(t *testing.T, entries []tarEntry) fs.FS {
	t.Helper()
	f, _, err := buildEROFS(t, buildTar(t, entries))
	if err != nil {
		t.Fatalf("buildLayerEROFS: %v", err)
	}
	fsys, err := erofs.Open(f)
	if err != nil {
		t.Fatalf("erofs.Open: %v", err)
	}
	return fsys
}

func lstatEROFS(t *testing.T, fsys fs.FS, name string) fs.FileInfo {
	t.Helper()
	fi, err := fs.Lstat(fsys, name)
	if err != nil {
		t.Fatalf("Lstat(%q): %v", name, err)
	}
	return fi
}

func readEROFS(t *testing.T, fsys fs.FS, name string) string {
	t.Helper()
	b, err := fs.ReadFile(fsys, name)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", name, err)
	}
	return string(b)
}

func erofsStat(t *testing.T, fi fs.FileInfo) *erofs.Stat {
	t.Helper()
	st, ok := fi.Sys().(*erofs.Stat)
	if !ok {
		t.Fatalf("Sys() = %T, want *erofs.Stat", fi.Sys())
	}
	return st
}

func TestBuildLayerEROFS_RoundTrip(t *testing.T) {
	fsys := openEROFS(t, []tarEntry{
		{name: "bin/", typeflag: tar.TypeDir, mode: 0o555},
		{name: "bin/sh", typeflag: tar.TypeReg, mode: 0o755, body: strings.Repeat("x", 10000)},
		{name: "bin/ash", typeflag: tar.TypeLink, linkname: "bin/sh"},
		{name: "etc/hosts", typeflag: tar.TypeReg, mode: 0o644, body: "localhost\n"},
		{name: "lib", typeflag: tar.TypeSymlink, linkname: "usr/lib"},
		{name: "tmp/", typeflag: tar.TypeDir, mode: 0o1777},
	})

	if got := readEROFS(t, fsys, "bin/sh"); len(got) != 10000 {
		t.Errorf("bin/sh size = %d, want 10000", len(got))
	}
	if got := readEROFS(t, fsys, "etc/hosts"); got != "localhost\n" {
		t.Errorf("etc/hosts = %q", got)
	}
	if fi := lstatEROFS(t, fsys, "bin"); fi.Mode().Perm() != 0o555 || !fi.IsDir() {
		t.Errorf("bin mode = %v, want dr-xr-xr-x", fi.Mode())
	}
	// etc/ was never declared: fabricated like unpackLayer's MkdirAll.
	if fi := lstatEROFS(t, fsys, "etc"); fi.Mode().Perm() != 0o755 || !fi.IsDir() {
		t.Errorf("etc mode = %v, want drwxr-xr-x", fi.Mode())
	}
	if fi := lstatEROFS(t, fsys, "tmp"); fi.Mode()&fs.ModeSticky == 0 || fi.Mode().Perm() != 0o777 {
		t.Errorf("tmp mode = %v, want sticky 0777", fi.Mode())
	}
	if fi := lstatEROFS(t, fsys, "lib"); fi.Mode()&fs.ModeSymlink == 0 {
		t.Errorf("lib mode = %v, want symlink", fi.Mode())
	}
	if target, err := fs.ReadLink(fsys, "lib"); err != nil || target != "usr/lib" {
		t.Errorf("ReadLink(lib) = %q, %v; want usr/lib", target, err)
	}
	sh, ash := erofsStat(t, lstatEROFS(t, fsys, "bin/sh")), erofsStat(t, lstatEROFS(t, fsys, "bin/ash"))
	if sh.Ino != ash.Ino || sh.Nlink != 2 {
		t.Errorf("hardlink: sh ino=%d nlink=%d, ash ino=%d; want same inode with nlink 2", sh.Ino, sh.Nlink, ash.Ino)
	}
}

func TestBuildLayerEROFS_OwnershipTimesXattrs(t *testing.T) {
	mtime := time.Unix(1700000000, 0)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, hdr := range []*tar.Header{
		{Name: "app/", Typeflag: tar.TypeDir, Mode: 0o750, Uid: 1000, Gid: 1000, ModTime: mtime},
		{Name: "app/run", Typeflag: tar.TypeReg, Mode: 0o4755, Uid: 0, Gid: 65534, Size: 3, ModTime: mtime,
			PAXRecords: map[string]string{
				paxXattrPrefix + "security.capability": "cap",
				paxXattrPrefix + "user.note":           "n",
				// Image-supplied overlay markers must not reach the lower.
				paxXattrPrefix + "trusted.overlay.opaque": "y",
				paxXattrPrefix + "user.overlay.redirect":  "/etc",
				"SCHILY.fflags":                           "nodump",
			}},
	} {
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader: %v", err)
		}
		if hdr.Size > 0 {
			if _, err := tw.Write([]byte("run")); err != nil {
				t.Fatalf("Write: %v", err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar.Close: %v", err)
	}
	f, _, err := buildEROFS(t, buf.Bytes())
	if err != nil {
		t.Fatalf("buildLayerEROFS: %v", err)
	}
	fsys, err := erofs.Open(f)
	if err != nil {
		t.Fatalf("erofs.Open: %v", err)
	}

	app := lstatEROFS(t, fsys, "app")
	if st := erofsStat(t, app); st.UID != 1000 || st.GID != 1000 || app.Mode().Perm() != 0o750 {
		t.Errorf("app: uid=%d gid=%d mode=%v; want 1000:1000 0750", st.UID, st.GID, app.Mode())
	}
	if !app.ModTime().Equal(mtime) {
		t.Errorf("app mtime = %v, want %v", app.ModTime(), mtime)
	}
	run := lstatEROFS(t, fsys, "app/run")
	if run.Mode()&fs.ModeSetuid == 0 || run.Mode().Perm() != 0o755 {
		t.Errorf("app/run mode = %v, want setuid 0755", run.Mode())
	}
	st := erofsStat(t, run)
	if st.UID != 0 || st.GID != 65534 {
		t.Errorf("app/run owner = %d:%d, want 0:65534", st.UID, st.GID)
	}
	want := map[string]string{"security.capability": "cap", "user.note": "n"}
	if len(st.Xattrs) != len(want) {
		t.Errorf("app/run xattrs = %v, want %v", st.Xattrs, want)
	}
	for k, v := range want {
		if st.Xattrs[k] != v {
			t.Errorf("app/run xattr %q = %q, want %q", k, st.Xattrs[k], v)
		}
	}
}

func TestBuildLayerEROFS_WhiteoutsAndOpaque(t *testing.T) {
	fsys := openEROFS(t, []tarEntry{
		{name: "etc/", typeflag: tar.TypeDir},
		{name: "etc/.wh.old.conf", typeflag: tar.TypeReg},
		// Parent only exists in a lower layer: created on the way.
		{name: "var/cache/.wh.stale", typeflag: tar.TypeReg},
		{name: "opt/app/", typeflag: tar.TypeDir, mode: 0o700},
		{name: "opt/app/.wh..wh..opq", typeflag: tar.TypeReg},
		{name: "opt/app/new", typeflag: tar.TypeReg, body: "n"},
		// AUFS bookkeeping: dropped.
		{name: ".wh..wh.plnk/", typeflag: tar.TypeDir},
		{name: ".wh..wh.aufs", typeflag: tar.TypeReg},
	})

	for _, p := range []string{"etc/old.conf", "var/cache/stale"} {
		fi := lstatEROFS(t, fsys, p)
		if fi.Mode()&fs.ModeCharDevice == 0 {
			t.Errorf("%s mode = %v, want char device", p, fi.Mode())
		}
		if st := erofsStat(t, fi); st.Rdev != 0 {
			t.Errorf("%s rdev = %d, want 0", p, st.Rdev)
		}
	}
	app := lstatEROFS(t, fsys, "opt/app")
	// Both marker dialects: overlayfs reads trusted.* by default and user.*
	// under userxattr, on lowers as well as the upper.
	for _, attr := range []string{overlayOpaqueXattr, overlayUserOpaqueXattr} {
		if st := erofsStat(t, app); st.Xattrs[attr] != "y" {
			t.Errorf("opt/app xattrs = %v, want %s=y", st.Xattrs, attr)
		}
	}
	if app.Mode().Perm() != 0o700 {
		t.Errorf("opt/app mode = %v, want 0700 (opaque marker must not reset it)", app.Mode())
	}
	for _, p := range []string{".wh..wh.plnk", ".wh..wh.aufs", "etc/.wh.old.conf", "opt/app/.wh..wh..opq"} {
		if _, err := fs.Lstat(fsys, p); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Lstat(%q) err = %v, want not exist", p, err)
		}
	}
}

func TestBuildLayerEROFS_LaterEntryWins(t *testing.T) {
	fsys := openEROFS(t, []tarEntry{
		// Repeated directory entry (ko images do this): the later mode wins.
		{name: "ko-app/", typeflag: tar.TypeDir, mode: 0o755},
		{name: "ko-app/", typeflag: tar.TypeDir, mode: 0o555},
		// File replaced by a later file.
		{name: "f", typeflag: tar.TypeReg, body: "one"},
		{name: "f", typeflag: tar.TypeReg, body: "two"},
		// Directory redeclared as a symlink (var/run -> /run).
		{name: "var/run/", typeflag: tar.TypeDir},
		{name: "var/run/pid", typeflag: tar.TypeReg, body: "1"},
		{name: "var/run", typeflag: tar.TypeSymlink, linkname: "/run"},
		// Symlink replaced by a file: the file must not land at the
		// link's target.
		{name: "cfg", typeflag: tar.TypeSymlink, linkname: "real"},
		{name: "cfg", typeflag: tar.TypeReg, body: "c"},
		// Whiteout then a file at the same path.
		{name: ".wh.w", typeflag: tar.TypeReg},
		{name: "w", typeflag: tar.TypeReg, body: "w"},
		// Hardlink replaced by a file keeps the original intact.
		{name: "orig", typeflag: tar.TypeReg, body: "orig"},
		{name: "ln", typeflag: tar.TypeLink, linkname: "orig"},
		{name: "ln", typeflag: tar.TypeReg, body: "ln"},
	})

	if fi := lstatEROFS(t, fsys, "ko-app"); fi.Mode().Perm() != 0o555 {
		t.Errorf("ko-app mode = %v, want 0555", fi.Mode())
	}
	if got := readEROFS(t, fsys, "f"); got != "two" {
		t.Errorf("f = %q, want two", got)
	}
	if fi := lstatEROFS(t, fsys, "var/run"); fi.Mode()&fs.ModeSymlink == 0 {
		t.Errorf("var/run mode = %v, want symlink", fi.Mode())
	}
	if _, err := fs.Lstat(fsys, "var/run/pid"); err == nil {
		t.Error("var/run/pid still present under the replaced directory")
	}
	if fi := lstatEROFS(t, fsys, "cfg"); !fi.Mode().IsRegular() {
		t.Errorf("cfg mode = %v, want regular", fi.Mode())
	}
	if _, err := fs.Lstat(fsys, "real"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Lstat(real) err = %v, want not exist (file written through symlink)", err)
	}
	if fi := lstatEROFS(t, fsys, "w"); !fi.Mode().IsRegular() {
		t.Errorf("w mode = %v, want regular", fi.Mode())
	}
	if got := readEROFS(t, fsys, "orig"); got != "orig" {
		t.Errorf("orig = %q, want orig", got)
	}
	if got := readEROFS(t, fsys, "ln"); got != "ln" {
		t.Errorf("ln = %q, want ln", got)
	}
	if st := erofsStat(t, lstatEROFS(t, fsys, "orig")); st.Nlink != 1 {
		t.Errorf("orig nlink = %d, want 1", st.Nlink)
	}
}

func TestBuildLayerEROFS_ParentThroughSymlink(t *testing.T) {
	fsys := openEROFS(t, []tarEntry{
		{name: "usr/bin/", typeflag: tar.TypeDir},
		{name: "bin", typeflag: tar.TypeSymlink, linkname: "usr/bin"},
		{name: "bin/sh", typeflag: tar.TypeReg, body: "sh"},
		{name: "lib64", typeflag: tar.TypeSymlink, linkname: "/usr/lib64"},
		{name: "lib64/ld.so", typeflag: tar.TypeReg, body: "ld"},
		{name: "usr/local/", typeflag: tar.TypeDir},
		{name: "usr/local/up", typeflag: tar.TypeSymlink, linkname: "../bin"},
		{name: "usr/local/up/tool", typeflag: tar.TypeReg, body: "t"},
		{name: "bin/.wh.rm", typeflag: tar.TypeReg},
		{name: "bin/hl", typeflag: tar.TypeLink, linkname: "bin/sh"},
	})
	for p, want := range map[string]string{
		"usr/bin/sh":      "sh",
		"usr/lib64/ld.so": "ld",
		"usr/bin/tool":    "t",
		"usr/bin/hl":      "sh",
	} {
		if got := readEROFS(t, fsys, p); got != want {
			t.Errorf("%s = %q, want %q", p, got, want)
		}
	}
	if fi := lstatEROFS(t, fsys, "usr/bin/rm"); fi.Mode()&fs.ModeCharDevice == 0 {
		t.Errorf("usr/bin/rm mode = %v, want whiteout", fi.Mode())
	}
	if fi := lstatEROFS(t, fsys, "bin"); fi.Mode()&fs.ModeSymlink == 0 {
		t.Errorf("bin mode = %v, want symlink left in place", fi.Mode())
	}
}

func TestBuildLayerEROFS_SymlinkLoop(t *testing.T) {
	_, _, err := buildEROFS(t, buildTar(t, []tarEntry{
		{name: "a", typeflag: tar.TypeSymlink, linkname: "b"},
		{name: "b", typeflag: tar.TypeSymlink, linkname: "a"},
		{name: "a/x", typeflag: tar.TypeReg, body: "x"},
	}))
	if err == nil || !strings.Contains(err.Error(), "symbolic links") {
		t.Fatalf("err = %v, want symlink loop error", err)
	}
}

func TestBuildLayerEROFS_FileAsParent(t *testing.T) {
	_, _, err := buildEROFS(t, buildTar(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, body: "f"},
		{name: "f/child", typeflag: tar.TypeReg, body: "c"},
	}))
	if !errors.Is(err, erofs.ErrNotDirectory) {
		t.Fatalf("err = %v, want ErrNotDirectory", err)
	}
}

func TestBuildLayerEROFS_BlockSizeAndPadding(t *testing.T) {
	f, size, err := buildEROFS(t, buildTar(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, body: "f"},
	}))
	if err != nil {
		t.Fatalf("buildLayerEROFS: %v", err)
	}
	if size == 0 || size%erofsImageAlign != 0 {
		t.Errorf("image size = %d, want a non-zero multiple of %d", size, erofsImageAlign)
	}
	// Superblock at byte 1024: magic then checksum, feature_compat,
	// blkszbits (see erofs_super_block in the kernel).
	var sb [16]byte
	if _, err := f.ReadAt(sb[:], 1024); err != nil {
		t.Fatalf("ReadAt superblock: %v", err)
	}
	if magic := uint32(sb[0]) | uint32(sb[1])<<8 | uint32(sb[2])<<16 | uint32(sb[3])<<24; magic != 0xE0F5E1E2 {
		t.Errorf("superblock magic = %#x, want 0xe0f5e1e2", magic)
	}
	if blkszbits := sb[12]; 1<<blkszbits != erofsBlockSize {
		t.Errorf("block size = %d, want %d", 1<<blkszbits, erofsBlockSize)
	}
	// Small regular files must also use flat-plain layout (0) rather than
	// tail-packing so the guest kernel enables S_DAX on them.
	smallFS, err := erofs.Open(f)
	if err != nil {
		t.Fatalf("erofs.Open: %v", err)
	}
	if st := erofsStat(t, lstatEROFS(t, smallFS, "f")); st.InodeLayout != 0 {
		t.Errorf("small file InodeLayout = %d, want 0 (flat-plain for DAX)", st.InodeLayout)
	}
	// Large files must map directly (DAX needs a flat layout), which the
	// reader exposes as a single data range covering the whole file.
	big, _, err := buildEROFS(t, buildTar(t, []tarEntry{
		{name: "big", typeflag: tar.TypeReg, body: strings.Repeat("b", 3*erofsBlockSize+17)},
	}))
	if err != nil {
		t.Fatalf("buildLayerEROFS: %v", err)
	}
	fsys, err := erofs.Open(big)
	if err != nil {
		t.Fatalf("erofs.Open: %v", err)
	}
	if got := readEROFS(t, fsys, "big"); len(got) != 3*erofsBlockSize+17 {
		t.Errorf("big size = %d", len(got))
	}
}

func TestBuildLayerEROFS_Rejects(t *testing.T) {
	tests := []struct {
		name    string
		entries []tarEntry
		want    string
	}{
		{"traversal", []tarEntry{{name: "../x", typeflag: tar.TypeReg}}, "not a local path"},
		{"fifo", []tarEntry{{name: "p", typeflag: tar.TypeFifo}}, "unhandled tar entry typeflag"},
		{"whiteout of parent", []tarEntry{{name: "a/.wh.", typeflag: tar.TypeReg}}, "invalid tar entry"},
		{"whiteout of dot", []tarEntry{{name: "a/.wh..", typeflag: tar.TypeReg}}, "invalid tar entry"},
		{"whiteout of dotdot", []tarEntry{{name: "a/.wh...", typeflag: tar.TypeReg}}, "invalid tar entry"},
		{"hardlink escape", []tarEntry{{name: "l", typeflag: tar.TypeLink, linkname: "../etc/passwd"}}, "invalid hardlink target"},
		{"hardlink to missing", []tarEntry{{name: "l", typeflag: tar.TypeLink, linkname: "missing"}}, "while creating hardlink"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := buildEROFS(t, buildTar(t, tc.entries))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
	t.Run("truncated", func(t *testing.T) {
		b := buildTar(t, []tarEntry{{name: "f", typeflag: tar.TypeReg, body: strings.Repeat("x", 4096)}})
		_, _, err := buildEROFS(t, b[:len(b)/2])
		if err == nil {
			t.Fatal("truncated archive built without error")
		}
	})
}

// Device nodes are skipped, as the fs/ unpack does, and replace an earlier
// entry at their path.
func TestBuildLayerEROFS_SkipsDeviceNodes(t *testing.T) {
	fsys := openEROFS(t, []tarEntry{
		{name: "dev/", typeflag: tar.TypeDir},
		{name: "dev/null", typeflag: tar.TypeReg, body: "earlier"},
		{name: "dev/null", typeflag: tar.TypeChar, devmajor: 1, devminor: 3},
		{name: "dev/sda", typeflag: tar.TypeBlock, devmajor: 8, devminor: 0},
		{name: "etc/hosts", typeflag: tar.TypeReg, body: "h"},
	})
	for _, p := range []string{"dev/null", "dev/sda"} {
		if _, err := fs.Lstat(fsys, p); err == nil {
			t.Errorf("%s is in the image, want it skipped", p)
		}
	}
	if got := readEROFS(t, fsys, "etc/hosts"); got != "h" {
		t.Errorf("etc/hosts = %q, want h", got)
	}
}

// A whiteout applies to lower layers only: an entry earlier in the same
// layer survives it, and a same-layer directory hides the lowers instead.
func TestBuildLayerEROFS_WhiteoutKeepsSameLayerEntry(t *testing.T) {
	fsys := openEROFS(t, []tarEntry{
		{name: "w", typeflag: tar.TypeReg, body: "keep-me"},
		{name: ".wh.w", typeflag: tar.TypeReg},
		{name: "d/", typeflag: tar.TypeDir},
		{name: "d/x", typeflag: tar.TypeReg, body: "x"},
		{name: ".wh.d", typeflag: tar.TypeReg},
		// Implicitly created directory: the same rule applies.
		{name: "i/y", typeflag: tar.TypeReg, body: "y"},
		{name: ".wh.i", typeflag: tar.TypeReg},
		// Whiteout first, then the entry: the entry replaces the whiteout.
		{name: ".wh.z", typeflag: tar.TypeReg},
		{name: "z", typeflag: tar.TypeReg, body: "z"},
	})
	if got := readEROFS(t, fsys, "w"); got != "keep-me" {
		t.Errorf("w = %q, want the same-layer file kept", got)
	}
	for _, dir := range []string{"d", "i"} {
		if got := readEROFS(t, fsys, dir+"/"+map[string]string{"d": "x", "i": "y"}[dir]); got != map[string]string{"d": "x", "i": "y"}[dir] {
			t.Errorf("%s/ child = %q, want the same-layer directory kept", dir, got)
		}
		st := erofsStat(t, lstatEROFS(t, fsys, dir))
		if st.Xattrs[overlayOpaqueXattr] != "y" || st.Xattrs[overlayUserOpaqueXattr] != "y" {
			t.Errorf("%s xattrs = %v, want both opaque markers so lower %s stays deleted", dir, st.Xattrs, dir)
		}
	}
	if got := readEROFS(t, fsys, "z"); got != "z" {
		t.Errorf("z = %q, want the later entry over the whiteout", got)
	}
}
