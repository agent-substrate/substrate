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
	"archive/tar"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/internal/roottest"
)

// PrepareEROFSUpper writes the planned directories with their owner, mode,
// time, and xattrs, and leaves what is already there alone. The layers name
// the test's own uid/gid so the chown succeeds unprivileged.
func TestPrepareEROFSUpper(t *testing.T) {
	uid, gid := os.Getuid(), os.Getgid()
	spec := newTestEROFSSpec(t,
		[]*tar.Header{
			{Name: "etc/", Typeflag: tar.TypeDir, Mode: 0o2750, Uid: uid, Gid: gid,
				PAXRecords: map[string]string{paxXattrPrefix + "user.note": "n"}},
			{Name: "tmp/", Typeflag: tar.TypeDir, Mode: 0o1777, Uid: uid, Gid: gid},
			{Name: "home/", Typeflag: tar.TypeDir, Mode: 0o755, Uid: uid, Gid: gid},
		},
		[]*tar.Header{
			{Name: "tmp/x", Typeflag: tar.TypeReg, Size: 1},
		},
	)
	upper := t.TempDir()
	if err := os.Mkdir(filepath.Join(upper, "home"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := PrepareEROFSUpper(upper, spec, []string{"/etc", "home", "run/app"}, UpperUserXattr); err != nil {
		t.Fatalf("PrepareEROFSUpper: %v", err)
	}

	etc, err := os.Lstat(filepath.Join(upper, "etc"))
	if err != nil {
		t.Fatal(err)
	}
	if etc.Mode() != fs.ModeDir|fs.ModeSetgid|0o750 || !etc.ModTime().Equal(testLayerTime) {
		t.Errorf("etc = %v at %v, want setgid 0750 at %v", etc.Mode(), etc.ModTime(), testLayerTime)
	}
	if st := etc.Sys().(*syscall.Stat_t); int(st.Uid) != uid || int(st.Gid) != gid {
		t.Errorf("etc owner = %d:%d, want %d:%d", st.Uid, st.Gid, uid, gid)
	}
	note := make([]byte, 8)
	if n, err := unix.Lgetxattr(filepath.Join(upper, "etc"), "user.note", note); err != nil || string(note[:n]) != "n" {
		t.Errorf("etc user.note = %q, %v; want n", note[:max(n, 0)], err)
	}
	tmp, err := os.Lstat(filepath.Join(upper, "tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if tmp.Mode() != fs.ModeDir|fs.ModeSticky|0o777 {
		t.Errorf("tmp = %v, want sticky 0777 repaired from the base layer", tmp.Mode())
	}
	// home existed: untouched (its 0700 stays, not the layer's 0755).
	if home, _ := os.Lstat(filepath.Join(upper, "home")); home.Mode().Perm() != 0o700 {
		t.Errorf("home = %v, want the pre-existing 0700", home.Mode())
	}
	for _, p := range []string{"run", "run/app"} {
		fi, err := os.Lstat(filepath.Join(upper, p))
		if err != nil || fi.Mode() != fs.ModeDir|0o755 {
			t.Errorf("%s = %v, %v; want a fabricated 0755 directory", p, fi, err)
		}
	}
}

// A non-directory already at a planned path is an error, not replaced.
func TestPrepareEROFSUpper_RefusesNonDir(t *testing.T) {
	spec := newTestEROFSSpec(t, []*tar.Header{
		{Name: "etc/", Typeflag: tar.TypeDir, Mode: 0o755, Uid: os.Getuid(), Gid: os.Getgid()},
	})
	upper := t.TempDir()
	if err := os.WriteFile(filepath.Join(upper, "etc"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	err := PrepareEROFSUpper(upper, spec, []string{"etc"}, UpperUserXattr)
	if err == nil || !strings.Contains(err.Error(), "non-directory") {
		t.Errorf("err = %v, want a non-directory refusal", err)
	}
}

// A layer whose EROFS image is missing fails the preparation: the spec
// promised one, and the guest cannot run without it.
func TestPrepareEROFSUpper_MissingImage(t *testing.T) {
	spec := newTestEROFSSpec(t, []*tar.Header{{Name: "etc/", Typeflag: tar.TypeDir}})
	if err := os.Remove(LayerEROFSPath(spec.Layers[0])); err != nil {
		t.Fatal(err)
	}
	if err := PrepareEROFSUpper(t.TempDir(), spec, []string{"etc"}, UpperUserXattr); err == nil || !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("err = %v, want ErrNotExist", err)
	}
}

// A ROOTFS restore puts back an upper in which the actor may have deleted a
// directory the plan touches. A repair-only path keeps its whiteout; a needed
// path becomes a directory that hides the lowers, in the mount's dialect.
func TestPrepareEROFSUpper_OverWhiteouts(t *testing.T) {
	roottest.Require(t, "mknod overlay whiteouts")
	for _, tc := range []struct {
		dialect UpperDialect
		opaque  string
	}{{UpperUserXattr, overlayUserOpaqueXattr}, {UpperTrustedXattr, overlayOpaqueXattr}} {
		t.Run(tc.opaque, func(t *testing.T) {
			spec := newTestEROFSSpec(t,
				[]*tar.Header{
					{Name: "tmp/", Typeflag: tar.TypeDir, Mode: 0o1777},
					{Name: "etc/", Typeflag: tar.TypeDir, Mode: 0o755},
					{Name: "etc/passwd", Typeflag: tar.TypeReg, Size: 1},
					{Name: "var/", Typeflag: tar.TypeDir, Mode: 0o755},
					{Name: "var/lib/", Typeflag: tar.TypeDir, Mode: 0o700},
				},
				// tmp's implicit parent here shadows its 1777 (a repair);
				// var/lib/x makes var and var/lib implicit too.
				[]*tar.Header{{Name: "tmp/x", Typeflag: tar.TypeReg, Size: 1}, {Name: "var/lib/x", Typeflag: tar.TypeReg, Size: 1}},
			)
			upper := t.TempDir()
			for _, p := range []string{"tmp", "etc", "var"} { // what `rm -rf /tmp /etc /var` leaves
				if err := unix.Mknod(filepath.Join(upper, p), unix.S_IFCHR, 0); err != nil {
					t.Fatal(err)
				}
			}
			if err := PrepareEROFSUpper(upper, spec, []string{"etc"}, tc.dialect); err != nil {
				t.Fatalf("PrepareEROFSUpper: %v", err)
			}
			for _, p := range []string{"tmp", "var"} {
				var st unix.Stat_t
				if err := unix.Lstat(filepath.Join(upper, p), &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFCHR || st.Rdev != 0 {
					t.Errorf("%s = mode %o rdev %d (%v), want the actor's whiteout kept", p, st.Mode, st.Rdev, err)
				}
			}
			// var is still the whiteout (a char device), so nothing can
			// have been created under it: the lookup fails with ENOTDIR.
			if _, err := os.Lstat(filepath.Join(upper, "var/lib")); !errors.Is(err, unix.ENOTDIR) {
				t.Errorf("var/lib under the deleted var: %v, want ENOTDIR (nothing created)", err)
			}
			fi, err := os.Lstat(filepath.Join(upper, "etc"))
			if err != nil || !fi.IsDir() {
				t.Fatalf("etc = %v (%v), want a directory", fi, err)
			}
			buf := make([]byte, 4)
			n, err := unix.Lgetxattr(filepath.Join(upper, "etc"), tc.opaque, buf)
			if err != nil || string(buf[:n]) != "y" {
				t.Errorf("etc %s = %q (%v), want y so the deleted /etc stays hidden", tc.opaque, buf[:max(n, 0)], err)
			}
			other := overlayOpaqueXattr
			if tc.dialect == UpperTrustedXattr {
				other = overlayUserOpaqueXattr
			}
			if _, err := unix.Lgetxattr(filepath.Join(upper, "etc"), other, buf); err == nil {
				t.Errorf("etc also carries %s, want only the mount's dialect", other)
			}
		})
	}
}
