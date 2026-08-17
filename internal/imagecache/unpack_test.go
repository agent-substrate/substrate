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
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

type tarEntry struct {
	name     string
	typeflag byte
	mode     int64
	body     string
	linkname string
}

func defaultMode(typeflag byte) int64 {
	switch typeflag {
	case tar.TypeDir:
		return 0o755
	case tar.TypeSymlink:
		return 0o777
	default:
		return 0o644
	}
}

func buildTar(t *testing.T, entries []tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		mode := e.mode
		if mode == 0 {
			mode = defaultMode(e.typeflag)
		}
		hdr := &tar.Header{
			Name:     e.name,
			Typeflag: e.typeflag,
			Mode:     mode,
			Size:     int64(len(e.body)),
			Linkname: e.linkname,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("tar.WriteHeader(%+v): %v", hdr, err)
		}
		if e.body != "" {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatalf("tar.Write(%q): %v", e.name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar.Close: %v", err)
	}
	return buf.Bytes()
}

func unpackInto(t *testing.T, dir string, tarBytes []byte) (*whiteoutSet, error) {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("os.OpenRoot(%q): %v", dir, err)
	}
	defer root.Close()
	return unpackLayer(context.Background(), bytes.NewReader(tarBytes), root)
}

func runUnpack(t *testing.T, entries []tarEntry) (string, *whiteoutSet, error) {
	t.Helper()
	dir := t.TempDir()
	wh, err := unpackInto(t, dir, buildTar(t, entries))
	return dir, wh, err
}

func TestValidateTarName(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantClean string
		wantSkip  bool
		wantErr   bool
	}{
		{name: "regular file", input: "etc/passwd", wantClean: "etc/passwd"},
		{name: "current dir", input: ".", wantSkip: true},
		{name: "empty", input: "", wantSkip: true},
		{name: "trailing slash", input: "etc/", wantClean: "etc"},
		{name: "absolute path", input: "/etc/passwd", wantClean: "etc/passwd"},
		{name: "double slash absolute", input: "//etc/passwd", wantClean: "etc/passwd"},
		{name: "parent escape", input: "../etc/passwd", wantErr: true},
		{name: "parent only", input: "..", wantErr: true},
		{name: "embedded escape", input: "a/../../escape", wantErr: true},
		{name: "ok with dot segments", input: "./a/./b", wantClean: "a/b"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotClean, gotSkip, err := validateTarName(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateTarName(%q) err = %v, wantErr %v", tc.input, err, tc.wantErr)
			}
			if err != nil {
				return
			}
			if gotSkip != tc.wantSkip {
				t.Errorf("skip = %v, want %v", gotSkip, tc.wantSkip)
			}
			if gotClean != tc.wantClean {
				t.Errorf("clean = %q, want %q", gotClean, tc.wantClean)
			}
		})
	}
}

func TestUnpackLayer_HappyPath(t *testing.T) {
	entries := []tarEntry{
		{name: ".", typeflag: tar.TypeDir},
		{name: "etc/", typeflag: tar.TypeDir},
		{name: "etc/hostname", typeflag: tar.TypeReg, body: "demo\n"},
		{name: "bin/", typeflag: tar.TypeDir},
		{name: "bin/sh", typeflag: tar.TypeReg, mode: 0o755, body: "#!/sh\n"},
		{name: "bin/bash", typeflag: tar.TypeLink, linkname: "bin/sh"},
		{name: "etc/host-link", typeflag: tar.TypeSymlink, linkname: "hostname"},
	}
	dir, wh, err := runUnpack(t, entries)
	if err != nil {
		t.Fatalf("unpackLayer: %v", err)
	}
	if len(wh.Whiteouts) != 0 || len(wh.Opaques) != 0 {
		t.Errorf("whiteouts recorded for a layer with none: %+v", wh)
	}

	if got, err := os.ReadFile(filepath.Join(dir, "etc/hostname")); err != nil {
		t.Errorf("read etc/hostname: %v", err)
	} else if string(got) != "demo\n" {
		t.Errorf("etc/hostname = %q, want %q", got, "demo\n")
	}

	if target, err := os.Readlink(filepath.Join(dir, "etc/host-link")); err != nil {
		t.Errorf("readlink etc/host-link: %v", err)
	} else if target != "hostname" {
		t.Errorf("symlink target = %q, want %q", target, "hostname")
	}

	srcInfo, err := os.Stat(filepath.Join(dir, "bin/sh"))
	if err != nil {
		t.Fatalf("stat bin/sh: %v", err)
	}
	dstInfo, err := os.Stat(filepath.Join(dir, "bin/bash"))
	if err != nil {
		t.Fatalf("stat bin/bash: %v", err)
	}
	if !os.SameFile(srcInfo, dstInfo) {
		t.Errorf("bin/bash is not a hardlink to bin/sh")
	}
}

// Whiteout entries must be recorded — never written into the layer tree,
// where a literal .wh. file would leak into the composed rootfs.
func TestUnpackLayer_Whiteouts(t *testing.T) {
	entries := []tarEntry{
		{name: "a/", typeflag: tar.TypeDir},
		{name: "a/.wh.deleted", typeflag: tar.TypeReg},
		{name: "b/", typeflag: tar.TypeDir},
		{name: "b/.wh..wh..opq", typeflag: tar.TypeReg},
		{name: ".wh.toplevel", typeflag: tar.TypeReg},
		// AUFS bookkeeping entries carry no overlayfs meaning; silently skipped.
		{name: ".wh..wh.plnk", typeflag: tar.TypeReg},
		{name: "a/kept", typeflag: tar.TypeReg, body: "kept"},
	}
	dir, wh, err := runUnpack(t, entries)
	if err != nil {
		t.Fatalf("unpackLayer: %v", err)
	}

	if want := []string{"a/deleted", "toplevel"}; !slices.Equal(wh.Whiteouts, want) {
		t.Errorf("whiteouts = %v, want %v", wh.Whiteouts, want)
	}
	if want := []string{"b"}; !slices.Equal(wh.Opaques, want) {
		t.Errorf("opaques = %v, want %v", wh.Opaques, want)
	}

	// No .wh. artifact may exist in the tree.
	for _, p := range []string{"a/.wh.deleted", "b/.wh..wh..opq", ".wh.toplevel", ".wh..wh.plnk"} {
		if _, err := os.Lstat(filepath.Join(dir, p)); err == nil {
			t.Errorf("whiteout entry %q was written into the layer tree", p)
		}
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "a/kept")); string(got) != "kept" {
		t.Errorf("a/kept = %q, want %q", got, "kept")
	}
}

// A layer tar routinely omits parent-directory entries when the parent
// exists only in a lower layer (the flattened-extract path never saw this;
// per-layer unpack must create the parents). Regression test for the
// "etc/nsswitch.conf: no such file or directory" failure on real images.
func TestUnpackLayer_MissingParentDirs(t *testing.T) {
	entries := []tarEntry{
		{name: "etc/nsswitch.conf", typeflag: tar.TypeReg, body: "hosts: files dns"},
		{name: "a/b/c/deep", typeflag: tar.TypeReg, body: "deep"},
		{name: "usr/share/doc/", typeflag: tar.TypeDir},
		{name: "lnk/target", typeflag: tar.TypeReg, body: "t"},
		{name: "other/dir/sym", typeflag: tar.TypeSymlink, linkname: "../../lnk/target"},
		{name: "hard/link", typeflag: tar.TypeLink, linkname: "lnk/target"},
	}
	dir, _, err := runUnpack(t, entries)
	if err != nil {
		t.Fatalf("unpackLayer: %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "etc/nsswitch.conf")); string(got) != "hosts: files dns" {
		t.Errorf("etc/nsswitch.conf = %q, want %q", got, "hosts: files dns")
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "a/b/c/deep")); string(got) != "deep" {
		t.Errorf("a/b/c/deep = %q, want %q", got, "deep")
	}
	if fi, err := os.Stat(filepath.Join(dir, "usr/share/doc")); err != nil || !fi.IsDir() {
		t.Errorf("usr/share/doc not created: fi=%v err=%v", fi, err)
	}
	if got, err := os.Readlink(filepath.Join(dir, "other/dir/sym")); err != nil || got != "../../lnk/target" {
		t.Errorf("other/dir/sym = %q (%v), want ../../lnk/target", got, err)
	}
	srcInfo, err := os.Stat(filepath.Join(dir, "lnk/target"))
	if err != nil {
		t.Fatalf("stat lnk/target: %v", err)
	}
	if dstInfo, err := os.Stat(filepath.Join(dir, "hard/link")); err != nil || !os.SameFile(srcInfo, dstInfo) {
		t.Errorf("hard/link is not a hardlink to lnk/target (err=%v)", err)
	}
}

// Implicitly-created parent dirs must be recorded (their attrs are
// fabricated and can shadow lower-layer metadata in the merged view);
// declared dirs must not be, and candidates replaced by non-dirs drop out.
func TestUnpackLayer_ImplicitDirRecording(t *testing.T) {
	entries := []tarEntry{
		{name: "declared/", typeflag: tar.TypeDir},
		{name: "declared/file", typeflag: tar.TypeReg, body: "x"},
		{name: "etc/nsswitch.conf", typeflag: tar.TypeReg, body: "hosts: files"},
		{name: "a/b/c/deep", typeflag: tar.TypeReg, body: "d"},
		// Parent exists only for a whiteout marker: created at finalize, but
		// must be recorded now.
		{name: "gone/.wh.victim", typeflag: tar.TypeReg},
		// Implicitly created, then declared later: the declaration wins.
		{name: "late/child", typeflag: tar.TypeReg, body: "c"},
		{name: "late/", typeflag: tar.TypeDir, mode: 0o700},
		// Implicit parent whose subtree is later replaced by a file:
		// no longer a dir, must not be recorded.
		{name: "swap/inner", typeflag: tar.TypeReg, body: "i"},
		{name: "swap", typeflag: tar.TypeReg, body: "now a file"},
	}
	_, wh, err := runUnpack(t, entries)
	if err != nil {
		t.Fatalf("unpackLayer: %v", err)
	}
	want := []string{"a", "a/b", "a/b/c", "etc", "gone"}
	if !slices.Equal(wh.ImplicitDirs, want) {
		t.Errorf("ImplicitDirs = %v, want %v", wh.ImplicitDirs, want)
	}
}

func TestUnpackLayer_LaterEntryWins(t *testing.T) {
	t.Run("dir then symlink", func(t *testing.T) {
		entries := []tarEntry{
			{name: "var/", typeflag: tar.TypeDir},
			{name: "var/run/", typeflag: tar.TypeDir},
			{name: "run/", typeflag: tar.TypeDir},
			{name: "run/sock", typeflag: tar.TypeReg, body: "sock"},
			{name: "var/run", typeflag: tar.TypeSymlink, linkname: "../run"},
		}
		dir, _, err := runUnpack(t, entries)
		if err != nil {
			t.Fatalf("unpackLayer: %v", err)
		}
		fi, err := os.Lstat(filepath.Join(dir, "var/run"))
		if err != nil {
			t.Fatalf("lstat var/run: %v", err)
		}
		if fi.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("var/run not a symlink, mode = %v", fi.Mode())
		}
		if got, _ := os.Readlink(filepath.Join(dir, "var/run")); got != "../run" {
			t.Errorf("symlink target = %q, want %q", got, "../run")
		}
	})

	t.Run("file overwrite", func(t *testing.T) {
		entries := []tarEntry{
			{name: "etc/", typeflag: tar.TypeDir},
			{name: "etc/conf", typeflag: tar.TypeReg, body: "v1"},
			{name: "etc/conf", typeflag: tar.TypeReg, body: "v2"},
		}
		dir, _, err := runUnpack(t, entries)
		if err != nil {
			t.Fatalf("unpackLayer: %v", err)
		}
		if got, _ := os.ReadFile(filepath.Join(dir, "etc/conf")); string(got) != "v2" {
			t.Errorf("etc/conf = %q, want %q", got, "v2")
		}
	})

	t.Run("symlink retargeted", func(t *testing.T) {
		entries := []tarEntry{
			{name: "etc/", typeflag: tar.TypeDir},
			{name: "etc/x", typeflag: tar.TypeReg, body: "x"},
			{name: "etc/y", typeflag: tar.TypeReg, body: "y"},
			{name: "etc/link", typeflag: tar.TypeSymlink, linkname: "x"},
			{name: "etc/link", typeflag: tar.TypeSymlink, linkname: "y"},
		}
		dir, _, err := runUnpack(t, entries)
		if err != nil {
			t.Fatalf("unpackLayer: %v", err)
		}
		if got, _ := os.Readlink(filepath.Join(dir, "etc/link")); got != "y" {
			t.Errorf("symlink target = %q, want %q", got, "y")
		}
	})

	t.Run("repeated dir entry tolerated", func(t *testing.T) {
		entries := []tarEntry{
			{name: "etc/", typeflag: tar.TypeDir},
			{name: "etc/", typeflag: tar.TypeDir},
		}
		if _, _, err := runUnpack(t, entries); err != nil {
			t.Errorf("unpackLayer: %v", err)
		}
	})

	t.Run("identical symlink redeclaration is a no-op", func(t *testing.T) {
		entries := []tarEntry{
			{name: "etc/", typeflag: tar.TypeDir},
			{name: "etc/x", typeflag: tar.TypeReg, body: "x"},
			{name: "etc/link", typeflag: tar.TypeSymlink, linkname: "x"},
			{name: "etc/link", typeflag: tar.TypeSymlink, linkname: "x"},
		}
		dir, _, err := runUnpack(t, entries)
		if err != nil {
			t.Fatalf("unpackLayer: %v", err)
		}
		if got, _ := os.Readlink(filepath.Join(dir, "etc/link")); got != "x" {
			t.Errorf("symlink target = %q, want %q", got, "x")
		}
	})

	t.Run("symlink overwritten by file", func(t *testing.T) {
		entries := []tarEntry{
			{name: "etc/", typeflag: tar.TypeDir},
			{name: "etc/x", typeflag: tar.TypeReg, body: "original"},
			{name: "etc/link", typeflag: tar.TypeSymlink, linkname: "x"},
			{name: "etc/link", typeflag: tar.TypeReg, body: "replacement"},
		}
		dir, _, err := runUnpack(t, entries)
		if err != nil {
			t.Fatalf("unpackLayer: %v", err)
		}
		fi, err := os.Lstat(filepath.Join(dir, "etc/link"))
		if err != nil {
			t.Fatalf("lstat etc/link: %v", err)
		}
		if fi.Mode().IsRegular() {
			got, err := os.ReadFile(filepath.Join(dir, "etc/link"))
			if err != nil {
				t.Fatalf("read etc/link: %v", err)
			}
			if string(got) != "replacement" {
				t.Errorf("etc/link content = %q, want %q", got, "replacement")
			}
		} else {
			t.Errorf("etc/link mode is not regular file: %v", fi.Mode())
		}
		// Also verify etc/x was NOT overwritten
		gotX, err := os.ReadFile(filepath.Join(dir, "etc/x"))
		if err != nil {
			t.Fatalf("read etc/x: %v", err)
		}
		if string(gotX) != "original" {
			t.Errorf("etc/x content was overwritten to %q", gotX)
		}
	})

	t.Run("file overwritten by symlink", func(t *testing.T) {
		entries := []tarEntry{
			{name: "etc/", typeflag: tar.TypeDir},
			{name: "etc/link", typeflag: tar.TypeReg, body: "original-file"},
			{name: "etc/link", typeflag: tar.TypeSymlink, linkname: "target-doesnt-exist"},
		}
		dir, _, err := runUnpack(t, entries)
		if err != nil {
			t.Fatalf("unpackLayer: %v", err)
		}
		fi, err := os.Lstat(filepath.Join(dir, "etc/link"))
		if err != nil {
			t.Fatalf("lstat etc/link: %v", err)
		}
		if fi.Mode()&os.ModeSymlink == 0 {
			t.Errorf("etc/link mode is not a symlink: %v", fi.Mode())
		}
		got, err := os.Readlink(filepath.Join(dir, "etc/link"))
		if err != nil {
			t.Fatalf("readlink etc/link: %v", err)
		}
		if got != "target-doesnt-exist" {
			t.Errorf("etc/link target = %q, want %q", got, "target-doesnt-exist")
		}
	})

	t.Run("hardlink overwritten by file", func(t *testing.T) {
		entries := []tarEntry{
			{name: "bin/", typeflag: tar.TypeDir},
			{name: "bin/sh", typeflag: tar.TypeReg, body: "sh-original"},
			{name: "bin/bash", typeflag: tar.TypeLink, linkname: "bin/sh"},
			{name: "bin/bash", typeflag: tar.TypeReg, body: "bash-new"},
		}
		dir, _, err := runUnpack(t, entries)
		if err != nil {
			t.Fatalf("unpackLayer: %v", err)
		}
		gotBash, err := os.ReadFile(filepath.Join(dir, "bin/bash"))
		if err != nil {
			t.Fatalf("read bin/bash: %v", err)
		}
		if string(gotBash) != "bash-new" {
			t.Errorf("bin/bash content = %q, want %q", gotBash, "bash-new")
		}
		// Verify bin/sh was NOT modified!
		gotSh, err := os.ReadFile(filepath.Join(dir, "bin/sh"))
		if err != nil {
			t.Fatalf("read bin/sh: %v", err)
		}
		if string(gotSh) != "sh-original" {
			t.Errorf("bin/sh content was overwritten to %q (hardlink was not unlinked)", gotSh)
		}
	})
}

// A read-only directory in the image (e.g. ko ships /ko-app as 0555) must still
// get its child written AND keep the image's mode, so atelet can unpack arbitrary
// actor images as plain root without CAP_DAC_OVERRIDE. RemoveAllWritable must
// then still be able to delete the restored read-only tree. (Meaningful as a
// non-root test run; as root the dir-permission checks are bypassed.)
func TestUnpackLayer_ReadOnlyDir(t *testing.T) {
	entries := []tarEntry{
		{name: "ko-app", typeflag: tar.TypeDir, mode: 0o555},
		{name: "ko-app/counter", typeflag: tar.TypeReg, mode: 0o755, body: "bin"},
	}
	dir, _, err := runUnpack(t, entries)
	if err != nil {
		t.Fatalf("unpack into read-only dir: %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "ko-app/counter")); string(got) != "bin" {
		t.Errorf("ko-app/counter = %q, want %q", got, "bin")
	}
	info, err := os.Stat(filepath.Join(dir, "ko-app"))
	if err != nil {
		t.Fatalf("stat ko-app: %v", err)
	}
	if info.Mode().Perm() != 0o555 {
		t.Errorf("ko-app mode = %v, want the image's 0555 preserved", info.Mode().Perm())
	}
	// atelet must be able to delete the restored read-only tree (this also lets
	// t.TempDir's cleanup succeed, which plain os.RemoveAll could not on 0555).
	if err := RemoveAllWritable(filepath.Join(dir, "ko-app")); err != nil {
		t.Errorf("RemoveAllWritable on restored read-only dir: %v", err)
	}
}

func TestUnpackLayer_PathTraversal(t *testing.T) {
	tests := []struct {
		name  string
		entry tarEntry
	}{
		{name: "parent prefix", entry: tarEntry{name: "../escape", typeflag: tar.TypeReg, body: "x"}},
		{name: "embedded parent", entry: tarEntry{name: "a/b/../../../escape", typeflag: tar.TypeReg, body: "x"}},
		{name: "parent only", entry: tarEntry{name: "..", typeflag: tar.TypeReg, body: "x"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runUnpack(t, []tarEntry{tc.entry})
			if err == nil {
				t.Fatalf("unpackLayer(%q) succeeded, want error", tc.entry.name)
			}
			if !strings.Contains(err.Error(), "invalid tar entry") {
				t.Errorf("error = %q, want it to mention 'invalid tar entry'", err.Error())
			}
		})
	}
}

func TestUnpackLayer_SymlinkEscape(t *testing.T) {
	// CVE-2024-24579 / CVE-2020-27833 pattern: a tar declares a symlink
	// pointing outside the rootfs, then a later entry writes through it.
	parent := t.TempDir()
	rootfsDir := filepath.Join(parent, "rootfs")
	if err := os.Mkdir(rootfsDir, 0o755); err != nil {
		t.Fatalf("mkdir rootfs: %v", err)
	}
	hostDir := filepath.Join(parent, "host")
	if err := os.Mkdir(hostDir, 0o755); err != nil {
		t.Fatalf("mkdir host: %v", err)
	}
	hostFile := filepath.Join(hostDir, "passwd")
	if err := os.WriteFile(hostFile, []byte("original"), 0o644); err != nil {
		t.Fatalf("write host file: %v", err)
	}

	entries := []tarEntry{
		{name: "etc", typeflag: tar.TypeSymlink, linkname: hostDir},
		{name: "etc/passwd", typeflag: tar.TypeReg, body: "OWNED"},
	}
	if _, err := unpackInto(t, rootfsDir, buildTar(t, entries)); err == nil {
		t.Fatalf("unpackLayer succeeded; expected escape via symlink to be refused")
	}

	got, err := os.ReadFile(hostFile)
	if err != nil {
		t.Fatalf("read host file: %v", err)
	}
	if string(got) != "original" {
		t.Errorf("host file modified to %q -- symlink escape was NOT prevented", got)
	}
}

func TestUnpackLayer_HardlinkEscape(t *testing.T) {
	tests := []struct {
		name  string
		entry tarEntry
	}{
		{name: "parent target", entry: tarEntry{name: "etc/passwd", typeflag: tar.TypeLink, linkname: "../host/passwd"}},
		{name: "embedded escape target", entry: tarEntry{name: "etc/passwd", typeflag: tar.TypeLink, linkname: "a/../../host"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runUnpack(t, []tarEntry{tc.entry})
			if err == nil {
				t.Fatalf("unpackLayer succeeded, want hardlink escape refused")
			}
			if !strings.Contains(err.Error(), "invalid hardlink target") {
				t.Errorf("error = %q, want it to mention 'invalid hardlink target'", err.Error())
			}
		})
	}
}

func TestUnpackLayer_RejectSpecialFiles(t *testing.T) {
	tests := []struct {
		name     string
		typeflag byte
	}{
		{name: "char device", typeflag: tar.TypeChar},
		{name: "block device", typeflag: tar.TypeBlock},
		{name: "fifo", typeflag: tar.TypeFifo},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runUnpack(t, []tarEntry{{name: "weird", typeflag: tc.typeflag}})
			if err == nil {
				t.Fatalf("unpackLayer succeeded, want unhandled-typeflag error")
			}
			if !strings.Contains(err.Error(), "unhandled tar entry typeflag") {
				t.Errorf("error = %q, want it to mention 'unhandled tar entry typeflag'", err.Error())
			}
		})
	}
}

func TestUnpackLayer_TruncatedArchive(t *testing.T) {
	full := buildTar(t, []tarEntry{
		{name: "ok", typeflag: tar.TypeReg, body: "hello"},
	})
	if len(full) < 64 {
		t.Fatalf("buildTar produced suspiciously small output: %d bytes", len(full))
	}
	truncated := full[:len(full)-64]

	dir := t.TempDir()
	_, err := unpackInto(t, dir, truncated)
	if err == nil {
		t.Fatalf("unpackLayer on truncated archive succeeded; want error")
	}
	if !strings.Contains(err.Error(), "in tarReader.Next") &&
		!strings.Contains(err.Error(), "unexpected EOF") {
		t.Errorf("error = %v, want it to surface the underlying tar/copy error", err)
	}
}
