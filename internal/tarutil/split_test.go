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

package tarutil

import (
	"archive/tar"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Splitting an archive of several directories and extracting each part
// reproduces each directory: contents, names only PAX can hold, symlinks,
// hardlinks, user xattrs, and the directory's own mode and time.
func TestSplitRoundTrip(t *testing.T) {
	src := t.TempDir()
	long := strings.Repeat("d", 60) + "/" + strings.Repeat("f", 200)
	for rel, body := range map[string]string{
		"a/file.txt":  "a",
		"a/" + long:   "long",
		"b/sub/b.txt": "b",
	} {
		p := filepath.Join(src, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("file.txt", filepath.Join(src, "a/link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(src, "a/file.txt"), filepath.Join(src, "a/hard")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(src, "a"), 0o750); err != nil {
		t.Fatal(err)
	}
	mtime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(src, "b"), mtime, mtime); err != nil {
		t.Fatal(err)
	}
	xattrs := unix.Lsetxattr(filepath.Join(src, "b/sub/b.txt"), "user.custom", []byte("val"), 0) == nil
	work := t.TempDir()
	whole := filepath.Join(work, "whole.tar")
	if err := Create(t.Context(), whole, src); err != nil {
		t.Fatal(err)
	}

	dsts := map[string]string{"a": filepath.Join(work, "a.tar"), "b": filepath.Join(work, "b.tar"), "c": filepath.Join(work, "c.tar")}
	if err := Split(whole, dsts); err != nil {
		t.Fatalf("Split: %v", err)
	}
	if _, err := os.Stat(whole); err != nil {
		t.Errorf("Split removed its input: %v", err)
	}
	if _, err := os.Stat(dsts["c"]); !os.IsNotExist(err) {
		t.Errorf("c.tar: Stat = %v; want no archive for a directory without entries", err)
	}

	dst := t.TempDir()
	for _, d := range []string{"a", "b"} {
		if err := os.Mkdir(filepath.Join(dst, d), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := Extract(dsts[d], filepath.Join(dst, d)); err != nil {
			t.Fatalf("Extract %s: %v", d, err)
		}
	}
	for rel, want := range map[string]string{"a/file.txt": "a", "a/" + long: "long", "b/sub/b.txt": "b", "a/link": "a"} {
		if got, err := os.ReadFile(filepath.Join(dst, rel)); err != nil || string(got) != want {
			t.Errorf("%s = %q, %v; want %q", rel, got, err, want)
		}
	}
	if target, err := os.Readlink(filepath.Join(dst, "a/link")); err != nil || target != "file.txt" {
		t.Errorf("a/link -> %q, %v; want file.txt", target, err)
	}
	fa, errA := os.Stat(filepath.Join(dst, "a/file.txt"))
	fh, errH := os.Stat(filepath.Join(dst, "a/hard"))
	if errA != nil || errH != nil || !os.SameFile(fa, fh) {
		t.Errorf("a/hard is not a hardlink of a/file.txt (%v, %v)", errA, errH)
	}
	if fi, err := os.Stat(filepath.Join(dst, "a")); err != nil || fi.Mode().Perm() != 0o750 {
		t.Errorf("a: Stat = %v, %v; want mode 0750", fi, err)
	}
	if fi, err := os.Stat(filepath.Join(dst, "b")); err != nil || !fi.ModTime().Equal(mtime) {
		t.Errorf("b: Stat = %v, %v; want mtime %v", fi, err, mtime)
	}
	if xattrs {
		buf := make([]byte, 16)
		if n, err := unix.Lgetxattr(filepath.Join(dst, "b/sub/b.txt"), "user.custom", buf); err != nil || string(buf[:n]) != "val" {
			t.Errorf("b/sub/b.txt user.custom = %q, %v; want %q", buf[:n], err, "val")
		}
	}
}

// Split refuses what the per-directory archives cannot hold, writes nothing
// when it does, and leaves its input alone.
func TestSplitRefuses(t *testing.T) {
	for name, entries := range map[string][]tar.Header{
		"entry outside every directory": {
			{Name: "a/", Typeflag: tar.TypeDir, Mode: 0o755},
			{Name: "a/x", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1},
			{Name: "other/y", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1},
		},
		"top-level file": {
			{Name: "a", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1},
		},
		"hardlink into another directory": {
			{Name: "a/x", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1},
			{Name: "b/y", Typeflag: tar.TypeLink, Linkname: "a/x"},
		},
		"hardlink to a directory root": {
			{Name: "a/x", Typeflag: tar.TypeLink, Linkname: "a"},
		},
		"escaping name": {
			{Name: "a/../../x", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1},
		},
	} {
		t.Run(name, func(t *testing.T) {
			work := t.TempDir()
			whole := filepath.Join(work, "whole.tar")
			writeTar(t, whole, entries...)
			before, err := os.ReadFile(whole)
			if err != nil {
				t.Fatal(err)
			}
			if err := Split(whole, map[string]string{"a": filepath.Join(work, "a.tar"), "b": filepath.Join(work, "b.tar")}); err == nil {
				t.Fatal("Split = nil; want an error")
			}
			if after, err := os.ReadFile(whole); err != nil || string(after) != string(before) {
				t.Errorf("Split changed its input (%v)", err)
			}
			if got, err := os.ReadDir(work); err != nil || len(got) != 1 {
				t.Errorf("work dir after a refused Split = %v, %v; want only the input", got, err)
			}
		})
	}
}
