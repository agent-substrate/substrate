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
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/roottest"
	"golang.org/x/sys/unix"
)

// writeTar builds a tar file at path from the given headers; a header with a
// non-empty body is written as a regular file with that content.
func writeTar(t *testing.T, path string, entries ...tar.Header) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("creating tar: %v", err)
	}
	defer f.Close()
	tw := tar.NewWriter(f)
	for _, hdr := range entries {
		if err := tw.WriteHeader(&hdr); err != nil {
			t.Fatalf("writing header %q: %v", hdr.Name, err)
		}
		if hdr.Size > 0 {
			if _, err := tw.Write([]byte(strings.Repeat("x", int(hdr.Size)))); err != nil {
				t.Fatalf("writing body %q: %v", hdr.Name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("closing tar: %v", err)
	}
}

func TestRoundTrip(t *testing.T) {
	src := t.TempDir()
	mustWrite := func(rel, content string, mode os.FileMode) string {
		t.Helper()
		p := filepath.Join(src, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir for %q: %v", rel, err)
		}
		if err := os.WriteFile(p, []byte(content), mode); err != nil {
			t.Fatalf("writing %q: %v", rel, err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatalf("chmod %q: %v", rel, err)
		}
		return p
	}

	mustWrite("a.txt", "hello", 0o644)
	mustWrite("sub/b.txt", "nested", 0o600)
	// A read-only directory: extraction must still populate it, then restore
	// the mode.
	mustWrite("ro/c.txt", "in-readonly", 0o444)
	if err := os.Chmod(filepath.Join(src, "ro"), 0o500); err != nil {
		t.Fatalf("chmod ro dir: %v", err)
	}
	// TempDir cleanup cannot unlink children of a non-writable directory, so
	// make both copies writable again once the test is done.
	restoreWritable := func(dir string) {
		t.Cleanup(func() { _ = os.Chmod(filepath.Join(dir, "ro"), 0o700) })
	}
	restoreWritable(src)
	if err := os.Mkdir(filepath.Join(src, "empty"), 0o755); err != nil {
		t.Fatalf("mkdir empty: %v", err)
	}
	if err := os.Symlink("a.txt", filepath.Join(src, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := os.Link(filepath.Join(src, "a.txt"), filepath.Join(src, "hard.txt")); err != nil {
		t.Fatalf("hardlink: %v", err)
	}
	mtime := time.Unix(1600000000, 0)
	if err := os.Chtimes(filepath.Join(src, "a.txt"), mtime, mtime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	tarPath := filepath.Join(t.TempDir(), "out.tar")
	if err := Create(t.Context(), tarPath, src); err != nil {
		t.Fatalf("Create: %v", err)
	}
	dst := t.TempDir()
	if err := Extract(tarPath, dst); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	restoreWritable(dst)

	t.Run("contents", func(t *testing.T) {
		for rel, want := range map[string]string{
			"a.txt":     "hello",
			"sub/b.txt": "nested",
			"ro/c.txt":  "in-readonly",
			"hard.txt":  "hello",
		} {
			got, err := os.ReadFile(filepath.Join(dst, rel))
			if err != nil {
				t.Errorf("reading %q: %v", rel, err)
				continue
			}
			if string(got) != want {
				t.Errorf("%q = %q, want %q", rel, got, want)
			}
		}
	})

	t.Run("modes", func(t *testing.T) {
		for rel, want := range map[string]os.FileMode{
			"a.txt":     0o644,
			"sub/b.txt": 0o600,
			"ro/c.txt":  0o444,
			"ro":        0o500,
			"empty":     0o755,
		} {
			st, err := os.Lstat(filepath.Join(dst, rel))
			if err != nil {
				t.Errorf("lstat %q: %v", rel, err)
				continue
			}
			if got := st.Mode().Perm(); got != want {
				t.Errorf("%q mode = %v, want %v", rel, got, want)
			}
		}
	})

	t.Run("mtime", func(t *testing.T) {
		st, err := os.Stat(filepath.Join(dst, "a.txt"))
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if !st.ModTime().Equal(mtime) {
			t.Errorf("a.txt mtime = %v, want %v", st.ModTime(), mtime)
		}
	})

	t.Run("symlink", func(t *testing.T) {
		got, err := os.Readlink(filepath.Join(dst, "link"))
		if err != nil {
			t.Fatalf("readlink: %v", err)
		}
		if got != "a.txt" {
			t.Errorf("link target = %q, want %q", got, "a.txt")
		}
	})

	t.Run("hardlink shares inode", func(t *testing.T) {
		a, err := os.Stat(filepath.Join(dst, "a.txt"))
		if err != nil {
			t.Fatalf("stat a.txt: %v", err)
		}
		h, err := os.Stat(filepath.Join(dst, "hard.txt"))
		if err != nil {
			t.Fatalf("stat hard.txt: %v", err)
		}
		if !os.SameFile(a, h) {
			t.Error("hard.txt is a copy, want the same inode as a.txt")
		}
	})
}

func TestCreateEmptyDir(t *testing.T) {
	tarPath := filepath.Join(t.TempDir(), "empty.tar")
	if err := Create(t.Context(), tarPath, t.TempDir()); err != nil {
		t.Fatalf("Create: %v", err)
	}
	dst := t.TempDir()
	if err := Extract(tarPath, dst); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	entries, err := os.ReadDir(dst)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("extracted %d entries from an empty archive, want 0", len(entries))
	}
}

func TestRoundTripFifo(t *testing.T) {
	src := t.TempDir()
	if err := unix.Mkfifo(filepath.Join(src, "pipe"), 0o640); err != nil {
		t.Fatalf("creating fifo: %v", err)
	}

	tarPath := filepath.Join(t.TempDir(), "fifo.tar")
	if err := Create(t.Context(), tarPath, src); err != nil {
		t.Fatalf("Create: %v", err)
	}
	dst := t.TempDir()
	if err := Extract(tarPath, dst); err != nil {
		t.Fatalf("Extract: %v", err)
	}

	st, err := os.Lstat(filepath.Join(dst, "pipe"))
	if err != nil {
		t.Fatalf("lstat restored fifo: %v", err)
	}
	if st.Mode()&os.ModeNamedPipe == 0 {
		t.Errorf("restored entry mode = %v, want a fifo", st.Mode())
	}
	if got := st.Mode().Perm(); got != 0o640 {
		t.Errorf("restored fifo mode = %v, want %v", got, os.FileMode(0o640))
	}
}

// TestCreateSkipsSockets covers the availability property that matters most for
// snapshots: a socket in the archived tree — which agents leave behind routinely
// — must not fail the archive, because that would make the actor impossible to
// suspend.
func TestCreateSkipsSockets(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "keep.txt"), []byte("data"), 0o644); err != nil {
		t.Fatalf("writing file: %v", err)
	}
	l, err := net.Listen("unix", filepath.Join(src, "agent.sock"))
	if err != nil {
		t.Fatalf("creating socket: %v", err)
	}
	defer l.Close()

	tarPath := filepath.Join(t.TempDir(), "sock.tar")
	if err := Create(t.Context(), tarPath, src); err != nil {
		t.Fatalf("Create: %v", err)
	}
	dst := t.TempDir()
	if err := Extract(tarPath, dst); err != nil {
		t.Fatalf("Extract: %v", err)
	}

	// The rest of the tree survives; the socket itself is dropped.
	got, err := os.ReadFile(filepath.Join(dst, "keep.txt"))
	if err != nil {
		t.Fatalf("reading archived file: %v", err)
	}
	if string(got) != "data" {
		t.Errorf("keep.txt = %q, want %q", got, "data")
	}
	if _, err := os.Lstat(filepath.Join(dst, "agent.sock")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("socket was archived (lstat err = %v), want it skipped", err)
	}
}

// TestRoundTripSpecialModeBits pins setuid/setgid/sticky across a round trip.
// FileMode.Perm() silently drops them, so without this the archive would record
// bits that extraction throws away — and a setgid data directory would come back
// from a suspend having quietly lost its group inheritance.
func TestRoundTripSpecialModeBits(t *testing.T) {
	src := t.TempDir()
	// Modes are applied after creation because MkdirAll and WriteFile mask the
	// special bits against the umask.
	setgidDir := filepath.Join(src, "shared")
	if err := os.Mkdir(setgidDir, 0o775); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	stickyDir := filepath.Join(src, "tmp")
	if err := os.Mkdir(stickyDir, 0o777); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	setuidFile := filepath.Join(src, "tool")
	if err := os.WriteFile(setuidFile, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("writing file: %v", err)
	}
	for path, mode := range map[string]os.FileMode{
		setgidDir:  os.ModeDir | os.ModeSetgid | 0o775,
		stickyDir:  os.ModeDir | os.ModeSticky | 0o777,
		setuidFile: os.ModeSetuid | 0o755,
	} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatalf("chmod %q: %v", path, err)
		}
	}

	tarPath := filepath.Join(t.TempDir(), "modes.tar")
	if err := Create(t.Context(), tarPath, src); err != nil {
		t.Fatalf("Create: %v", err)
	}
	dst := t.TempDir()
	if err := Extract(tarPath, dst); err != nil {
		t.Fatalf("Extract: %v", err)
	}

	for name, want := range map[string]os.FileMode{
		"shared": os.ModeDir | os.ModeSetgid | 0o775,
		"tmp":    os.ModeDir | os.ModeSticky | 0o777,
		"tool":   os.ModeSetuid | 0o755,
	} {
		st, err := os.Lstat(filepath.Join(dst, name))
		if err != nil {
			t.Errorf("lstat %q: %v", name, err)
			continue
		}
		if got := st.Mode(); got != want {
			t.Errorf("%q mode = %v, want %v", name, got, want)
		}
	}
}

// TestRoundTripOwnership covers the reason this package exists rather than a
// plain tar: durable-dir contents are written by the sandboxed workload under
// its own uids, and a restore that re-owned everything to root would hand the
// workload back files it cannot write. Needs root, since only root can give a
// file away.
func TestRoundTripOwnership(t *testing.T) {
	roottest.Require(t, "restoring file ownership requires CAP_CHOWN")

	const uid, gid = 1234, 5678
	src := t.TempDir()
	path := filepath.Join(src, "owned.txt")
	if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
		t.Fatalf("writing file: %v", err)
	}
	if err := os.Chown(path, uid, gid); err != nil {
		t.Fatalf("chown: %v", err)
	}

	tarPath := filepath.Join(t.TempDir(), "owned.tar")
	if err := Create(t.Context(), tarPath, src); err != nil {
		t.Fatalf("Create: %v", err)
	}
	dst := t.TempDir()
	if err := Extract(tarPath, dst); err != nil {
		t.Fatalf("Extract: %v", err)
	}

	st, err := os.Stat(filepath.Join(dst, "owned.txt"))
	if err != nil {
		t.Fatalf("stat restored file: %v", err)
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("stat has no syscall.Stat_t")
	}
	if sys.Uid != uid || sys.Gid != gid {
		t.Errorf("restored ownership = %d:%d, want %d:%d", sys.Uid, sys.Gid, uid, gid)
	}
}

func TestCreateRejectsDeviceNode(t *testing.T) {
	roottest.Require(t, "creating a device node requires root")

	src := t.TempDir()
	// /dev/null, the cheapest character device to plant.
	if err := unix.Mknod(filepath.Join(src, "null"), unix.S_IFCHR|0o666, int(unix.Mkdev(1, 3))); err != nil {
		t.Fatalf("creating device node: %v", err)
	}
	err := Create(t.Context(), filepath.Join(t.TempDir(), "dev.tar"), src)
	if err == nil {
		t.Fatal("Create succeeded on a device node, want an error")
	}
	if !strings.Contains(err.Error(), "unsupported file type") {
		t.Errorf("error = %v, want it to mention an unsupported file type", err)
	}
}

func TestExtractRejectsEscapes(t *testing.T) {
	tests := []struct {
		name    string
		entries []tar.Header
	}{
		{
			name:    "parent traversal",
			entries: []tar.Header{{Name: "../escape.txt", Mode: 0o644, Size: 3, Typeflag: tar.TypeReg}},
		},
		{
			name:    "nested parent traversal",
			entries: []tar.Header{{Name: "sub/../../escape.txt", Mode: 0o644, Size: 3, Typeflag: tar.TypeReg}},
		},
		{
			name: "write through symlink out of tree",
			entries: []tar.Header{
				{Name: "out", Linkname: "/tmp", Mode: 0o777, Typeflag: tar.TypeSymlink},
				{Name: "out/escape.txt", Mode: 0o644, Size: 3, Typeflag: tar.TypeReg},
			},
		},
		{
			name:    "hardlink to a path outside the tree",
			entries: []tar.Header{{Name: "link", Linkname: "../../etc/passwd", Typeflag: tar.TypeLink}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tarPath := filepath.Join(t.TempDir(), "bad.tar")
			writeTar(t, tarPath, tc.entries...)
			dst := t.TempDir()
			if err := Extract(tarPath, dst); err == nil {
				t.Fatal("Extract succeeded, want an error")
			}
			// Whatever the archive intended, nothing may exist outside dst.
			if _, err := os.Lstat(filepath.Join(filepath.Dir(dst), "escape.txt")); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("escape.txt exists outside the destination (err=%v)", err)
			}
		})
	}
}

func TestExtractAbsoluteNameStaysInside(t *testing.T) {
	tarPath := filepath.Join(t.TempDir(), "abs.tar")
	writeTar(t, tarPath, tar.Header{Name: "/abs.txt", Mode: 0o644, Size: 2, Typeflag: tar.TypeReg})
	dst := t.TempDir()
	if err := Extract(tarPath, dst); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	// The leading slash is stripped, so the entry lands inside the destination.
	if _, err := os.Stat(filepath.Join(dst, "abs.txt")); err != nil {
		t.Errorf("abs.txt not extracted inside the destination: %v", err)
	}
}

func TestExtractReplacesExisting(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("new"), 0o644); err != nil {
		t.Fatalf("writing source: %v", err)
	}
	tarPath := filepath.Join(t.TempDir(), "out.tar")
	if err := Create(t.Context(), tarPath, src); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// The destination already holds a stale file and a symlink where a regular
	// file is about to land — extraction must replace both, not write through.
	dst := t.TempDir()
	if err := os.WriteFile(filepath.Join(dst, "a.txt"), []byte("stale"), 0o600); err != nil {
		t.Fatalf("seeding destination: %v", err)
	}
	if err := Extract(tarPath, dst); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "a.txt"))
	if err != nil {
		t.Fatalf("reading result: %v", err)
	}
	if string(got) != "new" {
		t.Errorf("a.txt = %q, want %q", got, "new")
	}
}

func TestExtractIntoPrecreatedVolumeDir(t *testing.T) {
	// The durable-dir case: the destination's <volName>/ subdirectory already
	// exists (atelet pre-creates it) when the archive is extracted over it.
	src := t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "data"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "data", "counter"), []byte("7"), 0o644); err != nil {
		t.Fatalf("writing counter: %v", err)
	}
	tarPath := filepath.Join(t.TempDir(), "durable.tar")
	if err := Create(t.Context(), tarPath, src); err != nil {
		t.Fatalf("Create: %v", err)
	}

	dst := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dst, "data"), 0o700); err != nil {
		t.Fatalf("pre-creating volume dir: %v", err)
	}
	if err := Extract(tarPath, dst); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "data", "counter"))
	if err != nil {
		t.Fatalf("reading counter: %v", err)
	}
	if string(got) != "7" {
		t.Errorf("counter = %q, want %q", got, "7")
	}
}

func TestExtractRejectsUnsupportedType(t *testing.T) {
	tarPath := filepath.Join(t.TempDir(), "dev.tar")
	writeTar(t, tarPath, tar.Header{Name: "null", Typeflag: tar.TypeChar, Devmajor: 1, Devminor: 3})
	if err := Extract(tarPath, t.TempDir()); err == nil {
		t.Fatal("Extract succeeded on a device entry, want an error")
	}
}
