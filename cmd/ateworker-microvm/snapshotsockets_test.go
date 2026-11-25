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

package main

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/internal/roottest"
	"go.ate.dev/substrate/internal/tarutil"
)

// bindSocket creates a listening Unix socket at path, the way a workload
// daemon would, and returns a cleanup that also removes the inode: closing the
// listener leaves the path behind, which is exactly the state this package's
// manifests are written for.
func bindSocket(t *testing.T, path string) net.Listener {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("creating %q: %v", filepath.Dir(path), err)
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("binding socket at %q: %v", path, err)
	}
	t.Cleanup(func() {
		l.Close()
		os.Remove(path)
	})
	return l
}

// TestCaptureSocketManifest pins what a checkpoint records for the sockets its
// tars must drop, and that a tree without sockets stays manifest-free, so the
// common snapshot is unchanged.
func TestCaptureSocketManifest(t *testing.T) {
	src := t.TempDir()
	sockPath := filepath.Join(src, "run", "agent.sock")
	bindSocket(t, sockPath)
	if err := os.WriteFile(filepath.Join(src, "data.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	manifestPath := filepath.Join(t.TempDir(), "socket.manifest.json")
	written, err := captureSocketManifest(t.Context(), manifestPath, src)
	if err != nil {
		t.Fatalf("captureSocketManifest: %v", err)
	}
	if !written {
		t.Fatal("captureSocketManifest wrote no manifest, want one for a tree with a socket")
	}
	b, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("reading manifest: %v", err)
	}
	var records []socketRecord
	if err := json.Unmarshal(b, &records); err != nil {
		t.Fatalf("parsing manifest: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("manifest records = %d, want 1 (%v)", len(records), records)
	}
	rec := records[0]
	if want := "run/agent.sock"; rec.Path != want {
		t.Errorf("recorded path = %q, want %q", rec.Path, want)
	}
	var st unix.Stat_t
	if err := unix.Lstat(sockPath, &st); err != nil {
		t.Fatalf("lstat socket: %v", err)
	}
	if got := os.FileMode(rec.Mode) & tarutil.ModeBits; got.Perm() != os.FileMode(st.Mode).Perm() {
		t.Errorf("recorded mode = %v, want %v", got.Perm(), os.FileMode(st.Mode).Perm())
	}
	if rec.UID != st.Uid || rec.GID != st.Gid {
		t.Errorf("recorded owner = %d:%d, want %d:%d", rec.UID, rec.GID, st.Uid, st.Gid)
	}

	// No sockets, no manifest: absence is what a pre-manifest snapshot looks
	// like, and restore must treat it as "nothing to recreate".
	plain := t.TempDir()
	if err := os.WriteFile(filepath.Join(plain, "data.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	manifestPath = filepath.Join(t.TempDir(), "socket.manifest.json")
	written, err = captureSocketManifest(t.Context(), manifestPath, plain)
	if err != nil {
		t.Fatalf("captureSocketManifest without sockets: %v", err)
	}
	if written {
		t.Error("captureSocketManifest wrote a manifest for a tree without sockets")
	}
	if _, err := os.Stat(manifestPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("manifest stat after capture without sockets = %v, want none", err)
	}
}

// A manifest restores the inodes the tar dropped: socket placeholders with the
// recorded mode and owner, replacing whatever occupies the path, and refusing
// to serve anything to a connect — the daemon behind the path, if the restore
// resurrected it, holds its own listening fd and answers retries; nobody else
// does (needs root: creating socket inodes is mknod).
func TestRestoreSocketManifest(t *testing.T) {
	roottest.Require(t, "creating socket inodes requires mknod")

	mtime := time.Now().Add(-3 * time.Hour).Round(0)
	records := []socketRecord{
		{Path: "run/agent.sock", Mode: 0o660, UID: 1234, GID: 5678, ModTimeUnixNano: mtime.UnixNano()},
		{Path: "tmp/tmux-1000/default", Mode: 0o770, UID: 1000, GID: 1000, ModTimeUnixNano: mtime.UnixNano()},
	}
	b, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(t.TempDir(), "socket.manifest.json")
	if err := os.WriteFile(manifestPath, b, 0o600); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	// The parents exist at restore: the tar archived the directories (the
	// manifest only records the sockets its walk found under them).
	for _, dir := range []string{"run", "tmp/tmux-1000"} {
		if err := os.MkdirAll(filepath.Join(dst, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// Whatever occupies a recorded path is replaced, like any tar entry would be.
	if err := os.WriteFile(filepath.Join(dst, "run", "agent.sock"), []byte("stale file"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "run", "agent.sock"), []byte("stale file"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := restoreSocketManifest(manifestPath, dst); err != nil {
		t.Fatalf("restoreSocketManifest: %v", err)
	}

	for _, rec := range records {
		fi, err := os.Lstat(filepath.Join(dst, filepath.FromSlash(rec.Path)))
		if err != nil {
			t.Errorf("lstat restored %q: %v", rec.Path, err)
			continue
		}
		if fi.Mode()&os.ModeSocket == 0 {
			t.Errorf("restored %q mode = %v, want a socket", rec.Path, fi.Mode())
		}
		if got := fi.Mode().Perm(); got != os.FileMode(rec.Mode).Perm() {
			t.Errorf("restored %q mode = %v, want %v", rec.Path, got, os.FileMode(rec.Mode).Perm())
		}
		var st unix.Stat_t
		if err := unix.Lstat(filepath.Join(dst, filepath.FromSlash(rec.Path)), &st); err != nil {
			t.Fatalf("lstat restored %q: %v", rec.Path, err)
		}
		if st.Uid != rec.UID || st.Gid != rec.GID {
			t.Errorf("restored %q owner = %d:%d, want %d:%d", rec.Path, st.Uid, st.Gid, rec.UID, rec.GID)
		}
		if d := fi.ModTime().Sub(mtime); d < -2*time.Second || d > 2*time.Second {
			t.Errorf("restored %q mtime = %v, want ~%v", rec.Path, fi.ModTime(), mtime)
		}
	}

	// The placeholder must never serve: a connect gets ECONNREFUSED, the
	// classic stale-socket signal, not the ENOENT this fixes.
	_, err = net.DialTimeout("unix", filepath.Join(dst, "run", "agent.sock"), time.Second)
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Errorf("dialing the placeholder = %v, want ECONNREFUSED", err)
	}
}

// The manifest is snapshot content read back from storage, so a record that
// does not name a plain in-tree path must fail the restore instead of writing
// through it.
func TestRestoreSocketManifestRefusesBadPaths(t *testing.T) {
	for _, path := range []string{"../escape.sock", "/etc/passwd", "a/../../out.sock"} {
		b, err := json.Marshal([]socketRecord{{Path: path, Mode: 0o600}})
		if err != nil {
			t.Fatal(err)
		}
		manifestPath := filepath.Join(t.TempDir(), "socket.manifest.json")
		if err := os.WriteFile(manifestPath, b, 0o600); err != nil {
			t.Fatal(err)
		}
		dst := t.TempDir()
		if err := restoreSocketManifest(manifestPath, dst); err == nil {
			t.Errorf("restoreSocketManifest with path %q = nil error, want one", path)
		}
	}
}

// A snapshot without a manifest — anything taken before this change — restores
// as before.
func TestRestoreSocketManifestWithoutManifest(t *testing.T) {
	if err := restoreSocketManifest(filepath.Join(t.TempDir(), "absent.json"), t.TempDir()); err != nil {
		t.Fatalf("restoreSocketManifest without a manifest: %v", err)
	}
}

// The end-to-end property behind #2751: a socket in a rootfs upper survives a
// MEMORY restore (which brings the listening daemon back) as a placeholder
// inode, and stays out of a cold boot, whose workload re-binds its own (needs
// root: creating socket inodes is mknod).
func TestRootfsUpperSocketRoundTrip(t *testing.T) {
	roottest.Require(t, "creating socket inodes requires mknod")

	src := t.TempDir()
	sockRel := "app_ovl/fs/tmp/tmux-1000/default"
	bindSocket(t, filepath.Join(src, sockRel))
	if err := os.WriteFile(filepath.Join(src, "app_ovl/fs/home/notes.txt"), []byte("rootfs write"), 0o644); err != nil {
		t.Fatal(err)
	}
	containers := []string{"app_ovl"}

	checkpointDir := t.TempDir()
	if err := (&AteWorkerService{}).tarRootfsUpper(t.Context(), src, checkpointDir, containers); err != nil {
		t.Fatalf("tarRootfsUpper: %v", err)
	}
	tarName, err := rootfsUpperTarFile("app_ovl")
	if err != nil {
		t.Fatal(err)
	}
	manifestName := socketsManifestFile(tarName)
	if _, err := os.Stat(filepath.Join(checkpointDir, manifestName)); err != nil {
		t.Fatalf("checkpoint socket manifest: %v", err)
	}

	for _, restoreSockets := range []bool{true, false} {
		dst := upperDirWith(t, nil)
		if err := untarRootfsUpper(dst, checkpointDir, containers, restoreSockets); err != nil {
			t.Fatalf("untarRootfsUpper(restoreSockets=%v): %v", restoreSockets, err)
		}
		if got, err := os.ReadFile(filepath.Join(dst, "app_ovl/fs/home/notes.txt")); err != nil || string(got) != "rootfs write" {
			t.Errorf("restored upper content = %q, %v; want the snapshot's write", got, err)
		}
		_, err := os.Lstat(filepath.Join(dst, sockRel))
		if restoreSockets && err != nil {
			t.Errorf("MEMORY restore: socket %q not recreated: %v", sockRel, err)
		}
		if !restoreSockets && !errors.Is(err, os.ErrNotExist) {
			t.Errorf("cold boot: socket %q stat = %v, want it absent", sockRel, err)
		}
	}
}

// A durable-dir volume behaves the same way, and its manifest is a data file
// like its tar: on VOLUMES the whole snapshot must be data files, so a manifest
// left out of the returned list would never ship (needs root: creating socket
// inodes is mknod).
func TestDurableVolumesSocketRoundTrip(t *testing.T) {
	roottest.Require(t, "creating socket inodes requires mknod")

	src := t.TempDir()
	mkVolumeDirs(t, src, "data")
	sockRel := "data/run/agent.sock"
	bindSocket(t, filepath.Join(src, sockRel))
	if err := os.WriteFile(filepath.Join(src, "data", "a.txt"), []byte("42"), 0o644); err != nil {
		t.Fatal(err)
	}

	checkpointDir := t.TempDir()
	files, err := (&AteWorkerService{}).tarDurableVolumes(t.Context(), src, checkpointDir, []string{"data"})
	if err != nil {
		t.Fatalf("tarDurableVolumes: %v", err)
	}
	want := []string{"durable-dir-data.tar", "durable-dir-data.sockets.json"}
	if !slices.Equal(files, want) {
		t.Fatalf("tarDurableVolumes files = %v, want %v", files, want)
	}

	for _, restoreSockets := range []bool{true, false} {
		dst := t.TempDir()
		mkVolumeDirs(t, dst, "data")
		if err := untarDurableVolumes(dst, checkpointDir, []string{"data"}, restoreSockets); err != nil {
			t.Fatalf("untarDurableVolumes(restoreSockets=%v): %v", restoreSockets, err)
		}
		if got, err := os.ReadFile(filepath.Join(dst, "data/a.txt")); err != nil || string(got) != "42" {
			t.Errorf("restored volume content = %q, %v; want the snapshot's write", got, err)
		}
		_, err := os.Lstat(filepath.Join(dst, sockRel))
		if restoreSockets && err != nil {
			t.Errorf("MEMORY restore: socket %q not recreated: %v", sockRel, err)
		}
		if !restoreSockets && !errors.Is(err, os.ErrNotExist) {
			t.Errorf("cold boot: socket %q stat = %v, want it absent", sockRel, err)
		}
	}
}
