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

package tarutil

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestCreateKeepsArchiveCached(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "big"), make([]byte, 4*writeBehindChunk), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	// tmpfs pages can't be dropped, and overlayfs caches the upper inode
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil {
		t.Fatal(err)
	}
	if fs.Type == unix.TMPFS_MAGIC || fs.Type == unix.OVERLAYFS_SUPER_MAGIC {
		t.Skip("TMPDIR is on tmpfs or overlayfs, where cachestat can't see eviction")
	}
	tarPath := filepath.Join(dir, "out.tar")
	if err := Create(context.Background(), tarPath, src); err != nil {
		t.Fatalf("Create: %v", err)
	}
	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var cs unix.Cachestat_t
	if err := unix.Cachestat(uint(f.Fd()), &unix.CachestatRange{}, &cs, 0); err != nil {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) {
			t.Skipf("cachestat: %v", err)
		}
		t.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pages := uint64(info.Size()) / uint64(os.Getpagesize())
	if cs.Cache < pages*9/10 {
		t.Errorf("%d of %d archive pages cached, want nearly all", cs.Cache, pages)
	}
	if chunks := uint64(2*writeBehindChunk)/uint64(os.Getpagesize()) + 1; cs.Dirty > chunks {
		t.Errorf("%d archive pages dirty, want at most %d", cs.Dirty, chunks)
	}
}

func TestCreateWithoutWriteBehind(t *testing.T) {
	src := t.TempDir()
	want := make([]byte, 3*writeBehindChunk)
	for i := range want {
		want[i] = byte(i % 251)
	}
	if err := os.WriteFile(filepath.Join(src, "big"), want, 0o644); err != nil {
		t.Fatal(err)
	}
	tarPath := filepath.Join(t.TempDir(), "out.tar")
	if err := Create(context.Background(), tarPath, src, WriteBehind(0)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	dst := t.TempDir()
	if err := Extract(tarPath, dst); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "big"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("extracted %d bytes that differ from the %d archived", len(got), len(want))
	}
}
