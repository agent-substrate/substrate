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

package main

import (
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/internal/ateworker/memborrow"
)

func TestUnbookedMemory(t *testing.T) {
	const limit = 4 << 30
	s := &AteWorkerService{actors: map[string]*hostedActor{
		"a": {memoryBytes: 1 << 30},
		"b": {memoryBytes: 512 << 20},
	}}
	// 4MiB each, plus 8MiB per GiB
	want := int64(limit - memoryReservedBytes - (1<<30 + 4<<20 + 8<<20) - (512<<20 + 4<<20 + 4<<20))
	if got := s.unbookedMemory(limit); got != want {
		t.Errorf("unbookedMemory() = %d, want %d", got, want)
	}
}

func TestDirSize(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "f"), make([]byte, 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("f", filepath.Join(dir, "sub", "l")); err != nil {
		t.Fatal(err)
	}
	// dir, sub, f and l are 4 entries, and f's 1000 bytes pad to 1024
	if got, err := dirSize(t.Context(), dir); err != nil || got != 4*2048+1024 {
		t.Errorf("dirSize() = %d, %v, want %d", got, err, 4*2048+1024)
	}
}

func TestArchiveDirReturnsTheLoanOnError(t *testing.T) {
	l := &memborrow.Ledger{
		Unbooked:  func(int64) int64 { return 1 << 30 },
		PodMemory: func() (int64, int64, error) { return 4 << 30, 0, nil },
	}
	s := &AteWorkerService{borrow: l}
	if err := s.archiveDir(t.Context(), filepath.Join(t.TempDir(), "missing", "upper.tar"), t.TempDir()); err == nil {
		t.Fatal("archiveDir() into a missing directory succeeded")
	}
	if got := l.Take(1 << 30); got != 1<<30 {
		t.Errorf("Take() after a failed archiveDir = %d, want all %d back", got, int64(1<<30))
	}
}

func startTestSnapshotCH(t *testing.T, status int) *ch.Client {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "ch.sock")
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/vm.snapshot", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(lis)
	t.Cleanup(func() { _ = srv.Close() })
	return ch.NewClient(sockPath)
}

func TestWriteSnapshotDefersReleaseUntilCallerInvokesIt(t *testing.T) {
	client := startTestSnapshotCH(t, http.StatusNoContent)
	s := &AteWorkerService{}
	ra := &runningActor{}
	const limit = 1 << 30
	ra.memory.Resume(limit)

	_, release, err := s.writeSnapshot(t.Context(), client, ra, "actor-a", t.TempDir())
	if err != nil {
		t.Fatalf("writeSnapshot() unexpected error: %v", err)
	}
	if release == nil {
		t.Fatal("writeSnapshot() returned nil release func")
	}
	// While the paused guest still exists alongside the newly written snapshot
	// page cache, writeSnapshot must NOT have resumed the guard (or restored
	// memory.high) yet.
	if got := ra.memory.Pause(); got != 0 {
		t.Fatalf("ra.memory limit before release() = %d, want 0 (still paused)", got)
	}
	release()
	if got := ra.memory.Pause(); got != limit {
		t.Errorf("ra.memory limit after release() = %d, want %d", got, uint64(limit))
	}
}

func TestWriteSnapshotRestoresGuardOnSnapshotError(t *testing.T) {
	client := startTestSnapshotCH(t, http.StatusInternalServerError)
	s := &AteWorkerService{}
	ra := &runningActor{}
	const limit = 1 << 30
	ra.memory.Resume(limit)

	_, release, err := s.writeSnapshot(t.Context(), client, ra, "actor-a", t.TempDir())
	if err == nil {
		t.Fatal("writeSnapshot() succeeded on 500 response, want error")
	}
	if release != nil {
		t.Errorf("writeSnapshot() returned non-nil release func on error")
	}
	if got := ra.memory.Pause(); got != limit {
		t.Errorf("ra.memory limit after failed writeSnapshot() = %d, want %d", got, uint64(limit))
	}
}

