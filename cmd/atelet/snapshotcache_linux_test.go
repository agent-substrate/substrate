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

//go:build linux

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDownloadExternalCheckpointLinkFallsBackToCopyAcrossMounts needs two
// real filesystems, which /dev/shm gives on Linux: a link-mode restore into
// a directory on another mount than the cache must degrade to a private
// copy, not to a download. An unusable /dev/shm fails the test rather than
// skipping it; this is where the fallback is proven.
func TestDownloadExternalCheckpointLinkFallsBackToCopyAcrossMounts(t *testing.T) {
	s, store, base := newSnapshotCacheHerder(t, map[string]string{"memory-ranges": "golden memory"})
	ctx := context.Background()
	files := []string{"memory-ranges"}
	logs := captureLogs(t)

	otherFS, err := os.MkdirTemp("/dev/shm", "snapshot-cache-test-")
	if err != nil {
		t.Fatalf("creating a directory on /dev/shm: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(otherFS) })
	// Prove the two dirs really are different mounts; same-mount tmpdirs
	// would silently test the plain link path.
	probe := filepath.Join(base, "probe")
	if err := os.WriteFile(probe, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(probe, filepath.Join(otherFS, "probe")); err == nil {
		t.Fatal("test dirs share a filesystem; cannot exercise EXDEV")
	}

	first := filepath.Join(otherFS, "first")
	if err := os.Mkdir(first, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := s.downloadExternalCheckpoint(ctx, testActor, testSnapshotURI, first, files, cacheModeLink); err != nil {
		t.Fatalf("cross-mount link-mode restore: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(first, "memory-ranges")); err != nil || string(got) != "golden memory" {
		t.Fatalf("staged %q, %v", got, err)
	}
	if n := store.gets.Load(); n != 1 {
		t.Errorf("cross-mount fallback performed %d downloads, want 1 (the copy must reuse the published entry)", n)
	}

	// And the fallback stays cache-served: a second cross-mount restore is a
	// local copy, not a download.
	second := filepath.Join(otherFS, "second")
	if err := os.Mkdir(second, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := s.downloadExternalCheckpoint(ctx, testActor, testSnapshotURI, second, files, cacheModeLink); err != nil {
		t.Fatal(err)
	}
	if n := store.gets.Load(); n != 1 {
		t.Errorf("second cross-mount restore downloaded again (gets=%d, want 1)", n)
	}
	// The misconfiguration is the node's, not the restore's: one warning per
	// process, not one per file per restore.
	if n := strings.Count(logs.String(), "serving every hit as a copy"); n != 1 {
		t.Errorf("cross-mount warning logged %d times over two restores, want once", n)
	}
}
