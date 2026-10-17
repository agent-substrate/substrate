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
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestValidateSnapshotCacheFlags(t *testing.T) {
	origDir, origMinAge, origPeriod, origFetch := *snapshotCacheDir, *snapshotCacheMinAge, *snapshotCacheGCFlags.Period, *snapshotCacheFetchTimeout
	t.Cleanup(func() {
		*snapshotCacheDir, *snapshotCacheMinAge, *snapshotCacheGCFlags.Period, *snapshotCacheFetchTimeout = origDir, origMinAge, origPeriod, origFetch
	})

	cases := []struct {
		name    string
		dir     string
		minAge  time.Duration
		wantErr bool
	}{
		{"defaults", origDir, 10 * time.Minute, false},
		{"disabled", "", 10 * time.Minute, false},
		{"disabled still rejects a bad min-age", "", -time.Second, true},
		{"zero min-age", origDir, 0, false},
		{"outside base path: the plugin could not fill it", "/var/lib/elsewhere/snapshot-cache", 10 * time.Minute, true},
		{"negative min-age inverts the veto", origDir, -time.Second, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			*snapshotCacheDir, *snapshotCacheMinAge = tc.dir, tc.minAge
			err := validateSnapshotCacheFlags()
			if (err != nil) != tc.wantErr {
				t.Errorf("dir=%q minAge=%v: err=%v, wantErr=%v", tc.dir, tc.minAge, err, tc.wantErr)
			}
		})
	}
	// The eviction flags are validated here too, enabled or not.
	for _, dir := range []string{origDir, ""} {
		*snapshotCacheDir, *snapshotCacheMinAge, *snapshotCacheGCFlags.Period = dir, 10*time.Minute, -time.Second
		if err := validateSnapshotCacheFlags(); err == nil {
			t.Errorf("validateSnapshotCacheFlags() = nil with a negative GC period and dir %q, want an error", dir)
		}
	}
	// So is the fetch timeout: a zero or negative bound would end every
	// download at once.
	*snapshotCacheGCFlags.Period = origPeriod
	for _, timeout := range []time.Duration{0, -time.Second} {
		*snapshotCacheFetchTimeout = timeout
		if err := validateSnapshotCacheFlags(); err == nil {
			t.Errorf("validateSnapshotCacheFlags() = nil with fetch timeout %v, want an error", timeout)
		}
	}
}

func TestOpenSnapshotCacheDisabled(t *testing.T) {
	store, err := openSnapshotCache(context.Background(), "", 10*time.Minute, time.Minute)
	if err != nil {
		t.Fatalf("openSnapshotCache(\"\") error: %v", err)
	}
	if store != nil {
		t.Errorf("openSnapshotCache(\"\") = %v, want nil (disabled)", store)
	}
}

func TestOpenSnapshotCacheCreatesRoot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "snapshot-cache")
	store, err := openSnapshotCache(context.Background(), dir, 10*time.Minute, time.Minute)
	if err != nil {
		t.Fatalf("openSnapshotCache(%q) error: %v", dir, err)
	}
	if store == nil {
		t.Fatal("openSnapshotCache returned a nil store for a non-empty dir")
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Errorf("cache root %s not created: %v", dir, err)
	}
}

// TestOpenSnapshotCacheSweepsDebris pins the startup ordering contract: crash
// debris (unfinished fetches in tmp/, interrupted evictions as .rm-*) is gone
// by the time openSnapshotCache returns, while published entries survive.
func TestOpenSnapshotCacheSweepsDebris(t *testing.T) {
	dir := t.TempDir()
	// A prior life's layout: one published entry, plus debris of both kinds.
	entryDir := filepath.Join(dir, "entries", "aaaa")
	for _, d := range []string{
		filepath.Join(dir, "tmp", "aaaa-12345"),
		filepath.Join(dir, ".rm-bbbb-xyz"),
		entryDir,
	} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "data"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	store, err := openSnapshotCache(context.Background(), dir, 10*time.Minute, time.Minute)
	if err != nil {
		t.Fatalf("openSnapshotCache error: %v", err)
	}
	if store == nil {
		t.Fatal("openSnapshotCache returned a nil store for a non-empty dir")
	}

	if children, err := os.ReadDir(filepath.Join(dir, "tmp")); err != nil || len(children) != 0 {
		t.Errorf("tmp debris not swept: %d children, %v", len(children), err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".rm-bbbb-xyz")); !os.IsNotExist(err) {
		t.Errorf("retired debris not swept: %v", err)
	}
	if _, err := os.Stat(filepath.Join(entryDir, "data")); err != nil {
		t.Errorf("published entry did not survive the sweep: %v", err)
	}
}
