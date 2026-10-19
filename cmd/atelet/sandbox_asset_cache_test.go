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
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/nodepath"
)

func TestValidateSandboxAssetCacheFlags(t *testing.T) {
	origMinAge, origTimeout := *sandboxAssetCacheMinAge, *sandboxAssetFetchTimeout
	t.Cleanup(func() { *sandboxAssetCacheMinAge, *sandboxAssetFetchTimeout = origMinAge, origTimeout })
	for _, tc := range []struct {
		name    string
		minAge  time.Duration
		timeout time.Duration
		wantErr bool
	}{
		{"defaults", origMinAge, origTimeout, false},
		{"zero min age", 0, time.Minute, false},
		{"negative min age", -time.Second, time.Minute, true},
		{"zero fetch timeout", time.Minute, 0, true},
		{"negative fetch timeout", time.Minute, -time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			*sandboxAssetCacheMinAge, *sandboxAssetFetchTimeout = tc.minAge, tc.timeout
			if err := validateSandboxAssetCacheFlags(); (err != nil) != tc.wantErr {
				t.Errorf("validateSandboxAssetCacheFlags() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestOpenSandboxAssetCacheResetsScratch pins the startup contract: the
// cache opens under the given dir, and whatever a crashed prewarm left in
// the scratch dir is gone.
func TestOpenSandboxAssetCacheResetsScratch(t *testing.T) {
	useTempNodeDirs(t)
	useTempAssetScratch(t)
	leftover := filepath.Join(ateletpath.SandboxAssetScratchDir, "prewarm-123", "runsc")
	if err := os.MkdirAll(filepath.Dir(leftover), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(leftover, []byte("stale link"), 0o555); err != nil {
		t.Fatal(err)
	}

	dir := filepath.Join(t.TempDir(), "cache")
	store, err := openSandboxAssetCache(t.Context(), dir, time.Hour, 5*time.Minute, nodepath.ActorsDir, filepath.Join(t.TempDir(), "workers"))
	if err != nil {
		t.Fatalf("openSandboxAssetCache: %v", err)
	}
	if store == nil {
		t.Fatal("openSandboxAssetCache returned no store")
	}
	if _, err := os.Stat(filepath.Join(dir, "entries")); err != nil {
		t.Errorf("cache layout not created under %s: %v", dir, err)
	}
	entries, err := os.ReadDir(ateletpath.SandboxAssetScratchDir)
	if err != nil {
		t.Fatalf("scratch dir after open: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("scratch dir holds %d entries after open, want it emptied", len(entries))
	}
}

func TestCanHardLink(t *testing.T) {
	root := t.TempDir()
	cache, actors := filepath.Join(root, "cache"), filepath.Join(root, "actors")
	if err := os.MkdirAll(cache, 0o700); err != nil {
		t.Fatal(err)
	}
	// The actors dir need not exist yet; the probe creates it and leaves
	// nothing else behind.
	if err := canHardLink(cache, actors); err != nil {
		t.Errorf("two dirs under one temp dir: %v", err)
	}
	for _, dir := range []string{cache, actors} {
		if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
			t.Errorf("%s holds %d entries after the probe (%v), want none", dir, len(entries), err)
		}
	}
	// The cross-mount case needs a second mount a non-root user can write
	// to; see sandbox_asset_cache_linux_test.go.
}
