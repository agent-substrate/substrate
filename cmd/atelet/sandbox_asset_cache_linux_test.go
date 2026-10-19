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
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// otherMount returns a fresh directory on a mount other than the one
// t.TempDir is on: /dev/shm is a tmpfs of its own and world-writable on
// every Linux, so the probe reaches the cross-mount link as any user. An
// unusable /dev/shm fails the test rather than skipping it: this is where
// the guard is seen red for the reason it exists.
func otherMount(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/dev/shm", "ate-link-probe-")
	if err != nil {
		t.Fatalf("creating a directory on /dev/shm: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// TestCanHardLinkRejectsAnotherMount pins the one failure the probe exists
// for: the actors dir on a different mount than the cache, which Linux
// reports as EXDEV on the link and which no path or device check can see.
func TestCanHardLinkRejectsAnotherMount(t *testing.T) {
	cache := filepath.Join(t.TempDir(), "cache")
	if err := os.MkdirAll(cache, 0o700); err != nil {
		t.Fatal(err)
	}
	actors := filepath.Join(otherMount(t), "actors")
	err := canHardLink(cache, actors)
	if !errors.Is(err, syscall.EXDEV) {
		t.Fatalf("canHardLink across mounts = %v, want EXDEV", err)
	}
	// The failed probe leaves nothing behind on either side.
	for _, dir := range []string{cache, actors} {
		if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
			t.Errorf("%s holds %d entries after the failed probe (%v), want none", dir, len(entries), err)
		}
	}
}

// TestOpenSandboxAssetCacheRejectsAnotherMount pins the startup refusal for
// each directory links go into: the actors dir, and the workers dir where
// each worker's default runtime is linked.
func TestOpenSandboxAssetCacheRejectsAnotherMount(t *testing.T) {
	useTempAssetScratch(t)
	for _, tt := range []struct{ name, actors, workers string }{
		{"actors dir", filepath.Join(otherMount(t), "actors"), filepath.Join(t.TempDir(), "workers")},
		{"workers dir", filepath.Join(t.TempDir(), "actors"), filepath.Join(otherMount(t), "workers")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := openSandboxAssetCache(t.Context(), filepath.Join(t.TempDir(), "cache"), time.Hour, time.Minute, tt.actors, tt.workers)
			if !errors.Is(err, syscall.EXDEV) {
				t.Fatalf("openSandboxAssetCache with the %s on another mount = %v, want EXDEV", tt.name, err)
			}
		})
	}
}
