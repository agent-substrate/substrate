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
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
)

// localSnapshotBootCheck matches a local snapshot only when its manifest
// records the given boot ID.
func TestLocalSnapshotBootCheck(t *testing.T) {
	const (
		actorUID     = "01234567-89ab-cdef-0123-456789abcdef"
		snapshotUUID = "pause-snap-1"
	)
	for _, tc := range []struct {
		name string
		// manifest is the manifest written beside the snapshot, or empty
		// for none.
		manifest string
		want     bool
	}{
		{name: "records the current boot", manifest: `{"bootId":"boot-2"}`, want: true},
		{name: "records an earlier boot", manifest: `{"bootId":"boot-1"}`},
		{name: "records no boot", manifest: `{}`},
		{name: "does not parse", manifest: `{"bootId":`},
		{name: "is missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useTempNodeDirs(t)
			if tc.manifest != "" {
				dir := ateletpath.LocalSnapshotDir(actorUID, snapshotUUID)
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, sandboxManifestName), []byte(tc.manifest), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if got := localSnapshotBootCheck("boot-2")(actorUID, snapshotUUID); got != tc.want {
				t.Errorf("localSnapshotBootCheck(%q) = %v, want %v", "boot-2", got, tc.want)
			}
		})
	}
}

// clearCachesAfterReboot clears the caches, the actors' bundle dirs and the
// workers' default-runtime dirs only once the node has rebooted since the
// recorded boot, and leaves everything else in place.
func TestClearCachesAfterReboot(t *testing.T) {
	writeLegacy := func(content string) func(t *testing.T, path string) {
		return func(t *testing.T, path string) {
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, tc := range []struct {
		name string
		// marker is the recorded boot ID, or empty for no marker file.
		marker string
		// legacy makes what is at the legacy marker path, if anything.
		legacy      func(t *testing.T, path string)
		wantCleared bool
	}{
		{name: "rebooted since the recorded boot", marker: "boot-1\n", wantCleared: true},
		{name: "same boot", marker: "boot-2\n"},
		{name: "no recorded boot", marker: ""},
		{name: "upgrade on the same boot", legacy: writeLegacy("boot-2\n")},
		{name: "upgrade across a reboot", legacy: writeLegacy("boot-1\n"), wantCleared: true},
		{name: "upgrade again after a rollback and a reboot", marker: "boot-1\n", legacy: writeLegacy("boot-2\n")},
		{name: "legacy marker forged since this release started", marker: "boot-2\n", legacy: writeLegacy("forged\n")},
		{name: "oversized legacy marker", legacy: writeLegacy(strings.Repeat("x", maxCacheBootIDBytes+1))},
		{name: "legacy marker is a symlink", legacy: func(t *testing.T, path string) {
			target := filepath.Join(t.TempDir(), "target")
			if err := os.WriteFile(target, []byte("boot-1\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "legacy marker is a FIFO", legacy: func(t *testing.T, path string) {
			if err := unix.Mkfifo(path, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "legacy marker is a directory", legacy: func(t *testing.T, path string) {
			if err := os.MkdirAll(filepath.Join(path, "sub"), 0o700); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			marker := filepath.Join(root, "atelet", "cache-boot-id")
			legacyMarker := filepath.Join(root, "cache-boot-id")
			imageCache := filepath.Join(root, "image-cache")
			assetCache := filepath.Join(root, "sandbox-asset-cache")
			actors := filepath.Join(root, "actors")
			workers := filepath.Join(root, "workers")

			cleared := []string{
				filepath.Join(imageCache, "layers", "sha256", "abc", "fs", "etc", "ca.crt"),
				filepath.Join(imageCache, "version"),
				filepath.Join(assetCache, "entries", "k1", "data"),
				filepath.Join(actors, "a1", "bundles", "c1", "overlay.json"),
				filepath.Join(workers, "w1", "default-runtime", "sha256-x", "runsc"),
			}
			kept := []string{
				filepath.Join(actors, "a1", "snapshot-state.json"),
				filepath.Join(actors, "a1", "sandbox-assets.json"),
				filepath.Join(workers, "w1", "sandbox-runtimes.json"),
				filepath.Join(root, "snapshots", "a1", "local-checkpoint"),
			}
			for _, p := range append(append([]string{}, cleared...), kept...) {
				if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			// A plain file directly in the actors dir has no bundle dir.
			if err := os.WriteFile(filepath.Join(actors, "stray"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			// Unpacked layers keep the image's directory modes.
			readOnly := filepath.Join(imageCache, "layers", "sha256", "abc", "fs", "etc")
			if err := os.Chmod(readOnly, 0o500); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(readOnly, 0o700) })
			if err := os.Mkdir(filepath.Dir(marker), 0o700); err != nil {
				t.Fatal(err)
			}
			if tc.marker != "" {
				if err := os.WriteFile(marker, []byte(tc.marker), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.legacy != nil {
				tc.legacy(t, legacyMarker)
			}

			got, err := clearCachesAfterReboot("boot-2", marker, legacyMarker, actors, workers, imageCache, assetCache, "")
			if err != nil {
				t.Fatalf("clearCachesAfterReboot: %v", err)
			}
			if got != tc.wantCleared {
				t.Errorf("clearCachesAfterReboot cleared = %v, want %v", got, tc.wantCleared)
			}
			for _, p := range cleared {
				_, err := os.Stat(p)
				if gone := errors.Is(err, fs.ErrNotExist); gone != tc.wantCleared {
					t.Errorf("%s removed = %v, want %v (stat err %v)", p, gone, tc.wantCleared, err)
				}
			}
			for _, p := range append(kept, imageCache, assetCache, filepath.Join(actors, "stray")) {
				if _, err := os.Stat(p); err != nil {
					t.Errorf("%s should stay: %v", p, err)
				}
			}
			for _, p := range []string{marker, legacyMarker} {
				if fi, err := os.Lstat(p); err != nil || !fi.Mode().IsRegular() {
					t.Errorf("%s: Lstat = (%v, %v), want a regular file", p, fi, err)
					continue
				}
				if b, err := os.ReadFile(p); err != nil || string(b) != "boot-2\n" {
					t.Errorf("%s = (%q, %v), want (%q, nil)", p, b, err, "boot-2\n")
				}
			}
		})
	}
}
