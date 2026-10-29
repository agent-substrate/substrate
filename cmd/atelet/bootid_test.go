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
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

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
	for _, tc := range []struct {
		name string
		// marker is the recorded boot ID, or empty for no marker file.
		marker      string
		wantCleared bool
	}{
		{name: "rebooted since the recorded boot", marker: "boot-1\n", wantCleared: true},
		{name: "same boot", marker: "boot-2\n"},
		{name: "no recorded boot", marker: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			marker := filepath.Join(root, "cache-boot-id")
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
			if tc.marker != "" {
				if err := os.WriteFile(marker, []byte(tc.marker), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			got, err := clearCachesAfterReboot("boot-2", marker, actors, workers, imageCache, assetCache, "")
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
			if b, err := os.ReadFile(marker); err != nil || string(b) != "boot-2\n" {
				t.Errorf("marker = (%q, %v), want (%q, nil)", b, err, "boot-2\n")
			}
		})
	}
}
