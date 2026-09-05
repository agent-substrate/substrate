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
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

func TestHasDurableVolumes(t *testing.T) {
	tests := []struct {
		name       string
		containers []*ateompb.Container
		want       bool
	}{
		{name: "no containers"},
		{
			name:       "container without durable volumes",
			containers: []*ateompb.Container{{Name: "app"}},
		},
		{
			name: "one of several containers has a durable volume",
			containers: []*ateompb.Container{
				{Name: "sidecar"},
				{Name: "app", DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{
					{VolumeName: "data", MountPath: "/home/counter"},
				}},
			},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasDurableVolumes(tc.containers); got != tc.want {
				t.Errorf("hasDurableVolumes() = %v, want %v", got, tc.want)
			}
		})
	}
}

// durableDirWith returns a durable-dir volumes directory laid out the way atelet
// prepares one: a subdirectory per volume, plus optionally a stray regular file.
func durableDirWith(t *testing.T, volumes []string, strayFile bool) string {
	t.Helper()
	dir := t.TempDir()
	for _, v := range volumes {
		if err := os.Mkdir(filepath.Join(dir, v), 0o700); err != nil {
			t.Fatalf("creating volume dir %q: %v", v, err)
		}
	}
	if strayFile {
		if err := os.WriteFile(filepath.Join(dir, "not-a-volume"), nil, 0o600); err != nil {
			t.Fatalf("creating stray file: %v", err)
		}
	}
	return dir
}

// volumeContents is the two-volume tree the round-trip tests capture.
var volumeContents = map[string]string{"data": "42", "cache": "7"}

// captureTwoVolumes builds that tree and checkpoints it, returning the
// checkpoint directory.
func captureTwoVolumes(t *testing.T) string {
	t.Helper()
	src := durableDirWith(t, []string{"data", "cache"}, false)
	for vol, content := range volumeContents {
		if err := os.WriteFile(filepath.Join(src, vol, "a.txt"), []byte(content), 0o644); err != nil {
			t.Fatalf("writing %q content: %v", vol, err)
		}
	}
	checkpointDir := t.TempDir()
	if err := captureDurableVolumes(t.Context(), src, checkpointDir); err != nil {
		t.Fatalf("captureDurableVolumes: %v", err)
	}
	return checkpointDir
}

// assertVolumesRestored checks the tree came back under the same volume names,
// which are what the guest mount paths are built from after a restore onto
// another node.
func assertVolumesRestored(t *testing.T, dst string) {
	t.Helper()
	for vol, want := range volumeContents {
		got, err := os.ReadFile(filepath.Join(dst, vol, "a.txt"))
		if err != nil {
			t.Errorf("reading restored %q content: %v", vol, err)
			continue
		}
		if string(got) != want {
			t.Errorf("restored %q content = %q, want %q", vol, got, want)
		}
	}
}

func TestDurableVolumesRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name    string
		backend string
		// The file whose presence identifies the arrangement.
		marker string
	}{
		{name: "tar", backend: "", marker: durableTarFile},
		{name: "split", backend: durableBackendFiles, marker: durableIndexFile},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(durableBackendEnvVar, tc.backend)

			checkpointDir := captureTwoVolumes(t)
			if _, err := os.Stat(filepath.Join(checkpointDir, tc.marker)); err != nil {
				t.Fatalf("checkpoint is missing %s: %v", tc.marker, err)
			}

			// Restore onto the empty directory atelet re-creates for the actor.
			dst := t.TempDir()
			if err := restoreDurableVolumes(dst, checkpointDir); err != nil {
				t.Fatalf("restoreDurableVolumes: %v", err)
			}
			assertVolumesRestored(t, dst)
		})
	}
}

// A snapshot outlives the ateom that wrote it and can be restored on a node
// configured the other way, so the arrangement has to be read off the snapshot
// rather than off the environment.
func TestDurableVolumesRestoreIgnoresTheBackendSetting(t *testing.T) {
	for _, tc := range []struct{ capture, restore string }{
		{capture: "", restore: durableBackendFiles},
		{capture: durableBackendFiles, restore: ""},
	} {
		t.Run(tc.capture+"-then-"+tc.restore, func(t *testing.T) {
			t.Setenv(durableBackendEnvVar, tc.capture)
			checkpointDir := captureTwoVolumes(t)

			t.Setenv(durableBackendEnvVar, tc.restore)
			dst := t.TempDir()
			if err := restoreDurableVolumes(dst, checkpointDir); err != nil {
				t.Fatalf("restoreDurableVolumes: %v", err)
			}
			assertVolumesRestored(t, dst)
		})
	}
}

// The split arrangement earns its keep by not copying the actor's bytes on the
// paused path: every blob must share its inode with the file it came from, and
// the index must stay small however large the tree is.
func TestSplitDurableCaptureLinksRatherThanCopies(t *testing.T) {
	t.Setenv(durableBackendEnvVar, durableBackendFiles)

	src := durableDirWith(t, []string{"data"}, false)
	big := filepath.Join(src, "data", "big.bin")
	if err := os.WriteFile(big, make([]byte, 1<<20), 0o644); err != nil {
		t.Fatalf("writing large file: %v", err)
	}
	checkpointDir := t.TempDir()
	if err := captureDurableVolumes(t.Context(), src, checkpointDir); err != nil {
		t.Fatalf("captureDurableVolumes: %v", err)
	}

	blob := filepath.Join(checkpointDir, durableBlobPrefix+"0000")
	if !sameInode(t, big, blob) {
		t.Errorf("%s is a copy of %s, not a link to it", blob, big)
	}
	index, err := os.Stat(filepath.Join(checkpointDir, durableIndexFile))
	if err != nil {
		t.Fatalf("stat index: %v", err)
	}
	if index.Size() >= 1<<20 {
		t.Errorf("index is %d bytes; it carries the file contents rather than just the metadata", index.Size())
	}
}

func sameInode(t *testing.T, a, b string) bool {
	t.Helper()
	fa, err := os.Stat(a)
	if err != nil {
		t.Fatalf("stat %q: %v", a, err)
	}
	fb, err := os.Stat(b)
	if err != nil {
		t.Fatalf("stat %q: %v", b, err)
	}
	return os.SameFile(fa, fb)
}
