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

package imagecache

import (
	"archive/tar"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// backdate shifts path's mtime age into the past.
func backdate(t *testing.T, path string, age time.Duration) {
	t.Helper()
	past := time.Now().Add(-age)
	if err := os.Chtimes(path, past, past); err != nil {
		t.Fatalf("backdating %q: %v", path, err)
	}
}

func mustEnsure(t *testing.T, s *Store, ref string) *Image {
	t.Helper()
	img, err := s.EnsureImage(context.Background(), ref)
	if err != nil {
		t.Fatalf("EnsureImage(%q): %v", ref, err)
	}
	return img
}

func TestLayerSizeRecordedAndBackfilled(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/sized:latest"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "data", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("x", 4096)},
	}))

	store := newTestStore(t)
	img := mustEnsure(t, store, ref)

	sizePath := filepath.Join(img.LayerDirs[0], layerSizeFileName)
	if _, err := os.Stat(sizePath); err != nil {
		t.Fatalf("size file not written at unpack: %v", err)
	}
	recorded, err := store.layerSize(img.LayerDirs[0])
	if err != nil {
		t.Fatalf("layerSize: %v", err)
	}
	if recorded < 4096 {
		t.Errorf("recorded size = %d, want >= 4096 (content bytes)", recorded)
	}

	// Backfill: delete the size file (simulating a pre-size-file layer) and
	// backdate the dir, since the delete itself bumps its mtime. Backfill
	// counts file content bytes, so it may be smaller than the unpack-time
	// tar-stream count; both are valid optimistic estimates.
	if err := os.Remove(sizePath); err != nil {
		t.Fatal(err)
	}
	backdate(t, img.LayerDirs[0], time.Hour)
	before, _ := os.Stat(img.LayerDirs[0])
	refilled, err := store.layerSize(img.LayerDirs[0])
	if err != nil {
		t.Fatalf("layerSize backfill: %v", err)
	}
	if refilled < 4096 {
		t.Errorf("backfilled size = %d, want >= 4096", refilled)
	}
	if _, err := os.Stat(sizePath); err != nil {
		t.Errorf("backfill did not rewrite the size file: %v", err)
	}
	after, _ := os.Stat(img.LayerDirs[0])
	if !after.ModTime().After(before.ModTime()) {
		t.Errorf("backfill must bump the layer dir mtime (not restore it — a restore could rewind a concurrent reuse-touch): %v -> %v", before.ModTime(), after.ModTime())
	}

	// Corrupt size file: healed the same way as a missing one.
	if err := os.WriteFile(sizePath, []byte("not-a-number\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	repaired, err := store.layerSize(img.LayerDirs[0])
	if err != nil {
		t.Fatalf("layerSize on corrupt size file: %v", err)
	}
	if repaired < 4096 {
		t.Errorf("layerSize = %d, want >= 4096: corrupt file not healed by backfill", repaired)
	}
	if b, _ := os.ReadFile(sizePath); strings.TrimSpace(string(b)) == "not-a-number" {
		t.Error("backfill did not overwrite the corrupt size file")
	}

	if total, err := store.CacheSize(); err != nil || total < refilled {
		t.Errorf("CacheSize() = %d, %v; want >= %d", total, err, refilled)
	}
}

func TestEnsureImageHitTouchesRecord(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/touch:latest"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: "hi"},
	}))

	store := newTestStore(t)
	img := mustEnsure(t, store, ref)

	recPath := store.recordPath(img.Digest)
	backdate(t, recPath, time.Hour)
	stale, _ := os.Stat(recPath)

	mustEnsure(t, store, ref) // cache hit
	fresh, _ := os.Stat(recPath)
	if !fresh.ModTime().After(stale.ModTime()) {
		t.Errorf("cache hit did not advance record mtime: %v -> %v", stale.ModTime(), fresh.ModTime())
	}
}

func TestLayerSizeBackfillSkipsUnreadableDirs(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root with CAP_DAC_READ_SEARCH can list mode-0111 dirs")
	}
	store := newTestStore(t)

	dir := filepath.Join(store.layersDir(), strings.Repeat("11", 32))
	fsDir := filepath.Join(dir, layerFSDirName)
	readable := filepath.Join(fsDir, "usr")
	inner := filepath.Join(fsDir, "opt", "secret")
	for _, d := range []string{readable, inner} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(readable, "blob"), make([]byte, 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inner, "blob"), make([]byte, 8192), 0o644); err != nil {
		t.Fatal(err)
	}
	// A layer that ships a search-only dir, as some distro images do.
	if err := os.Chmod(inner, 0o111); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(inner, 0o700) })

	// The contract is "never abort, count what is countable": the 0111
	// content cannot be enumerated without CAP_DAC_READ_SEARCH (atelet
	// drops it), and chmod'ing the shared pool would change metadata
	// running actors see through overlayfs. Under-counting is the correct
	// outcome; aborting would make CacheSize credit the layer nothing.
	got, err := store.layerSize(dir)
	if err != nil {
		t.Errorf("layerSize aborted on a layer with an unreadable subdir: %v", err)
	}
	if got < 4096 {
		t.Errorf("layerSize = %d, want >= 4096: readable content was not counted", got)
	}

	// The size file must still be written, so the walk happens once ever.
	if _, err := os.Stat(filepath.Join(dir, layerSizeFileName)); err != nil {
		t.Errorf("backfill did not persist a size file: %v", err)
	}
	total, err := store.CacheSize()
	if err != nil {
		t.Fatalf("CacheSize: %v", err)
	}
	if total < 4096 {
		t.Errorf("CacheSize = %d, want >= 4096: layer omitted from the pool total", total)
	}
}
