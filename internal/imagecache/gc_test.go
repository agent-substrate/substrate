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
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// backdateStore ages every image record and layer dir so the min-age veto
// no longer applies, making eviction tests deterministic without sleeps.
func backdateStore(t *testing.T, s *Store, age time.Duration) {
	t.Helper()
	for _, dir := range []string{s.manifestsDir(), s.layersDir()} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("listing %q: %v", dir, err)
		}
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), ".") {
				continue
			}
			backdate(t, filepath.Join(dir, e.Name()), age)
		}
	}
}

func layerDirsOnDisk(t *testing.T, s *Store) []string {
	t.Helper()
	entries, err := os.ReadDir(s.layersDir())
	if err != nil {
		t.Fatalf("listing layer pool: %v", err)
	}
	var out []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			out = append(out, e.Name())
		}
	}
	return out
}

func TestEvictUnusedMinAgeVeto(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/fresh:latest"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: "hi"},
	}))

	store := newTestStore(t) // default minAge = 2m; everything is younger
	mustEnsure(t, store, ref)

	stats, err := store.EvictUnused(context.Background(), math.MaxInt64, false)
	if err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if stats.EvictedImages != 0 || stats.EvictedLayers != 0 {
		t.Errorf("evicted fresh content: %+v", stats)
	}
	if got := layerDirsOnDisk(t, store); len(got) == 0 {
		t.Error("fresh layers were removed")
	}
}

func TestEvictUnusedLRUSharedLayersAndRepull(t *testing.T) {
	_, host := newTestRegistry(t)
	shared := layerFromEntries(t, []tarEntry{
		{name: "shared", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("s", 2048)},
	})
	onlyA := layerFromEntries(t, []tarEntry{
		{name: "a", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("a", 2048)},
	})
	onlyB := layerFromEntries(t, []tarEntry{
		{name: "b", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("b", 2048)},
	})
	refA := host + "/test/a:latest"
	refB := host + "/test/b:latest"
	pushImage(t, refA, v1.Config{}, shared, onlyA)
	pushImage(t, refB, v1.Config{}, shared, onlyB)

	store := newTestStore(t)
	imgA := mustEnsure(t, store, refA)
	imgB := mustEnsure(t, store, refB)
	if imgA.LayerDirs[0] != imgB.LayerDirs[0] {
		t.Fatalf("shared layer not deduplicated: %q vs %q", imgA.LayerDirs[0], imgB.LayerDirs[0])
	}

	backdateStore(t, store, 3*time.Hour)
	// Make A strictly older than B so LRU picks A first.
	backdate(t, store.recordPath(imgA.Digest), 4*time.Hour)

	// A small target: only the LRU image (A) should go. Its private layer is
	// deleted; the shared layer survives via B's reference.
	stats, err := store.EvictUnused(context.Background(), 1, false)
	if err != nil {
		t.Fatalf("EvictUnused(1): %v", err)
	}
	if stats.EvictedImages != 1 {
		t.Fatalf("evicted %d images, want 1 (stats %+v)", stats.EvictedImages, stats)
	}
	if stats.FreedBytes <= 0 {
		t.Errorf("FreedBytes = %d, want > 0", stats.FreedBytes)
	}
	if _, err := os.Stat(store.recordPath(imgA.Digest)); !os.IsNotExist(err) {
		t.Error("LRU image record A still present")
	}
	if _, err := os.Stat(store.recordPath(imgB.Digest)); err != nil {
		t.Errorf("newer image record B missing: %v", err)
	}
	if _, err := os.Stat(imgA.LayerDirs[1]); !os.IsNotExist(err) {
		t.Error("A's private layer not evicted")
	}
	if _, err := os.Stat(imgA.LayerDirs[0]); err != nil {
		t.Errorf("shared layer evicted while B still references it: %v", err)
	}

	// Free-everything pass: B goes too, and the pool empties.
	if _, err := store.EvictUnused(context.Background(), math.MaxInt64, false); err != nil {
		t.Fatalf("EvictUnused(max): %v", err)
	}
	if got := layerDirsOnDisk(t, store); len(got) != 0 {
		t.Errorf("layers remain after free-everything pass: %v", got)
	}

	// The registry is still up: an evicted image is simply a re-pull away.
	imgA2 := mustEnsure(t, store, refA)
	if _, err := os.Stat(filepath.Join(imgA2.LayerDirs[1], layerFSDirName, "a")); err != nil {
		t.Errorf("re-pulled image content missing: %v", err)
	}
}

func TestEvictUnusedRootSet(t *testing.T) {
	_, host := newTestRegistry(t)
	refRooted := host + "/test/rooted:latest"
	refLayerRooted := host + "/test/layer-rooted:latest"
	refLoose := host + "/test/loose:latest"
	for _, r := range []string{refRooted, refLayerRooted, refLoose} {
		pushImage(t, r, v1.Config{}, layerFromEntries(t, []tarEntry{
			{name: "f-" + r[len(r)-8:], typeflag: tar.TypeReg, mode: 0o644, body: r},
		}))
	}

	actorsDir := t.TempDir()
	store := newTestStore(t, WithActorsDir(actorsDir))
	imgRooted := mustEnsure(t, store, refRooted)
	imgLayerRooted := mustEnsure(t, store, refLayerRooted)
	imgLoose := mustEnsure(t, store, refLoose)

	// A modern bundle spec (with digest) roots imgRooted entirely.
	bundle1 := filepath.Join(actorsDir, "actor-1", "bundles", "main")
	if err := os.MkdirAll(bundle1, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := WriteSpec(bundle1, &OverlaySpec{ImageDigest: imgRooted.Digest.String(), Layers: imgRooted.LayerDirs}); err != nil {
		t.Fatal(err)
	}
	// A digestless (pre-ImageDigest) spec roots only imgLayerRooted's layers.
	bundle2 := filepath.Join(actorsDir, "actor-2", "bundles", "main")
	if err := os.MkdirAll(bundle2, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := WriteSpec(bundle2, &OverlaySpec{Layers: imgLayerRooted.LayerDirs}); err != nil {
		t.Fatal(err)
	}

	rs, err := store.InUse()
	if err != nil {
		t.Fatalf("InUse: %v", err)
	}
	if !rs.ImageDigests[imgRooted.Digest.String()] {
		t.Error("InUse missing digest-rooted image")
	}
	if !rs.LayerHexes[filepath.Base(imgLayerRooted.LayerDirs[0])] {
		t.Error("InUse missing layer-rooted layer")
	}

	backdateStore(t, store, 3*time.Hour)
	stats, err := store.EvictUnused(context.Background(), math.MaxInt64, false)
	if err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}

	// Rooted image: record and layers untouched.
	if _, err := os.Stat(store.recordPath(imgRooted.Digest)); err != nil {
		t.Errorf("digest-rooted image record evicted: %v", err)
	}
	if _, err := os.Stat(imgRooted.LayerDirs[0]); err != nil {
		t.Errorf("digest-rooted image layer evicted: %v", err)
	}
	// Layer-rooted (old, digestless spec): the layers must stay, and so must
	// the record — a record all of whose layers are rooted is itself treated
	// as rooted. Without that rule the record is evicted while its layers
	// survive, which manufactures an orphan the moment the bundle goes away
	// (see TestDigestlessSpecLayersReclaimedAfterBundleGone) and churns the
	// record on every pull.
	if _, err := os.Stat(imgLayerRooted.LayerDirs[0]); err != nil {
		t.Errorf("layer-rooted layer evicted: %v", err)
	}
	if _, err := os.Stat(store.recordPath(imgLayerRooted.Digest)); err != nil {
		t.Errorf("record with all layers rooted was evicted: %v", err)
	}
	if stats.RootedImages < 2 {
		t.Errorf("RootedImages = %d, want >= 2 (digest-rooted + all-layers-rooted), stats %+v", stats.RootedImages, stats)
	}
	// The loose image is fully evicted.
	if _, err := os.Stat(store.recordPath(imgLoose.Digest)); !os.IsNotExist(err) {
		t.Error("unrooted image record survived a free-everything pass")
	}
	if _, err := os.Stat(imgLoose.LayerDirs[0]); !os.IsNotExist(err) {
		t.Error("unrooted layer survived a free-everything pass")
	}
}

// An unreadable record must gate the whole pass: its refcounts are
// invisible, so a layer it shares with a readable candidate would hit
// zero and be retired while the unreadable record still names it.
func TestEvictUnusedSkipsPassOnUnreadableRecord(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("chmod 0000 does not block reads for root")
	}
	_, host := newTestRegistry(t)
	shared := layerFromEntries(t, []tarEntry{
		{name: "shared", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("s", 2048)},
	})
	onlyA := layerFromEntries(t, []tarEntry{
		{name: "a", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("a", 2048)},
	})
	onlyB := layerFromEntries(t, []tarEntry{
		{name: "b", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("b", 2048)},
	})
	refA := host + "/test/unreadable-a:latest"
	refB := host + "/test/unreadable-b:latest"
	pushImage(t, refA, v1.Config{}, shared, onlyA)
	pushImage(t, refB, v1.Config{}, shared, onlyB)

	store := newTestStore(t)
	imgA := mustEnsure(t, store, refA)
	imgB := mustEnsure(t, store, refB)
	backdateStore(t, store, 3*time.Hour)

	if err := os.Chmod(store.recordPath(imgA.Digest), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(store.recordPath(imgA.Digest), 0o600) })

	stats, err := store.EvictUnused(context.Background(), math.MaxInt64, false)
	if err == nil {
		t.Fatal("EvictUnused returned no error on an unreadable record")
	}
	if stats.EvictedImages != 0 || stats.EvictedLayers != 0 || stats.FreedBytes != 0 {
		t.Errorf("gated pass still evicted: %+v", stats)
	}
	// B and the shared layer survive: evicting B on partial refcounts would
	// have retired the shared layer out from under A.
	if _, err := os.Stat(store.recordPath(imgB.Digest)); err != nil {
		t.Errorf("record B evicted during a gated pass: %v", err)
	}
	if _, err := os.Stat(imgB.LayerDirs[0]); err != nil {
		t.Errorf("shared layer retired while an unreadable record references it: %v", err)
	}
}

// An unreadable actors dir, bundles dir, or bundle spec must gate the
// whole pass, exactly like an unreadable record: a missing root fails
// toward retiring a running actor's layers, and for a long-running actor
// the spec is the only protection (its record mtime can be arbitrarily
// old, so min-age cannot save it).
func TestEvictUnusedSkipsPassOnUnreadableRoots(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("chmod 0000 does not block reads for root")
	}
	cases := []struct {
		name string
		deny func(t *testing.T, actorsDir, bundleDir string)
	}{
		{"actors dir unreadable", func(t *testing.T, actorsDir, _ string) {
			denyRead(t, actorsDir)
		}},
		{"bundles dir unreadable", func(t *testing.T, actorsDir, _ string) {
			denyRead(t, filepath.Join(actorsDir, "actor-1", "bundles"))
		}},
		{"bundle spec unreadable", func(t *testing.T, _, bundleDir string) {
			denyRead(t, filepath.Join(bundleDir, OverlaySpecFileName))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, host := newTestRegistry(t)
			ref := host + "/test/rootgate:latest"
			pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
				{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("g", 1024)},
			}))

			actorsDir := t.TempDir()
			store := newTestStore(t, WithActorsDir(actorsDir))
			img := mustEnsure(t, store, ref)

			// A running actor roots the image; its record is arbitrarily old.
			bundleDir := filepath.Join(actorsDir, "actor-1", "bundles", "main")
			if err := os.MkdirAll(bundleDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := WriteSpec(bundleDir, &OverlaySpec{ImageDigest: img.Digest.String(), Layers: img.LayerDirs}); err != nil {
				t.Fatal(err)
			}
			backdateStore(t, store, 3*time.Hour)

			tc.deny(t, actorsDir, bundleDir)

			stats, err := store.EvictUnused(context.Background(), math.MaxInt64, false)
			if err == nil {
				t.Fatal("EvictUnused returned no error with an unenumerable root set")
			}
			if stats.EvictedImages != 0 || stats.EvictedLayers != 0 {
				t.Errorf("gated pass still evicted: %+v", stats)
			}
			if _, err := os.Stat(store.recordPath(img.Digest)); err != nil {
				t.Errorf("record evicted while its actor's roots were unreadable: %v", err)
			}
			if _, err := os.Stat(img.LayerDirs[0]); err != nil {
				t.Errorf("mounted layer retired while its actor's roots were unreadable: %v", err)
			}
		})
	}
}

// denyRead removes all permissions and restores them at cleanup (so
// t.TempDir removal works).
func denyRead(t *testing.T, path string) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, fi.Mode().Perm()) })
}

// A record whose layer references cannot be established — undecodable
// JSON, or a garbled diffID inside valid JSON — contributes no refcounts,
// so a layer referenced only through it would look unreferenced. Either
// shape must gate the pass, and the record must survive it: evicting the
// record would strand its unknown layers as orphans until restart.
func TestEvictUnusedSkipsPassOnBadRecord(t *testing.T) {
	cases := []struct {
		name      string
		badRecord string
	}{
		{"undecodable JSON", `{not json`},
		{"garbled diffID", `{"version":1,"diffIDs":["not-a-hash"]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			layer := filepath.Join(store.layersDir(), strings.Repeat("cc", 32))
			if err := os.MkdirAll(filepath.Join(layer, layerFSDirName), 0o700); err != nil {
				t.Fatal(err)
			}
			badPath := store.recordPath(v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("dd", 32)})
			if err := os.WriteFile(badPath, []byte(tc.badRecord), 0o600); err != nil {
				t.Fatal(err)
			}
			backdateStore(t, store, 3*time.Hour)

			stats, err := store.EvictUnused(context.Background(), math.MaxInt64, false)
			if err == nil {
				t.Fatal("EvictUnused returned no error on a bad record")
			}
			if stats.EvictedImages != 0 || stats.EvictedLayers != 0 {
				t.Errorf("gated pass still evicted: %+v", stats)
			}
			if _, err := os.Stat(badPath); err != nil {
				t.Errorf("bad record was evicted (its unknown layers would be stranded): %v", err)
			}
			if _, err := os.Stat(layer); err != nil {
				t.Errorf("layer removed during a gated pass: %v", err)
			}
		})
	}
}

// Dry-run must not write anything — including the size-file backfill for
// layers unpacked before sizes were recorded, the exact population a
// dry-run soak on upgraded nodes exists to observe.
func TestEvictUnusedDryRunDoesNotBackfillSizes(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/nobackfill:latest"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("n", 1024)},
	}))

	store := newTestStore(t)
	img := mustEnsure(t, store, ref)
	sizePath := filepath.Join(img.LayerDirs[0], layerSizeFileName)
	if err := os.Remove(sizePath); err != nil {
		t.Fatal(err)
	}
	backdateStore(t, store, 3*time.Hour)

	stats, err := store.EvictUnused(context.Background(), math.MaxInt64, true)
	if err != nil {
		t.Fatalf("EvictUnused(dry): %v", err)
	}
	if stats.FreedBytes <= 0 {
		t.Errorf("FreedBytes = %d, want > 0 (walked size despite missing size file)", stats.FreedBytes)
	}
	if _, err := os.Stat(sizePath); !os.IsNotExist(err) {
		t.Error("dry run wrote a size file into the layer pool")
	}
	if _, err := os.Stat(img.LayerDirs[0]); err != nil {
		t.Errorf("dry run deleted the layer: %v", err)
	}
}

func TestEvictUnusedDryRun(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/dryrun:latest"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("d", 1024)},
	}))

	store := newTestStore(t)
	img := mustEnsure(t, store, ref)
	backdateStore(t, store, 3*time.Hour)

	stats, err := store.EvictUnused(context.Background(), math.MaxInt64, true)
	if err != nil {
		t.Fatalf("EvictUnused(dry): %v", err)
	}
	if stats.EvictedImages != 1 || stats.EvictedLayers != 1 || stats.FreedBytes <= 0 {
		t.Errorf("dry-run stats = %+v, want 1 image / 1 layer / >0 bytes", stats)
	}
	if _, err := os.Stat(store.recordPath(img.Digest)); err != nil {
		t.Errorf("dry run deleted the record: %v", err)
	}
	if _, err := os.Stat(img.LayerDirs[0]); err != nil {
		t.Errorf("dry run deleted the layer: %v", err)
	}
}

// TestConcurrentEnsureImageAndEvict races cache hits, re-pulls, and
// free-everything eviction passes. The invariant under test: whatever
// interleaving happens, EnsureImage never returns an Image whose layer
// dirs have been (or will be) retired — either its touch wins and the
// evictor skips, or the evictor wins and EnsureImage re-pulls fresh dirs.
// Run with -race.
func TestConcurrentEnsureImageAndEvict(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/race:latest"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("r", 512)},
	}))

	store := newTestStore(t)
	digestRef := host + "/test/race@" + mustEnsure(t, store, ref).Digest.String()

	for i := 0; i < 25; i++ {
		backdateStore(t, store, 3*time.Hour)

		var wg sync.WaitGroup
		var img *Image
		var ensureErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			img, ensureErr = store.EnsureImage(context.Background(), digestRef)
		}()
		go func() {
			defer wg.Done()
			if _, err := store.EvictUnused(context.Background(), math.MaxInt64, false); err != nil {
				t.Errorf("EvictUnused: %v", err)
			}
		}()
		wg.Wait()

		if ensureErr != nil {
			t.Fatalf("iteration %d: EnsureImage: %v", i, ensureErr)
		}
		// The returned image must reference live, complete layer dirs. Its
		// record must be fresh (either the hit's touch or the re-pull wrote
		// it), which is what protects it until an ateom would mount it.
		for _, dir := range img.LayerDirs {
			if _, err := os.Stat(filepath.Join(dir, layerFSDirName, "f")); err != nil {
				t.Fatalf("iteration %d: returned layer dir unusable: %v", i, err)
			}
		}
		if fi, err := os.Stat(store.recordPath(img.Digest)); err != nil {
			t.Fatalf("iteration %d: record of returned image missing: %v", i, err)
		} else if time.Since(fi.ModTime()) > time.Minute {
			t.Fatalf("iteration %d: returned image's record is stale (mtime %v)", i, fi.ModTime())
		}
	}
}
