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

// Eviction tests for damaged and in-between pool states — crash debris,
// interrupted or wedged pulls, unreadable records — where the required
// behavior is failing toward retention. Mainline eviction behavior is
// covered in gc_test.go.

import (
	"archive/tar"
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

// A layer left in the pool with no referencing image record must be
// reclaimable. That state is crash debris by definition (pull pre-writes
// its record; eviction retires layers in the pass that drops their
// records), so it is reclaimed by the STARTUP scan — and deliberately NOT
// by the periodic pass, which reaches layers only through records.
func TestOrphanLayerReclaimedAtStartup(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/orphan:latest"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("o", 4096)},
	}))

	store := newTestStore(t)
	img := mustEnsure(t, store, ref)

	// Crash-debris state: layer on disk, no record referencing it.
	if err := os.Remove(store.recordPath(img.Digest)); err != nil {
		t.Fatal(err)
	}
	backdateStore(t, store, 3*time.Hour)

	// The periodic pass must NOT touch it: no online whole-pool scans.
	if _, err := store.EvictUnused(context.Background(), math.MaxInt64, false); err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if _, err := os.Stat(img.LayerDirs[0]); err != nil {
		t.Fatalf("periodic pass swept an orphan; that scan belongs to startup only: %v", err)
	}

	// "Restart": reopening the store runs RecoverOrphans and reclaims it.
	reopened, err := New(store.root)
	if err != nil {
		t.Fatalf("New (restart): %v", err)
	}
	if got := layerDirsOnDisk(t, reopened); len(got) != 0 {
		t.Errorf("orphan layers survive startup recovery: %v", got)
	}
	if size, err := reopened.CacheSize(); err != nil || size != 0 {
		t.Errorf("CacheSize() = %d, %v after startup recovery; orphan bytes still counted", size, err)
	}
}

// A digestless bundle spec (written before OverlaySpec.ImageDigest
// existed, i.e. an actor running across that upgrade) must not strand its
// layers. The exact-layer-set rooting rule keeps the RECORD alive while
// the bundle exists, so when the bundle goes the record and layers are
// evicted together through the ordinary path — no orphan is ever
// manufactured, and no sweep is needed.
func TestDigestlessSpecLayersReclaimedAfterBundleGone(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/upgrade:latest"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("u", 4096)},
	}))

	actorsDir := t.TempDir()
	store := newTestStore(t, WithActorsDir(actorsDir))
	img := mustEnsure(t, store, ref)

	// A bundle written before ImageDigest existed: layers only.
	bundle := filepath.Join(actorsDir, "actor-old", "bundles", "main")
	if err := os.MkdirAll(bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := WriteSpec(bundle, &OverlaySpec{Layers: img.LayerDirs}); err != nil {
		t.Fatal(err)
	}
	backdateStore(t, store, 3*time.Hour)

	// Pass 1, while the actor runs: the exact-layer-set rule roots the
	// record, so record AND layer both survive.
	if _, err := store.EvictUnused(context.Background(), math.MaxInt64, false); err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if _, err := os.Stat(img.LayerDirs[0]); err != nil {
		t.Fatalf("layer of a running (digestless) actor was evicted: %v", err)
	}
	if _, err := os.Stat(store.recordPath(img.Digest)); err != nil {
		t.Fatalf("record of a running (digestless) actor was evicted — that would strand its layers: %v", err)
	}

	// The actor finishes; atelet removes the bundle.
	if err := os.RemoveAll(filepath.Join(actorsDir, "actor-old")); err != nil {
		t.Fatal(err)
	}
	backdateStore(t, store, 3*time.Hour)

	if _, err := store.EvictUnused(context.Background(), math.MaxInt64, false); err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if got := layerDirsOnDisk(t, store); len(got) != 0 {
		t.Errorf("digestless-spec layers not reclaimed after bundle removal: %v", got)
	}
}

// Layers of an in-flight pull must survive eviction, structurally: pull
// writes the record before unpacking, so a mid-pull layer is held by the
// record's refcount, renewed by the per-layer progress touch for as long
// as the pull advances. The shape here: a fresh record protecting even an
// OLD layer (which may have been in the pool for months from another
// image).
func TestRecordFirstProtectsInFlightPull(t *testing.T) {
	store := newTestStore(t) // default min-age: 2m

	diffID := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("5c", 32)}
	pending := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("6d", 32)}
	digest := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("7e", 32)}
	dir := store.layerDir(diffID)
	if err := os.MkdirAll(filepath.Join(dir, layerFSDirName), 0o700); err != nil {
		t.Fatal(err)
	}
	rec := imageRecord{Version: 1, DiffIDs: []string{diffID.String(), pending.String()}}
	if err := store.writeRecord(digest, rec); err != nil {
		t.Fatal(err)
	}
	backdate(t, dir, 3*time.Hour)

	if _, err := store.EvictUnused(context.Background(), math.MaxInt64, false); err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("eviction reclaimed a layer referenced by a fresh (in-flight) record: %v", err)
	}
	if _, err := os.Stat(store.recordPath(digest)); err != nil {
		t.Fatalf("eviction removed a fresh (in-flight) record: %v", err)
	}
}

// The wedged-pull disposal, with the partial layer FRESH — the realistic
// state, since a wedged pull's landed layers are seconds old. The
// contract: the stale record may be selected, but because min-age vetoes
// the fresh layer, the record is RESTORED — nothing is stranded, and the
// whole unit becomes evictable together once the layer ages.
func TestWedgedPullFreshLayersNotStranded(t *testing.T) {
	store := newTestStore(t) // default min-age: 2m

	diffID := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("5c", 32)}
	digest := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("7e", 32)}
	dir := store.layerDir(diffID)
	if err := os.MkdirAll(filepath.Join(dir, layerFSDirName), 0o700); err != nil {
		t.Fatal(err)
	}
	rec := imageRecord{Version: 1, DiffIDs: []string{diffID.String()}}
	if err := store.writeRecord(digest, rec); err != nil {
		t.Fatal(err)
	}
	// Wedged: the record has seen no progress touch for > min-age, but the
	// layer itself landed moments ago.
	backdate(t, store.recordPath(digest), 3*time.Hour)

	for i := 0; i < 4; i++ {
		if _, err := store.EvictUnused(context.Background(), math.MaxInt64, false); err != nil {
			t.Fatalf("EvictUnused pass %d: %v", i, err)
		}
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("fresh partial layer was evicted: %v", err)
	}
	// The layer must not be stranded: its record must still exist (restored
	// after the min-age veto fired), keeping it reachable by the runtime
	// pass.
	if _, err := os.Stat(store.recordPath(digest)); err != nil {
		t.Fatalf("record gone while its fresh layer survives: the layer is stranded until restart: %v", err)
	}

	// Once the layer ages past min-age (and the restored record does too),
	// the unit is evicted together — reclaimed, not leaked.
	backdate(t, store.recordPath(digest), 3*time.Hour)
	backdate(t, dir, 3*time.Hour)
	if _, err := store.EvictUnused(context.Background(), math.MaxInt64, false); err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if _, err := os.Stat(store.recordPath(digest)); !os.IsNotExist(err) {
		t.Errorf("aged wedged-pull record survived: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("aged wedged-pull layer survived: %v", err)
	}
}

// The direct restore-on-keep shape: a record whose deletion would strand
// a kept layer must be restored. Here the keep is caused by a rooted layer
// from a digestless bundle spec whose layer set does NOT exactly match the
// record (so LayerSets does not root the record itself).
func TestEvictionRestoresRecordWhenLayerRooted(t *testing.T) {
	actorsDir := t.TempDir()
	store := newTestStore(t, WithActorsDir(actorsDir))

	shared := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("1a", 32)}
	private := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("2b", 32)}
	digest := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("3c", 32)}
	for _, d := range []v1.Hash{shared, private} {
		if err := os.MkdirAll(filepath.Join(store.layerDir(d), layerFSDirName), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	rec := imageRecord{Version: 1, DiffIDs: []string{shared.String(), private.String()}}
	if err := store.writeRecord(digest, rec); err != nil {
		t.Fatal(err)
	}
	// A digestless spec roots ONLY the shared layer (subset — LayerSets
	// cannot root the record).
	bundle := filepath.Join(actorsDir, "actor-x", "bundles", "main")
	if err := os.MkdirAll(bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := WriteSpec(bundle, &OverlaySpec{Layers: []string{store.layerDir(shared)}}); err != nil {
		t.Fatal(err)
	}
	backdateStore(t, store, 3*time.Hour)

	if _, err := store.EvictUnused(context.Background(), math.MaxInt64, false); err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	// The private layer may go; the rooted layer stays — and therefore the
	// record must have been restored, or the rooted layer is stranded the
	// moment the bundle disappears.
	if _, err := os.Stat(store.layerDir(shared)); err != nil {
		t.Fatalf("rooted layer evicted: %v", err)
	}
	if _, err := os.Stat(store.recordPath(digest)); err != nil {
		t.Fatalf("record gone while its rooted layer survives (stranded once the bundle goes): %v", err)
	}
}

// Refcounts derived from a partial record enumeration must never drive
// orphan reclamation: the startup scan skips itself entirely
// (conservative, logged) when any record fails to read or decode — while
// New still succeeds, because a corrupt record must not keep atelet from
// serving actors.
func TestStartupOrphanScanSkippedWhenEnumerationIncomplete(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/enum:latest"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("e", 2048)},
	}))
	store := newTestStore(t)
	img := mustEnsure(t, store, ref)
	backdateStore(t, store, 3*time.Hour)

	// A genuine orphan AND a corrupt record: the orphan must be spared
	// because the corrupt record makes the enumeration untrustworthy.
	orphan := filepath.Join(store.layersDir(), strings.Repeat("aa", 32))
	if err := os.MkdirAll(filepath.Join(orphan, layerFSDirName), 0o700); err != nil {
		t.Fatal(err)
	}
	backdate(t, orphan, 3*time.Hour)
	if err := os.WriteFile(store.recordPath(v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("bb", 32)}), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(store.root)
	if err != nil {
		t.Fatalf("New must survive a corrupt record (atelet must still serve): %v", err)
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Errorf("startup scan ran on an incomplete enumeration and swept a layer: %v", err)
	}
	// The intact image is untouched either way.
	for _, d := range img.LayerDirs {
		if _, err := os.Stat(d); err != nil {
			t.Errorf("intact image layer swept: %v", err)
		}
	}
	_ = reopened
}

// A directory name that is not a layer digest must never be treated as a
// layer — not by the periodic pass, and not by the startup scan (which
// would otherwise also panic abbreviating a short name for rename-aside).
func TestNonLayerDirsIgnored(t *testing.T) {
	store := newTestStore(t, WithMinAge(0))
	junk := filepath.Join(store.layersDir(), "notadigest")
	if err := os.MkdirAll(junk, 0o700); err != nil {
		t.Fatal(err)
	}
	stats, err := store.EvictUnused(context.Background(), math.MaxInt64, false)
	if err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if stats.OrphanLayers != 0 {
		t.Errorf("periodic pass swept something: %+v", stats)
	}
	if _, err := New(store.root, WithMinAge(0)); err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := os.Stat(junk); err != nil {
		t.Errorf("non-layer dir was removed: %v", err)
	}
}

// The record must exist BEFORE unpacking — the load-bearing ordering. A
// pull that fails partway therefore leaves a record (resumable progress
// that ages out via LRU), never unexplained layers.
func TestFailedPullLeavesResumableRecordNotOrphans(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/badlayer:latest"
	good := layerFromEntries(t, []tarEntry{
		{name: "ok", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("g", 2048)},
	})
	// Valid blob, invalid tar: unpack fails after download succeeds.
	bad, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("this is not a tar archive at all")), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	pushImage(t, ref, v1.Config{}, good, bad)

	store := newTestStore(t)
	_, err = store.EnsureImage(context.Background(), ref)
	if err == nil {
		t.Fatal("EnsureImage succeeded on an image with an untarrable layer")
	}

	recs, err := os.ReadDir(store.manifestsDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) == 0 {
		t.Fatal("failed pull left no record: its landed layers are unexplained orphans")
	}
	// Whatever layers landed are referenced by that record — assert none is
	// an orphan by running the startup scan and checking nothing is swept.
	backdateStore(t, store, 3*time.Hour)
	for _, r := range recs {
		backdate(t, filepath.Join(store.manifestsDir(), r.Name()), 0) // keep records fresh
	}
	before := layerDirsOnDisk(t, store)
	reopened, err := New(store.root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if after := layerDirsOnDisk(t, reopened); len(after) != len(before) {
		t.Errorf("startup scan swept layers of a failed-but-recorded pull: %v -> %v", before, after)
	}
}
