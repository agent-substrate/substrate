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

func TestRetireLayerStatuses(t *testing.T) {
	store := newTestStore(t)
	hex := strings.Repeat("ab", 32)

	if _, _, err := store.retireLayer("../escape", time.Now()); err == nil {
		t.Error("retireLayer accepted a non-layer name")
	}

	if _, st, err := store.retireLayer(hex, time.Now()); err != nil || st != retireGone {
		t.Errorf("retireLayer(absent) = %v, %v; want retireGone, nil", st, err)
	}

	dir := filepath.Join(store.layersDir(), hex)
	if err := os.MkdirAll(filepath.Join(dir, layerFSDirName), 0o700); err != nil {
		t.Fatal(err)
	}

	// Fresh dir, cutoff in the past: vetoed, dir untouched.
	if _, st, err := store.retireLayer(hex, time.Now().Add(-time.Minute)); err != nil || st != retireVetoed {
		t.Errorf("retireLayer(fresh) = %v, %v; want retireVetoed, nil", st, err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("vetoed layer dir was touched: %v", err)
	}

	// Old dir: retired — gone from its diffid name, present under .rm-*.
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(dir, past, past); err != nil {
		t.Fatal(err)
	}
	retired, st, err := store.retireLayer(hex, time.Now().Add(-time.Minute))
	if err != nil || st != retireRetired {
		t.Fatalf("retireLayer(old) = %v, %v; want retireRetired, nil", st, err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("retired layer still present at %q", dir)
	}
	if base := filepath.Base(retired); !strings.HasPrefix(base, retiredPrefix) {
		t.Errorf("retired path %q does not carry the %q prefix", retired, retiredPrefix)
	}
	if _, err := os.Stat(retired); err != nil {
		t.Errorf("renamed-aside dir missing: %v", err)
	}
}

func TestNewSweepsRetiredDirs(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Plant a retired dir (crash between rename and RemoveAll) with a
	// read-only subdir, which plain os.RemoveAll cannot delete.
	retired := filepath.Join(store.layersDir(), retiredPrefix+"deadbeef-1")
	if err := os.MkdirAll(filepath.Join(retired, "fs", "ro"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(retired, "fs", "ro", "f"), []byte("x"), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(retired, "fs", "ro"), 0o500); err != nil {
		t.Fatal(err)
	}
	if _, err := New(root); err != nil {
		t.Fatalf("New (recovery): %v", err)
	}
	if _, err := os.Stat(retired); !os.IsNotExist(err) {
		t.Errorf("retired dir not swept at startup: %v", err)
	}
}

func TestSweepLeavesNonPrefixedEntries(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// A complete layer dir, an operator artifact, and a stray file: none
	// carry the temp/retired prefixes, so the sweep must not touch them.
	keep := []string{
		filepath.Join(store.layersDir(), strings.Repeat("cd", 32)),
		filepath.Join(store.layersDir(), "lost+found"),
	}
	for _, d := range keep {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	strayFile := filepath.Join(store.layersDir(), "README")
	if err := os.WriteFile(strayFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := New(root); err != nil {
		t.Fatalf("New (recovery): %v", err)
	}
	for _, p := range append(keep, strayFile) {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("startup sweep removed non-prefixed entry %q: %v", p, err)
		}
	}
}

// TestRetireLayerVsEnsureImageRace races retirement against pulls of an
// image using the same layer. A pull pins its layers, so a retirement
// racing it is vetoed or finds nothing to take — neither side may ever
// error. Run with -race.
func TestRetireLayerVsEnsureImageRace(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/retire-race:latest"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: "hi"},
	}))
	store := newTestStore(t)

	img, err := store.EnsureImage(context.Background(), ref)
	if err != nil {
		t.Fatalf("EnsureImage: %v", err)
	}
	hex := filepath.Base(img.LayerDirs[0])

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 25; i++ {
			img, err := store.EnsureImage(context.Background(), ref)
			if err != nil {
				errCh <- err
				return
			}
			// Age the layer so the other goroutine's cutoff can retire it.
			past := time.Now().Add(-2 * time.Hour)
			_ = os.Chtimes(img.LayerDirs[0], past, past) // best-effort: may already be retired
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if _, _, err := store.retireLayer(hex, time.Now().Add(-time.Minute)); err != nil {
				errCh <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("race worker failed: %v", err)
	}

	// The pool must end in a consistent state: a final pull succeeds and
	// its layer dir exists under the diffid name.
	img, err = store.EnsureImage(context.Background(), ref)
	if err != nil {
		t.Fatalf("EnsureImage (final): %v", err)
	}
	if _, err := os.Stat(filepath.Join(img.LayerDirs[0], layerFSDirName)); err != nil {
		t.Errorf("final layer dir missing: %v", err)
	}
}

// A pinned layer is never retired, however old; the veto lifts with the
// last unpin, and an unpin called twice releases only its own pin.
func TestRetireLayerVetoedWhilePinned(t *testing.T) {
	store := newTestStore(t)
	hex := strings.Repeat("ab", 32)
	dir := filepath.Join(store.layersDir(), hex)
	if err := os.MkdirAll(filepath.Join(dir, layerFSDirName), 0o700); err != nil {
		t.Fatal(err)
	}
	backdate(t, dir, 3*time.Hour)

	unpinA, unpinB := store.pinLayer(hex), store.pinLayer(hex)
	unpinA()
	unpinA() // must not release B's pin
	if _, st, err := store.retireLayer(hex, time.Now()); err != nil || st != retireVetoed {
		t.Fatalf("retireLayer(pinned) = %v, %v; want retireVetoed, nil", st, err)
	}
	if _, err := os.Stat(filepath.Join(dir, layerFSDirName)); err != nil {
		t.Fatalf("vetoed layer was disturbed: %v", err)
	}

	unpinB()
	if store.pinned(hex) {
		t.Fatal("layer still pinned after its last unpin")
	}
	if _, st, err := store.retireLayer(hex, time.Now()); err != nil || st != retireRetired {
		t.Fatalf("retireLayer(unpinned) = %v, %v; want retireRetired, nil", st, err)
	}
}

// The layers a pull has already landed survive an eviction pass that runs
// while the pull is still downloading the rest — even one that removes the
// pull's record as wedged. Without pins the free layer here is unreferenced
// the moment the record goes, and the pull fails its final re-verify with
// "layer dir vanished during pull".
func TestPullPinsLayersAgainstEviction(t *testing.T) {
	reg := newGatedRegistry(t)
	free, gated, _ := gatedTestLayers(t)
	ref := reg.host + "/test/pinned:latest"
	pushImage(t, ref, v1.Config{}, free, gated)
	store := newTestStore(t)
	release := reg.gate(t, gated)

	done := make(chan error, 1)
	var img *Image
	go func() {
		var err error
		img, err = store.EnsureImage(context.Background(), ref)
		done <- err
	}()
	freeDir := layerDirOf(t, store, free)
	waitFor(t, "free layer to land", func() bool {
		_, err := os.Stat(filepath.Join(freeDir, layerFSDirName))
		return err == nil
	})

	// Age everything past min-age: this pass removes the record as wedged
	// and would retire the free layer as unreferenced.
	backdateStore(t, store, 3*time.Hour)
	dry, err := store.EvictUnused(context.Background(), math.MaxInt64, true)
	if err != nil {
		t.Fatalf("EvictUnused(dry run): %v", err)
	}
	if dry.FreedBytes != 0 || dry.EvictedLayers != 0 {
		t.Errorf("dry run counted the pinned layer as reclaimable: freed=%d layers=%d", dry.FreedBytes, dry.EvictedLayers)
	}
	stats, err := store.EvictUnused(context.Background(), math.MaxInt64, false)
	if err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if _, err := os.Stat(filepath.Join(freeDir, layerFSDirName)); err != nil {
		t.Fatalf("pinned layer was retired mid-pull: %v", err)
	}
	if stats.EvictedLayers != 0 {
		t.Errorf("EvictedLayers = %d, want 0: only pinned layers were eligible", stats.EvictedLayers)
	}

	release()
	if err := <-done; err != nil {
		t.Fatalf("EnsureImage across a mid-pull eviction pass: %v", err)
	}
	for _, dir := range img.LayerDirs {
		if _, err := os.Stat(filepath.Join(dir, layerFSDirName)); err != nil {
			t.Errorf("returned layer dir unusable: %v", err)
		}
	}
	freeDiff, err := free.DiffID()
	if err != nil {
		t.Fatal(err)
	}
	if store.pinned(freeDiff.Hex) {
		t.Error("pin outlived its pull")
	}
}

// A pull must pin before it enters the layer flight. A pull that joins
// another's flight runs no closure, so a pin taken inside one would never
// happen for it, and its layer would be unprotected the moment the leader
// returned.
func TestPullPinsBeforeJoiningLayerFlight(t *testing.T) {
	_, host := newTestRegistry(t)
	store := newTestStore(t)
	layer := layerFromEntries(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("p", 512)},
	})
	ref := host + "/test/pin-before-join:latest"
	pushImage(t, ref, v1.Config{}, layer)
	diffID, err := layer.DiffID()
	if err != nil {
		t.Fatal(err)
	}

	// Lead the layer's flight ourselves, and do its real work only once
	// released, so the pull below has to join and wait.
	held, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	go func() {
		_, _, _ = store.layerSF.Do(diffID.String(), func() (any, error) {
			close(held)
			<-release
			return nil, store.unpackLayerToPool(context.Background(), diffID, layer)
		})
	}()
	<-held

	done := make(chan error, 1)
	go func() {
		_, err := store.EnsureImage(context.Background(), ref)
		done <- err
	}()
	waitFor(t, "the pull to pin its layer", func() bool { return store.pinned(diffID.Hex) })
	select {
	case err := <-done:
		t.Fatalf("pull returned while its layer flight was still held: %v", err)
	default:
	}

	once.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatalf("EnsureImage: %v", err)
	}
	if store.pinned(diffID.Hex) {
		t.Error("pin outlived its pull")
	}
}

// whileUnpinned must hold pinMu for all of fn: that is what stops a pull
// pinning a layer between retirement's pin check and its rename.
func TestWhileUnpinnedHoldsPinLock(t *testing.T) {
	store := newTestStore(t)
	hex := strings.Repeat("ab", 32)

	ran := false
	n := store.whileUnpinned(hex, func() {
		ran = true
		if store.pinMu.TryLock() {
			store.pinMu.Unlock()
			t.Error("pinMu not held while fn runs")
		}
	})
	if n != 0 || !ran {
		t.Fatalf("whileUnpinned(unpinned) = %d, ran=%v; want 0, true", n, ran)
	}

	defer store.pinLayer(hex)()
	if n := store.whileUnpinned(hex, func() { t.Error("fn ran for a pinned layer") }); n != 1 {
		t.Fatalf("whileUnpinned(pinned) = %d, want 1", n)
	}
}
