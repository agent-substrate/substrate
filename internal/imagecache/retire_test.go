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
	"errors"
	"os"
	"path/filepath"
	"runtime"
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
// image using the same layer. The layer interlock serializes the retire
// rename against unpack, and the mtime touch it covers turns concurrent
// reuse into a veto — so neither side may ever error. Run with -race.
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

// seedLayer pushes a one-layer image and pulls it, returning the layer, its
// diffID and its dir in the pool.
func seedLayer(t *testing.T, store *Store, ref string) (v1.Layer, v1.Hash, string) {
	t.Helper()
	layer := layerFromEntries(t, []tarEntry{
		{name: "f", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("j", 512)},
	})
	pushImage(t, ref, v1.Config{}, layer)
	if _, err := store.EnsureImage(context.Background(), ref); err != nil {
		t.Fatalf("EnsureImage: %v", err)
	}
	diffID, err := layer.DiffID()
	if err != nil {
		t.Fatalf("layer diffID: %v", err)
	}
	return layer, diffID, layerDirOf(t, store, layer)
}

// waitForBlockedEnsure blocks until an ensureLayer goroutine is parked on
// frame. Observing the block is what makes these tests deterministic:
// releasing on a timer would let a loaded machine free the layer early, so
// ensureLayer would sail through and the contended path would go untested
// while the test still passed.
func waitForBlockedEnsure(t *testing.T, frame string) {
	t.Helper()
	buf := make([]byte, 1<<20)
	waitFor(t, "ensureLayer to block on "+frame, func() bool {
		n := runtime.Stack(buf, true)
		for n == len(buf) { // runtime.Stack truncates silently at cap
			buf = make([]byte, 2*len(buf))
			n = runtime.Stack(buf, true)
		}
		for _, g := range strings.Split(string(buf[:n]), "\n\ngoroutine ") {
			if strings.Contains(g, "ensureLayer") && strings.Contains(g, frame) {
				return true
			}
		}
		return false
	})
}

// ensureLayerAsync runs ensureLayer on its own goroutine.
func ensureLayerAsync(store *Store, diffID v1.Hash, layer v1.Layer) <-chan struct {
	dir string
	err error
} {
	done := make(chan struct {
		dir string
		err error
	}, 1)
	go func() {
		dir, err := store.ensureLayer(context.Background(), diffID, layer)
		done <- struct {
			dir string
			err error
		}{dir, err}
	}()
	return done
}

// A GC pass must never block behind the pull path: a layer whose interlock
// is held is reused or being unpacked right now, which is a veto.
func TestRetireVetoedWhileLayerInterlockHeld(t *testing.T) {
	_, host := newTestRegistry(t)
	store := newTestStore(t)
	_, diffID, dir := seedLayer(t, store, host+"/test/held:latest")
	backdate(t, dir, 3*time.Hour)

	lock := store.layerLock(diffID.Hex)
	if err := lock.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	defer lock.Release(1)

	_, st, err := store.retireLayer(diffID.Hex, time.Now())
	if err != nil || st != retireVetoed {
		t.Fatalf("retireLayer while interlock held = %v, %v; want retireVetoed, nil", st, err)
	}
	if _, err := os.Stat(filepath.Join(dir, layerFSDirName)); err != nil {
		t.Errorf("vetoed layer was disturbed: %v", err)
	}
}

// ensureLayer must never hand back a dir a retirement renamed away. It waits
// out the retirement on the interlock and then unpacks the layer again.
func TestEnsureLayerWaitsOutRetirementThenRepacks(t *testing.T) {
	_, host := newTestRegistry(t)
	store := newTestStore(t)
	layer, diffID, dir := seedLayer(t, store, host+"/test/retire-wait:latest")

	// Hold the interlock across a rename, exactly as retireLayer does.
	lock := store.layerLock(diffID.Hex)
	if err := lock.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir, dir+"-retired"); err != nil {
		t.Fatal(err)
	}

	done := ensureLayerAsync(store, diffID, layer)
	waitForBlockedEnsure(t, "semaphore.(*Weighted).Acquire")
	lock.Release(1)

	got := <-done
	if got.err != nil {
		t.Fatalf("ensureLayer: %v", got.err)
	}
	if _, err := os.Stat(filepath.Join(got.dir, layerFSDirName)); err != nil {
		t.Fatalf("ensureLayer returned a retired dir: %v", err)
	}
}

// A retirement whose rename fails leaves the layer live and usable, so the
// ensureLayer waiting behind it must reuse that layer, not fail the pull.
func TestEnsureLayerWaitsOutFailedRetirementAndReusesLayer(t *testing.T) {
	_, host := newTestRegistry(t)
	store := newTestStore(t)
	layer, diffID, dir := seedLayer(t, store, host+"/test/retire-failed:latest")

	before, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Rename "fails": the interlock is held but the dir is left in place.
	lock := store.layerLock(diffID.Hex)
	if err := lock.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}

	done := ensureLayerAsync(store, diffID, layer)
	waitForBlockedEnsure(t, "semaphore.(*Weighted).Acquire")
	lock.Release(1)

	got := <-done
	if got.err != nil {
		t.Fatalf("ensureLayer: %v", got.err)
	}
	// Same inode, not just same path: an unpack would have renamed a fresh
	// tree into place, so SameFile is what tells reuse from re-download.
	after, err := os.Stat(got.dir)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Errorf("layer was re-downloaded instead of reused")
	}
}

// holdEnsureFlight occupies the layer's dedup flight, finishing with err, so
// an ensureLayer called meanwhile joins rather than leads.
func holdEnsureFlight(t *testing.T, store *Store, diffID v1.Hash, body func() error) (release func()) {
	t.Helper()
	held, releaseCh := make(chan struct{}), make(chan struct{})
	go func() {
		_, _, _ = store.layerSF.Do(diffID.String(), func() (any, error) {
			err := body()
			close(held)
			<-releaseCh
			return nil, err
		})
	}()
	<-held
	var once sync.Once
	release = func() { once.Do(func() { close(releaseCh) }) }
	t.Cleanup(release)
	return release
}

// Joining an ensure flight is the dedup working: one download shared by the
// herd, failure included. Retrying per waiter would multiply full-size
// downloads under a persistent failure.
func TestEnsureLayerJoiningFailedEnsureSharesError(t *testing.T) {
	_, host := newTestRegistry(t)
	store := newTestStore(t)
	layer, diffID, dir := seedLayer(t, store, host+"/test/join-pullfail:latest")
	wantErr := errors.New("while unpacking layer: connection refused")

	release := holdEnsureFlight(t, store, diffID, func() error {
		return errors.Join(os.RemoveAll(dir), wantErr)
	})
	done := ensureLayerAsync(store, diffID, layer)
	waitForBlockedEnsure(t, "sync.(*WaitGroup).Wait")
	release()

	if got := <-done; !errors.Is(got.err, wantErr) {
		t.Fatalf("ensureLayer = %v, want the joined flight's error %v", got.err, wantErr)
	}
}

// Distinct layers must never share an interlock. It is held across a whole
// unpack, so sharing would serialize unrelated multi-GiB downloads and make
// a retirement veto — and so re-date for a whole min-age window — a layer it
// could have taken.
func TestLayerLocksAreIndependentPerLayer(t *testing.T) {
	store := newTestStore(t)
	a, b := strings.Repeat("ab", 32), strings.Repeat("cd", 32)

	lockA, lockAgain, lockB := store.layerLock(a), store.layerLock(a), store.layerLock(b)
	if lockA != lockAgain {
		t.Fatal("one layer resolved to two different interlocks")
	}
	if lockA == lockB {
		t.Fatal("two distinct layers share one interlock")
	}
	if !store.layerLock(a).TryAcquire(1) {
		t.Fatal("could not take the first layer's interlock")
	}
	defer store.layerLock(a).Release(1)
	if !store.layerLock(b).TryAcquire(1) {
		t.Fatal("holding one layer's interlock blocked an unrelated layer")
	}
	store.layerLock(b).Release(1)
}

// A cancelled context must not fail a lookup whose layer is already in the
// pool: the semaphore fails a cancelled Acquire even when the lock is free,
// and singleflight would hand that failure to healthy joiners. Reachable
// when a sibling layer's failure cancels the pull's errgroup while queued
// ensureLayer calls are still starting.
func TestEnsureLayerCachedLayerSurvivesCancelledContext(t *testing.T) {
	_, host := newTestRegistry(t)
	store := newTestStore(t)
	layer, diffID, dir := seedLayer(t, store, host+"/test/cancelled-ctx:latest")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := store.ensureLayer(ctx, diffID, layer)
	if err != nil {
		t.Fatalf("ensureLayer on a cached layer with a cancelled ctx: %v", err)
	}
	if got != dir {
		t.Errorf("ensureLayer = %q, want %q", got, dir)
	}

	// Only the hit is served: with the layer absent, the cancelled ctx must
	// fail the call rather than start a download — the layer streams are
	// bound to the pull's parent ctx, so nothing would cancel it.
	if err := RemoveAllWritable(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ensureLayer(ctx, diffID, layer); !errors.Is(err, context.Canceled) {
		t.Fatalf("ensureLayer on an absent layer with a cancelled ctx = %v, want context.Canceled", err)
	}
}
