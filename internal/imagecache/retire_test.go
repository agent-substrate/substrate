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

// The retire/reuse interlock depends on retireLayer and ensureLayer using
// the same singleflight key; pin the key format to diffID.String()'s.
func TestLayerFlightKeyMatchesDiffIDString(t *testing.T) {
	hex := strings.Repeat("ab", 32)
	want := v1.Hash{Algorithm: "sha256", Hex: hex}.String()
	if got := layerFlightKey(hex); got != want {
		t.Errorf("layerFlightKey = %q, want %q", got, want)
	}
}

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
// image using the same layer. The singleflight serializes the retire
// rename against unpack, and the in-flight mtime touch turns concurrent
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

// joinLayerFlight seeds a layer, then holds its singleflight open running
// body, so that an ensureLayer called while it is held has to join rather
// than lead. Returns the layer, its diffID, its dir, and a release func.
func joinLayerFlight(t *testing.T, store *Store, ref string, body func(dir string) error) (v1.Layer, v1.Hash, string, func()) {
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
	dir := layerDirOf(t, store, layer)

	held, release := make(chan struct{}), make(chan struct{})
	go func() {
		_, _, _ = store.layerSF.Do(layerFlightKey(diffID.Hex), func() (any, error) {
			err := body(dir)
			close(held)
			<-release
			return nil, err
		})
	}()
	<-held
	var once sync.Once
	releaseOnce := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseOnce) // never leave the flight held at test exit
	return layer, diffID, dir, releaseOnce
}

// waitForFlightJoiner blocks until some goroutine is parked inside
// ensureLayer waiting on a singleflight it joined. Observing the join is what
// makes these tests deterministic: releasing on a timer instead would let a
// loaded machine close the flight early, so ensureLayer would lead its own
// and the join path would go untested while the test still passed.
func waitForFlightJoiner(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	buf := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		dump := string(buf[:runtime.Stack(buf, true)])
		for _, g := range strings.Split(dump, "\n\ngoroutine ") {
			if strings.Contains(g, "ensureLayer") && strings.Contains(g, "sync.(*WaitGroup).Wait") {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no goroutine ever joined the layer flight")
}

// ensureLayer must never hand back a dir that a retirement renamed away.
// Joining a flight runs no closure, so the reuse/retire interlock says
// nothing about the dir in that case — the layer has to be unpacked again.
//
// Deterministic on purpose: the natural window is a few microseconds wide, so
// a timing-dependent test would only catch this under load (it surfaced as a
// rare "layer dir vanished during pull" in CI).
func TestEnsureLayerJoiningRetireFlightRepacksLayer(t *testing.T) {
	_, host := newTestRegistry(t)
	store := newTestStore(t)
	// Rename the layer aside inside the flight, exactly as retireLayer does.
	layer, diffID, _, release := joinLayerFlight(t, store, host+"/test/join-retire:latest",
		func(dir string) error { return os.Rename(dir, dir+"-retired") })

	got, err := ensureLayerWhileHeld(t, store, diffID, layer, release)
	if err != nil {
		t.Fatalf("ensureLayer: %v", err)
	}
	if _, err := os.Stat(filepath.Join(got, layerFSDirName)); err != nil {
		t.Fatalf("ensureLayer returned a retired dir: %v", err)
	}
}

// A joined flight's error is not this caller's error. retireLayer reports a
// failed rename through the shared flight, but that leaves the layer on disk
// and usable, so a joiner must return it rather than fail the pull.
func TestEnsureLayerJoiningFailedFlightKeepsLiveLayer(t *testing.T) {
	_, host := newTestRegistry(t)
	store := newTestStore(t)
	// Leave the dir intact and fail the flight, as a retirement whose rename
	// fails does.
	layer, diffID, dir, release := joinLayerFlight(t, store, host+"/test/join-failed:latest",
		func(string) error { return errors.New("while retiring layer: rename failed") })

	got, err := ensureLayerWhileHeld(t, store, diffID, layer, release)
	if err != nil {
		t.Fatalf("ensureLayer failed on a joined flight's error: %v", err)
	}
	if got != dir {
		t.Errorf("ensureLayer = %q, want the live layer dir %q", got, dir)
	}
	if _, err := os.Stat(filepath.Join(got, layerFSDirName)); err != nil {
		t.Errorf("returned dir is not the live layer: %v", err)
	}
}

// ensureLayerWhileHeld runs ensureLayer against a held flight, releasing the
// flight only once the call is observably blocked joining it.
func ensureLayerWhileHeld(t *testing.T, store *Store, diffID v1.Hash, layer v1.Layer, release func()) (string, error) {
	t.Helper()
	type result struct {
		dir string
		err error
	}
	done := make(chan result, 1)
	go func() {
		dir, err := store.ensureLayer(context.Background(), diffID, layer)
		done <- result{dir, err}
	}()
	waitForFlightJoiner(t)
	release()
	res := <-done
	return res.dir, res.err
}
