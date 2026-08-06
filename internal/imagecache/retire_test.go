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
