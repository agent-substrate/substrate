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

// Mid-pull behavior tests against a registry whose blob downloads can be
// gated per digest, so a pull can be held "in flight" at a chosen layer
// while the test manipulates the store underneath it.

import (
	"archive/tar"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// gatedRegistry is an in-memory registry whose blob GETs block while their
// digest hex is gated.
type gatedRegistry struct {
	host string

	mu    sync.Mutex
	gates map[string]chan struct{}
}

func newGatedRegistry(t *testing.T) *gatedRegistry {
	t.Helper()
	g := &gatedRegistry{gates: map[string]chan struct{}{}}
	inner := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/blobs/") {
			g.mu.Lock()
			var ch chan struct{}
			for hex, c := range g.gates {
				if strings.Contains(r.URL.Path, hex) {
					ch = c
					break
				}
			}
			g.mu.Unlock()
			if ch != nil {
				<-ch
			}
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parsing registry URL: %v", err)
	}
	g.host = u.Host
	return g
}

// gate blocks future GETs of the layer's compressed blob until release.
func (g *gatedRegistry) gate(t *testing.T, layer v1.Layer) (release func()) {
	t.Helper()
	d, err := layer.Digest()
	if err != nil {
		t.Fatalf("layer digest: %v", err)
	}
	ch := make(chan struct{})
	g.mu.Lock()
	g.gates[d.Hex] = ch
	g.mu.Unlock()
	var once sync.Once
	release = func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release) // never leave a pull blocked at test exit
	return release
}

// waitFor polls until cond returns true or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func gatedTestLayers(t *testing.T) (free, gated1, gated2 v1.Layer) {
	t.Helper()
	free = layerFromEntries(t, []tarEntry{
		{name: "a", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("a", 2048)},
	})
	gated1 = layerFromEntries(t, []tarEntry{
		{name: "b", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("b", 2048)},
	})
	gated2 = layerFromEntries(t, []tarEntry{
		{name: "c", typeflag: tar.TypeReg, mode: 0o644, body: strings.Repeat("c", 2048)},
	})
	return free, gated1, gated2
}

func layerDirOf(t *testing.T, s *Store, layer v1.Layer) string {
	t.Helper()
	diffID, err := layer.DiffID()
	if err != nil {
		t.Fatalf("layer diffID: %v", err)
	}
	return s.layerDir(diffID)
}

// A record removed mid-pull must not let the pull return success with its
// layers unreferenced. The pull rewrites the record after the unpack loop,
// so even if eviction (or anything else) removed the pre-written record
// while the pull was in flight, success implies the record exists and the
// layers are reachable.
func TestPullRewritesRecordEvictedMidPull(t *testing.T) {
	reg := newGatedRegistry(t)
	free, gatedLayer, _ := gatedTestLayers(t)
	ref := reg.host + "/test/midpull:latest"
	pushImage(t, ref, v1.Config{}, free, gatedLayer)

	store := newTestStore(t)
	release := reg.gate(t, gatedLayer)

	done := make(chan error, 1)
	var img *Image
	go func() {
		var err error
		img, err = store.EnsureImage(context.Background(), ref)
		done <- err
	}()

	// Wait until the free layer has landed — the pull is now mid-flight,
	// holding only the pre-written record as protection.
	freeDir := layerDirOf(t, store, free)
	waitFor(t, "free layer to land", func() bool {
		_, err := os.Stat(filepath.Join(freeDir, layerFSDirName))
		return err == nil
	})

	// Simulate the wedged-pull eviction: the record disappears mid-pull.
	// (Real eviction would also retire the layers; the harder half for the
	// pull is the record, which nothing used to restore.)
	records, err := os.ReadDir(store.manifestsDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if !strings.HasPrefix(r.Name(), ".") {
			if err := os.Remove(filepath.Join(store.manifestsDir(), r.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}

	release()
	if err := <-done; err != nil {
		t.Fatalf("EnsureImage: %v", err)
	}

	// Success must imply a record: without the end-of-pull rewrite, the
	// layers just unpacked would be unreferenced (stranded until restart)
	// while the caller happily writes a bundle spec.
	if _, err := os.Stat(store.recordPath(img.Digest)); err != nil {
		t.Fatalf("pull returned success but its record is gone — layers stranded: %v", err)
	}
	// And the record must actually reach the layers: a cache probe sees a
	// complete image.
	cached, err := store.cachedImage(img.Digest)
	if err != nil || cached == nil {
		t.Fatalf("cachedImage after mid-pull eviction: %v, %v", cached, err)
	}
}

// The per-layer progress touch keeps a slow pull's record fresh mid-flight:
// freshness must come from layer completions, not only from the final
// rewrite — and that freshness must veto an eviction pass running while
// the pull is still in flight.
func TestProgressTouchKeepsSlowPullRecordFresh(t *testing.T) {
	reg := newGatedRegistry(t)
	free, gated1, gated2 := gatedTestLayers(t)
	ref := reg.host + "/test/slowpull:latest"
	pushImage(t, ref, v1.Config{}, free, gated1, gated2)

	store := newTestStore(t)
	release1 := reg.gate(t, gated1)
	release2 := reg.gate(t, gated2)

	done := make(chan error, 1)
	go func() {
		_, err := store.EnsureImage(context.Background(), ref)
		done <- err
	}()

	// After the free layer lands, age the record far past min-age while two
	// layers are still gated: the pull is now "slow".
	freeDir := layerDirOf(t, store, free)
	waitFor(t, "free layer to land", func() bool {
		_, err := os.Stat(filepath.Join(freeDir, layerFSDirName))
		return err == nil
	})
	var recPath string
	records, err := os.ReadDir(store.manifestsDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if !strings.HasPrefix(r.Name(), ".") {
			recPath = filepath.Join(store.manifestsDir(), r.Name())
		}
	}
	if recPath == "" {
		t.Fatal("no record mid-pull: record-first ordering broken")
	}
	backdate(t, recPath, 3*time.Hour)

	// Release ONE gated layer. Its completion must touch the record — while
	// the pull is still in flight on the other gated layer.
	release1()
	waitFor(t, "progress touch after a layer completes", func() bool {
		fi, err := os.Stat(recPath)
		return err == nil && time.Since(fi.ModTime()) < time.Hour
	})

	// The pull is still mid-flight: the freshened record must veto eviction.
	if _, err := store.EvictUnused(context.Background(), 1<<62, false); err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if _, err := os.Stat(recPath); err != nil {
		t.Fatalf("freshly-touched mid-pull record evicted: %v", err)
	}

	// Unblock the last layer and drain the pull.
	release2()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("EnsureImage after releasing gates: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("pull did not finish after gates released")
	}
}

// A layer dir yanked mid-pull despite everything must fail the pull
// cleanly — a retryable error — rather than returning LayerDirs that point
// at nothing.
func TestPullReverifyFailsCleanlyOnYankedLayer(t *testing.T) {
	reg := newGatedRegistry(t)
	free, gatedLayer, _ := gatedTestLayers(t)
	ref := reg.host + "/test/yank:latest"
	pushImage(t, ref, v1.Config{}, free, gatedLayer)

	store := newTestStore(t)
	release := reg.gate(t, gatedLayer)

	done := make(chan error, 1)
	go func() {
		_, err := store.EnsureImage(context.Background(), ref)
		done <- err
	}()

	freeDir := layerDirOf(t, store, free)
	waitFor(t, "free layer to land", func() bool {
		_, err := os.Stat(filepath.Join(freeDir, layerFSDirName))
		return err == nil
	})
	// Yank the landed layer out from under the pull (simulates the residual
	// hole the re-verify exists for).
	if err := RemoveAllWritable(freeDir); err != nil {
		t.Fatal(err)
	}

	release()
	err := <-done
	if err == nil {
		t.Fatal("EnsureImage returned success with a yanked layer dir")
	}
	if !strings.Contains(err.Error(), "vanished during pull") {
		t.Errorf("expected the re-verify error, got: %v", err)
	}
}

// The one behavior change visible without GC: an interrupted pull leaves a
// valid partial record — resumable progress — rather than nothing.
func TestInterruptedPullLeavesResumableRecord(t *testing.T) {
	reg := newGatedRegistry(t)
	free, gatedLayer, _ := gatedTestLayers(t)
	ref := reg.host + "/test/interrupted:latest"
	pushImage(t, ref, v1.Config{}, free, gatedLayer)

	store := newTestStore(t)
	release := reg.gate(t, gatedLayer)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := store.EnsureImage(ctx, ref)
		done <- err
	}()

	freeDir := layerDirOf(t, store, free)
	waitFor(t, "free layer to land", func() bool {
		_, err := os.Stat(filepath.Join(freeDir, layerFSDirName))
		return err == nil
	})

	cancel()
	if err := <-done; err == nil {
		t.Fatal("EnsureImage succeeded despite cancellation")
	}
	release()

	// The pre-written record survives the failure, referencing both layers.
	var recs []string
	entries, err := os.ReadDir(store.manifestsDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			recs = append(recs, e.Name())
		}
	}
	if len(recs) == 0 {
		t.Fatal("interrupted pull left no record")
	}
	b, err := os.ReadFile(filepath.Join(store.manifestsDir(), recs[0]))
	if err != nil {
		t.Fatal(err)
	}
	var rec imageRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		t.Fatalf("record left by interrupted pull does not parse: %v", err)
	}
	if len(rec.DiffIDs) != 2 {
		t.Errorf("record references %d layers, want 2 (the full image)", len(rec.DiffIDs))
	}

	// The completed layer is on disk; the gated one is not.
	if _, err := os.Stat(filepath.Join(freeDir, layerFSDirName)); err != nil {
		t.Errorf("completed layer gone after interrupted pull: %v", err)
	}
	if _, err := os.Stat(layerDirOf(t, store, gatedLayer)); !os.IsNotExist(err) {
		t.Errorf("gated layer present after interrupted pull: %v", err)
	}

	// A retry completes the image and yields a full cache hit.
	img, err := store.EnsureImage(context.Background(), ref)
	if err != nil {
		t.Fatalf("EnsureImage retry: %v", err)
	}
	cached, err := store.cachedImage(img.Digest)
	if err != nil || cached == nil {
		t.Fatalf("no complete cachedImage after retry: %v, %v", cached, err)
	}
}
