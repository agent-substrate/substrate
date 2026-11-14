// Copyright 2026 The Agent Substrate Authors
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
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
)

func gvisorConfig(name, url, sha string) *atev1.SandboxConfig {
	return &atev1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: atev1.SandboxConfigSpec{
			SandboxClass:   atev1.SandboxClassGvisor,
			DefaultVersion: "v1",
			Versions: []atev1.SandboxVersionConfig{{
				Name: "v1",
				Assets: map[string]map[string]atev1.AssetFile{
					runtime.GOARCH: {
						gvisorAssetName: {URL: url, SHA256: sha},
					},
				},
			}},
		},
	}
}

// hangingObjectStorage blocks GetObject until the caller's context ends,
// simulating a bucket read that stalls without failing.
type hangingObjectStorage struct{}

func (hangingObjectStorage) GetObject(ctx context.Context, _, _ string) (io.ReadCloser, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (hangingObjectStorage) PutObject(_ context.Context, _, _ string, _ io.Reader) error { return nil }

// indexedTestRuntime registers a runtime with one gvisor release asset, the
// tarball content, as a worker's default, and returns its hash.
func indexedTestRuntime(t *testing.T, index *runtimeIndex, content []byte) string {
	t.Helper()
	return indexedTestRuntimeAt(t, index, "gs://bucket/gvisor.tar", content)
}

// indexedTestRuntimeAt is indexedTestRuntime with the asset at url.
func indexedTestRuntimeAt(t *testing.T, index *runtimeIndex, url string, content []byte) string {
	t.Helper()
	rec := &sandboxAssetsRecord{
		SandboxClass: "gvisor",
		Assets:       map[string]assetEntry{gvisorAssetName: {URL: url, SHA256: fmt.Sprintf("%x", sha256.Sum256(content))}},
	}
	hash := bundleHash(rec)
	index.register("pod-a", advertisedClass{class: atev1.SandboxClassGvisor, config: "gvisor-release", hashed: []hashedVersion{{
		versionPlan: versionPlan{name: "v1", isDefault: true, rec: rec}, hash: hash,
	}}})
	return hash
}

// defaultItem makes the directory the syncer would make for hash as a
// default of pod-a under root, so the links land on the cache's
// filesystem, and returns the item the syncer would hand the prewarmer.
func defaultItem(t *testing.T, root, hash string) prewarmItem {
	t.Helper()
	dir := filepath.Join(root, "workers", "pod-a", defaultRuntimeDirsName, strings.ReplaceAll(hash, ":", "-"))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return prewarmItem{hash: hash, dir: dir}
}

// TestRuntimePrewarmProcess covers the worker's failure handling: a failed
// prewarm is requeued with backoff until prewarmMaxRetries and then dropped,
// and an item naming a hash that is not in the index is dropped at once.
func TestRuntimePrewarmProcess(t *testing.T) {
	ctx := context.Background()
	index := newRuntimeIndex()
	hash := indexedTestRuntime(t, index, gvisorReleaseTarball(t))
	root := t.TempDir()
	herder := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: fakeObjectStorage{err: errors.New("bucket unavailable")}}
	p := newRuntimePrewarmer(index, herder)

	item := defaultItem(t, root, hash)
	for i := 1; i <= prewarmMaxRetries; i++ {
		p.process(ctx, item)
		if got := p.queue.NumRequeues(item); got != i {
			t.Fatalf("NumRequeues after %d failed prewarms = %d, want %d (requeued with backoff)", i, got, i)
		}
	}
	p.process(ctx, item)
	if got := p.queue.NumRequeues(item); got != 0 {
		t.Errorf("NumRequeues after giving up = %d, want 0 (forgotten)", got)
	}

	unknown := prewarmItem{hash: "sha256:not-in-the-index", dir: item.dir}
	p.process(ctx, unknown)
	if got := p.queue.NumRequeues(unknown); got != 0 {
		t.Errorf("NumRequeues for an unknown hash = %d, want 0 (forgotten)", got)
	}
}

// TestRuntimePrewarmSuperseded pins what a prewarm does once the syncer
// removed its directory, after a later sync or the worker's removal: no
// fetch, nothing recreated, and no retry, whether the directory went
// before the item ran or while it staged.
func TestRuntimePrewarmSuperseded(t *testing.T) {
	ctx := context.Background()
	index := newRuntimeIndex()
	content := gvisorReleaseTarball(t)
	hash := indexedTestRuntime(t, index, content)
	root := t.TempDir()
	herder := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: fakeObjectStorage{data: content}}

	t.Run("directory gone before the item runs", func(t *testing.T) {
		item := defaultItem(t, root, hash)
		if err := os.RemoveAll(item.dir); err != nil {
			t.Fatal(err)
		}
		p := newRuntimePrewarmer(index, herder)
		p.process(ctx, item)
		if got := p.queue.NumRequeues(item); got != 0 {
			t.Errorf("NumRequeues after a superseded prewarm = %d, want 0 (not retried)", got)
		}
		if n := cachedAssets(t, root); n != 0 {
			t.Errorf("a superseded prewarm fetched %d assets, want 0", n)
		}
		if exists(item.dir) {
			t.Error("a superseded prewarm recreated its directory")
		}
	})

	t.Run("worker gone while staging", func(t *testing.T) {
		item := defaultItem(t, root, hash)
		removing := &removingFetcher{inner: herder, remove: filepath.Join(root, "workers", "pod-a")}
		p := newRuntimePrewarmer(index, removing)
		p.process(ctx, item)
		if got := p.queue.NumRequeues(item); got != 0 {
			t.Errorf("NumRequeues after the worker went mid-staging = %d, want 0 (not retried)", got)
		}
		if exists(filepath.Join(root, "workers", "pod-a")) {
			t.Error("a prewarm recreated the directory of a worker removed while it staged")
		}
	})
}

// removingFetcher removes a directory the moment staging starts, standing in
// for a worker deleted after the prewarm found its directory in place.
type removingFetcher struct {
	inner  sandboxAssetFetcher
	remove string
}

func (r *removingFetcher) ensureSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord, dstDir string) (map[string]string, error) {
	return r.inner.ensureSandboxAssets(ctx, rec, dstDir)
}

func (r *removingFetcher) stageSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord, dstDir string) (map[string]string, error) {
	if err := os.RemoveAll(r.remove); err != nil {
		return nil, err
	}
	return r.inner.stageSandboxAssets(ctx, rec, dstDir)
}

// TestRuntimePrewarmFillsTheAssetCache covers a successful prewarm: the asset
// lands in the asset cache, linked into the runtime's directory so an
// eviction pass keeps it. A second prewarm of the same directory fetches
// nothing.
func TestRuntimePrewarmFillsTheAssetCache(t *testing.T) {
	ctx := context.Background()
	content := gvisorReleaseTarball(t)
	index := newRuntimeIndex()
	hash := indexedTestRuntime(t, index, content)
	rt, _ := index.lookup(hash)

	root := t.TempDir()
	var gets atomic.Int32
	// No min age, so the only thing standing between the entry and a pass is
	// the directory's link.
	s := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root, filecache.WithMinAge(0)), anonGCSClient: fakeObjectStorage{data: content, gets: &gets}}
	p := newRuntimePrewarmer(index, s)
	item := defaultItem(t, root, hash)
	if err := p.prewarm(ctx, item, rt); err != nil {
		t.Fatalf("prewarm: %v", err)
	}
	if n := cachedAssets(t, root); n != 1 {
		t.Errorf("asset cache holds %d assets after prewarm, want 1", n)
	}
	if _, err := os.Stat(filepath.Join(item.dir, gvisorAssetName, "runsc")); err != nil {
		t.Errorf("prewarm did not link the runtime's asset into its directory: %v", err)
	}
	stats, err := s.sandboxAssetCache.EvictUnused(ctx, 1<<40, false)
	if err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if stats.Retired != 0 || stats.SkippedLinked != 1 || cachedAssets(t, root) != 1 {
		t.Errorf("after an eviction pass: stats=%+v, cached=%d; want the linked asset kept", stats, cachedAssets(t, root))
	}

	// A re-sync with the same default hands the same directory over again:
	// the staging finds every asset in place and downloads nothing.
	before := gets.Load()
	if err := p.prewarm(ctx, item, rt); err != nil {
		t.Fatalf("second prewarm: %v", err)
	}
	if got := gets.Load(); got != before {
		t.Errorf("a prewarm of a complete directory fetched %d times, want 0", got-before)
	}

	// The worker goes, and its directory with it: the entry is evictable.
	if err := os.RemoveAll(filepath.Join(root, "workers", "pod-a")); err != nil {
		t.Fatal(err)
	}
	if stats, _ := s.sandboxAssetCache.EvictUnused(ctx, 1<<40, true); stats.Retired != 1 {
		t.Errorf("after the worker went, a pass would retire %d entries, want 1", stats.Retired)
	}
}

// TestRuntimePrewarmAssetFailure pins that a failing asset fetch fails the
// prewarm.
func TestRuntimePrewarmAssetFailure(t *testing.T) {
	ctx := context.Background()
	index := newRuntimeIndex()
	hash := indexedTestRuntime(t, index, gvisorReleaseTarball(t))
	rt, _ := index.lookup(hash)

	root := t.TempDir()
	p := newRuntimePrewarmer(index, &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: fakeObjectStorage{err: errors.New("bucket unavailable")}})
	if err := p.prewarm(ctx, defaultItem(t, root, hash), rt); err == nil {
		t.Error("prewarm returned nil despite the asset fetch failing")
	}
}

// TestRuntimePrewarmAttemptTimeout verifies a single attempt is bounded by
// the prewarmer's attempt timeout: the queue has one worker, so an attempt
// that never returned would block every other runtime's prewarm.
func TestRuntimePrewarmAttemptTimeout(t *testing.T) {
	index := newRuntimeIndex()
	hash := indexedTestRuntime(t, index, gvisorReleaseTarball(t))
	rt, _ := index.lookup(hash)
	root := t.TempDir()
	p := newRuntimePrewarmer(index, &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: hangingObjectStorage{}})
	p.attemptTimeout = 50 * time.Millisecond

	item := defaultItem(t, root, hash)
	done := make(chan error, 1)
	go func() { done <- p.prewarm(context.Background(), item, rt) }()
	select {
	case err := <-done:
		if err == nil {
			t.Error("prewarm returned nil despite the download hanging")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("prewarm never returned; a hung download would block the worker forever")
	}
}
