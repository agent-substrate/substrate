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
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	kubeinformers "k8s.io/client-go/informers"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/pkg/api/v1alpha1"
	atefake "go.ate.dev/substrate/pkg/client/clientset/versioned/fake"
	"go.ate.dev/substrate/pkg/client/informers/externalversions"
)

// pushPauseImage pushes a tiny image to ref and returns its manifest digest,
// so tests can later assert a cache hit by digest with the registry gone.
func pushPauseImage(t *testing.T, ref string) v1.Hash {
	t.Helper()
	img, err := mutate.AppendLayers(empty.Image, singleFileLayer(t, "pause", "pause bytes"))
	if err != nil {
		t.Fatalf("mutate.AppendLayers: %v", err)
	}
	digest, err := img.Digest()
	if err != nil {
		t.Fatalf("img.Digest: %v", err)
	}
	tag, err := name.ParseReference(ref, name.Insecure)
	if err != nil {
		t.Fatalf("name.ParseReference(%q): %v", ref, err)
	}
	if err := remote.Write(tag, img); err != nil {
		t.Fatalf("remote.Write(%q): %v", ref, err)
	}
	return digest
}

func gvisorConfig(name, url, sha string) *v1alpha1.SandboxConfig {
	return &v1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.SandboxConfigSpec{
			SandboxClass:   v1alpha1.SandboxClassGvisor,
			DefaultVersion: "v1",
			Versions: []v1alpha1.SandboxVersionConfig{{
				Name:       "v1",
				PauseImage: "registry.k8s.io/pause@sha256:abc",
				Assets: map[string]map[string]v1alpha1.AssetFile{
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

// pauseImageInTestRegistry pushes a pause image to a test registry and
// returns its digest-pinned reference, the only form a runtime hash accepts.
func pauseImageInTestRegistry(t *testing.T) (ref string, host string) {
	t.Helper()
	host = imageVolumeTestRegistry(t)
	digest := pushPauseImage(t, host+"/pause:3.10")
	return host + "/pause@" + digest.String(), host
}

// indexedTestRuntime registers a runtime with one gvisor release asset, the
// tarball content, and the given pause image as a worker's default, and
// returns its hash.
func indexedTestRuntime(t *testing.T, index *runtimeIndex, content []byte, pauseImage string) string {
	t.Helper()
	return indexedTestRuntimeAt(t, index, "gs://bucket/gvisor.tar", content, pauseImage)
}

// indexedTestRuntimeAt is indexedTestRuntime with the asset at url.
func indexedTestRuntimeAt(t *testing.T, index *runtimeIndex, url string, content []byte, pauseImage string) string {
	t.Helper()
	rec := &sandboxAssetsRecord{
		SandboxClass: "gvisor",
		PauseImage:   pauseImage,
		Assets:       map[string]assetEntry{gvisorAssetName: {URL: url, SHA256: fmt.Sprintf("%x", sha256.Sum256(content))}},
	}
	hash, err := versionBundleHash(rec)
	if err != nil {
		t.Fatal(err)
	}
	index.register("pod-a", advertisedClass{class: v1alpha1.SandboxClassGvisor, config: "gvisor-release", hashed: []hashedVersion{{
		versionPlan: versionPlan{name: "v1", isDefault: true, rec: rec}, hash: hash,
	}}})
	return hash
}

// syncItem makes the directory the syncer would make for one sync of pod-a
// under root, so the links land on the cache's filesystem, named in sync
// order like newDefaultRuntimeDir names them, and returns the item the
// syncer would hand the prewarmer for it.
func syncItem(t *testing.T, root string, n int, hashes ...string) prewarmItem {
	t.Helper()
	dir := filepath.Join(root, "workers", "pod-a", defaultRuntimeDirsName, fmt.Sprintf("%016x", n))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return prewarmItem{dir: dir, hashes: strings.Join(hashes, hashSep)}
}

// runtimesOf looks the item's runtimes up, as process does before prewarm.
func runtimesOf(t *testing.T, index *runtimeIndex, item prewarmItem) map[string]*indexedRuntime {
	t.Helper()
	out := map[string]*indexedRuntime{}
	for _, hash := range item.hashList() {
		rt, ok := index.lookup(hash)
		if !ok {
			t.Fatalf("runtime %s not indexed", hash)
		}
		out[hash] = rt
	}
	return out
}

func newTestImageStore(t *testing.T) *imagecache.Store {
	t.Helper()
	store, err := imagecache.New(t.TempDir())
	if err != nil {
		t.Fatalf("imagecache.New: %v", err)
	}
	return store
}

// TestRuntimePrewarmProcess covers the worker's failure handling: a failed
// prewarm is requeued with backoff until prewarmMaxRetries and then dropped,
// and an item naming a hash that is not in the index is dropped at once.
func TestRuntimePrewarmProcess(t *testing.T) {
	ctx := context.Background()
	index := newRuntimeIndex()
	hash := indexedTestRuntime(t, index, gvisorReleaseTarball(t), "")
	root := t.TempDir()
	herder := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: fakeObjectStorage{err: errors.New("bucket unavailable")}}
	p := newRuntimePrewarmer(index, herder, newTestImageStore(t))

	item := syncItem(t, root, 1, hash)
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

	unknown := prewarmItem{dir: item.dir, hashes: "sha256:not-in-the-index"}
	p.process(ctx, unknown)
	if got := p.queue.NumRequeues(unknown); got != 0 {
		t.Errorf("NumRequeues for an unknown hash = %d, want 0 (forgotten)", got)
	}
}

// TestRuntimePrewarmSuperseded pins what a prewarm does once a later sync
// of the worker, or the worker's removal, made it moot: no fetch, nothing
// recreated, nothing removed, and no retry. The later sync's prewarm then
// does the work and removes the earlier directory, whichever order the two
// run in.
func TestRuntimePrewarmSuperseded(t *testing.T) {
	ctx := context.Background()
	index := newRuntimeIndex()
	content := gvisorReleaseTarball(t)
	hash := indexedTestRuntime(t, index, content, "")
	root := t.TempDir()
	herder := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root, filecache.WithMinAge(0)), anonGCSClient: fakeObjectStorage{data: content}}
	p := newRuntimePrewarmer(index, herder, newTestImageStore(t))

	t.Run("by a later sync, run out of order", func(t *testing.T) {
		first, second := syncItem(t, root, 1, hash), syncItem(t, root, 2, hash)
		// The earlier sync's prewarm runs after the later sync was planned,
		// as when its download was still queued: it must not link, and
		// must not remove the later directory.
		p.process(ctx, first)
		if got := p.queue.NumRequeues(first); got != 0 {
			t.Errorf("NumRequeues after a superseded prewarm = %d, want 0 (not retried)", got)
		}
		if n := cachedAssets(t, root); n != 0 {
			t.Errorf("a superseded prewarm fetched %d assets, want 0", n)
		}
		if !exists(second.dir) {
			t.Fatal("a superseded prewarm removed the later sync's directory")
		}
		p.process(ctx, second)
		if _, err := os.Stat(filepath.Join(defaultRuntimeSubdir(second.dir, hash), gvisorAssetName, "runsc")); err != nil {
			t.Errorf("the later sync's prewarm did not link the runtime: %v", err)
		}
		if exists(first.dir) {
			t.Error("the later sync's prewarm left the earlier directory")
		}
	})

	t.Run("by the worker's removal", func(t *testing.T) {
		item := syncItem(t, root, 3, hash)
		if err := os.RemoveAll(filepath.Join(root, "workers", "pod-a")); err != nil {
			t.Fatal(err)
		}
		p.process(ctx, item)
		if got := p.queue.NumRequeues(item); got != 0 {
			t.Errorf("NumRequeues after the worker went = %d, want 0 (not retried)", got)
		}
		if exists(filepath.Join(root, "workers", "pod-a")) {
			t.Error("a prewarm recreated the directory of a worker that is gone")
		}
	})

	t.Run("by the worker's removal while staging", func(t *testing.T) {
		// The worker goes after the sibling check passed: the staging must
		// fail rather than recreate the worker's directory, and the item
		// ends quietly.
		item := syncItem(t, root, 4, hash)
		removing := &removingFetcher{inner: herder, remove: filepath.Join(root, "workers", "pod-a")}
		q := newRuntimePrewarmer(index, removing, newTestImageStore(t))
		q.process(ctx, item)
		if got := q.queue.NumRequeues(item); got != 0 {
			t.Errorf("NumRequeues after the worker went mid-staging = %d, want 0 (not retried)", got)
		}
		if exists(filepath.Join(root, "workers", "pod-a")) {
			t.Error("a prewarm recreated the directory of a worker removed while it staged")
		}
	})
}

// removingFetcher removes a directory the moment staging starts, standing in
// for a worker deleted after the prewarm's sibling check.
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

// TestRuntimePrewarmFillsBothCaches covers a successful prewarm: the asset
// lands in the asset cache, linked into the sync's directory so an
// eviction pass keeps it, and the pause image lands in the image cache.
func TestRuntimePrewarmFillsBothCaches(t *testing.T) {
	ctx := context.Background()
	pauseRef, _ := pauseImageInTestRegistry(t)
	content := gvisorReleaseTarball(t)
	index := newRuntimeIndex()
	hash := indexedTestRuntime(t, index, content, pauseRef)

	root := t.TempDir()
	images := newTestImageStore(t)
	// No min age, so the only thing standing between the entry and a pass is
	// the sync directory's link.
	s := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root, filecache.WithMinAge(0)), anonGCSClient: fakeObjectStorage{data: content}}
	p := newRuntimePrewarmer(index, s, images)
	item := syncItem(t, root, 1, hash)
	if err := p.prewarm(ctx, item, runtimesOf(t, index, item)); err != nil {
		t.Fatalf("prewarm: %v", err)
	}
	if n := cachedAssets(t, root); n != 1 {
		t.Errorf("asset cache holds %d assets after prewarm, want 1", n)
	}
	if _, err := os.Stat(filepath.Join(defaultRuntimeSubdir(item.dir, hash), gvisorAssetName, "runsc")); err != nil {
		t.Errorf("prewarm did not link the runtime's asset into the sync's directory: %v", err)
	}
	if _, err := images.EnsureImage(ctx, pauseRef); err != nil {
		t.Errorf("pause image not prewarmed into the image cache: %v", err)
	}
	stats, err := s.sandboxAssetCache.EvictUnused(ctx, 1<<40, false)
	if err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if stats.Retired != 0 || stats.SkippedLinked != 1 || cachedAssets(t, root) != 1 {
		t.Errorf("after an eviction pass: stats=%+v, cached=%d; want the linked asset kept", stats, cachedAssets(t, root))
	}
}

// TestRuntimePrewarmReplacesEarlierSyncs pins the lifetime of the links: a
// prewarm that staged its sync removes the directories of the worker's
// earlier syncs, with the entry linked throughout, and the links go with
// the worker's directory, which makes the assets evictable.
func TestRuntimePrewarmReplacesEarlierSyncs(t *testing.T) {
	ctx := context.Background()
	index := newRuntimeIndex()
	content := gvisorReleaseTarball(t)
	hash := indexedTestRuntime(t, index, content, "")
	root := t.TempDir()
	s := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root, filecache.WithMinAge(0)), anonGCSClient: fakeObjectStorage{data: content}}
	p := newRuntimePrewarmer(index, s, newTestImageStore(t))

	first := syncItem(t, root, 1, hash)
	if err := p.prewarm(ctx, first, runtimesOf(t, index, first)); err != nil {
		t.Fatal(err)
	}
	second := syncItem(t, root, 2, hash)
	if err := p.prewarm(ctx, second, runtimesOf(t, index, second)); err != nil {
		t.Fatal(err)
	}
	if exists(first.dir) {
		t.Error("the earlier sync's directory survived the later sync's prewarm")
	}
	if _, err := os.Stat(filepath.Join(defaultRuntimeSubdir(second.dir, hash), gvisorAssetName, "runsc")); err != nil {
		t.Errorf("the later sync's directory lacks the asset: %v", err)
	}
	if stats, _ := s.sandboxAssetCache.EvictUnused(ctx, 1<<40, true); stats.Retired != 0 {
		t.Errorf("a pass would retire %d entries while the worker links its default, want 0", stats.Retired)
	}

	// The worker goes, and its directory with it.
	if err := os.RemoveAll(filepath.Join(root, "workers", "pod-a")); err != nil {
		t.Fatal(err)
	}
	if stats, _ := s.sandboxAssetCache.EvictUnused(ctx, 1<<40, true); stats.Retired != 1 {
		t.Errorf("after the worker went, a pass would retire %d entries, want 1", stats.Retired)
	}
}

// TestRuntimePrewarmStagesEverySyncRuntime pins a sync with several default
// runtimes, as a pool running several sandbox classes will have: the
// earlier sync's directory stays until every runtime of the new sync is
// linked, so no default is unprotected in between.
func TestRuntimePrewarmStagesEverySyncRuntime(t *testing.T) {
	ctx := context.Background()
	index := newRuntimeIndex()
	contentA, contentB := gvisorReleaseTarball(t), gvisorReleaseTarballWith(t, "another runsc")
	hashA := indexedTestRuntimeAt(t, index, "gs://bucket/a.tar", contentA, "")
	hashB := indexedTestRuntimeAt(t, index, "gs://bucket/b.tar", contentB, "")
	root := t.TempDir()
	storage := &recordingObjectStorage{objects: map[string][]byte{"bucket/a.tar": contentA}}
	s := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root, filecache.WithMinAge(0)), anonGCSClient: storage}
	p := newRuntimePrewarmer(index, s, newTestImageStore(t))

	earlier := syncItem(t, root, 1, hashA)
	if err := p.prewarm(ctx, earlier, runtimesOf(t, index, earlier)); err != nil {
		t.Fatal(err)
	}
	// B cannot be fetched yet: the sync fails, A is linked in the new
	// directory already, and the earlier directory stays.
	later := syncItem(t, root, 2, hashA, hashB)
	if err := p.prewarm(ctx, later, runtimesOf(t, index, later)); err == nil {
		t.Fatal("prewarm = nil with one runtime of the sync unavailable, want an error")
	}
	if !exists(earlier.dir) {
		t.Error("the earlier sync's directory was removed before every runtime of the later sync was linked")
	}
	storage.mu.Lock()
	storage.objects["bucket/b.tar"] = contentB
	storage.mu.Unlock()
	if err := p.prewarm(ctx, later, runtimesOf(t, index, later)); err != nil {
		t.Fatalf("prewarm retry: %v", err)
	}
	for _, hash := range []string{hashA, hashB} {
		if _, err := os.Stat(filepath.Join(defaultRuntimeSubdir(later.dir, hash), gvisorAssetName, "runsc")); err != nil {
			t.Errorf("runtime %s not linked in the sync's directory: %v", hash, err)
		}
	}
	if exists(earlier.dir) {
		t.Error("the earlier sync's directory survived the completed later sync")
	}
}

// TestRuntimePrewarmAssetFailureStillPullsImage pins that the two halves
// are independent: a failing asset fetch does not stop the pause image pull.
func TestRuntimePrewarmAssetFailureStillPullsImage(t *testing.T) {
	ctx := context.Background()
	pauseRef, _ := pauseImageInTestRegistry(t)
	index := newRuntimeIndex()
	hash := indexedTestRuntime(t, index, gvisorReleaseTarball(t), pauseRef)

	images := newTestImageStore(t)
	root := t.TempDir()
	p := newRuntimePrewarmer(index, &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: fakeObjectStorage{err: errors.New("bucket unavailable")}}, images)
	item := syncItem(t, root, 1, hash)
	if err := p.prewarm(ctx, item, runtimesOf(t, index, item)); err == nil {
		t.Error("prewarm returned nil despite the asset fetch failing")
	}
	if _, err := images.EnsureImage(ctx, pauseRef); err != nil {
		t.Errorf("pause image not pulled when the asset fetch failed: %v", err)
	}
}

// TestRuntimePrewarmAttemptTimeout verifies a single attempt is bounded by
// the prewarmer's attempt timeout: the queue has one worker, so an attempt
// that never returned would block every other runtime's prewarm.
func TestRuntimePrewarmAttemptTimeout(t *testing.T) {
	index := newRuntimeIndex()
	hash := indexedTestRuntime(t, index, gvisorReleaseTarball(t), "")
	root := t.TempDir()
	p := newRuntimePrewarmer(index, &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: hangingObjectStorage{}}, newTestImageStore(t))
	p.attemptTimeout = 50 * time.Millisecond

	item := syncItem(t, root, 1, hash)
	done := make(chan error, 1)
	go func() { done <- p.prewarm(context.Background(), item, runtimesOf(t, index, item)) }()
	select {
	case err := <-done:
		if err == nil {
			t.Error("prewarm returned nil despite the download hanging")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("prewarm never returned; a hung download would block the worker forever")
	}
}

// TestSyncPodPrewarmsDefaultRuntime pins the hand-off: a worker's default
// runtime, and only that, is handed to the prewarmer in a numbered sync
// directory under the pod's, and before the build fetches anything for its
// probes, so the probes are the prewarm's head start. A build that then
// fails has still handed it over. Each sync gets the next number, counted
// on from what a previous atelet left, and the pod's removal takes them
// all.
func TestSyncPodPrewarmsDefaultRuntime(t *testing.T) {
	pod := workerPod("pod-a", "pool")
	s := newTestSyncer(t, pod, resolvedPool("pool", "gvisor-release"), multiVersionConfig())
	rec := &recordingPrewarm{}
	s.prewarm = rec
	s.assets = &fakeAssetFetcher{
		fn: func(ctx context.Context, r *sandboxAssetsRecord) (map[string]string, error) {
			if rec.count() == 0 {
				t.Error("the build fetched for a probe before the default runtime was handed to the prewarmer")
			}
			return newTestAssetFetcher().ensureSandboxAssets(ctx, r, "")
		},
	}
	// Left by a previous atelet: the numbering continues after it.
	syncs := filepath.Join(s.podDir("pod-a"), defaultRuntimeDirsName)
	if err := os.MkdirAll(filepath.Join(syncs, fmt.Sprintf("%016x", 5)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := s.syncPod(context.Background(), pod); err != nil {
		t.Fatalf("syncPod: %v", err)
	}
	wr, ok := s.index.worker("pod-a")
	if !ok {
		t.Fatal("pod-a not indexed")
	}
	cr, _ := wr.class(v1alpha1.SandboxClassGvisor)
	if got, want := rec.items(), []prewarmItem{{dir: filepath.Join(syncs, fmt.Sprintf("%016x", 6)), hashes: cr.defaultHash}}; !slices.Equal(got, want) {
		t.Errorf("prewarm handed %+v, want %+v", got, want)
	}
	if !exists(filepath.Join(syncs, fmt.Sprintf("%016x", 6))) {
		t.Error("the sync's directory was not created")
	}
	if len(cr.hashes) < 2 {
		t.Fatalf("config advertises %d runtimes, want several so the test proves only the default is prewarmed", len(cr.hashes))
	}
	// A re-sync hands the default over again, in the next directory.
	if err := s.syncPod(context.Background(), pod); err != nil {
		t.Fatalf("syncPod again: %v", err)
	}
	if got := rec.items(); len(got) != 2 || got[1].dir != filepath.Join(syncs, fmt.Sprintf("%016x", 7)) {
		t.Errorf("prewarm handed %+v after a re-sync, want a second item in the next directory", got)
	}
	if err := s.removeDir(context.Background(), "pod-a"); err != nil {
		t.Fatal(err)
	}
	if exists(syncs) {
		t.Error("the sync directories survived the pod's removal")
	}

	// A build that fails on the default's probe writes nothing, but the
	// prewarm was queued before it started.
	s = newTestSyncer(t, pod, resolvedPool("pool", "gvisor-release"), multiVersionConfig())
	rec = &recordingPrewarm{}
	s.prewarm = rec
	s.runCPUFeatures = func(context.Context, string) ([]string, error) { return nil, errors.New("runsc failed") }
	if err := s.syncPod(context.Background(), pod); err == nil {
		t.Fatal("syncPod with the default's probe failing = nil, want an error")
	}
	if got := rec.items(); len(got) != 1 || got[0].hashes != cr.defaultHash {
		t.Errorf("prewarm handed %+v after a failed build, want the default runtime %s once", got, cr.defaultHash)
	}
	if _, ok := s.index.worker("pod-a"); ok {
		t.Error("a failed build registered the worker")
	}
}

// recordingPrewarm stands in for the prewarmer in syncer tests. Safe to
// read while a syncer's workers call it.
type recordingPrewarm struct {
	mu       sync.Mutex
	enqueued []prewarmItem
}

func (r *recordingPrewarm) enqueue(dir string, hashes []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.enqueued = append(r.enqueued, prewarmItem{dir: dir, hashes: strings.Join(hashes, hashSep)})
}

func (r *recordingPrewarm) items() []prewarmItem {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]prewarmItem(nil), r.enqueued...)
}

func (r *recordingPrewarm) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.enqueued)
}

// TestDefaultRuntimePrewarmThroughSyncer runs the whole path: a worker pod,
// its pool and SandboxConfig in fake clientsets flow through the informers
// into the syncer, which indexes the pod's runtimes, writes its file, and
// hands its default runtime to the prewarmer, which lands the asset in the
// cache, linked under the worker's directory, without any Run/Restore
// request.
func TestDefaultRuntimePrewarmThroughSyncer(t *testing.T) {
	useTempAssetScratch(t) // the syncer's runsc probe stages into it
	// The worker's own prewarm call returns only when the asset has landed
	// and the pause-image pull has finished. Waiting for the call keeps the
	// pull's last writes from racing the TempDir cleanup.
	prewarmed := make(chan error, 1)
	onFinished := func(_ prewarmItem, err error) {
		// Non-blocking: a failed attempt is retried, and the worker must not
		// block on a result nobody reads.
		select {
		case prewarmed <- err:
		default:
		}
	}

	pauseRef, _ := pauseImageInTestRegistry(t)
	content := gvisorReleaseTarball(t)
	cfg := gvisorConfig("gvisor-release", "gs://bucket/gvisor.tar", fmt.Sprintf("%x", sha256.Sum256(content)))
	cfg.Spec.Versions[0].PauseImage = pauseRef

	kube := kubefake.NewClientset([]k8sruntime.Object{workerPod("pod-a", "pool")}...)
	ate := atefake.NewSimpleClientset(resolvedPool("pool", "gvisor-release"), cfg)
	podFactory := kubeinformers.NewSharedInformerFactory(kube, 0)
	ateFactory := externalversions.NewSharedInformerFactory(ate, 0)
	ctx, cancel := context.WithCancel(context.Background())

	root := t.TempDir()
	workersDir := filepath.Join(root, "workers") // on the cache's filesystem
	index := newRuntimeIndex()
	herder := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: fakeObjectStorage{data: content}}
	p := newRuntimePrewarmer(index, herder, newTestImageStore(t))
	p.onFinished = onFinished
	go p.run(ctx)
	// Join the worker before the TempDir cleanups remove what it writes into.
	t.Cleanup(func() {
		cancel()
		p.wait()
		podFactory.Shutdown()
		ateFactory.Shutdown()
	})

	s, err := startWorkerSandboxSyncer(ctx,
		podFactory.Core().V1().Pods(),
		ateFactory.Api().V1alpha1().WorkerPools(),
		ateFactory.Api().V1alpha1().SandboxConfigs(),
		newScriptAssetFetcher(t), // the syncer's runsc probe; prewarm goes through herder
		workersDir, index, p)
	if err != nil {
		t.Fatalf("startWorkerSandboxSyncer: %v", err)
	}
	ateFactory.Start(ctx.Done())
	podFactory.Start(ctx.Done())

	select {
	case err := <-prewarmed:
		if err != nil {
			t.Fatalf("prewarm: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("prewarm never finished")
	}
	if n := cachedAssets(t, root); n != 1 {
		t.Fatalf("asset cache holds %d assets after prewarm, want 1", n)
	}
	// The prewarm is queued before the build probes, so it can finish
	// before the file is written; the syncer catches up on its own.
	waitFor(t, "the worker's runtimes file to be written", func() bool { return exists(s.runtimesFile("pod-a")) })
	waitFor(t, "the probe's scratch dir to be removed", func() bool {
		left, err := os.ReadDir(ateletpath.SandboxAssetScratchDir)
		return err == nil && len(left) == 0
	})
	syncs, err := os.ReadDir(filepath.Join(s.podDir("pod-a"), defaultRuntimeDirsName))
	if err != nil || len(syncs) != 1 {
		t.Fatalf("sync directories under the worker's = %v, %v; want exactly one", syncs, err)
	}
	wr, _ := index.worker("pod-a")
	cr, _ := wr.class(v1alpha1.SandboxClassGvisor)
	linked := filepath.Join(defaultRuntimeSubdir(filepath.Join(s.podDir("pod-a"), defaultRuntimeDirsName, syncs[0].Name()), cr.defaultHash), gvisorAssetName, "runsc")
	if _, err := os.Stat(linked); err != nil {
		t.Errorf("the worker's default runtime is not linked under its directory: %v", err)
	}
}
