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
						runscAssetName: {URL: url, SHA256: sha},
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

// indexedTestRuntime registers a runtime with one runsc asset of content and
// the given pause image as a worker's default, and returns its hash.
func indexedTestRuntime(t *testing.T, index *runtimeIndex, content []byte, pauseImage string) string {
	t.Helper()
	rec := &sandboxAssetsRecord{
		SandboxClass: "gvisor",
		PauseImage:   pauseImage,
		Assets:       map[string]assetEntry{runscAssetName: {URL: "gs://bucket/runsc", SHA256: fmt.Sprintf("%x", sha256.Sum256(content))}},
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
// and a hash that is not in the index is dropped at once.
func TestRuntimePrewarmProcess(t *testing.T) {
	useTempAssetPins(t)
	ctx := context.Background()
	index := newRuntimeIndex()
	hash := indexedTestRuntime(t, index, []byte("runsc"), "")
	herder := &Atelet{sandboxAssetCache: newTestAssetCache(t), anonGCSClient: fakeObjectStorage{err: errors.New("bucket unavailable")}}
	p := newRuntimePrewarmer(index, herder, newTestImageStore(t))

	for i := 1; i <= prewarmMaxRetries; i++ {
		p.process(ctx, hash)
		if got := p.queue.NumRequeues(hash); got != i {
			t.Fatalf("NumRequeues after %d failed prewarms = %d, want %d (requeued with backoff)", i, got, i)
		}
	}
	p.process(ctx, hash)
	if got := p.queue.NumRequeues(hash); got != 0 {
		t.Errorf("NumRequeues after giving up = %d, want 0 (forgotten)", got)
	}

	p.process(ctx, "sha256:not-in-the-index")
	if got := p.queue.NumRequeues("sha256:not-in-the-index"); got != 0 {
		t.Errorf("NumRequeues for an unknown hash = %d, want 0 (forgotten)", got)
	}
}

// TestRuntimePrewarmOnce pins that a runtime is prewarmed once: a hash
// whose prewarm succeeded is skipped by every later enqueue, while one whose
// prewarm failed is queued again.
func TestRuntimePrewarmOnce(t *testing.T) {
	useTempAssetPins(t)
	ctx := context.Background()
	index := newRuntimeIndex()
	good := indexedTestRuntime(t, index, []byte("runsc"), "")
	// The store serves the same bytes for every asset, so this one's digest
	// never matches and its prewarm fails.
	bad := indexedTestRuntime(t, index, []byte("another runsc"), "")
	herder := &Atelet{sandboxAssetCache: newTestAssetCache(t), anonGCSClient: fakeObjectStorage{data: []byte("runsc")}}
	p := newRuntimePrewarmer(index, herder, newTestImageStore(t))

	drain := func() {
		for p.queue.Len() > 0 {
			hash, _ := p.queue.Get()
			p.process(ctx, hash)
		}
	}
	p.enqueue(good)
	p.enqueue(good) // a second pod of the pool: collapsed while it waits
	if got := p.queue.Len(); got != 1 {
		t.Fatalf("queue holds %d items after two enqueues of one hash, want 1", got)
	}
	drain()
	p.enqueue(good) // a later pod, re-plan or retry: already prewarmed
	if got := p.queue.Len(); got != 0 {
		t.Errorf("queue holds %d items after enqueueing a prewarmed hash, want 0", got)
	}

	p.enqueue(bad)
	drain()
	p.queue.Forget(bad) // drop the retry backoff for the test
	p.enqueue(bad)
	if got := p.queue.Len(); got != 1 {
		t.Errorf("queue holds %d items after enqueueing a hash whose prewarm failed, want 1 (not skipped)", got)
	}
}

// TestRuntimePrewarmFillsBothCaches covers a successful prewarm: the asset
// lands in the asset cache, linked into the runtime's pin so it stays
// there, and the pause image lands in the image cache.
func TestRuntimePrewarmFillsBothCaches(t *testing.T) {
	useTempAssetPins(t)
	ctx := context.Background()
	pauseRef, _ := pauseImageInTestRegistry(t)
	content := []byte("runsc binary bytes")
	index := newRuntimeIndex()
	hash := indexedTestRuntime(t, index, content, pauseRef)
	rt, _ := index.lookup(hash)

	root := t.TempDir()
	images := newTestImageStore(t)
	s := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: fakeObjectStorage{data: content}}
	p := newRuntimePrewarmer(index, s, images)
	if err := p.prewarm(ctx, hash, rt); err != nil {
		t.Fatalf("prewarm: %v", err)
	}
	if n := cachedAssets(t, root); n != 1 {
		t.Errorf("asset cache holds %d assets after prewarm, want 1", n)
	}
	if _, err := os.Stat(filepath.Join(ateletpath.SandboxAssetPinDir(hash), runscAssetName)); err != nil {
		t.Errorf("prewarm did not pin the runtime's asset: %v", err)
	}
	if _, err := images.EnsureImage(ctx, pauseRef); err != nil {
		t.Errorf("pause image not prewarmed into the image cache: %v", err)
	}

	// The pin is a link, so the cache keeps the entry under any pressure.
	stats, err := s.sandboxAssetCache.EvictUnused(ctx, 1<<40, false)
	if err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if stats.Retired != 0 || stats.SkippedLinked != 1 || cachedAssets(t, root) != 1 {
		t.Errorf("after an eviction pass: stats=%+v, cached=%d; want the pinned asset kept", stats, cachedAssets(t, root))
	}
}

// TestRuntimePrewarmPrunesPinsOfRetiredDefaults pins the pin's lifetime:
// it lasts while some worker advertises the runtime as its default, and
// pruning removes it once none does, which makes the assets evictable.
func TestRuntimePrewarmPrunesPinsOfRetiredDefaults(t *testing.T) {
	useTempAssetPins(t)
	ctx := context.Background()
	index := newRuntimeIndex()
	kept := indexedTestRuntime(t, index, []byte("kept runsc"), "")
	root := t.TempDir()
	// No min age, so the only thing standing between the entry and a pass is
	// its pin.
	s := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root, filecache.WithMinAge(0)), anonGCSClient: fakeObjectStorage{data: []byte("kept runsc")}}
	p := newRuntimePrewarmer(index, s, newTestImageStore(t))
	keptRT, _ := index.lookup(kept)
	if err := p.prewarm(ctx, kept, keptRT); err != nil {
		t.Fatal(err)
	}
	// A pin a previous atelet left for a runtime nobody advertises now.
	stale := ateletpath.SandboxAssetPinDir("sha256:stale")
	if err := os.MkdirAll(stale, 0o700); err != nil {
		t.Fatal(err)
	}

	p.prune()
	item, _ := p.queue.Get()
	p.process(ctx, item)
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale pin survived pruning (stat err = %v)", err)
	}
	if _, err := os.Stat(ateletpath.SandboxAssetPinDir(kept)); err != nil {
		t.Errorf("pin of an advertised default removed by pruning: %v", err)
	}

	// The worker stops advertising it: the next prune drops it, and the
	// cache entry becomes an ordinary candidate.
	index.forget("pod-a")
	p.prune()
	item, _ = p.queue.Get()
	p.process(ctx, item)
	if _, err := os.Stat(ateletpath.SandboxAssetPinDir(kept)); !os.IsNotExist(err) {
		t.Errorf("pin survived its runtime being retired (stat err = %v)", err)
	}
	if stats, _ := s.sandboxAssetCache.EvictUnused(ctx, 1<<40, true); stats.Retired != 1 {
		t.Errorf("after unpinning, a pass would retire %d entries, want 1", stats.Retired)
	}

	// Pruned means forgotten: handing the runtime over again prewarms and
	// pins it again, rather than being skipped as already prewarmed.
	p.enqueue(kept)
	if got := p.queue.Len(); got != 1 {
		t.Fatalf("queue holds %d items after re-handing a pruned runtime over, want 1", got)
	}
	item, _ = p.queue.Get()
	p.process(ctx, item)
	if _, err := os.Stat(ateletpath.SandboxAssetPinDir(kept)); err != nil {
		t.Fatalf("pin not rebuilt after the runtime was handed over again: %v", err)
	}

	// Planned but not advertised yet, as while the worker's build runs: a
	// prune in between keeps the pin. Once the worker advertises it, the
	// default set keeps it; once none does again, it goes.
	index.plan("pod-b", []string{kept})
	p.prune()
	item, _ = p.queue.Get()
	p.process(ctx, item)
	if _, err := os.Stat(ateletpath.SandboxAssetPinDir(kept)); err != nil {
		t.Errorf("pin of a runtime planned but not advertised yet removed by pruning: %v", err)
	}
	if err := index.register("pod-b", advertisedClass{class: v1alpha1.SandboxClassGvisor, config: "config-x", hashed: []hashedVersion{{versionPlan: versionPlan{name: "v1", isDefault: true, rec: keptRT.rec}, hash: kept}}}); err != nil {
		t.Fatal(err)
	}
	p.prune()
	item, _ = p.queue.Get()
	p.process(ctx, item)
	if _, err := os.Stat(ateletpath.SandboxAssetPinDir(kept)); err != nil {
		t.Errorf("pin of an advertised default removed by pruning after it was planned: %v", err)
	}
	index.forget("pod-b")
	p.prune()
	item, _ = p.queue.Get()
	p.process(ctx, item)
	if _, err := os.Stat(ateletpath.SandboxAssetPinDir(kept)); !os.IsNotExist(err) {
		t.Errorf("pin survived its runtime being retired a second time (stat err = %v)", err)
	}
}

// TestRuntimePrewarmAssetFailureStillPullsImage pins that the two halves
// are independent: a failing asset fetch does not stop the pause image pull.
func TestRuntimePrewarmAssetFailureStillPullsImage(t *testing.T) {
	useTempAssetPins(t)
	ctx := context.Background()
	pauseRef, _ := pauseImageInTestRegistry(t)
	index := newRuntimeIndex()
	hash := indexedTestRuntime(t, index, []byte("runsc"), pauseRef)
	rt, _ := index.lookup(hash)

	images := newTestImageStore(t)
	p := newRuntimePrewarmer(index, &Atelet{sandboxAssetCache: newTestAssetCache(t), anonGCSClient: fakeObjectStorage{err: errors.New("bucket unavailable")}}, images)
	if err := p.prewarm(ctx, hash, rt); err == nil {
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
	useTempAssetPins(t)
	index := newRuntimeIndex()
	hash := indexedTestRuntime(t, index, []byte("hung runsc"), "")
	rt, _ := index.lookup(hash)
	p := newRuntimePrewarmer(index, &Atelet{sandboxAssetCache: newTestAssetCache(t), anonGCSClient: hangingObjectStorage{}}, newTestImageStore(t))
	p.attemptTimeout = 50 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- p.prewarm(context.Background(), hash, rt) }()
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
// runtime, and only that, is handed to the prewarmer, and before the build
// fetches anything for its probes, so the probes are the prewarm's head
// start. A build that then fails has still handed it over.
func TestSyncPodPrewarmsDefaultRuntime(t *testing.T) {
	pod := workerPod("pod-a", "pool")
	s := newTestSyncer(t, pod, resolvedPool("pool", "gvisor-release"), multiVersionConfig())
	rec := &recordingPrewarm{}
	s.prewarm = rec
	s.assets = &fakeAssetFetcher{
		fn: func(ctx context.Context, r *sandboxAssetsRecord) (map[string]string, error) {
			if rec.enqueuedCount() == 0 {
				t.Error("the build fetched for a probe before the default runtime was handed to the prewarmer")
			}
			return newTestAssetFetcher().ensureSandboxAssets(ctx, r, "")
		},
	}
	if err := s.syncPod(context.Background(), pod); err != nil {
		t.Fatalf("syncPod: %v", err)
	}
	prewarmed := rec.enqueued
	wr, ok := s.index.worker("pod-a")
	if !ok {
		t.Fatal("pod-a not indexed")
	}
	cr, _ := wr.class(v1alpha1.SandboxClassGvisor)
	if len(prewarmed) != 1 || prewarmed[0] != cr.defaultHash {
		t.Errorf("prewarmed %v, want the default runtime %s once", prewarmed, cr.defaultHash)
	}
	if len(cr.hashes) < 2 {
		t.Fatalf("config advertises %d runtimes, want several so the test proves only the default is prewarmed", len(cr.hashes))
	}
	if rec.pruneCount() != 1 {
		t.Errorf("prune asked %d times after a plan, want 1 (the default may have changed)", rec.pruneCount())
	}
	if err := s.removeDir(context.Background(), "pod-a"); err != nil {
		t.Fatal(err)
	}
	if rec.pruneCount() != 2 {
		t.Errorf("prune asked %d times after the pod went, want 2", rec.pruneCount())
	}

	// A build that fails on the default's probe writes nothing and prunes
	// nothing, but the prewarm was queued before it started.
	s = newTestSyncer(t, pod, resolvedPool("pool", "gvisor-release"), multiVersionConfig())
	rec = &recordingPrewarm{}
	s.prewarm = rec
	s.runCPUFeatures = func(context.Context, string) ([]string, error) { return nil, errors.New("runsc failed") }
	if err := s.syncPod(context.Background(), pod); err == nil {
		t.Fatal("syncPod with the default's probe failing = nil, want an error")
	}
	if got := rec.enqueued; len(got) != 1 || got[0] != cr.defaultHash {
		t.Errorf("prewarmed %v after a failed build, want the default runtime %s once", got, cr.defaultHash)
	}
	if rec.pruneCount() != 0 {
		t.Errorf("prune asked %d times after a failed build, want 0", rec.pruneCount())
	}
	if _, ok := s.index.worker("pod-a"); ok {
		t.Error("a failed build registered the worker")
	}
	// The plan stands for the retry, so the prewarm's pin survives a prune
	// meanwhile; the pod's end takes it away.
	if !s.index.defaultHashes()[cr.defaultHash] {
		t.Error("the default of a failed build is not held for the retry")
	}
	if err := s.removeDir(context.Background(), "pod-a"); err != nil {
		t.Fatal(err)
	}
	if s.index.defaultHashes()[cr.defaultHash] {
		t.Error("the default of a gone pod is still held")
	}
}

// recordingPrewarm stands in for the prewarmer in syncer tests. Safe to
// read while a syncer's workers call it.
type recordingPrewarm struct {
	mu       sync.Mutex
	enqueued []string
	prunes   int
}

func (r *recordingPrewarm) enqueue(hash string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.enqueued = append(r.enqueued, hash)
}

func (r *recordingPrewarm) prune() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prunes++
}

func (r *recordingPrewarm) pruneCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prunes
}

func (r *recordingPrewarm) enqueuedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.enqueued)
}

// TestDefaultRuntimePrewarmThroughSyncer runs the whole path: a worker pod,
// its pool and SandboxConfig in fake clientsets flow through the informers
// into the syncer, which indexes the pod's runtimes, writes its file, and
// hands its default runtime to the prewarmer, which lands the asset in the
// cache without any Run/Restore request.
func TestDefaultRuntimePrewarmThroughSyncer(t *testing.T) {
	useTempAssetPins(t)
	useTempAssetScratch(t) // the syncer's runsc probe stages into it
	// The worker's own prewarm call returns only when the asset has landed
	// and the pause-image pull has finished. Waiting for the call keeps the
	// pull's last writes from racing the TempDir cleanup.
	prewarmed := make(chan error, 1)
	onFinished := func(_ string, err error) {
		// Non-blocking: a failed attempt is retried, and the worker must not
		// block on a result nobody reads.
		select {
		case prewarmed <- err:
		default:
		}
	}

	pauseRef, _ := pauseImageInTestRegistry(t)
	content := []byte("runsc binary bytes")
	cfg := gvisorConfig("gvisor-release", "gs://bucket/runsc", fmt.Sprintf("%x", sha256.Sum256(content)))
	cfg.Spec.Versions[0].PauseImage = pauseRef

	kube := kubefake.NewClientset([]k8sruntime.Object{workerPod("pod-a", "pool")}...)
	ate := atefake.NewSimpleClientset(resolvedPool("pool", "gvisor-release"), cfg)
	podFactory := kubeinformers.NewSharedInformerFactory(kube, 0)
	ateFactory := externalversions.NewSharedInformerFactory(ate, 0)
	ctx, cancel := context.WithCancel(context.Background())

	root := t.TempDir()
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
		t.TempDir(), index, p)
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
	waitFor(t, "the prewarm's and the probe's scratch dirs to be removed", func() bool {
		left, err := os.ReadDir(ateletpath.SandboxAssetScratchDir)
		return err == nil && len(left) == 0
	})
	wr, _ := index.worker("pod-a")
	cr, _ := wr.class(v1alpha1.SandboxClassGvisor)
	if _, err := os.Stat(filepath.Join(ateletpath.SandboxAssetPinDir(cr.defaultHash), runscAssetName)); err != nil {
		t.Errorf("the worker's default runtime is not pinned: %v", err)
	}
}
