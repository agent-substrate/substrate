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
	"log"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned/fake"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
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
			SandboxClass: v1alpha1.SandboxClassGvisor,
			PauseImage:   "registry.k8s.io/pause@sha256:abc",
			Assets: map[string]map[string]v1alpha1.AssetFile{
				runtime.GOARCH: {
					runscAssetName: {URL: url, SHA256: sha},
				},
			},
		},
	}
}

func TestRecordFromSandboxConfig(t *testing.T) {
	sha := fmt.Sprintf("%x", sha256.Sum256([]byte("runsc")))
	cfg := gvisorConfig("gvisor-default", "gs://bucket/runsc", sha)

	rec, err := recordFromSandboxConfig(cfg)
	if err != nil {
		t.Fatalf("recordFromSandboxConfig: %v", err)
	}
	if rec.SandboxClass != string(v1alpha1.SandboxClassGvisor) {
		t.Errorf("SandboxClass = %q, want %q", rec.SandboxClass, v1alpha1.SandboxClassGvisor)
	}
	if rec.PauseImage != cfg.Spec.PauseImage {
		t.Errorf("PauseImage = %q, want %q", rec.PauseImage, cfg.Spec.PauseImage)
	}
	want := assetEntry{URL: "gs://bucket/runsc", SHA256: sha}
	if got := rec.Assets[runscAssetName]; got != want {
		t.Errorf("Assets[%q] = %+v, want %+v", runscAssetName, got, want)
	}

	// A config with no assets for this node's architecture cannot be
	// projected, and the error carries the sentinel that keeps prewarm from
	// retrying a condition only a config change can clear.
	cfg.Spec.Assets = map[string]map[string]v1alpha1.AssetFile{
		"other-arch": {runscAssetName: {URL: "gs://bucket/runsc", SHA256: sha}},
	}
	if _, err := recordFromSandboxConfig(cfg); !errors.Is(err, errNoAssetsForArch) {
		t.Errorf("recordFromSandboxConfig with no assets for the local architecture = %v, want errNoAssetsForArch", err)
	}
}

func TestPrewarmEnqueueFilters(t *testing.T) {
	origJitter := prewarmMaxJitter
	prewarmMaxJitter = 0 // enqueue synchronously so Len is observable
	t.Cleanup(func() { prewarmMaxJitter = origJitter })

	ctx := context.Background()
	microvm := &v1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "microvm-default"},
		Spec:       v1alpha1.SandboxConfigSpec{SandboxClass: v1alpha1.SandboxClassMicroVM},
	}
	gvisor := gvisorConfig("gvisor-default", "gs://bucket/runsc", fmt.Sprintf("%x", sha256.Sum256([]byte("runsc"))))

	t.Run("node without KVM", func(t *testing.T) {
		p := newSandboxPrewarmer(nil, nil, nil, false)

		p.enqueue(ctx, "not a sandbox config")
		p.enqueue(ctx, microvm)
		p.enqueue(ctx, &v1alpha1.SandboxConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "future-class"},
			Spec:       v1alpha1.SandboxConfigSpec{SandboxClass: "future-class"},
		})
		if p.queue.Len() != 0 {
			t.Fatalf("queue holds %d configs after filtered enqueues, want 0", p.queue.Len())
		}

		p.enqueue(ctx, gvisor)
		if p.queue.Len() != 1 {
			t.Fatalf("queue holds %d configs after gvisor enqueue, want 1", p.queue.Len())
		}
		// Duplicate events (e.g. a relist) must coalesce, not stack work.
		p.enqueue(ctx, gvisor)
		if p.queue.Len() != 1 {
			t.Errorf("queue holds %d configs after duplicate enqueue, want 1", p.queue.Len())
		}
	})

	t.Run("node with KVM", func(t *testing.T) {
		p := newSandboxPrewarmer(nil, nil, nil, true)
		p.enqueue(ctx, microvm)
		p.enqueue(ctx, gvisor)
		if p.queue.Len() != 2 {
			t.Errorf("queue holds %d configs, want both microvm and gvisor queued", p.queue.Len())
		}
	})
}

// TestPrewarmProcessRetries covers the worker's failure handling: a failed
// prewarm is requeued with backoff, and a config deleted between enqueue and
// processing is forgotten without retries.
func TestPrewarmProcessRetries(t *testing.T) {
	origDir := ateompath.StaticFilesDir
	ateompath.StaticFilesDir = t.TempDir()
	t.Cleanup(func() { ateompath.StaticFilesDir = origDir })

	ctx := context.Background()
	cfg := gvisorConfig("gvisor-default", "gs://bucket/runsc", fmt.Sprintf("%x", sha256.Sum256([]byte("runsc"))))
	// No pause image, so a failing prewarm exercises only the asset path.
	cfg.Spec.PauseImage = ""

	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := indexer.Add(cfg); err != nil {
		t.Fatalf("indexer.Add: %v", err)
	}
	herder := &AteomHerder{anonGCSClient: fakeObjectStorage{err: errors.New("bucket unavailable")}}
	p := newSandboxPrewarmer(herder, nil, listersv1alpha1.NewSandboxConfigLister(indexer), false)

	p.queue.Add(cfg.Name)
	name, _ := p.queue.Get()
	p.process(ctx, name)
	if got := p.queue.NumRequeues(cfg.Name); got != 1 {
		t.Errorf("NumRequeues after failed prewarm = %d, want 1 (requeued with backoff)", got)
	}

	p.process(ctx, "deleted-config")
	if got := p.queue.NumRequeues("deleted-config"); got != 0 {
		t.Errorf("NumRequeues for a deleted config = %d, want 0 (forgotten)", got)
	}
}

// TestPrewarmProcessReappliesClassGate covers the gap between enqueue and
// processing: sandboxClass is mutable, so a config that passed the enqueue
// filter as gVisor may be micro-VM by the time the worker resolves it, and a
// node without KVM must skip it rather than download guest assets.
func TestPrewarmProcessReappliesClassGate(t *testing.T) {
	origDir := ateompath.StaticFilesDir
	ateompath.StaticFilesDir = t.TempDir()
	t.Cleanup(func() { ateompath.StaticFilesDir = origDir })

	ctx := context.Background()
	// Enqueued while gVisor, edited to micro-VM before the worker ran.
	cfg := gvisorConfig("mutating-config", "gs://bucket/runsc", fmt.Sprintf("%x", sha256.Sum256([]byte("runsc"))))
	cfg.Spec.SandboxClass = v1alpha1.SandboxClassMicroVM

	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := indexer.Add(cfg); err != nil {
		t.Fatalf("indexer.Add: %v", err)
	}
	// Any fetch would fail and requeue, so NumRequeues distinguishes
	// "skipped" from "attempted and failed".
	herder := &AteomHerder{anonGCSClient: fakeObjectStorage{err: errors.New("bucket unavailable")}}
	p := newSandboxPrewarmer(herder, nil, listersv1alpha1.NewSandboxConfigLister(indexer), false)

	p.queue.Add(cfg.Name)
	name, _ := p.queue.Get()
	p.process(ctx, name)
	if got := p.queue.NumRequeues(cfg.Name); got != 0 {
		t.Errorf("NumRequeues after a class change to micro-VM on a non-KVM node = %d, want 0 (skipped, not retried)", got)
	}
}

// TestMicrovmNodeCapable covers the detectable negative cases; the positive
// case needs a /dev/kvm character device, which a test cannot mknod.
func TestMicrovmNodeCapable(t *testing.T) {
	devRoot := t.TempDir()
	if microvmNodeCapable(devRoot) {
		t.Error("microvmNodeCapable = true for a dev root without kvm")
	}
	// A plain file named kvm is not a character device and must not count.
	if err := os.WriteFile(devRoot+"/kvm", []byte("not a device"), 0o600); err != nil {
		t.Fatal(err)
	}
	if microvmNodeCapable(devRoot) {
		t.Error("microvmNodeCapable = true for a regular file named kvm")
	}
}

// TestPrewarmPauseImage covers the pause-image half of prewarm: the image
// lands in the image cache, and an asset fetch failure does not stop the
// pull (the two live in different backends).
func TestPrewarmPauseImage(t *testing.T) {
	origDir := ateompath.StaticFilesDir
	ateompath.StaticFilesDir = t.TempDir()
	t.Cleanup(func() { ateompath.StaticFilesDir = origDir })

	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parsing registry URL: %v", err)
	}
	pauseRef := u.Host + "/pause:3.10"
	pauseDigest := pushPauseImage(t, pauseRef)

	newStore := func() *imagecache.Store {
		s, err := imagecache.New(t.TempDir())
		if err != nil {
			t.Fatalf("imagecache.New: %v", err)
		}
		return s
	}
	okStore, failStore, archStore := newStore(), newStore(), newStore()

	ctx := context.Background()
	content := []byte("runsc binary bytes")
	cfg := gvisorConfig("gvisor-default", "gs://bucket/runsc", fmt.Sprintf("%x", sha256.Sum256(content)))
	cfg.Spec.PauseImage = pauseRef

	p := &sandboxPrewarmer{
		assets: &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}},
		images: okStore,
	}
	if err := p.prewarm(ctx, cfg); err != nil {
		t.Fatalf("prewarm: %v", err)
	}

	// A different asset hash misses the shared static-files cache, so the
	// failing object storage is actually consulted — and must not keep the
	// pause image from being pulled.
	failCfg := gvisorConfig("gvisor-default", "gs://bucket/runsc", fmt.Sprintf("%x", sha256.Sum256([]byte("other runsc"))))
	failCfg.Spec.PauseImage = pauseRef
	p = &sandboxPrewarmer{
		assets: &AteomHerder{anonGCSClient: fakeObjectStorage{err: errors.New("bucket unavailable")}},
		images: failStore,
	}
	if err := p.prewarm(ctx, failCfg); err == nil {
		t.Error("prewarm returned nil despite the asset fetch failing")
	}

	// A config with no assets for this node's architecture still gets its
	// pause image pulled, and prewarm succeeds: the missing assets are
	// permanent until the config changes, not a retryable failure.
	archCfg := gvisorConfig("gvisor-default", "gs://bucket/runsc", fmt.Sprintf("%x", sha256.Sum256(content)))
	archCfg.Spec.Assets = map[string]map[string]v1alpha1.AssetFile{
		"other-arch": {runscAssetName: archCfg.Spec.Assets[runtime.GOARCH][runscAssetName]},
	}
	archCfg.Spec.PauseImage = pauseRef
	p = &sandboxPrewarmer{images: archStore}
	if err := p.prewarm(ctx, archCfg); err != nil {
		t.Errorf("prewarm with no assets for the local architecture: %v", err)
	}

	// With the registry gone, only a cache hit can satisfy a digest ref.
	srv.Close()
	digestRef := u.Host + "/pause@" + pauseDigest.String()
	for name, store := range map[string]*imagecache.Store{"ok": okStore, "asset-failure": failStore, "no-local-arch": archStore} {
		if _, err := store.EnsureImage(ctx, digestRef); err != nil {
			t.Errorf("pause image not prewarmed into the %s store: %v", name, err)
		}
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

// TestPrewarmTimeout verifies a single prewarm attempt is bounded by
// prewarmTimeout: the queue has one worker, so an attempt that never returned
// would block every other config's prewarm.
func TestPrewarmTimeout(t *testing.T) {
	origDir, origTimeout := ateompath.StaticFilesDir, prewarmTimeout
	ateompath.StaticFilesDir = t.TempDir()
	prewarmTimeout = 50 * time.Millisecond
	t.Cleanup(func() { ateompath.StaticFilesDir, prewarmTimeout = origDir, origTimeout })

	cfg := gvisorConfig("gvisor-default", "gs://bucket/runsc", fmt.Sprintf("%x", sha256.Sum256([]byte("hung runsc"))))
	cfg.Spec.PauseImage = ""
	p := &sandboxPrewarmer{assets: &AteomHerder{anonGCSClient: hangingObjectStorage{}}}

	done := make(chan error, 1)
	go func() { done <- p.prewarm(context.Background(), cfg) }()
	select {
	case err := <-done:
		if err == nil {
			t.Error("prewarm returned nil despite the download hanging")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("prewarm never returned; a hung download would block the worker forever")
	}
}

// TestSandboxAssetPrewarmDownloads runs the whole path: a SandboxConfig in a
// fake clientset flows through the informer into the prewarm worker, which
// lands the asset in the static-files cache without any Run/Restore request.
func TestSandboxAssetPrewarmDownloads(t *testing.T) {
	origDir, origJitter := ateompath.StaticFilesDir, prewarmMaxJitter
	ateompath.StaticFilesDir = t.TempDir()
	prewarmMaxJitter = 0
	t.Cleanup(func() { ateompath.StaticFilesDir, prewarmMaxJitter = origDir, origJitter })

	host := imageVolumeTestRegistry(t)
	pauseRef := host + "/pause:3.10"
	pushPauseImage(t, pauseRef)

	content := []byte("runsc binary bytes")
	sha := fmt.Sprintf("%x", sha256.Sum256(content))
	cfg := gvisorConfig("gvisor-default", "gs://bucket/runsc", sha)
	cfg.Spec.PauseImage = pauseRef

	ctx := t.Context()
	client := fake.NewSimpleClientset(cfg)
	factory := externalversions.NewSharedInformerFactory(client, 0)
	informer := factory.Api().V1alpha1().SandboxConfigs().Informer()

	store, err := imagecache.New(t.TempDir())
	if err != nil {
		t.Fatalf("imagecache.New: %v", err)
	}
	herder := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
	// Handler first, informer start second, mirroring main: atelet startup
	// must never wait on this informer's sync, and the initial List replays
	// the pre-existing config into the handler as an Add.
	if err := startSandboxAssetPrewarm(ctx, informer, herder, store, false); err != nil {
		t.Fatalf("startSandboxAssetPrewarm: %v", err)
	}
	stopCh := make(chan struct{})
	defer close(stopCh)
	factory.Start(stopCh)

	wantPath := ateompath.RunSCBinaryPath(sha)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(wantPath); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat %s: %v", wantPath, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("asset never prewarmed to %s", wantPath)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
