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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// countingObjectStorage counts reads, to tell a restore that downloaded from one
// that did not.
type countingObjectStorage struct {
	orderedObjectStorage
	gets atomic.Int64
}

func (c *countingObjectStorage) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	c.gets.Add(1)
	return c.orderedObjectStorage.GetObject(ctx, bucket, object)
}

// suspendCacheHarness runs one actor through atelet with a fake ateom and an
// object store behind the real snapshot plugin.
type suspendCacheHarness struct {
	t     *testing.T
	s     *AteomHerder
	ateom *fakeAteom
	store *countingObjectStorage
	spec  *ateletpb.WorkloadSpec
	sa    *ateletpb.SandboxAssets
}

const suspendCacheActorUID = snapshotOwnerUID

func newSuspendCacheHarness(t *testing.T, maxBytes int64) *suspendCacheHarness {
	t.Helper()
	useTempNodeDirs(t)
	h := &suspendCacheHarness{t: t, store: &countingObjectStorage{}}
	h.ateom = &fakeAteom{snapshotFiles: map[string]string{"checkpoint.img": "guest-memory"}}
	serveFakeAteom(t, h.ateom)

	host := imageVolumeTestRegistry(t)
	image := host + "/actor:v1"
	pushTestImage(t, image, singleFileLayer(t, "bin/app", "app"))
	runsc := []byte("runsc binary")
	h.s = newPluginHerder(t, h.store)
	h.s.ateomDialer = newAteomDialer(1)
	h.s.imageCache = newImageVolumeStore(t)
	h.s.anonGCSClient = fakeObjectStorage{data: runsc}
	h.s.systemInfoVolumes = newSystemInfoVolumeRefresher(nil, nil)
	h.s.suspendCacheMaxBytes = maxBytes
	h.sa = &ateletpb.SandboxAssets{
		SandboxClass: "gvisor",
		PauseImage:   image,
		Assets: map[string]*ateletpb.ArchAssets{
			runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
				runscAssetName: {Url: "gs://test-bucket/runsc", Sha256: fmt.Sprintf("%x", sha256.Sum256(runsc))},
			}},
		},
	}
	h.spec = &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: image, Command: []string{"/bin/app"}}},
	}
	if _, err := h.s.Run(t.Context(), &ateletpb.RunRequest{
		Atespace: "ate-demo", ActorName: "counter", ActorUid: suspendCacheActorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		WorkerPodUid: "worker-pod-uid-1", SandboxAssets: h.sa, Spec: h.spec,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return h
}

func (h *suspendCacheHarness) suspend(uri, memory string) {
	h.t.Helper()
	h.ateom.snapshotFiles = map[string]string{"checkpoint.img": memory}
	if _, err := h.s.Checkpoint(h.t.Context(), &ateletpb.CheckpointRequest{
		Atespace: "ate-demo", ActorName: "counter", ActorUid: suspendCacheActorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		WorkerPodUid: "worker-pod-uid-1", Spec: h.spec,
		Fidelity: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Type:     ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.CheckpointRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{SnapshotUri: uri},
		},
	}); err != nil {
		h.t.Fatalf("Checkpoint: %v", err)
	}
}

// resume restores from uri and returns how many objects it read.
func (h *suspendCacheHarness) resume(uri string) int64 {
	h.t.Helper()
	before := h.store.gets.Load()
	if _, err := h.s.Restore(h.t.Context(), &ateletpb.RestoreRequest{
		Atespace: "ate-demo", ActorName: "counter", ActorUid: suspendCacheActorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		WorkerPodUid: "worker-pod-uid-1", SandboxAssets: h.sa, Spec: h.spec,
		Fidelity: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Type:     ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.RestoreRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalRestoreConfiguration{SnapshotUri: uri},
		},
	}); err != nil {
		h.t.Fatalf("Restore: %v", err)
	}
	return h.store.gets.Load() - before
}

func cacheEntries(t *testing.T) []string {
	t.Helper()
	got, err := filepath.Glob(filepath.Join(ateletpath.SuspendCacheDir(suspendCacheActorUID), "*"))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestSuspendCacheResumeOnSameNodeSkipsDownload(t *testing.T) {
	h := newSuspendCacheHarness(t, 1<<30)

	h.suspend(testSnapshotURI, "memory-1")
	if got := cacheEntries(t); len(got) != 1 {
		t.Fatalf("after suspend, cache entries = %v, want one", got)
	}
	if gets := h.resume(testSnapshotURI); gets != 0 {
		t.Errorf("resume from the cached snapshot read %d objects, want 0", gets)
	}
	if got := h.ateom.restored["checkpoint.img"]; got != "memory-1" {
		t.Errorf("restore staged %q, want %q", got, "memory-1")
	}
	if got := cacheEntries(t); len(got) != 0 {
		t.Errorf("after resume, cache entries = %v, want the entry consumed", got)
	}

	// A newer suspend replaces the entry, so the older snapshot is downloaded.
	h.suspend(pausedSnapshotURI, "memory-2")
	h.suspend(testSnapshotURI, "memory-3")
	if got := cacheEntries(t); len(got) != 1 {
		t.Fatalf("cache entries = %v, want only the newest suspend's", got)
	}
	if gets := h.resume(pausedSnapshotURI); gets == 0 {
		t.Error("resume from an uncached snapshot read no objects, want a download")
	}
	if got := h.ateom.restored["checkpoint.img"]; got != "memory-2" {
		t.Errorf("restore staged %q, want the downloaded %q", got, "memory-2")
	}
	if got := cacheEntries(t); len(got) != 0 {
		t.Errorf("cache entries = %v after restoring another snapshot, want them dropped", got)
	}

	h.suspend(testSnapshotURI, "memory-4")
	if _, err := h.s.Terminate(t.Context(), &ateletpb.TerminateRequest{
		Atespace: "ate-demo", ActorName: "counter", ActorUid: suspendCacheActorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		WorkerPodUid: "worker-pod-uid-1", Spec: h.spec,
	}); err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	if _, err := os.Stat(ateletpath.SuspendCacheDir(suspendCacheActorUID)); !os.IsNotExist(err) {
		t.Errorf("suspend cache survived terminate (stat err = %v)", err)
	}
}

func TestSuspendCacheDisabled(t *testing.T) {
	h := newSuspendCacheHarness(t, 0)
	h.suspend(testSnapshotURI, "memory-1")
	if got := cacheEntries(t); len(got) != 0 {
		t.Fatalf("cache entries = %v with the cache disabled, want none", got)
	}
	if gets := h.resume(testSnapshotURI); gets == 0 {
		t.Error("resume read no objects with the cache disabled, want a download")
	}
}

// An entry with no manifest (a crash while caching) is never used.
func TestSuspendCacheIgnoresEntryWithoutManifest(t *testing.T) {
	h := newSuspendCacheHarness(t, 1<<30)
	h.suspend(testSnapshotURI, "memory-1")
	entry := suspendCacheEntryDir(suspendCacheActorUID, testSnapshotURI)
	if err := os.Remove(filepath.Join(entry, sandboxManifestName)); err != nil {
		t.Fatal(err)
	}
	if gets := h.resume(testSnapshotURI); gets == 0 {
		t.Error("resume used a cache entry with no manifest, want a download")
	}
	if got := h.ateom.restored["checkpoint.img"]; got != "memory-1" {
		t.Errorf("restore staged %q, want %q", got, "memory-1")
	}
}

func TestGCSuspendCacheEvictsOldestFirst(t *testing.T) {
	actors := t.TempDir()
	mkEntry := func(actor, name string, size int, age time.Duration) string {
		dir := filepath.Join(actors, actor, ateletpath.SuspendCacheDirName, name)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		f := filepath.Join(dir, "memory-ranges")
		if err := os.WriteFile(f, make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
		when := time.Now().Add(-age)
		for _, p := range []string{f, dir} {
			if err := os.Chtimes(p, when, when); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}
	const mib = 1 << 20
	newest := mkEntry("a", "e1", mib, time.Minute)
	middle := mkEntry("b", "e2", mib, time.Hour)
	oldest := mkEntry("c", "e3", mib, 2*time.Hour)

	gcSuspendCache(t.Context(), actors, 2*mib+mib/2)

	for _, kept := range []string{newest, middle} {
		if _, err := os.Stat(kept); err != nil {
			t.Errorf("entry %s evicted, want kept: %v", kept, err)
		}
	}
	if _, err := os.Stat(oldest); !os.IsNotExist(err) {
		t.Errorf("oldest entry %s kept (stat err = %v), want evicted", oldest, err)
	}
}
