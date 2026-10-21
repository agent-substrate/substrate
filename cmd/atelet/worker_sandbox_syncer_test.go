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
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubeinformers "k8s.io/client-go/informers"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"go.ate.dev/substrate/internal/sandboxruntimes"
	"go.ate.dev/substrate/pkg/api/v1alpha1"
	atefake "go.ate.dev/substrate/pkg/client/clientset/versioned/fake"
	"go.ate.dev/substrate/pkg/client/informers/externalversions"
	listersv1alpha1 "go.ate.dev/substrate/pkg/client/listers/api/v1alpha1"
)

const (
	testCPUVendor = "GenuineIntel"
	testCPUModel  = "6:143"
)

// testCPUFeatures are the first two CPU feature names for runtime.GOARCH,
// which encode to bit 0 | bit 1 = "3" on both amd64 and arm64.
func testCPUFeatures() []string {
	switch runtime.GOARCH {
	case "arm64":
		return []string{"fp", "asimd"}
	default:
		return []string{"pni", "pclmulqdq"}
	}
}

// fakeAssetFetcher answers ensureSandboxAssets from fn. The paths it returns
// are not under dstDir: the syncer only runs what it is handed, so where the
// fake says a binary is does not matter.
type fakeAssetFetcher struct {
	fn func(ctx context.Context, rec *sandboxAssetsRecord) (map[string]string, error)
}

func (f *fakeAssetFetcher) ensureSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord, _ string) (map[string]string, error) {
	return f.fn(ctx, rec)
}

func (f *fakeAssetFetcher) stageSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord, _ string) (map[string]string, error) {
	return f.fn(ctx, rec)
}

// newTestAssetFetcher returns a sandboxAssetFetcher that maps every asset of
// a record to a path under its digest without fetching anything, for syncers
// whose runsc probe is faked.
func newTestAssetFetcher() sandboxAssetFetcher {
	return &fakeAssetFetcher{
		fn: func(_ context.Context, rec *sandboxAssetsRecord) (map[string]string, error) {
			paths := make(map[string]string, len(rec.Assets))
			for name, a := range rec.Assets {
				paths[name] = "/cache/" + a.SHA256 + "/" + name
			}
			return paths, nil
		},
	}
}

// newScriptAssetFetcher returns a sandboxAssetFetcher that backs every asset
// with an executable script printing testCPUFeatures(), for a syncer that
// runs the real runsc probe.
func newScriptAssetFetcher(t *testing.T) sandboxAssetFetcher {
	t.Helper()
	script := filepath.Join(t.TempDir(), "runsc")
	body := "#!/bin/sh\necho " + strings.Join(testCPUFeatures(), ",") + "\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return &fakeAssetFetcher{
		fn: func(_ context.Context, rec *sandboxAssetsRecord) (map[string]string, error) {
			paths := make(map[string]string, len(rec.Assets))
			for name := range rec.Assets {
				paths[name] = script
			}
			return paths, nil
		},
	}
}

// workerPod is a worker pod on this node with the given UID, in pool when
// pool is not "".
func workerPod(uid, pool string) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "workers", Name: "worker-" + uid, UID: types.UID(uid)}}
	if pool != "" {
		pod.Labels = map[string]string{workerPoolLabel: pool}
	}
	return pod
}

// resolvedPool is a gVisor WorkerPool whose class resolved to config, or has
// not resolved when config is "".
func resolvedPool(name, config string) *v1alpha1.WorkerPool {
	return resolvedPoolOf(v1alpha1.SandboxClassGvisor, name, config)
}

// resolvedPoolOf is a WorkerPool of the given sandbox class whose class
// resolved to config, or has not resolved when config is "".
func resolvedPoolOf(class v1alpha1.SandboxClass, name, config string) *v1alpha1.WorkerPool {
	pool := &v1alpha1.WorkerPool{ObjectMeta: metav1.ObjectMeta{Namespace: "workers", Name: name}}
	pool.Spec.SandboxClasses = []v1alpha1.WorkerPoolSandboxClass{{Name: class}}
	status := v1alpha1.WorkerPoolSandboxClassStatus{Name: class}
	if config != "" {
		status.ConfigRef = &v1alpha1.SandboxConfigReference{Name: config}
	}
	pool.Status.SandboxClasses = []v1alpha1.WorkerPoolSandboxClassStatus{status}
	return pool
}

// newTestSyncer serves objs from synced caches and keeps worker directories
// under a temp dir. Its probes are faked: the host CPU is
// testCPUVendor/testCPUModel and every runsc reports testCPUFeatures();
// TestRunRunscCPUFeatures covers the real probe.
func newTestSyncer(t *testing.T, objs ...any) *workerSandboxSyncer {
	t.Helper()
	newIndexer := func() cache.Indexer {
		return cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	}
	pods := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
		podUIDIndex:          func(obj any) ([]string, error) { return []string{string(obj.(*corev1.Pod).UID)}, nil },
	})
	pools, configs := newIndexer(), newIndexer()
	for _, obj := range objs {
		var err error
		switch obj.(type) {
		case *corev1.Pod:
			err = pods.Add(obj)
		case *v1alpha1.WorkerPool:
			err = pools.Add(obj)
		case *v1alpha1.SandboxConfig:
			err = configs.Add(obj)
		default:
			t.Fatalf("unexpected object %T", obj)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	s := newWorkerSandboxSyncer(&workerSandboxResolver{
		workerPools: listersv1alpha1.NewWorkerPoolLister(pools),
		configs:     listersv1alpha1.NewSandboxConfigLister(configs),
	}, pods, newTestAssetFetcher(), t.TempDir(), newRuntimeIndex(), nil)
	// The queue starts a goroutine; stop it with the test.
	t.Cleanup(s.queue.ShutDown)
	s.scratchDir = t.TempDir() // the runsc probe stages the binary there
	s.probeCPUIdentity = func() (string, string, error) { return testCPUVendor, testCPUModel, nil }
	s.runCPUFeatures = func(context.Context, string) ([]string, error) { return testCPUFeatures(), nil }
	// Tests that check the jitter set it.
	s.maxJitter = 0
	return s
}

// drain returns the UIDs waiting in the syncer's queue, sorted.
func drain(s *workerSandboxSyncer) []string {
	var got []string
	for s.queue.Len() > 0 {
		uid, _ := s.queue.Get()
		got = append(got, uid)
		s.queue.Done(uid)
	}
	slices.Sort(got)
	return got
}

func TestWorkerSandboxSyncerEvents(t *testing.T) {
	otherNSPod := workerPod("pod-other-ns", "pool")
	otherNSPod.Namespace = "other"
	// Every test runs over these pods: two of "pool", one of "pool-b", and
	// one of a same-named pool in another namespace.
	pods := []any{workerPod("pod-a", "pool"), workerPod("pod-b", "pool"), workerPod("pod-c", "pool-b"), otherNSPod}
	pools := []any{resolvedPool("pool", "gvisor-release"), resolvedPool("pool-b", "gvisor-other")}
	withConfig := func(pool *v1alpha1.WorkerPool, config string) *v1alpha1.WorkerPool {
		pool = pool.DeepCopy()
		pool.Status.SandboxClasses[0].ConfigRef = &v1alpha1.SandboxConfigReference{Name: config}
		return pool
	}
	withGeneration := func(cfg *v1alpha1.SandboxConfig, gen int64) *v1alpha1.SandboxConfig {
		cfg = cfg.DeepCopy()
		cfg.Generation = gen
		return cfg
	}

	tests := []struct {
		name  string
		event func(s *workerSandboxSyncer)
		want  []string
	}{{
		name:  "pod added",
		event: func(s *workerSandboxSyncer) { s.podHandler().OnAdd(workerPod("pod-new", "pool"), false) },
		want:  []string{"pod-new"},
	}, {
		name:  "pod deleted",
		event: func(s *workerSandboxSyncer) { s.podHandler().OnDelete(workerPod("pod-gone", "pool")) },
		want:  []string{"pod-gone"},
	}, {
		name: "pod deleted with a tombstone",
		event: func(s *workerSandboxSyncer) {
			s.podHandler().OnDelete(cache.DeletedFinalStateUnknown{Key: "workers/worker-pod-gone", Obj: workerPod("pod-gone", "pool")})
		},
		want: []string{"pod-gone"},
	}, {
		name: "pod updated",
		event: func(s *workerSandboxSyncer) {
			s.podHandler().OnUpdate(workerPod("pod-a", "pool"), workerPod("pod-a", "pool"))
		},
	}, {
		name: "pool added",
		event: func(s *workerSandboxSyncer) {
			s.workerPoolHandler().OnAdd(resolvedPool("pool", "gvisor-release"), false)
		},
		want: []string{"pod-a", "pod-b"},
	}, {
		name: "pool configRef changed",
		event: func(s *workerSandboxSyncer) {
			s.workerPoolHandler().OnUpdate(resolvedPool("pool", "gvisor-release"), withConfig(resolvedPool("pool", ""), "gvisor-next"))
		},
		want: []string{"pod-a", "pod-b"},
	}, {
		name: "pool configRef resolved",
		event: func(s *workerSandboxSyncer) {
			s.workerPoolHandler().OnUpdate(resolvedPool("pool", ""), resolvedPool("pool", "gvisor-release"))
		},
		want: []string{"pod-a", "pod-b"},
	}, {
		name: "pool changed elsewhere",
		event: func(s *workerSandboxSyncer) {
			changed := resolvedPool("pool", "gvisor-release")
			changed.Status.Replicas = 3
			changed.Status.SandboxClasses[0].PendingConfigRef = &v1alpha1.SandboxConfigReference{Name: "gvisor-next"}
			s.workerPoolHandler().OnUpdate(resolvedPool("pool", "gvisor-release"), changed)
		},
	}, {
		// Pods are namespaced; a same-named pool elsewhere is another pool.
		name: "same-named pool in another namespace",
		event: func(s *workerSandboxSyncer) {
			other := resolvedPool("pool", "")
			other.Namespace = "other"
			s.workerPoolHandler().OnUpdate(other, withConfig(other, "gvisor-release"))
		},
		want: []string{"pod-other-ns"},
	}, {
		name:  "config added",
		event: func(s *workerSandboxSyncer) { s.sandboxConfigHandler().OnAdd(multiVersionConfig(), false) },
		want:  []string{"pod-a", "pod-b"},
	}, {
		name: "config spec changed",
		event: func(s *workerSandboxSyncer) {
			s.sandboxConfigHandler().OnUpdate(withGeneration(multiVersionConfig(), 1), withGeneration(multiVersionConfig(), 2))
		},
		want: []string{"pod-a", "pod-b"},
	}, {
		name: "config update without a spec change",
		event: func(s *workerSandboxSyncer) {
			s.sandboxConfigHandler().OnUpdate(withGeneration(multiVersionConfig(), 1), withGeneration(multiVersionConfig(), 1))
		},
	}, {
		name: "config no pool uses",
		event: func(s *workerSandboxSyncer) {
			cfg := multiVersionConfig()
			cfg.Name = "gvisor-unused"
			s.sandboxConfigHandler().OnAdd(cfg, false)
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestSyncer(t, append(pods, pools...)...)
			tt.event(s)
			if diff := cmp.Diff(tt.want, drain(s)); diff != "" {
				t.Errorf("enqueued pods mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestWorkerSandboxSyncerJitter verifies that pool and config changes, which
// every node sees at once, enqueue pods after a random delay, and that events
// only this node acts on do not.
func TestWorkerSandboxSyncerJitter(t *testing.T) {
	pods := []any{workerPod("pod-a", "pool"), workerPod("pod-b", "pool")}
	pool := resolvedPool("pool", "gvisor-release")
	withGeneration := func(gen int64) *v1alpha1.SandboxConfig {
		cfg := multiVersionConfig()
		cfg.Generation = gen
		return cfg
	}

	tests := []struct {
		name  string
		event func(s *workerSandboxSyncer)
		// want is enqueued right away, wantLater once the jitter has passed.
		want      []string
		wantLater []string
	}{{
		name:  "pod added",
		event: func(s *workerSandboxSyncer) { s.podHandler().OnAdd(workerPod("pod-new", "pool"), false) },
		want:  []string{"pod-new"},
	}, {
		name:  "pod deleted",
		event: func(s *workerSandboxSyncer) { s.podHandler().OnDelete(workerPod("pod-gone", "pool")) },
		want:  []string{"pod-gone"},
	}, {
		// A pool usually reaches the cache when atelet starts.
		name:  "pool added",
		event: func(s *workerSandboxSyncer) { s.workerPoolHandler().OnAdd(pool, false) },
		want:  []string{"pod-a", "pod-b"},
	}, {
		name: "pool configRef changed",
		event: func(s *workerSandboxSyncer) {
			s.workerPoolHandler().OnUpdate(resolvedPool("pool", ""), pool)
		},
		wantLater: []string{"pod-a", "pod-b"},
	}, {
		name:      "config added",
		event:     func(s *workerSandboxSyncer) { s.sandboxConfigHandler().OnAdd(multiVersionConfig(), false) },
		wantLater: []string{"pod-a", "pod-b"},
	}, {
		name: "config spec changed",
		event: func(s *workerSandboxSyncer) {
			s.sandboxConfigHandler().OnUpdate(withGeneration(1), withGeneration(2))
		},
		wantLater: []string{"pod-a", "pod-b"},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// On the bubble's clock the sleep fires every delay at once.
			synctest.Test(t, func(t *testing.T) {
				s := newTestSyncer(t, append(pods, pool)...)
				s.maxJitter = syncMaxJitter
				tt.event(s)
				synctest.Wait()
				if diff := cmp.Diff(tt.want, drain(s)); diff != "" {
					t.Errorf("UIDs enqueued right away mismatch (-want +got):\n%s", diff)
				}
				time.Sleep(s.maxJitter)
				synctest.Wait()
				if diff := cmp.Diff(tt.wantLater, drain(s)); diff != "" {
					t.Errorf("UIDs enqueued after the jitter mismatch (-want +got):\n%s", diff)
				}
			})
		})
	}
}

// TestWorkerSandboxSyncerRegistersAheadOfJitter pins what closes the window
// between a SandboxConfig change and the jittered sync: the config's
// runtimes resolve as soon as the event is handled, while the file, and
// with it what the worker advertises, waits for the sync.
func TestWorkerSandboxSyncerRegistersAheadOfJitter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Two pods of one pool: the pool is planned once, both pods are delayed.
		pod := workerPod("pod-a", "pool")
		s := newTestSyncer(t, pod, workerPod("pod-b", "pool"), resolvedPool("pool", "gvisor-release"), multiVersionConfig())
		s.maxJitter = syncMaxJitter

		cfg := multiVersionConfig()
		cfg.Generation = 2
		s.sandboxConfigHandler().OnUpdate(multiVersionConfig(), cfg)
		synctest.Wait()

		plan, err := s.resolver.plan(pod)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range hashVersions(plan) {
			if _, ok := s.index.lookup(v.hash); !ok {
				t.Errorf("runtime %s (%s) does not resolve before the jittered sync", v.name, v.hash)
			}
		}
		for _, uid := range []string{"pod-a", "pod-b"} {
			if _, ok := s.index.worker(uid); ok {
				t.Errorf("%s advertises runtimes before its file is written", uid)
			}
		}
		if got := drain(s); len(got) != 0 {
			t.Errorf("enqueued %v right away, want the jittered delay", got)
		}
		time.Sleep(s.maxJitter)
		synctest.Wait()
		if diff := cmp.Diff([]string{"pod-a", "pod-b"}, drain(s)); diff != "" {
			t.Errorf("UIDs enqueued after the jitter mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestWorkerSandboxSyncerProcess(t *testing.T) {
	ctx := context.Background()
	deleting := workerPod("pod-a", "pool")
	deleting.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	stale := wantVersions()
	stale.SandboxClasses[0].DefaultRuntime = sandboxruntimes.Runtime{Version: wantRuntime("v0")}

	tests := []struct {
		name string
		objs []any
		// existing is written to pod-a's file before processing, when set.
		existing *sandboxruntimes.File
		// assets overrides s.assets when set.
		assets sandboxAssetFetcher
		// priorRetries is how often pod-a has been retried before.
		priorRetries int
		// want is the file afterwards; nil means no file.
		want    *sandboxruntimes.File
		wantDir bool
		// wantUntouched is whether the existing file must not be rewritten.
		wantUntouched bool
		wantRetries   int
		// indexed registers pod-a in the index before processing, as an
		// earlier sync would have; wantIndexed is whether it is there after.
		indexed, wantIndexed bool
	}{{
		name:        "writes the file",
		wantIndexed: true,
		objs:        []any{workerPod("pod-a", "pool"), resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		want:        wantVersions(),
		wantDir:     true,
	}, {
		name:        "replaces a stale file",
		wantIndexed: true,
		objs:        []any{workerPod("pod-a", "pool"), resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		existing:    stale,
		want:        wantVersions(),
		wantDir:     true,
	}, {
		name:          "leaves an equal file alone",
		wantIndexed:   true,
		objs:          []any{workerPod("pod-a", "pool"), resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		existing:      wantVersions(),
		want:          wantVersions(),
		wantDir:       true,
		wantUntouched: true,
	}, {
		// The Delete event removes the directory, and the index entry with it.
		name:        "pod being deleted",
		objs:        []any{deleting, resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		existing:    stale,
		indexed:     true,
		want:        stale,
		wantDir:     true,
		wantIndexed: true,
	}, {
		name:     "pod gone",
		objs:     []any{resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		existing: stale,
		indexed:  true,
	}, {
		name:    "pod gone, no directory",
		objs:    []any{resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		indexed: true,
	}, {
		// A sync that fails before the write leaves the index entry as it
		// left the file.
		name:        "unresolved pool keeps the existing file",
		objs:        []any{workerPod("pod-a", "pool"), multiVersionConfig()},
		existing:    stale,
		indexed:     true,
		want:        stale,
		wantDir:     true,
		wantRetries: 1,
		wantIndexed: true,
	}, {
		name:         "unresolved pool retries up to syncMaxRetries",
		objs:         []any{workerPod("pod-a", "pool"), multiVersionConfig()},
		priorRetries: syncMaxRetries - 1,
		wantRetries:  syncMaxRetries,
	}, {
		name:         "unresolved pool gives up after syncMaxRetries",
		objs:         []any{workerPod("pod-a", "pool"), multiVersionConfig()},
		priorRetries: syncMaxRetries,
	}, {
		// A transient failure on the default version leaves the existing file
		// in place and requeues without the syncMaxRetries cap, since no
		// informer event enqueues the pod when an asset fetch recovers.
		name:     "transient failure on default version keeps existing file and requeues past syncMaxRetries",
		objs:     []any{workerPod("pod-a", "pool"), resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		existing: stale,
		assets: &fakeAssetFetcher{
			fn: func(context.Context, *sandboxAssetsRecord) (map[string]string, error) {
				return nil, errors.New("temporary download failure on default version")
			},
		},
		priorRetries:  syncMaxRetries,
		want:          stale,
		wantDir:       true,
		wantUntouched: true,
		wantRetries:   syncMaxRetries + 1,
	}, {
		// A transient failure on a restorable version writes the default
		// runtime right away and requeues without the syncMaxRetries cap.
		name:        "incomplete restorable version writes partial file and requeues past syncMaxRetries",
		wantIndexed: true,
		objs:        []any{workerPod("pod-a", "pool"), resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		assets: &fakeAssetFetcher{
			fn: func(ctx context.Context, rec *sandboxAssetsRecord) (map[string]string, error) {
				if rec.Assets[gvisorAssetName].SHA256 != "sha-v2" {
					return nil, errors.New("temporary download failure on restorable version")
				}
				return newTestAssetFetcher().ensureSandboxAssets(ctx, rec, "")
			},
		},
		priorRetries: syncMaxRetries,
		want:         gvisorFile(wantRuntime("v2")),
		wantDir:      true,
		wantRetries:  syncMaxRetries + 1,
	}, {
		name: "pod of no pool is not retried",
		objs: []any{workerPod("pod-a", ""), resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestSyncer(t, tt.objs...)
			if tt.assets != nil {
				s.assets = tt.assets
			}
			if tt.indexed {
				s.index.register("pod-a")
			}
			limiter := workqueue.NewTypedItemExponentialFailureRateLimiter[string](time.Second, 5*time.Minute)
			for range tt.priorRetries {
				limiter.When("pod-a")
			}
			s.queue = workqueue.NewTypedRateLimitingQueue(limiter)
			dir := s.podDir("pod-a")
			if tt.existing != nil {
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				if _, err := sandboxruntimes.Write(s.runtimesFile("pod-a"), tt.existing); err != nil {
					t.Fatal(err)
				}
			}
			var fileBefore time.Time
			if _, err := os.Stat(dir); err == nil {
				file, err := os.Stat(s.runtimesFile("pod-a"))
				if err != nil {
					t.Fatal(err)
				}
				fileBefore = file.ModTime()
				// A rewrite must land on a later mtime to be told apart.
				time.Sleep(10 * time.Millisecond)
			}
			s.queue.Add("pod-a")
			uid, _ := s.queue.Get()

			s.process(ctx, uid)

			if _, err := os.Stat(dir); (err == nil) != tt.wantDir {
				t.Errorf("directory exists = %v (%v), want %v", err == nil, err, tt.wantDir)
			}
			got, err := sandboxruntimes.Read(s.runtimesFile("pod-a"))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("file mismatch (-want +got):\n%s", diff)
			}
			if tt.wantUntouched {
				if after, err := os.Stat(s.runtimesFile("pod-a")); err != nil || !after.ModTime().Equal(fileBefore) {
					t.Errorf("an equal file was rewritten (mtime %v -> %v, %v)", fileBefore, after.ModTime(), err)
				}
			}
			if got := s.queue.NumRequeues("pod-a"); got != tt.wantRetries {
				t.Errorf("NumRequeues = %d, want %d", got, tt.wantRetries)
			}
			if _, ok := s.index.worker("pod-a"); ok != tt.wantIndexed {
				t.Errorf("pod-a in the index = %v, want %v", ok, tt.wantIndexed)
			}
			requeued := make(chan struct{})
			go func() {
				defer close(requeued)
				if uid, shutdown := s.queue.Get(); !shutdown {
					s.queue.Done(uid)
				}
			}()
			select {
			case <-requeued:
				if tt.wantRetries == 0 {
					t.Error("item was requeued")
				}
			case <-time.After(500 * time.Millisecond):
			}
			s.queue.ShutDown()
			<-requeued
		})
	}
}

func TestWorkerSandboxSyncerExistingDirs(t *testing.T) {
	const (
		oldDir   = "old dir"
		youngDir = "young dir"
		file     = "file"
	)
	tests := []struct {
		name    string
		entries map[string]string // name -> oldDir, youngDir or file
		// want is enqueued right away, wantLater once minAge has passed.
		want      []string
		wantLater []string
	}{{
		name:    "worker directories",
		entries: map[string]string{"pod-a": oldDir, "pod-b": oldDir},
		want:    []string{"pod-a", "pod-b"},
	}, {
		// Its worker may have made it before the pod reached the cache.
		name:      "a young directory waits for minAge",
		entries:   map[string]string{"pod-a": oldDir, "pod-b": youngDir},
		want:      []string{"pod-a"},
		wantLater: []string{"pod-b"},
	}, {
		name:    "names that are no pod UID and files are skipped",
		entries: map[string]string{"pod-a": oldDir, "Not_A_UID": oldDir, "stray": file},
		want:    []string{"pod-a"},
	}, {
		name: "no directory yet",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// On the bubble's clock the sleep fires the delayed enqueue at
			// once. Directory ages are stamped from that clock too.
			synctest.Test(t, func(t *testing.T) {
				s := newTestSyncer(t)
				if tt.entries == nil {
					s.workersDir = filepath.Join(s.workersDir, "missing")
				}
				now := time.Now()
				for name, kind := range tt.entries {
					path := filepath.Join(s.workersDir, name)
					var err error
					switch kind {
					case file:
						err = os.WriteFile(path, nil, 0o600)
					case youngDir:
						if err = os.Mkdir(path, 0o700); err == nil {
							err = os.Chtimes(path, now, now)
						}
					case oldDir:
						if err = os.Mkdir(path, 0o700); err == nil {
							old := now.Add(-2 * s.minAge)
							err = os.Chtimes(path, old, old)
						}
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				s.enqueueExistingDirs(context.Background())
				synctest.Wait()
				if diff := cmp.Diff(tt.want, drain(s)); diff != "" {
					t.Errorf("enqueued UIDs mismatch (-want +got):\n%s", diff)
				}
				time.Sleep(s.minAge)
				synctest.Wait()
				if diff := cmp.Diff(tt.wantLater, drain(s)); diff != "" {
					t.Errorf("UIDs enqueued after minAge mismatch (-want +got):\n%s", diff)
				}
			})
		})
	}
}

// waitFor fails t unless cond holds within 10 seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// exists reports whether path exists.
func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// mkOldDir creates a worker directory old enough for the syncer to judge.
func mkOldDir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * orphanMinAge)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
}

// TestWorkerSandboxSyncerRun verifies that run drains the queue only once
// every cache has synced: before the pod cache syncs every pod looks gone, and
// before the WorkerPool cache syncs no pod resolves. It also verifies that run
// stops when ctx ends.
func TestWorkerSandboxSyncerRun(t *testing.T) {
	tests := []struct {
		name        string
		podsSynced  bool
		poolsSynced bool
		// wantOrphan is whether the directory of a pod that is gone remains.
		wantOrphan bool
		// wantFile is whether the directory of a live pod gets its file.
		wantFile bool
	}{{
		name:       "no cache synced",
		wantOrphan: true,
	}, {
		name:       "only the pod cache synced",
		podsSynced: true,
		wantOrphan: true,
	}, {
		name:        "every cache synced",
		podsSynced:  true,
		poolsSynced: true,
		wantFile:    true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestSyncer(t, workerPod("pod-live", "pool"), resolvedPool("pool", "gvisor-release"), multiVersionConfig())
			mkOldDir(t, s.podDir("pod-live"))
			mkOldDir(t, s.podDir("pod-gone"))
			// A pod Add event queued before the caches sync.
			s.queue.Add("pod-live")
			matches := func() bool {
				return exists(s.podDir("pod-gone")) == tt.wantOrphan && exists(s.runtimesFile("pod-live")) == tt.wantFile
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				s.run(ctx, func() bool { return tt.podsSynced }, func() bool { return tt.poolsSynced })
			}()
			if tt.podsSynced && tt.poolsSynced {
				waitFor(t, "the directories to be synced", matches)
			} else {
				// cache.WaitForCacheSync polls every 100ms.
				time.Sleep(300 * time.Millisecond)
				if !matches() {
					t.Errorf("before every cache synced: pod-gone directory exists = %v, pod-live file exists = %v; want %v, %v",
						exists(s.podDir("pod-gone")), exists(s.runtimesFile("pod-live")), tt.wantOrphan, tt.wantFile)
				}
			}

			cancel()
			select {
			case <-stopped:
			case <-time.After(10 * time.Second):
				t.Fatal("run did not stop when ctx ended")
			}
		})
	}
}

// TestStartWorkerSandboxSyncer runs the syncer against fake API servers
// through the informer wiring main uses: the pod UID index, the event
// handlers and the cache sync checks.
func TestStartWorkerSandboxSyncer(t *testing.T) {
	tests := []struct {
		name string
		// pods exist before the informers start.
		pods []*corev1.Pod
		// orphans are directories left by pods that are gone.
		orphans []string
		// change runs once the files of pods are written.
		change     func(ctx context.Context, kube *kubefake.Clientset) error
		wantFiles  []string
		wantNoDirs []string
	}{{
		name:      "writes the file of a pod already on the node",
		pods:      []*corev1.Pod{workerPod("pod-a", "pool")},
		wantFiles: []string{"pod-a"},
	}, {
		name: "writes the file of a pod bound later",
		change: func(ctx context.Context, kube *kubefake.Clientset) error {
			_, err := kube.CoreV1().Pods("workers").Create(ctx, workerPod("pod-b", "pool"), metav1.CreateOptions{})
			return err
		},
		wantFiles: []string{"pod-b"},
	}, {
		name: "removes the directory of a deleted pod",
		pods: []*corev1.Pod{workerPod("pod-a", "pool")},
		change: func(ctx context.Context, kube *kubefake.Clientset) error {
			return kube.CoreV1().Pods("workers").Delete(ctx, "worker-pod-a", metav1.DeleteOptions{})
		},
		wantNoDirs: []string{"pod-a"},
	}, {
		name:       "removes a directory left by a pod that went away while atelet was down",
		pods:       []*corev1.Pod{workerPod("pod-a", "pool")},
		orphans:    []string{"pod-gone"},
		wantFiles:  []string{"pod-a"},
		wantNoDirs: []string{"pod-gone"},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, uid := range tt.orphans {
				mkOldDir(t, filepath.Join(dir, uid))
			}
			var podObjs []k8sruntime.Object
			for _, pod := range tt.pods {
				podObjs = append(podObjs, pod)
			}
			kube := kubefake.NewClientset(podObjs...)
			ate := atefake.NewSimpleClientset(resolvedPool("pool", "gvisor-release"), multiVersionConfig())
			podFactory := kubeinformers.NewSharedInformerFactory(kube, 0)
			ateFactory := externalversions.NewSharedInformerFactory(ate, 0)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() {
				cancel()
				podFactory.Shutdown()
				ateFactory.Shutdown()
			})

			useTempAssetScratch(t)
			s, err := startWorkerSandboxSyncer(ctx,
				podFactory.Core().V1().Pods(),
				ateFactory.Api().V1alpha1().WorkerPools(),
				ateFactory.Api().V1alpha1().SandboxConfigs(),
				newScriptAssetFetcher(t),
				dir, newRuntimeIndex(), nil)
			if err != nil {
				t.Fatalf("startWorkerSandboxSyncer() error = %v", err)
			}
			ateFactory.Start(ctx.Done())
			podFactory.Start(ctx.Done())

			for _, pod := range tt.pods {
				waitFor(t, "the file of "+string(pod.UID), func() bool { return exists(s.runtimesFile(string(pod.UID))) })
			}
			if tt.change != nil {
				if err := tt.change(ctx, kube); err != nil {
					t.Fatal(err)
				}
			}
			for _, uid := range tt.wantFiles {
				waitFor(t, "the file of "+uid, func() bool { return exists(s.runtimesFile(uid)) })
			}
			for _, uid := range tt.wantNoDirs {
				waitFor(t, "the directory of "+uid+" to be removed", func() bool { return !exists(s.podDir(uid)) })
			}
		})
	}
}

// TestWorkerSandboxSyncerRunIndexesExistingPods pins what run does with the
// pods already on the node: each is processed once the caches synced, so a
// pod that resolves is indexed, and one whose pool does not exist is not
// and is retried in the background without holding anything else.
func TestWorkerSandboxSyncerRunIndexesExistingPods(t *testing.T) {
	s := newTestSyncer(t, workerPod("pod-live", "pool"), workerPod("pod-orphan", "missing-pool"), resolvedPool("pool", "gvisor-release"), multiVersionConfig())
	// The initial list replays each pod as an Add before the caches sync.
	s.queue.Add("pod-live")
	s.queue.Add("pod-orphan")
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		s.run(ctx, func() bool { return true })
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})

	waitFor(t, "pod-live to be indexed", func() bool { _, ok := s.index.worker("pod-live"); return ok })
	wr, _ := s.index.worker("pod-live")
	cr, ok := wr.class(v1alpha1.SandboxClassGvisor)
	if !ok || cr.config != "gvisor-release" || cr.defaultHash == "" {
		t.Errorf("index.worker(pod-live).class(gvisor) = %+v, %v; want gvisor-release with a default runtime", cr, ok)
	}
	if _, ok := s.index.lookup(cr.defaultHash); !ok {
		t.Error("pod-live's default runtime is not resolvable")
	}
	waitFor(t, "pod-orphan to be retried", func() bool { return s.queue.NumRequeues("pod-orphan") > 0 })
	if _, ok := s.index.worker("pod-orphan"); ok {
		t.Error("a pod whose pool does not exist was indexed")
	}
}
