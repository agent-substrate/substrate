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
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/sandboxversions"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
)

// recordingFetcher stands in for the AteomHerder's asset fetch, recording
// every record asked for and placing each asset at /static/<sha256>.
type recordingFetcher struct {
	mu  sync.Mutex
	got []*sandboxAssetsRecord
	err error
}

func (f *recordingFetcher) ensureSandboxAssets(_ context.Context, rec *sandboxAssetsRecord) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, rec)
	if f.err != nil {
		return nil, f.err
	}
	paths := make(map[string]string, len(rec.Assets))
	for name, a := range rec.Assets {
		paths[name] = "/static/" + a.SHA256
	}
	return paths, nil
}

// multiVersionConfig is a gVisor config whose default version "v2" is listed
// after an Enabled "v1", a Disabled "v0", and an Enabled "other-arch" with no
// assets for this node.
func multiVersionConfig() *v1alpha1.SandboxConfig {
	version := func(name, sha string, state *v1alpha1.SandboxVersionState) v1alpha1.SandboxVersionConfig {
		return v1alpha1.SandboxVersionConfig{
			Name:       name,
			State:      state,
			PauseImage: "pause-" + name,
			Assets: map[string]map[string]v1alpha1.AssetFile{
				runtime.GOARCH: {gvisorAssetName: {URL: "gs://bucket/" + name, SHA256: sha}},
			},
		}
	}
	otherArch := version("other-arch", "sha-other", nil)
	otherArch.Assets = map[string]map[string]v1alpha1.AssetFile{"other-arch": otherArch.Assets[runtime.GOARCH]}
	return &v1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor-release"},
		Spec: v1alpha1.SandboxConfigSpec{
			SandboxClass:   v1alpha1.SandboxClassGvisor,
			DefaultVersion: "v2",
			Versions: []v1alpha1.SandboxVersionConfig{
				version("v0", "sha-v0", ptr.To(v1alpha1.SandboxVersionStateDisabled)),
				version("v1", "sha-v1", ptr.To(v1alpha1.SandboxVersionStateEnabled)),
				otherArch,
				version("v2", "sha-v2", nil),
			},
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
	pool := &v1alpha1.WorkerPool{ObjectMeta: metav1.ObjectMeta{Namespace: "workers", Name: name}}
	pool.Spec.SandboxClasses = []v1alpha1.WorkerPoolSandboxClass{{Name: v1alpha1.SandboxClassGvisor}}
	class := v1alpha1.WorkerPoolSandboxClassStatus{Name: v1alpha1.SandboxClassGvisor}
	if config != "" {
		class.ConfigRef = &v1alpha1.SandboxConfigReference{Name: config}
	}
	pool.Status.SandboxClasses = []v1alpha1.WorkerPoolSandboxClassStatus{class}
	return pool
}

// newTestSyncer serves objs from synced caches and keeps worker directories
// under a temp dir.
func newTestSyncer(t *testing.T, fetcher *recordingFetcher, objs ...any) *workerSandboxSyncer {
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
	if fetcher == nil {
		fetcher = &recordingFetcher{}
	}
	s := newWorkerSandboxSyncer(&workerSandboxResolver{
		workerPools: listersv1alpha1.NewWorkerPoolLister(pools),
		configs:     listersv1alpha1.NewSandboxConfigLister(configs),
		synced:      func() bool { return true },
	}, pods, fetcher)
	s.ateomsDir = t.TempDir()
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

func TestWorkerSandboxResolverResolve(t *testing.T) {
	cfg := gvisorConfig("gvisor-default", "gs://bucket/runsc", "abc")

	tests := []struct {
		name          string
		objs          []any
		unsynced      bool
		pod           *corev1.Pod
		wantConfig    string
		wantErr       bool
		wantPermanent bool
	}{{
		name:       "the config the pool resolved for the pool's class",
		objs:       []any{resolvedPool("pool", "gvisor-default"), cfg},
		pod:        workerPod("pod-a", "pool"),
		wantConfig: "gvisor-default",
	}, {
		// Startup never waits on these informers.
		name:     "caches not synced",
		objs:     []any{resolvedPool("pool", "gvisor-default"), cfg},
		unsynced: true,
		pod:      workerPod("pod-a", "pool"),
		wantErr:  true,
	}, {
		name:          "pod belongs to no pool",
		objs:          []any{resolvedPool("pool", "gvisor-default"), cfg},
		pod:           workerPod("pod-a", ""),
		wantErr:       true,
		wantPermanent: true,
	}, {
		name:    "pool not found",
		objs:    []any{cfg},
		pod:     workerPod("pod-a", "pool"),
		wantErr: true,
	}, {
		name:    "pool has not resolved a config",
		objs:    []any{resolvedPool("pool", ""), cfg},
		pod:     workerPod("pod-a", "pool"),
		wantErr: true,
	}, {
		// A pending config is one the atecontroller has claimed but not yet
		// confirmed exists and is not being deleted.
		name: "pool is only switching to a config",
		objs: []any{func() *v1alpha1.WorkerPool {
			pool := resolvedPool("pool", "")
			pool.Status.SandboxClasses[0].PendingConfigRef = &v1alpha1.SandboxConfigReference{Name: "gvisor-default"}
			return pool
		}(), cfg},
		pod:     workerPod("pod-a", "pool"),
		wantErr: true,
	}, {
		// The class comes from the pool's spec, not from the status entry.
		name: "status resolved another class than the pool's",
		objs: []any{func() *v1alpha1.WorkerPool {
			pool := resolvedPool("pool", "gvisor-default")
			pool.Spec.SandboxClasses[0].Name = v1alpha1.SandboxClassMicroVM
			return pool
		}(), cfg},
		pod:     workerPod("pod-a", "pool"),
		wantErr: true,
	}, {
		name:    "config not found",
		objs:    []any{resolvedPool("pool", "gvisor-missing"), cfg},
		pod:     workerPod("pod-a", "pool"),
		wantErr: true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestSyncer(t, nil, tt.objs...)
			if tt.unsynced {
				s.resolver.synced = func() bool { return false }
			}
			class, got, err := s.resolver.resolve(tt.pod)
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolve() error = %v, want error %v", err, tt.wantErr)
			}
			if errors.Is(err, errPermanent) != tt.wantPermanent {
				t.Errorf("resolve() error = %v, want permanent %v", err, tt.wantPermanent)
			}
			if err != nil {
				return
			}
			if got.Name != tt.wantConfig || class != v1alpha1.SandboxClassGvisor {
				t.Errorf("resolve() = %q, %q, want %q, %q", class, got.Name, v1alpha1.SandboxClassGvisor, tt.wantConfig)
			}
		})
	}
}

// wantVersion is the SandboxVersion desiredVersions builds from
// multiVersionConfig's version name under recordingFetcher.
func wantVersion(name string, isDefault bool) *ateletpb.SandboxVersion {
	return &ateletpb.SandboxVersion{
		Name:         name,
		IsDefault:    isDefault,
		PauseImage:   "pause-" + name,
		AssetPaths:   map[string]string{gvisorAssetName: "/static/sha-" + name},
		AssetDigests: map[string]string{gvisorAssetName: "sha-" + name},
	}
}

// wantVersions is the file content for multiVersionConfig.
func wantVersions() *ateletpb.WorkerSandboxVersions {
	return &ateletpb.WorkerSandboxVersions{
		SandboxClass:  "gvisor",
		SandboxConfig: "gvisor-release",
		Versions:      []*ateletpb.SandboxVersion{wantVersion("v2", true), wantVersion("v1", false)},
	}
}

func TestDesiredVersions(t *testing.T) {
	defaultNoArch := multiVersionConfig()
	defaultNoArch.Spec.Versions[3].Assets = map[string]map[string]v1alpha1.AssetFile{"other-arch": defaultNoArch.Spec.Versions[3].Assets[runtime.GOARCH]}

	tests := []struct {
		name          string
		cfg           *v1alpha1.SandboxConfig
		fetchErr      error
		want          *ateletpb.WorkerSandboxVersions
		wantErr       bool
		wantPermanent bool
		wantFetched   int
	}{{
		// Disabled versions and versions with nothing for this node's
		// architecture are left out; the default leads.
		name:        "default first, then the other enabled versions",
		cfg:         multiVersionConfig(),
		want:        wantVersions(),
		wantFetched: 2,
	}, {
		name:          "default version has no assets for this architecture",
		cfg:           defaultNoArch,
		wantErr:       true,
		wantPermanent: true,
	}, {
		name:        "surfaces a failed fetch",
		cfg:         multiVersionConfig(),
		fetchErr:    errors.New("bucket unavailable"),
		wantErr:     true,
		wantFetched: 1,
	}, {
		name:    "surfaces an unresolved config",
		wantErr: true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := []any{resolvedPool("pool", "gvisor-release")}
			if tt.cfg != nil {
				objs = append(objs, tt.cfg)
			}
			fetcher := &recordingFetcher{err: tt.fetchErr}
			s := newTestSyncer(t, fetcher, objs...)

			got, err := s.desiredVersions(context.Background(), workerPod("pod-a", "pool"))
			if (err != nil) != tt.wantErr {
				t.Fatalf("desiredVersions() error = %v, want error %v", err, tt.wantErr)
			}
			if errors.Is(err, errPermanent) != tt.wantPermanent {
				t.Errorf("desiredVersions() error = %v, want permanent %v", err, tt.wantPermanent)
			}
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("desiredVersions() mismatch (-want +got):\n%s", diff)
			}
			if len(fetcher.got) != tt.wantFetched {
				t.Errorf("fetched %d versions, want %d", len(fetcher.got), tt.wantFetched)
			}
		})
	}
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
		name: "config status changed",
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
			s := newTestSyncer(t, nil, append(pods, pools...)...)
			tt.event(s)
			if diff := cmp.Diff(tt.want, drain(s)); diff != "" {
				t.Errorf("enqueued pods mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestWorkerSandboxSyncerProcess(t *testing.T) {
	ctx := context.Background()
	deleting := workerPod("pod-a", "pool")
	deleting.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	stale := wantVersions()
	stale.SandboxConfig = "gvisor-old"

	tests := []struct {
		name string
		objs []any
		// existing is written to pod-a's file before processing, when set.
		existing *ateletpb.WorkerSandboxVersions
		// dirAge is how old pod-a's directory looks when processed.
		dirAge   time.Duration
		fetchErr error
		// want is the file afterwards; nil means no file.
		want    *ateletpb.WorkerSandboxVersions
		wantDir bool
		// wantUntouched is whether the existing file must not be rewritten.
		wantUntouched bool
		wantRetries   int
		wantRequeue   bool
	}{{
		name:    "writes the file",
		objs:    []any{workerPod("pod-a", "pool"), resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		want:    wantVersions(),
		wantDir: true,
	}, {
		name:     "replaces a stale file",
		objs:     []any{workerPod("pod-a", "pool"), resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		existing: stale,
		want:     wantVersions(),
		wantDir:  true,
	}, {
		name:          "leaves an equal file alone",
		objs:          []any{workerPod("pod-a", "pool"), resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		existing:      wantVersions(),
		want:          wantVersions(),
		wantDir:       true,
		wantUntouched: true,
	}, {
		// The Delete event removes the directory.
		name:     "pod being deleted",
		objs:     []any{deleting, resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		existing: stale,
		want:     stale,
		wantDir:  true,
	}, {
		name:        "pod gone, directory young",
		objs:        []any{resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		existing:    stale,
		want:        stale,
		wantDir:     true,
		wantRequeue: true,
	}, {
		name:     "pod gone, directory old",
		objs:     []any{resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		existing: stale,
		dirAge:   2 * orphanMinAge,
	}, {
		name: "pod gone, no directory",
		objs: []any{resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
	}, {
		// A running ateom keeps the versions it last read.
		name:        "unresolved pool keeps the existing file",
		objs:        []any{workerPod("pod-a", "pool"), multiVersionConfig()},
		existing:    stale,
		want:        stale,
		wantDir:     true,
		wantRetries: 1,
	}, {
		name:        "failed fetch keeps the existing file",
		objs:        []any{workerPod("pod-a", "pool"), resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		existing:    stale,
		fetchErr:    errors.New("bucket unavailable"),
		want:        stale,
		wantDir:     true,
		wantRetries: 1,
	}, {
		name: "pod of no pool is not retried",
		objs: []any{workerPod("pod-a", ""), resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestSyncer(t, &recordingFetcher{err: tt.fetchErr}, tt.objs...)
			// Short enough to observe a requeue of a young directory.
			s.minAge = 50 * time.Millisecond
			dir := s.podDir("pod-a")
			if tt.existing != nil {
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				if _, err := sandboxversions.Write(s.versionsFile("pod-a"), tt.existing); err != nil {
					t.Fatal(err)
				}
			}
			var fileBefore time.Time
			if st, err := os.Stat(dir); err == nil {
				s.now = func() time.Time { return st.ModTime().Add(tt.dirAge) }
				file, err := os.Stat(s.versionsFile("pod-a"))
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
			got, err := sandboxversions.Read(s.versionsFile("pod-a"))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("file mismatch (-want +got):\n%s", diff)
			}
			if tt.wantUntouched {
				if after, err := os.Stat(s.versionsFile("pod-a")); err != nil || !after.ModTime().Equal(fileBefore) {
					t.Errorf("an equal file was rewritten (mtime %v -> %v, %v)", fileBefore, after.ModTime(), err)
				}
			}
			if got := s.queue.NumRequeues("pod-a"); got != tt.wantRetries {
				t.Errorf("NumRequeues = %d, want %d", got, tt.wantRetries)
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
				if !tt.wantRequeue && tt.wantRetries == 0 {
					t.Error("item was requeued")
				}
			case <-time.After(500 * time.Millisecond):
				if tt.wantRequeue {
					t.Error("item was not requeued")
				}
			}
			s.queue.ShutDown()
			<-requeued
		})
	}
}

func TestWorkerSandboxSyncerExistingDirs(t *testing.T) {
	tests := []struct {
		name    string
		entries map[string]bool // name -> is a directory
		want    []string
	}{{
		name:    "worker directories",
		entries: map[string]bool{"pod-a": true, "pod-b": true},
		want:    []string{"pod-a", "pod-b"},
	}, {
		name:    "names that are no pod UID and files are skipped",
		entries: map[string]bool{"pod-a": true, "Not_A_UID": true, "stray": false},
		want:    []string{"pod-a"},
	}, {
		name: "no directory yet",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestSyncer(t, nil)
			if tt.entries == nil {
				s.ateomsDir = filepath.Join(s.ateomsDir, "missing")
			}
			for name, isDir := range tt.entries {
				path := filepath.Join(s.ateomsDir, name)
				var err error
				if isDir {
					err = os.Mkdir(path, 0o700)
				} else {
					err = os.WriteFile(path, nil, 0o600)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			s.enqueueExistingDirs(context.Background())
			if diff := cmp.Diff(tt.want, drain(s)); diff != "" {
				t.Errorf("enqueued UIDs mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
