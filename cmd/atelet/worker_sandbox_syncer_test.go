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
	"slices"
	"testing"
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

	"github.com/agent-substrate/substrate/internal/sandboxversions"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	atefake "github.com/agent-substrate/substrate/pkg/client/clientset/versioned/fake"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
)

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
	}, pods, t.TempDir())
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
			s := newTestSyncer(t, append(pods, pool)...)
			s.maxJitter = 50 * time.Millisecond
			tt.event(s)
			if diff := cmp.Diff(tt.want, drain(s)); diff != "" {
				t.Errorf("UIDs enqueued right away mismatch (-want +got):\n%s", diff)
			}
			time.Sleep(2 * s.maxJitter)
			if diff := cmp.Diff(tt.wantLater, drain(s)); diff != "" {
				t.Errorf("UIDs enqueued after the jitter mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestWorkerSandboxSyncerProcess(t *testing.T) {
	ctx := context.Background()
	deleting := workerPod("pod-a", "pool")
	deleting.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	stale := wantVersions()
	stale.DefaultRuntime = wantRuntime("v0")

	tests := []struct {
		name string
		objs []any
		// existing is written to pod-a's file before processing, when set.
		existing *sandboxversions.File
		// priorRetries is how often pod-a has been retried before.
		priorRetries int
		// want is the file afterwards; nil means no file.
		want    *sandboxversions.File
		wantDir bool
		// wantUntouched is whether the existing file must not be rewritten.
		wantUntouched bool
		wantRetries   int
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
		name:     "pod gone",
		objs:     []any{resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
		existing: stale,
	}, {
		name: "pod gone, no directory",
		objs: []any{resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
	}, {
		name:        "unresolved pool keeps the existing file",
		objs:        []any{workerPod("pod-a", "pool"), multiVersionConfig()},
		existing:    stale,
		want:        stale,
		wantDir:     true,
		wantRetries: 1,
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
		name: "pod of no pool is not retried",
		objs: []any{workerPod("pod-a", ""), resolvedPool("pool", "gvisor-release"), multiVersionConfig()},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestSyncer(t, tt.objs...)
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
				if _, err := sandboxversions.Write(s.versionsFile("pod-a"), tt.existing); err != nil {
					t.Fatal(err)
				}
			}
			var fileBefore time.Time
			if _, err := os.Stat(dir); err == nil {
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
			if diff := cmp.Diff(tt.want, got); diff != "" {
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
		// Its ateom may have made it before the pod reached the cache.
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
			s := newTestSyncer(t)
			// Short enough to observe a delayed enqueue.
			s.minAge = 50 * time.Millisecond
			if tt.entries == nil {
				s.ateomsDir = filepath.Join(s.ateomsDir, "missing")
			}
			for name, kind := range tt.entries {
				path := filepath.Join(s.ateomsDir, name)
				var err error
				switch kind {
				case file:
					err = os.WriteFile(path, nil, 0o600)
				case youngDir:
					err = os.Mkdir(path, 0o700)
				case oldDir:
					if err = os.Mkdir(path, 0o700); err == nil {
						old := time.Now().Add(-2 * orphanMinAge)
						err = os.Chtimes(path, old, old)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			s.enqueueExistingDirs(context.Background())
			if diff := cmp.Diff(tt.want, drain(s)); diff != "" {
				t.Errorf("enqueued UIDs mismatch (-want +got):\n%s", diff)
			}
			time.Sleep(2 * s.minAge)
			if diff := cmp.Diff(tt.wantLater, drain(s)); diff != "" {
				t.Errorf("UIDs enqueued after minAge mismatch (-want +got):\n%s", diff)
			}
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
				return exists(s.podDir("pod-gone")) == tt.wantOrphan && exists(s.versionsFile("pod-live")) == tt.wantFile
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
						exists(s.podDir("pod-gone")), exists(s.versionsFile("pod-live")), tt.wantOrphan, tt.wantFile)
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

			s, err := startWorkerSandboxSyncer(ctx,
				podFactory.Core().V1().Pods(),
				ateFactory.Api().V1alpha1().WorkerPools(),
				ateFactory.Api().V1alpha1().SandboxConfigs(),
				dir)
			if err != nil {
				t.Fatalf("startWorkerSandboxSyncer() error = %v", err)
			}
			ateFactory.Start(ctx.Done())
			podFactory.Start(ctx.Done())

			for _, pod := range tt.pods {
				waitFor(t, "the file of "+string(pod.UID), func() bool { return exists(s.versionsFile(string(pod.UID))) })
			}
			if tt.change != nil {
				if err := tt.change(ctx, kube); err != nil {
					t.Fatal(err)
				}
			}
			for _, uid := range tt.wantFiles {
				waitFor(t, "the file of "+uid, func() bool { return exists(s.versionsFile(uid)) })
			}
			for _, uid := range tt.wantNoDirs {
				waitFor(t, "the directory of "+uid+" to be removed", func() bool { return !exists(s.podDir(uid)) })
			}
		})
	}
}
