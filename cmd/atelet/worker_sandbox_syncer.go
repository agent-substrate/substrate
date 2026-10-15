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

// This file decides when each worker pod on this node is synced:
//   - start: wire the syncer to the Pod, WorkerPool and SandboxConfig
//     informers
//   - handlers: turn informer events into pod UIDs on the queue, with a
//     random delay when a pool or config change reaches every node at once
//   - process: for each UID, sync the pod, or remove its worker directory
//     once the pod is gone
//   - finish: forget the UID, or retry it with backoff up to
//     syncMaxRetries times
//
// worker_sandbox_versions.go builds the sandbox config version file for each
// worker pod.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	coreinformers "k8s.io/client-go/informers/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	informersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/informers/externalversions/api/v1alpha1"
)

// podUIDIndex indexes this node's worker pods by UID, the syncer's queue key.
// The key is the UID, not namespace/name, because worker directories are
// named by UID, and a directory left on disk is all the syncer has of a pod
// that is gone.
const podUIDIndex = "uid"

// orphanMinAge is how old a worker directory found on disk at startup must be
// before the syncer looks at it. The grace covers a worker that made its own
// directory before its pod reached atelet's cache.
const orphanMinAge = time.Minute

// syncMaxJitter spreads the fleet's file rewrites after a WorkerPool or
// SandboxConfig change. Every atelet observes the change within about a
// second, and each rewrite makes the worker register again, so without
// jitter every worker in the fleet would hit ateapi at once. The jitter is
// applied as the enqueue delay, so duplicate events inside the window
// collapse into one queue entry. A pod bound to this node is
// enqueued without it, since only this atelet acts on it and its worker waits
// for the file.
const syncMaxJitter = 30 * time.Second

// syncMaxRetries bounds how often one pod's failed sync is retried before it
// is dropped. The pod is synced again only when its pool switches to another
// SandboxConfig, the config's spec changes, or atelet restarts. The backoff
// doubles from one second, so this covers a few minutes of informer lag or an
// unresolved pool without retrying forever.
const syncMaxRetries = 8

// workerSandboxSyncer keeps a sandbox versions file in each local worker's
// directory under workersDir: the runtimes of the enabled versions of the
// SandboxConfig the worker's pool uses. It creates a worker's directory once
// it writes the pod's file and removes it when the pod is gone.
type workerSandboxSyncer struct {
	resolver *workerSandboxResolver
	// pods holds this node's worker pods, indexed by podUIDIndex.
	pods cache.Indexer
	// queue carries pod UIDs. One worker drains it, so a pod's file is never
	// written and its directory removed at the same time.
	queue      workqueue.TypedRateLimitingInterface[string]
	workersDir string // nodepath.WorkersDir(); a temp dir in tests
	minAge     time.Duration
	maxJitter  time.Duration
}

func newWorkerSandboxSyncer(resolver *workerSandboxResolver, pods cache.Indexer, workersDir string) *workerSandboxSyncer {
	return &workerSandboxSyncer{
		resolver: resolver,
		pods:     pods,
		// Failures are informer lag and unresolved pools, both on the scale
		// of seconds to minutes.
		queue: workqueue.NewTypedRateLimitingQueue(
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](time.Second, 5*time.Minute)),
		workersDir: workersDir,
		minAge:     orphanMinAge,
		maxJitter:  syncMaxJitter,
	}
}

// startWorkerSandboxSyncer wires a syncer to the given informers, which the
// caller starts, and runs it until ctx ends. pods must hold only this node's
// worker pods and must not have started yet. Worker directories live under
// workersDir.
//
// The syncer never holds up atelet startup: until the informers sync, or
// while a list/watch fails (e.g. Forbidden while an RBAC rollout lags the
// binary), files wait and existing ones stay as they are.
func startWorkerSandboxSyncer(ctx context.Context, pods coreinformers.PodInformer, workerPools informersv1alpha1.WorkerPoolInformer, configs informersv1alpha1.SandboxConfigInformer, workersDir string) (*workerSandboxSyncer, error) {
	if err := pods.Informer().AddIndexers(cache.Indexers{podUIDIndex: func(obj any) ([]string, error) {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return nil, nil
		}
		return []string{string(pod.UID)}, nil
	}}); err != nil {
		return nil, fmt.Errorf("while indexing worker pods by UID: %w", err)
	}
	resolver := &workerSandboxResolver{
		workerPools: workerPools.Lister(),
		configs:     configs.Lister(),
	}
	s := newWorkerSandboxSyncer(resolver, pods.Informer().GetIndexer(), workersDir)
	if err := watch(ctx, "worker pod", pods.Informer(), s.podHandler()); err != nil {
		return nil, err
	}
	if err := watch(ctx, "WorkerPool", workerPools.Informer(), s.workerPoolHandler()); err != nil {
		return nil, err
	}
	if err := watch(ctx, "SandboxConfig", configs.Informer(), s.sandboxConfigHandler()); err != nil {
		return nil, err
	}
	go s.run(ctx, pods.Informer().HasSynced, workerPools.Informer().HasSynced, configs.Informer().HasSynced)
	slog.InfoContext(ctx, "Worker sandbox versions syncer started")
	return s, nil
}

// watch registers handler on inf and logs inf's list/watch failures.
func watch(ctx context.Context, kind string, inf cache.SharedIndexInformer, handler cache.ResourceEventHandler) error {
	if err := inf.SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
		slog.WarnContext(ctx, kind+" list/watch failed; workers on this node cannot update their sandbox versions until it recovers", slog.Any("err", err))
	}); err != nil {
		// Fails only on an already-started informer, where the default
		// reflector logging applies.
		slog.InfoContext(ctx, "Could not set watch error handler", slog.String("kind", kind), slog.Any("err", err))
	}
	if _, err := inf.AddEventHandler(handler); err != nil {
		return fmt.Errorf("while registering the %s handler: %w", kind, err)
	}
	return nil
}

// podHandler enqueues a pod when it is bound here and when it is gone. The
// initial list replays every existing pod as an Add, so a restarted atelet
// rewrites every file from current state.
func (s *workerSandboxSyncer) podHandler() cache.ResourceEventHandler {
	enqueue := func(obj any) {
		if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = d.Obj
		}
		if pod, ok := obj.(*corev1.Pod); ok {
			s.queue.Add(string(pod.UID))
		}
	}
	return cache.ResourceEventHandlerFuncs{AddFunc: enqueue, DeleteFunc: enqueue}
}

// workerPoolHandler enqueues a pool's local pods when it appears and when the
// config its workers run changes.
func (s *workerSandboxSyncer) workerPoolHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if pool, ok := obj.(*v1alpha1.WorkerPool); ok {
				s.enqueuePool(pool, 0)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldPool, oldOK := oldObj.(*v1alpha1.WorkerPool)
			newPool, newOK := newObj.(*v1alpha1.WorkerPool)
			if oldOK && newOK && poolConfigName(oldPool) != poolConfigName(newPool) {
				s.enqueuePool(newPool, s.maxJitter)
			}
		},
	}
}

// sandboxConfigHandler enqueues the local pods of every pool that runs the
// config, when it appears and when its spec changes.
func (s *workerSandboxSyncer) sandboxConfigHandler() cache.ResourceEventHandler {
	enqueue := func(cfg *v1alpha1.SandboxConfig) {
		pools, err := s.resolver.workerPools.List(labels.Everything())
		if err != nil {
			return
		}
		for _, pool := range pools {
			if poolConfigName(pool) == cfg.Name {
				s.enqueuePool(pool, s.maxJitter)
			}
		}
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if cfg, ok := obj.(*v1alpha1.SandboxConfig); ok {
				enqueue(cfg)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldCfg, oldOK := oldObj.(*v1alpha1.SandboxConfig)
			newCfg, newOK := newObj.(*v1alpha1.SandboxConfig)
			// Generation moves on spec changes only.
			if oldOK && newOK && oldCfg.Generation != newCfg.Generation {
				enqueue(newCfg)
			}
		},
	}
}

// enqueuePool enqueues every local worker pod of pool, each after a random
// delay below maxDelay when it is not zero.
func (s *workerSandboxSyncer) enqueuePool(pool *v1alpha1.WorkerPool, maxDelay time.Duration) {
	pods, err := corelisters.NewPodLister(s.pods).Pods(pool.Namespace).List(labels.SelectorFromSet(labels.Set{workerPoolLabel: pool.Name}))
	if err != nil {
		return
	}
	for _, pod := range pods {
		if maxDelay > 0 {
			s.queue.AddAfter(string(pod.UID), rand.N(maxDelay))
			continue
		}
		s.queue.Add(string(pod.UID))
	}
}

// enqueueExistingDirs enqueues the UID of every worker directory on disk, so
// one left by a pod that went away while atelet was down is removed. A
// directory younger than minAge is enqueued once it reaches that age.
func (s *workerSandboxSyncer) enqueueExistingDirs(ctx context.Context) {
	entries, err := os.ReadDir(s.workersDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.WarnContext(ctx, "Could not list worker directories", slog.String("dir", s.workersDir), slog.Any("err", err))
		}
		return
	}
	for _, e := range entries {
		if !e.IsDir() || resources.ValidateWorkerPodUID(e.Name()) != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			// Removed since the listing.
			continue
		}
		if age := time.Since(info.ModTime()); age < s.minAge {
			s.queue.AddAfter(e.Name(), s.minAge-age)
			continue
		}
		s.queue.Add(e.Name())
	}
}

// run drains the queue until ctx ends. It starts only once every cache in
// synced has synced:
//   - before the pod cache syncs, every pod looks gone
//   - before the WorkerPool and SandboxConfig caches sync, no pod resolves
//     and each sync would spend a retry
func (s *workerSandboxSyncer) run(ctx context.Context, synced ...cache.InformerSynced) {
	go func() {
		<-ctx.Done()
		s.queue.ShutDown()
	}()
	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		return
	}
	s.enqueueExistingDirs(ctx)
	for {
		uid, shutdown := s.queue.Get()
		if shutdown {
			return
		}
		s.process(ctx, uid)
	}
}

// process brings the directory of the pod with the given UID up to date:
// written from its pool's current config while the pod exists, removed once
// it is gone.
func (s *workerSandboxSyncer) process(ctx context.Context, uid string) {
	defer s.queue.Done(uid)
	pod, err := s.podByUID(uid)
	switch {
	case err != nil:
	case pod == nil:
		err = s.removeDir(ctx, uid)
	case pod.DeletionTimestamp != nil:
		// The Delete event removes the directory.
	default:
		err = s.syncPod(ctx, pod)
	}
	s.finish(ctx, uid, err)
}

// podByUID returns the cached pod with the given UID, or nil if there is none.
func (s *workerSandboxSyncer) podByUID(uid string) (*corev1.Pod, error) {
	objs, err := s.pods.ByIndex(podUIDIndex, uid)
	if err != nil {
		// Only a missing index fails, and startWorkerSandboxSyncer adds it.
		return nil, fmt.Errorf("%w: %w", errPermanent, err)
	}
	if len(objs) == 0 {
		return nil, nil
	}
	pod, ok := objs[0].(*corev1.Pod)
	if !ok {
		return nil, fmt.Errorf("%w: pod cache holds a %T", errPermanent, objs[0])
	}
	return pod, nil
}

// finish forgets uid on success or errPermanent, and otherwise retries it with
// backoff up to syncMaxRetries times. A sync that fails before it writes
// leaves the existing file in place.
func (s *workerSandboxSyncer) finish(ctx context.Context, uid string, err error) {
	switch {
	case err == nil:
		s.queue.Forget(uid)
	case errors.Is(err, errPermanent):
		slog.WarnContext(ctx, "Cannot sync worker sandbox versions; not retrying until the pool's SandboxConfig changes or atelet restarts", slog.String("pod_uid", uid), slog.Any("err", err))
		s.queue.Forget(uid)
	case ctx.Err() != nil:
		// Shutting down, not a sync failure.
	case s.queue.NumRequeues(uid) < syncMaxRetries:
		slog.WarnContext(ctx, "Could not sync worker sandbox versions; will retry",
			slog.String("pod_uid", uid), slog.Int("retries", s.queue.NumRequeues(uid)), slog.Any("err", err))
		s.queue.AddRateLimited(uid)
	default:
		slog.WarnContext(ctx, "Could not sync worker sandbox versions; giving up until the pool's SandboxConfig changes or atelet restarts",
			slog.String("pod_uid", uid), slog.Int("retries", syncMaxRetries), slog.Any("err", err))
		s.queue.Forget(uid)
	}
}

func (s *workerSandboxSyncer) removeDir(ctx context.Context, uid string) error {
	if err := os.RemoveAll(s.podDir(uid)); err != nil {
		return fmt.Errorf("while removing worker directory: %w", err)
	}
	slog.DebugContext(ctx, "Removed the directory of a worker pod that is gone", slog.String("pod_uid", uid))
	return nil
}

func (s *workerSandboxSyncer) podDir(uid string) string {
	return filepath.Join(s.workersDir, uid)
}
