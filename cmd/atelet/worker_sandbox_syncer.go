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
//   - finish: forget the UID, or retry it with backoff, capped at
//     syncMaxRetries while the pool's SandboxConfig is unresolved
//
// worker_sandbox_versions.go builds the sandbox runtimes file for each
// worker pod.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	coreinformers "k8s.io/client-go/informers/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/resources"
	ateinformers "go.ate.dev/substrate/pkg/client/informers/externalversions/ate/v1"
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
// for the file. Only the file waits: the changed config's runtimes are
// registered in the index at once (see enqueuePool), so an actor the
// control plane places for the new default inside the window resolves.
const syncMaxJitter = 30 * time.Second

// syncMaxRetries bounds how often one pod's errUnresolved sync is retried
// before it is dropped. The backoff doubles from one second, so this covers a
// few minutes of informer lag or an unresolved pool without retrying forever;
// the informer events that end those also enqueue the pod again. Every other
// failure is exempt and keeps retrying at the rate limiter's longest backoff:
// an asset fetch or probe that fails for the default version, one that leaves
// a restorable version out (errIncomplete), or a write to the worker
// directory, since nothing enqueues the pod when those recover.
const syncMaxRetries = 8

// syncWorkers is how many goroutines drain the sync queue in run. More than
// one keeps a slow asset fetch on one pod from blocking directory cleanup and
// cached syncs for other pods on the node.
const syncWorkers = 2

// workerSandboxSyncer keeps a sandbox runtimes file in each local worker's
// directory under workersDir: the runtimes of the enabled versions of the
// SandboxConfig the worker's pool uses. It creates a worker's directory once
// it writes the pod's file and removes it when the pod is gone.
type workerSandboxSyncer struct {
	resolver *workerSandboxResolver
	// index is told each pod's runtimes before its file is written, and
	// what the pod advertises once it is.
	index *runtimeIndex
	// prewarm, if set, is handed each default runtime a pod's file
	// advertises, with a directory under the pod's to link the assets into,
	// so they are fetched before an actor asks for them and stay cached
	// while none runs them.
	prewarm runtimePrewarm
	// pods holds this node's worker pods, indexed by podUIDIndex.
	pods   cache.Indexer
	assets sandboxAssetFetcher
	// scratchDir is where the runsc probe stages the binary it runs, read
	// from ateletpath once here so a test can point a syncer elsewhere.
	scratchDir string
	// queue carries pod UIDs. A workqueue never hands the same key to two
	// workers at once, so a pod's file is never written and its directory
	// removed at the same time.
	queue      workqueue.TypedRateLimitingInterface[string]
	workersDir string // nodepath.WorkersDir(); a temp dir in tests
	minAge     time.Duration
	maxJitter  time.Duration

	// probeCPUIdentity is hardware.ProbeCPUIdentity. Overridable in tests.
	probeCPUIdentity func() (vendor, model string, err error)
	// runCPUFeatures is runRunscCPUFeatures. Overridable in tests.
	runCPUFeatures func(ctx context.Context, runscPath string) ([]string, error)
	// mu guards hostCPU and cpuFeatures across the syncWorkers in run:
	// hostCPU is the probed host CPU identity, nil until a probe succeeds,
	// and cpuFeatures the encoded cpu_features of each runsc binary by its
	// gVisor asset's SHA256 digest. A binary is fetched and run once per
	// digest, except that two workers meeting a digest before either has
	// memoized it both probe it; the duplicate costs a fetch from the asset
	// cache and a run of runsc, and both store the same mask.
	mu          sync.Mutex
	hostCPU     *cpuIdentity
	cpuFeatures map[string]string
}

func newWorkerSandboxSyncer(resolver *workerSandboxResolver, pods cache.Indexer, assets sandboxAssetFetcher, workersDir string, index *runtimeIndex, prewarm runtimePrewarm) *workerSandboxSyncer {
	return &workerSandboxSyncer{
		resolver:   resolver,
		pods:       pods,
		assets:     assets,
		scratchDir: ateletpath.SandboxAssetScratchDir,
		index:      index,
		prewarm:    prewarm,
		// Backoff doubles from 1s for informer lag and unresolved pools, and
		// caps at 5m where uncapped retries for transient asset fetch or probe
		// failures stay until they recover.
		queue: workqueue.NewTypedRateLimitingQueue(
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](time.Second, 5*time.Minute)),
		workersDir:       workersDir,
		minAge:           orphanMinAge,
		maxJitter:        syncMaxJitter,
		probeCPUIdentity: hardware.ProbeCPUIdentity,
		runCPUFeatures:   runRunscCPUFeatures,
		cpuFeatures:      make(map[string]string),
	}
}

// runtimePrewarm is what the syncer asks of the runtime prewarmer: fetch a
// default runtime and link its assets into the given directory.
type runtimePrewarm interface {
	enqueue(hash, dir string)
}

// defaultRuntimeDirsName is the directory under a worker's own that holds
// one directory per default runtime the worker advertises, by bundle hash,
// one per sandbox class, which the prewarmer links the runtime's assets
// into. The links are what keeps the assets cached while no actor runs
// them: the cache never evicts a linked entry. The syncer makes a runtime's
// directory when the worker's file first advertises it as a default and
// removes it when the file stops; they all go with the worker's directory.
const defaultRuntimeDirsName = "default-runtime"

// defaultRuntimeDir is the directory for the default runtime hash of the
// worker pod.
func (s *workerSandboxSyncer) defaultRuntimeDir(uid, hash string) string {
	return filepath.Join(s.podDir(uid), defaultRuntimeDirsName, strings.ReplaceAll(hash, ":", "-"))
}

// syncDefaultRuntimeDirs makes the worker pod's default-runtime directory
// hold one directory per hash in defaults and nothing else: the others are
// removed, which unlinks the runtimes the worker no longer advertises, and
// the missing ones are created empty for the prewarmer to fill. Called
// once the pod's file advertises defaults, so nothing it removes is a
// runtime the worker still names.
func (s *workerSandboxSyncer) syncDefaultRuntimeDirs(uid string, defaults []string) error {
	parent := filepath.Join(s.podDir(uid), defaultRuntimeDirsName)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return fmt.Errorf("while creating the worker's default runtime dir: %w", err)
	}
	want := make(map[string]bool, len(defaults))
	for _, hash := range defaults {
		want[filepath.Base(s.defaultRuntimeDir(uid, hash))] = true
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		return fmt.Errorf("while listing the worker's default runtime dir: %w", err)
	}
	for _, e := range entries {
		if !want[e.Name()] {
			if err := os.RemoveAll(filepath.Join(parent, e.Name())); err != nil {
				return fmt.Errorf("while removing a retired default runtime dir: %w", err)
			}
		}
	}
	for name := range want {
		if err := os.Mkdir(filepath.Join(parent, name), 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("while creating a default runtime dir: %w", err)
		}
	}
	return nil
}

// startWorkerSandboxSyncer wires a syncer to the given informers, which the
// caller starts, and runs it until ctx ends. pods must hold only this node's
// worker pods and must not have started yet. Worker directories live under
// workersDir.
//
// A syncer that cannot start is fatal to atelet: without it no runtime is
// ever indexed, so no actor could run. A syncer that cannot sync does not
// hold up serving: every runtime a worker registered before a restart was
// loaded from disk with the index, so while the informers have not synced,
// or a list/watch fails (e.g. Forbidden while an RBAC rollout lags the
// binary), existing files and index entries stay as they are and only
// changes wait.
func startWorkerSandboxSyncer(ctx context.Context, pods coreinformers.PodInformer, workerPools ateinformers.WorkerPoolInformer, configs ateinformers.SandboxConfigInformer, assets sandboxAssetFetcher, workersDir string, index *runtimeIndex, prewarm runtimePrewarm) (*workerSandboxSyncer, error) {
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
	s := newWorkerSandboxSyncer(resolver, pods.Informer().GetIndexer(), assets, workersDir, index, prewarm)
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
	slog.InfoContext(ctx, "Worker sandbox runtimes syncer started")
	return s, nil
}

// watch registers handler on inf and logs inf's list/watch failures.
func watch(ctx context.Context, kind string, inf cache.SharedIndexInformer, handler cache.ResourceEventHandler) error {
	if err := inf.SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
		slog.WarnContext(ctx, kind+" list/watch failed; workers on this node cannot update their sandbox runtimes until it recovers", slog.Any("err", err))
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
			if pool, ok := obj.(*atev1.WorkerPool); ok {
				s.enqueuePool(pool, 0)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldPool, oldOK := oldObj.(*atev1.WorkerPool)
			newPool, newOK := newObj.(*atev1.WorkerPool)
			if oldOK && newOK && oldPool.ResolvedSandboxConfig() != newPool.ResolvedSandboxConfig() {
				s.enqueuePool(newPool, s.maxJitter)
			}
		},
	}
}

// sandboxConfigHandler enqueues the local pods of every pool that runs the
// config, when it appears and when its spec changes.
func (s *workerSandboxSyncer) sandboxConfigHandler() cache.ResourceEventHandler {
	enqueue := func(cfg *atev1.SandboxConfig) {
		pools, err := s.resolver.workerPools.List(labels.Everything())
		if err != nil {
			return
		}
		for _, pool := range pools {
			if pool.ResolvedSandboxConfig() == cfg.Name {
				s.enqueuePool(pool, s.maxJitter)
			}
		}
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if cfg, ok := obj.(*atev1.SandboxConfig); ok {
				enqueue(cfg)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldCfg, oldOK := oldObj.(*atev1.SandboxConfig)
			newCfg, newOK := newObj.(*atev1.SandboxConfig)
			// Generation moves on spec changes only.
			if oldOK && newOK && oldCfg.Generation != newCfg.Generation {
				enqueue(newCfg)
			}
		},
	}
}

// enqueuePool enqueues every local worker pod of pool, each after a random
// delay below maxDelay when it is not zero.
//
// A delayed pool has its runtimes registered first, so the index knows a
// config's runtimes as soon as the listers do rather than once the files
// are rewritten. A request for a runtime the index does not know fails the
// actor, and the registration costs no IO.
func (s *workerSandboxSyncer) enqueuePool(pool *atev1.WorkerPool, maxDelay time.Duration) {
	pods, err := corelisters.NewPodLister(s.pods).Pods(pool.Namespace).List(labels.SelectorFromSet(labels.Set{workerPoolLabel: pool.Name}))
	if err != nil || len(pods) == 0 {
		return
	}
	if maxDelay > 0 {
		// Once per pool: every pod of it plans the same config.
		s.registerAhead(pool)
	}
	for _, pod := range pods {
		if maxDelay > 0 {
			s.queue.AddAfter(string(pod.UID), rand.N(maxDelay))
			continue
		}
		s.queue.Add(string(pod.UID))
	}
}

// registerAhead makes the runtimes of pool's current plan resolvable before
// its pods' syncs run. Planning and hashing read the listers only, so this
// is cheap enough for an event handler. Only the hashes are registered; what
// a pod advertises changes when its sync writes the file. A pool that does
// not plan is left to the syncs, which report and retry it.
func (s *workerSandboxSyncer) registerAhead(pool *atev1.WorkerPool) {
	plan, err := s.resolver.planPool(pool)
	if err != nil {
		return
	}
	hashed := hashVersions(plan)
	if err := s.index.registerRuntimes(plan.config, hashed); err != nil {
		// The syncs write it again, and report it if it keeps failing.
		slog.Warn("Could not index a changed pool's runtimes ahead of its sync", slog.String("pool", pool.Name), slog.Any("err", err))
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
	var wg sync.WaitGroup
	for range syncWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				uid, shutdown := s.queue.Get()
				if shutdown {
					return
				}
				s.process(ctx, uid)
			}
		}()
	}
	wg.Wait()
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

// finish forgets uid on success or errPermanent, caps errUnresolved retries at
// syncMaxRetries, and otherwise requeues with backoff without a retry cap. A
// sync that fails before it writes leaves the existing file in place.
func (s *workerSandboxSyncer) finish(ctx context.Context, uid string, err error) {
	switch {
	case err == nil:
		s.queue.Forget(uid)
	case errors.Is(err, errPermanent):
		slog.WarnContext(ctx, "Cannot sync worker sandbox runtimes; not retrying until the pool's SandboxConfig changes or atelet restarts", slog.String("pod_uid", uid), slog.Any("err", err))
		s.queue.Forget(uid)
	case ctx.Err() != nil:
		// Shutting down, not a sync failure.
	case errors.Is(err, errIncomplete):
		slog.WarnContext(ctx, "Worker sandbox runtimes file is missing a restorable version; will retry",
			slog.String("pod_uid", uid), slog.Int("retries", s.queue.NumRequeues(uid)), slog.Any("err", err))
		s.queue.AddRateLimited(uid)
	case s.queue.NumRequeues(uid) < syncMaxRetries || !errors.Is(err, errUnresolved):
		slog.WarnContext(ctx, "Could not sync worker sandbox runtimes; will retry",
			slog.String("pod_uid", uid), slog.Int("retries", s.queue.NumRequeues(uid)), slog.Any("err", err))
		s.queue.AddRateLimited(uid)
	default:
		slog.WarnContext(ctx, "Could not sync worker sandbox runtimes; giving up until the pool's SandboxConfig changes or atelet restarts",
			slog.String("pod_uid", uid), slog.Int("retries", syncMaxRetries), slog.Any("err", err))
		s.queue.Forget(uid)
	}
}

func (s *workerSandboxSyncer) removeDir(ctx context.Context, uid string) error {
	s.index.forget(uid)
	if err := os.RemoveAll(s.podDir(uid)); err != nil {
		return fmt.Errorf("while removing worker directory: %w", err)
	}
	slog.DebugContext(ctx, "Removed the directory of a worker pod that is gone", slog.String("pod_uid", uid))
	return nil
}

func (s *workerSandboxSyncer) podDir(uid string) string {
	return filepath.Join(s.workersDir, uid)
}
