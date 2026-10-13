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
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	coreinformers "k8s.io/client-go/informers/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/sandboxversions"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	informersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/informers/externalversions/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
)

// errPermanent marks a failure that retrying cannot fix until the pod or its
// config changes, so the syncer drops the item instead of backing off.
var errPermanent = errors.New("permanent failure")

// podUIDIndex indexes this node's worker pods by UID, the syncer's queue key.
const podUIDIndex = "uid"

// orphanMinAge is how old a worker directory with no pod in the cache must be
// before the syncer removes it. The grace covers an ateom that made its own
// directory before atelet saw the pod.
const orphanMinAge = time.Minute

// workerSandboxResolver finds the SandboxConfig a worker on this node runs:
// the worker pod's WorkerPool, and the config the atecontroller resolved for
// the pool's sandbox class into the pool's status.
type workerSandboxResolver struct {
	workerPools listersv1alpha1.WorkerPoolLister
	configs     listersv1alpha1.SandboxConfigLister
	// synced reports whether every lister has completed its initial list;
	// atelet never blocks startup on these informers.
	synced func() bool
}

// resolve returns the sandbox class and SandboxConfig of a worker pod on this
// node. A pod that is no pool worker at all is errPermanent; anything else
// that fails can succeed once an informer catches up or the atecontroller
// resolves the pool.
func (r *workerSandboxResolver) resolve(pod *corev1.Pod) (v1alpha1.SandboxClass, *v1alpha1.SandboxConfig, error) {
	if !r.synced() {
		return "", nil, errors.New("WorkerPool and SandboxConfig caches have not synced yet")
	}
	poolName := pod.Labels[workerPoolLabel]
	if poolName == "" {
		return "", nil, fmt.Errorf("%w: worker pod %s/%s has no %s label", errPermanent, pod.Namespace, pod.Name, workerPoolLabel)
	}
	pool, err := r.workerPools.WorkerPools(pod.Namespace).Get(poolName)
	if err != nil {
		return "", nil, fmt.Errorf("while getting WorkerPool %s/%s: %w", pod.Namespace, poolName, err)
	}
	// The class the atecontroller built the pod spec from.
	class := pool.Spec.DefaultSandboxClass()
	var configName string
	for _, c := range pool.Status.SandboxClasses {
		// Never pendingConfigRef: that config is not confirmed yet.
		if c.Name == class && c.ConfigRef != nil {
			configName = c.ConfigRef.Name
		}
	}
	if configName == "" {
		return "", nil, fmt.Errorf("WorkerPool %s/%s has not resolved a SandboxConfig for sandbox class %q", pod.Namespace, poolName, class)
	}
	cfg, err := r.configs.Get(configName)
	if err != nil {
		return "", nil, fmt.Errorf("while getting SandboxConfig %q: %w", configName, err)
	}
	return class, cfg, nil
}

// sandboxVersionRecords projects the enabled versions of cfg onto this node's
// architecture, the default version first. Versions with no assets for this
// architecture are left out, except the default, which every worker of the
// pool must be able to start actors on.
func sandboxVersionRecords(cfg *v1alpha1.SandboxConfig) ([]*v1alpha1.SandboxVersionConfig, []*sandboxAssetsRecord, error) {
	all := enabledVersions(cfg)
	if len(all) == 0 {
		// The CRD rejects this shape, so this should not be reachable.
		return nil, nil, fmt.Errorf("%w: SandboxConfig %q has no enabled version %q named by its defaultVersion", errPermanent, cfg.Name, cfg.Spec.DefaultVersion)
	}
	var vers []*v1alpha1.SandboxVersionConfig
	var recs []*sandboxAssetsRecord
	for i, v := range all {
		rec, err := recordFromSandboxConfig(cfg, v)
		switch {
		case err == nil:
			vers = append(vers, v)
			recs = append(recs, rec)
		case i == 0:
			// The default version.
			return nil, nil, fmt.Errorf("%w: %w", errPermanent, err)
		case !errors.Is(err, errNoAssetsForArch):
			return nil, nil, err
		}
	}
	return vers, recs, nil
}

// workerSandboxSyncer keeps a sandbox versions file in each local worker's
// directory under ateomsDir: the enabled versions of the SandboxConfig the
// worker's pool uses, each with its assets fetched to the node. It creates a
// worker's directory when its pod is bound here and removes it when the pod
// is gone.
type workerSandboxSyncer struct {
	resolver *workerSandboxResolver
	// pods holds this node's worker pods, indexed by podUIDIndex.
	pods   cache.Indexer
	assets sandboxAssetFetcher
	// queue carries pod UIDs. One worker drains it, so a pod's file is never
	// written and its directory removed at the same time.
	queue     workqueue.TypedRateLimitingInterface[string]
	ateomsDir string // nodepath.AteomsDir(); a temp dir in tests
	minAge    time.Duration
	now       func() time.Time
}

func newWorkerSandboxSyncer(resolver *workerSandboxResolver, pods cache.Indexer, assets sandboxAssetFetcher) *workerSandboxSyncer {
	return &workerSandboxSyncer{
		resolver: resolver,
		pods:     pods,
		assets:   assets,
		// Failures are asset downloads and unresolved pools, both on the scale
		// of seconds to minutes, as for the prewarmer.
		queue: workqueue.NewTypedRateLimitingQueue(
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](time.Second, 5*time.Minute)),
		ateomsDir: nodepath.AteomsDir(),
		minAge:    orphanMinAge,
		now:       time.Now,
	}
}

// startWorkerSandboxSyncer wires a syncer to the given informers, which the
// caller starts, and runs it until ctx ends. pods must hold only this node's
// worker pods and must not have started yet.
//
// Like the prewarmer, the syncer never holds up atelet startup: until the
// informers sync, or while a list/watch fails (e.g. Forbidden while an RBAC
// rollout lags the binary), files wait and existing ones stay as they are.
func startWorkerSandboxSyncer(ctx context.Context, pods coreinformers.PodInformer, workerPools informersv1alpha1.WorkerPoolInformer, configs informersv1alpha1.SandboxConfigInformer, assets sandboxAssetFetcher) (*workerSandboxSyncer, error) {
	if err := pods.Informer().AddIndexers(cache.Indexers{podUIDIndex: func(obj any) ([]string, error) {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return nil, nil
		}
		return []string{string(pod.UID)}, nil
	}}); err != nil {
		return nil, fmt.Errorf("while indexing worker pods by UID: %w", err)
	}
	for kind, inf := range map[string]cache.SharedIndexInformer{
		"worker pod": pods.Informer(),
		"WorkerPool": workerPools.Informer(),
	} {
		if err := inf.SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
			slog.WarnContext(ctx, kind+" list/watch failed; workers on this node cannot update their sandbox versions until it recovers", slog.Any("err", err))
		}); err != nil {
			slog.InfoContext(ctx, "Could not set watch error handler", slog.String("kind", kind), slog.Any("err", err))
		}
	}
	resolver := &workerSandboxResolver{
		workerPools: workerPools.Lister(),
		configs:     configs.Lister(),
		synced: func() bool {
			return workerPools.Informer().HasSynced() && configs.Informer().HasSynced()
		},
	}
	s := newWorkerSandboxSyncer(resolver, pods.Informer().GetIndexer(), assets)
	for kind, reg := range map[string]struct {
		inf     cache.SharedIndexInformer
		handler cache.ResourceEventHandler
	}{
		"worker pod":    {pods.Informer(), s.podHandler()},
		"WorkerPool":    {workerPools.Informer(), s.workerPoolHandler()},
		"SandboxConfig": {configs.Informer(), s.sandboxConfigHandler()},
	} {
		if _, err := reg.inf.AddEventHandler(reg.handler); err != nil {
			return nil, fmt.Errorf("while registering the %s handler: %w", kind, err)
		}
	}
	go s.run(ctx, pods.Informer().HasSynced)
	slog.InfoContext(ctx, "Worker sandbox versions syncer started")
	return s, nil
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
// config of any of its classes changes.
func (s *workerSandboxSyncer) workerPoolHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if pool, ok := obj.(*v1alpha1.WorkerPool); ok {
				s.enqueuePool(pool)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldPool, ok := oldObj.(*v1alpha1.WorkerPool)
			if !ok {
				return
			}
			newPool, ok := newObj.(*v1alpha1.WorkerPool)
			if !ok {
				return
			}
			if configRefsChanged(oldPool, newPool) {
				s.enqueuePool(newPool)
			}
		},
	}
}

// configRefsChanged reports whether any class's configRef differs between
// two revisions of a pool. Other status and spec changes leave the files as
// they are.
func configRefsChanged(oldPool, newPool *v1alpha1.WorkerPool) bool {
	refs := func(pool *v1alpha1.WorkerPool) map[v1alpha1.SandboxClass]string {
		m := map[v1alpha1.SandboxClass]string{}
		for _, c := range pool.Status.SandboxClasses {
			if c.ConfigRef != nil {
				m[c.Name] = c.ConfigRef.Name
			}
		}
		return m
	}
	oldRefs, newRefs := refs(oldPool), refs(newPool)
	if len(oldRefs) != len(newRefs) {
		return true
	}
	for class, name := range newRefs {
		if oldRefs[class] != name {
			return true
		}
	}
	return false
}

// sandboxConfigHandler enqueues the local pods of every pool whose configRef
// names the config, when it appears and when its spec changes.
func (s *workerSandboxSyncer) sandboxConfigHandler() cache.ResourceEventHandler {
	enqueue := func(cfg *v1alpha1.SandboxConfig) {
		pools, err := s.resolver.workerPools.List(labels.Everything())
		if err != nil {
			return
		}
		for _, pool := range pools {
			for _, c := range pool.Status.SandboxClasses {
				if c.ConfigRef != nil && c.ConfigRef.Name == cfg.Name {
					s.enqueuePool(pool)
					break
				}
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
			oldCfg, ok := oldObj.(*v1alpha1.SandboxConfig)
			if !ok {
				return
			}
			newCfg, ok := newObj.(*v1alpha1.SandboxConfig)
			if !ok {
				return
			}
			// Generation moves on spec changes only.
			if oldCfg.Generation != newCfg.Generation {
				enqueue(newCfg)
			}
		},
	}
}

// enqueuePool enqueues every local worker pod of pool.
func (s *workerSandboxSyncer) enqueuePool(pool *v1alpha1.WorkerPool) {
	pods, err := corelisters.NewPodLister(s.pods).Pods(pool.Namespace).List(labels.SelectorFromSet(labels.Set{workerPoolLabel: pool.Name}))
	if err != nil {
		return
	}
	for _, pod := range pods {
		s.queue.Add(string(pod.UID))
	}
}

// enqueueExistingDirs enqueues the UID of every worker directory on disk, so
// one left by a pod that went away while atelet was down is removed.
func (s *workerSandboxSyncer) enqueueExistingDirs(ctx context.Context) {
	entries, err := os.ReadDir(s.ateomsDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.WarnContext(ctx, "Could not list worker directories", slog.String("dir", s.ateomsDir), slog.Any("err", err))
		}
		return
	}
	for _, e := range entries {
		if e.IsDir() && resources.ValidateWorkerPodUID(e.Name()) == nil {
			s.queue.Add(e.Name())
		}
	}
}

// run drains the queue until ctx ends. Directories left on disk are only
// judged once the pod cache has synced, since before that every pod looks
// gone.
func (s *workerSandboxSyncer) run(ctx context.Context, podsSynced cache.InformerSynced) {
	go func() {
		<-ctx.Done()
		s.queue.ShutDown()
	}()
	go func() {
		if cache.WaitForCacheSync(ctx.Done(), podsSynced) {
			s.enqueueExistingDirs(ctx)
		}
	}()
	for {
		uid, shutdown := s.queue.Get()
		if shutdown {
			return
		}
		s.process(ctx, uid)
	}
}

func (s *workerSandboxSyncer) podDir(uid string) string {
	return filepath.Join(s.ateomsDir, uid)
}

func (s *workerSandboxSyncer) versionsFile(uid string) string {
	return filepath.Join(s.podDir(uid), filepath.Base(nodepath.AteomSandboxVersionsFile(uid)))
}

// process brings the directory of the pod with the given UID up to date:
// written from its pool's current config while the pod exists, removed once
// it is gone.
func (s *workerSandboxSyncer) process(ctx context.Context, uid string) {
	defer s.queue.Done(uid)
	objs, err := s.pods.ByIndex(podUIDIndex, uid)
	if err != nil || len(objs) == 0 {
		s.queue.Forget(uid)
		s.removeOrphan(ctx, uid)
		return
	}
	pod, ok := objs[0].(*corev1.Pod)
	if !ok {
		s.queue.Forget(uid)
		return
	}
	if pod.DeletionTimestamp != nil {
		// The Delete event removes the directory.
		s.queue.Forget(uid)
		return
	}
	want, err := s.desiredVersions(ctx, pod)
	if err != nil {
		// Either way the existing file stays: a running ateom keeps the
		// versions it last read.
		if errors.Is(err, errPermanent) {
			slog.WarnContext(ctx, "Worker pod has no sandbox versions to write", slog.String("pod_uid", uid), slog.Any("err", err))
			s.queue.Forget(uid)
			return
		}
		if ctx.Err() != nil {
			return
		}
		slog.WarnContext(ctx, "Could not resolve worker sandbox versions; will retry",
			slog.String("pod_uid", uid), slog.Int("retries", s.queue.NumRequeues(uid)), slog.Any("err", err))
		s.queue.AddRateLimited(uid)
		return
	}
	if err := os.MkdirAll(s.podDir(uid), 0o700); err != nil {
		slog.WarnContext(ctx, "Could not create worker directory; will retry", slog.String("pod_uid", uid), slog.Any("err", err))
		s.queue.AddRateLimited(uid)
		return
	}
	wrote, err := sandboxversions.Write(s.versionsFile(uid), want)
	if err != nil {
		slog.WarnContext(ctx, "Could not write worker sandbox versions; will retry", slog.String("pod_uid", uid), slog.Any("err", err))
		s.queue.AddRateLimited(uid)
		return
	}
	if wrote {
		slog.InfoContext(ctx, "Wrote worker sandbox versions",
			slog.String("pod_uid", uid),
			slog.String("sandbox_config", want.GetSandboxConfig()),
			slog.Int("versions", len(want.GetVersions())))
	}
	s.queue.Forget(uid)
}

// removeOrphan removes the directory of a pod the cache does not hold, once
// it is older than minAge; a younger one is looked at again when it is.
func (s *workerSandboxSyncer) removeOrphan(ctx context.Context, uid string) {
	dir := s.podDir(uid)
	st, err := os.Stat(dir)
	if err != nil {
		return
	}
	if age := s.now().Sub(st.ModTime()); age < s.minAge {
		s.queue.AddAfter(uid, s.minAge-age)
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		slog.WarnContext(ctx, "Could not remove worker directory", slog.String("pod_uid", uid), slog.Any("err", err))
		return
	}
	slog.InfoContext(ctx, "Removed worker directory of a pod that is gone", slog.String("pod_uid", uid))
}

// desiredVersions builds the sandbox versions file content for pod, fetching
// every version's assets to the node first.
func (s *workerSandboxSyncer) desiredVersions(ctx context.Context, pod *corev1.Pod) (*ateletpb.WorkerSandboxVersions, error) {
	class, cfg, err := s.resolver.resolve(pod)
	if err != nil {
		return nil, err
	}
	vers, recs, err := sandboxVersionRecords(cfg)
	if err != nil {
		return nil, err
	}
	out := &ateletpb.WorkerSandboxVersions{SandboxClass: string(class), SandboxConfig: cfg.Name}
	for i, ver := range vers {
		rec := recs[i]
		// Usually a cache hit: the prewarmer fetches every enabled version.
		paths, err := s.assets.ensureSandboxAssets(ctx, rec)
		if err != nil {
			return nil, fmt.Errorf("SandboxConfig %q, version %q: %w", cfg.Name, ver.Name, err)
		}
		digests := make(map[string]string, len(rec.Assets))
		for name, a := range rec.Assets {
			digests[name] = a.SHA256
		}
		out.Versions = append(out.Versions, &ateletpb.SandboxVersion{
			Name:         ver.Name,
			IsDefault:    ver.Name == cfg.Spec.DefaultVersion,
			PauseImage:   rec.PauseImage,
			AssetPaths:   paths,
			AssetDigests: digests,
		})
	}
	return out, nil
}
