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

// This file decides how one worker pod's sandbox versions file is built,
// in the steps syncPod runs:
//   - resolve: find the SandboxConfig of the pod's WorkerPool
//   - plan: pick the config's versions this node can run, with no IO
//   - fetch: download each version's assets and pull its pause image
//   - write: replace the file in the worker directory if its content changed
//
// worker_sandbox_syncer.go decides when each pod is synced, and creates and
// removes the worker directories.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/sandboxversions"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
)

// syncTimeout bounds the fetches of one pod's sync. One worker drains the
// queue, so without it a download that stalls without failing (a wedged
// bucket read, a registry that accepts the connection and hangs) would block
// every other worker on the node. The versions are fetched in parallel, so
// each gets the whole timeout. Generous enough for multi-hundred-MiB micro-VM
// guest images on a busy node. A var so tests can shorten it.
var syncTimeout = 5 * time.Minute

// sandboxAssetFetcher is the one slice of AteomHerder the syncer needs.
// Fetching through the same method as the Run/Restore path keeps the two
// from ever diverging on cache layout or validation.
type sandboxAssetFetcher interface {
	ensureSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord) (map[string]string, error)
}

// pauseImageFetcher is the one slice of the image cache the syncer needs.
type pauseImageFetcher interface {
	EnsureImage(ctx context.Context, ref string) (*imagecache.Image, error)
}

// errPermanent marks a failure that retrying cannot fix, so the syncer drops
// the item instead of backing off until the next event for the pod:
//   - the pod has no worker pool label
//   - the config's default version is missing or has no assets for this
//     node's architecture
var errPermanent = errors.New("permanent failure")

// sandboxPlan is what one worker's sandbox versions file is built from, before
// anything is fetched.
type sandboxPlan struct {
	class    v1alpha1.SandboxClass
	config   string
	versions []versionPlan // the default first
}

// versionPlan is one SandboxConfig version projected onto this node's
// architecture.
type versionPlan struct {
	name      string
	isDefault bool
	rec       *sandboxAssetsRecord
}

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

// poolConfigName returns the SandboxConfig the atecontroller confirmed for the
// class the pool's workers run, or "" if it has not resolved one yet. It never
// returns pendingConfigRef: that config is not confirmed yet.
func poolConfigName(pool *v1alpha1.WorkerPool) string {
	class := pool.Spec.DefaultSandboxClass()
	for _, c := range pool.Status.SandboxClasses {
		if c.Name == class && c.ConfigRef != nil {
			return c.ConfigRef.Name
		}
	}
	return ""
}

// resolve returns the sandbox class and SandboxConfig of a worker pod on this
// node. Apart from a pod in no pool, every failure can succeed once an
// informer catches up or the atecontroller resolves the pool.
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
	configName := poolConfigName(pool)
	if configName == "" {
		return "", nil, fmt.Errorf("WorkerPool %s/%s has not resolved a SandboxConfig for sandbox class %q", pod.Namespace, poolName, pool.Spec.DefaultSandboxClass())
	}
	cfg, err := r.configs.Get(configName)
	if err != nil {
		return "", nil, fmt.Errorf("while getting SandboxConfig %q: %w", configName, err)
	}
	return pool.Spec.DefaultSandboxClass(), cfg, nil
}

// plan resolves pod's SandboxConfig and the versions of it this node can run.
func (r *workerSandboxResolver) plan(pod *corev1.Pod) (*sandboxPlan, error) {
	class, cfg, err := r.resolve(pod)
	if err != nil {
		return nil, err
	}
	vers, err := planVersions(cfg)
	if err != nil {
		return nil, err
	}
	return &sandboxPlan{class: class, config: cfg.Name, versions: vers}, nil
}

// planVersions returns the versions of cfg a worker on this node may use: the
// default version first, then the other Enabled versions in spec order.
// Versions with no assets for this node's architecture are left out, except
// the default, which every worker of the pool must be able to start actors on.
func planVersions(cfg *v1alpha1.SandboxConfig) ([]versionPlan, error) {
	def, ok := cfg.Spec.DefaultVersionConfig()
	if !ok {
		// The CRD rejects this shape, so this should not be reachable.
		return nil, fmt.Errorf("%w: SandboxConfig %q has no enabled version %q named by its defaultVersion", errPermanent, cfg.Name, cfg.Spec.DefaultVersion)
	}
	defRec, ok := recordForArch(cfg, def)
	if !ok {
		return nil, fmt.Errorf("%w: SandboxConfig %q, default version %q has no assets for architecture %q", errPermanent, cfg.Name, def.Name, runtime.GOARCH)
	}
	vers := []versionPlan{{name: def.Name, isDefault: true, rec: defRec}}
	for i := range cfg.Spec.Versions {
		v := &cfg.Spec.Versions[i]
		if v.Name == def.Name || (v.State != nil && *v.State == v1alpha1.SandboxVersionStateDisabled) {
			continue
		}
		if rec, ok := recordForArch(cfg, v); ok {
			vers = append(vers, versionPlan{name: v.Name, rec: rec})
		}
	}
	return vers, nil
}

// recordForArch projects the assets of one version of a SandboxConfig onto
// this node's architecture, mirroring recordFromRequest. It reports false when
// the version lists no assets for this architecture.
func recordForArch(cfg *v1alpha1.SandboxConfig, ver *v1alpha1.SandboxVersionConfig) (*sandboxAssetsRecord, bool) {
	files := ver.Assets[runtime.GOARCH]
	if len(files) == 0 {
		return nil, false
	}
	rec := &sandboxAssetsRecord{
		SandboxClass: string(cfg.Spec.SandboxClass),
		PauseImage:   ver.PauseImage,
		Assets:       make(map[string]assetEntry, len(files)),
	}
	for name, f := range files {
		rec.Assets[name] = assetEntry{URL: f.URL, SHA256: f.SHA256}
	}
	return rec, true
}

// fetch fetches every version in plan to the node, in parallel, and returns
// the file content naming their local paths.
//   - the default version fails: it returns no file, since no worker of the
//     pool could start actors on this node
//   - another version fails: it returns the file without that version, and
//     the error, so the syncer writes the file and retries the rest
func (s *workerSandboxSyncer) fetch(ctx context.Context, plan *sandboxPlan) (*sandboxversions.File, error) {
	ctx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()

	paths := make([]map[string]string, len(plan.versions))
	errs := make([]error, len(plan.versions))
	var wg sync.WaitGroup
	for i, v := range plan.versions {
		wg.Go(func() {
			p, err := s.fetchVersion(ctx, v.rec)
			if err != nil {
				errs[i] = fmt.Errorf("SandboxConfig %q, version %q: %w", plan.config, v.name, err)
				return
			}
			paths[i] = p
		})
	}
	wg.Wait()
	// plan.versions[0] is the default.
	if errs[0] != nil {
		return nil, errs[0]
	}

	out := &sandboxversions.File{SandboxClass: string(plan.class), SandboxConfig: plan.config}
	for i, v := range plan.versions {
		if errs[i] != nil {
			continue
		}
		digests := make(map[string]string, len(v.rec.Assets))
		for name, a := range v.rec.Assets {
			digests[name] = a.SHA256
		}
		out.Versions = append(out.Versions, sandboxversions.Version{
			Name:         v.name,
			IsDefault:    v.isDefault,
			PauseImage:   v.rec.PauseImage,
			AssetPaths:   paths[i],
			AssetDigests: digests,
		})
	}
	return out, errors.Join(errs...)
}

// fetchVersion downloads one version's assets and pulls its pause image into
// the image cache, so the first Run/Restore under it finds both on the node.
func (s *workerSandboxSyncer) fetchVersion(ctx context.Context, rec *sandboxAssetsRecord) (map[string]string, error) {
	paths, err := s.assets.ensureSandboxAssets(ctx, rec)
	if err != nil {
		return nil, err
	}
	if rec.PauseImage != "" {
		if _, err := s.images.EnsureImage(ctx, rec.PauseImage); err != nil {
			return nil, fmt.Errorf("while pulling pause image %q: %w", rec.PauseImage, err)
		}
	}
	return paths, nil
}

// syncPod writes pod's sandbox versions file:
//   - plan: resolve the pool's config and its versions for this node, no IO
//   - fetch: download every version's assets and pull its pause image
//   - write: replace the file if its content changed
//
// A failed non-default version is left out of the file and returned as the
// error, so the syncer retries it.
func (s *workerSandboxSyncer) syncPod(ctx context.Context, pod *corev1.Pod) error {
	plan, err := s.resolver.plan(pod)
	if err != nil {
		return err
	}
	want, fetchErr := s.fetch(ctx, plan)
	if want == nil {
		return fetchErr
	}
	if err := s.write(ctx, string(pod.UID), want); err != nil {
		return err
	}
	return fetchErr
}

func (s *workerSandboxSyncer) write(ctx context.Context, uid string, want *sandboxversions.File) error {
	if err := os.MkdirAll(s.podDir(uid), 0o700); err != nil {
		return fmt.Errorf("while creating worker directory: %w", err)
	}
	wrote, err := sandboxversions.Write(s.versionsFile(uid), want)
	if err != nil {
		return err
	}
	if wrote {
		slog.InfoContext(ctx, "Wrote worker sandbox versions",
			slog.String("pod_uid", uid),
			slog.String("sandbox_config", want.SandboxConfig),
			slog.Int("versions", len(want.Versions)))
	}
	return nil
}

func (s *workerSandboxSyncer) versionsFile(uid string) string {
	return filepath.Join(s.podDir(uid), nodepath.SandboxVersionsFileName)
}
