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
//   - plan: pick the config's versions this node can run
//   - build: hash each version's assets and pause image digests into its
//     sandbox_runtime_hash
//   - write: replace the file in the worker directory if its content changed
//
// worker_sandbox_syncer.go decides when each pod is synced, and creates and
// removes the worker directories.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	corev1 "k8s.io/api/core/v1"

	"github.com/agent-substrate/substrate/internal/hardware"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/sandboxversions"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
)

// errPermanent marks a failure that retrying cannot fix, so the syncer drops
// the item instead of backing off until the next event for the pod:
//   - the pod has no worker pool label
//   - the config's default version is missing, has no assets for this
//     node's architecture, or has a pause image not pinned by digest
var errPermanent = errors.New("permanent failure")

// sandboxPlan is what one worker's sandbox versions file is built from.
type sandboxPlan struct {
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
	_, cfg, err := r.resolve(pod)
	if err != nil {
		return nil, err
	}
	vers, err := planVersions(cfg)
	if err != nil {
		return nil, err
	}
	return &sandboxPlan{config: cfg.Name, versions: vers}, nil
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

// buildFile returns the file content naming the runtimes of the versions in
// plan. The hashes come from the SandboxConfig spec alone, so it does no IO.
// A version whose runtime hash equals an earlier one's is left out, so the
// file lists each runtime once.
//
// The file does not promise a runtime's assets are on the node. The
// prewarmer fetches each config's default version, and Run/Restore fetch any
// other version on demand.
//
// TODO: fetch the default version's assets and pause image through
// cmd/atelet/internal/filecache here, and remove sandbox_prewarm.go. Other
// versions stay fetched on demand at Restore.
func buildFile(ctx context.Context, plan *sandboxPlan) (*sandboxversions.File, error) {
	out := &sandboxversions.File{}
	seen := make(map[string]bool, len(plan.versions))
	for _, v := range plan.versions {
		h, err := versionRuntimeHash(v.rec)
		if err != nil {
			err = fmt.Errorf("SandboxConfig %q, version %q: %w", plan.config, v.name, err)
			if v.isDefault {
				// No worker of the pool could start actors on this node.
				return nil, fmt.Errorf("%w: %w", errPermanent, err)
			}
			slog.WarnContext(ctx, "Leaving a sandbox version out of the worker sandbox versions file", slog.Any("err", err))
			continue
		}
		if seen[h] {
			continue
		}
		seen[h] = true
		attrs := sandboxversions.Attributes{hardware.AttrSandboxRuntimeHash: h}
		if v.isDefault {
			out.DefaultRuntime = attrs
		} else {
			out.RestorableRuntimes = append(out.RestorableRuntimes, attrs)
		}
	}
	return out, nil
}

// versionRuntimeHash returns the runtime hash of one version. The CRD
// requires every pause image to be pinned by digest, which is the digest the
// image cache resolves the ref to, so the hash needs no pull.
func versionRuntimeHash(rec *sandboxAssetsRecord) (string, error) {
	var pauseDigest string
	if rec.PauseImage != "" {
		ref, err := name.NewDigest(rec.PauseImage)
		if err != nil {
			return "", fmt.Errorf("pause image %q is not pinned by digest: %w", rec.PauseImage, err)
		}
		pauseDigest = ref.DigestStr()
	}
	return runtimeHash(rec, pauseDigest), nil
}

// runtimeHash is the sandbox_runtime_hash of a version: a sha256 over one
// sorted "key=value" line per asset digest and pause image digest. Version names and URLs are left out, so the same binaries under
// another name or in another bucket are the same runtime.
func runtimeHash(rec *sandboxAssetsRecord, pauseDigest string) string {
	lines := make([]string, 0, len(rec.Assets)+1)
	for name, a := range rec.Assets {
		lines = append(lines, "asset/"+name+"="+a.SHA256)
	}
	if pauseDigest != "" {
		lines = append(lines, "pause_image="+pauseDigest)
	}
	slices.Sort(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// syncPod writes pod's sandbox versions file:
//   - plan: resolve the pool's config and its versions for this node
//   - build: hash each version's runtime
//   - write: replace the file if its content changed
func (s *workerSandboxSyncer) syncPod(ctx context.Context, pod *corev1.Pod) error {
	plan, err := s.resolver.plan(pod)
	if err != nil {
		return err
	}
	want, err := buildFile(ctx, plan)
	if err != nil {
		return err
	}
	return s.write(ctx, string(pod.UID), plan.config, want)
}

func (s *workerSandboxSyncer) write(ctx context.Context, uid, config string, want *sandboxversions.File) error {
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
			slog.String("sandbox_config", config),
			slog.String("default_runtime_hash", want.DefaultRuntime[hardware.AttrSandboxRuntimeHash]),
			slog.Int("restorable_runtimes", len(want.RestorableRuntimes)))
	}
	return nil
}

func (s *workerSandboxSyncer) versionsFile(uid string) string {
	return filepath.Join(s.podDir(uid), nodepath.SandboxVersionsFileName)
}
