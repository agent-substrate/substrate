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

// This file decides how one worker pod's sandbox runtimes file is built,
// in the steps syncPod runs:
//   - resolve: find the SandboxConfig of the pod's WorkerPool
//   - plan: pick the config's versions this node can run
//   - build: hash each version's assets and pause image digests into its
//     sandbox_runtime_hash, and probe what a snapshot taken on the runtime
//     must find again: the host CPU identity for microVM, the runsc
//     binary's CPU features for gVisor
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
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	corev1 "k8s.io/api/core/v1"

	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/sandboxruntimes"
	"go.ate.dev/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "go.ate.dev/substrate/pkg/client/listers/api/v1alpha1"
)

// errPermanent marks a failure that retrying cannot fix, so the syncer drops
// the item instead of backing off until the next event for the pod:
//   - the pod has no worker pool label
//   - the config's default version is missing, has no assets for this
//     node's architecture, or has a pause image not pinned by digest
//   - the default version's runsc runs but fails, prints no features, or
//     prints one this atelet does not know; the binary is content-addressed,
//     so running it again gives the same answer
//
// The same failures on a restorable version leave it out of the file instead.
var errPermanent = errors.New("permanent failure")

// errIncomplete marks a sync that wrote the worker's file with the default
// version and the restorable versions that resolved, while at least one
// restorable version had a transient failure. finish requeues the pod
// without the syncMaxRetries cap so the remaining restorable versions are
// added once the transient failure clears.
var errIncomplete = errors.New("incomplete sandbox runtimes")

// errUnresolved marks a resolve failure caused by informer lag or a WorkerPool
// that has not resolved a SandboxConfig yet. Because the informer events that
// end those states also enqueue the pod again, finish caps errUnresolved
// retries at syncMaxRetries.
var errUnresolved = errors.New("unresolved sandbox config")

// sandboxPlan is what one worker's sandbox runtimes file is built from.
type sandboxPlan struct {
	config       string
	sandboxClass v1alpha1.SandboxClass // the config's; decides which attributes its runtimes report
	versions     []versionPlan         // the default first
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

// resolve returns the SandboxConfig of a worker pod on this node. Apart from
// a pod in no pool, every failure can succeed once an informer catches up or
// the atecontroller resolves the pool.
func (r *workerSandboxResolver) resolve(pod *corev1.Pod) (*v1alpha1.SandboxConfig, error) {
	poolName := pod.Labels[workerPoolLabel]
	if poolName == "" {
		return nil, fmt.Errorf("%w: worker pod %s/%s has no %s label", errPermanent, pod.Namespace, pod.Name, workerPoolLabel)
	}
	pool, err := r.workerPools.WorkerPools(pod.Namespace).Get(poolName)
	if err != nil {
		return nil, fmt.Errorf("%w: while getting WorkerPool %s/%s: %w", errUnresolved, pod.Namespace, poolName, err)
	}
	configName := poolConfigName(pool)
	if configName == "" {
		return nil, fmt.Errorf("%w: WorkerPool %s/%s has not resolved a SandboxConfig for sandbox class %q", errUnresolved, pod.Namespace, poolName, pool.Spec.DefaultSandboxClass())
	}
	cfg, err := r.configs.Get(configName)
	if err != nil {
		return nil, fmt.Errorf("%w: while getting SandboxConfig %q: %w", errUnresolved, configName, err)
	}
	return cfg, nil
}

// plan resolves pod's SandboxConfig and the versions of it this node can run.
func (r *workerSandboxResolver) plan(pod *corev1.Pod) (*sandboxPlan, error) {
	cfg, err := r.resolve(pod)
	if err != nil {
		return nil, err
	}
	vers, err := planVersions(cfg)
	if err != nil {
		return nil, err
	}
	return &sandboxPlan{config: cfg.Name, sandboxClass: cfg.Spec.SandboxClass, versions: vers}, nil
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
// plan. A version whose runtime hash equals an earlier one's is left out, so
// the file lists each runtime once. The default version's failure fails the
// sync, which leaves the existing file in place until a retry succeeds. A
// restorable version whose attributes cannot be built is left out; when the
// failure is transient the file is returned with an error wrapping
// errIncomplete, so the caller writes the runtimes that resolved right away
// and retries for the rest.
//
// TODO: write the file as soon as the default version's attributes are known
// and add restorable runtimes on a later pass. On a fresh node the first
// worker cannot register until every restorable version's runsc has been
// fetched and probed, serially.
//
// Listing a runtime does not promise its assets are on the node: probing
// fetches a gVisor version's runsc the first time this atelet sees it, but
// not its pause image, and nothing of a microVM version. The prewarmer
// fetches each config's default version, and Run/Restore fetch any other
// version on demand.
func (s *workerSandboxSyncer) buildFile(ctx context.Context, plan *sandboxPlan) (*sandboxruntimes.File, error) {
	out := &sandboxruntimes.File{}
	seen := make(map[string]bool, len(plan.versions))
	var incomplete error
	for _, v := range plan.versions {
		h, err := versionRuntimeHash(v.rec)
		if err == nil && seen[h] {
			continue
		}
		var attrs sandboxruntimes.Attributes
		if err == nil {
			attrs, err = s.versionAttrs(ctx, plan.sandboxClass, v.rec, h)
		}
		if err != nil {
			err = fmt.Errorf("SandboxConfig %q, version %q: %w", plan.config, v.name, err)
			if v.isDefault {
				// Without the default no worker of the pool could start
				// actors on this node.
				return nil, err
			}
			if errors.Is(err, errPermanent) {
				slog.WarnContext(ctx, "Leaving a sandbox version out of the worker sandbox runtimes file", slog.Any("err", err))
			} else {
				// Logged once by finish, with every version this names.
				incomplete = errors.Join(incomplete, err)
			}
			continue
		}
		seen[h] = true
		if v.isDefault {
			out.DefaultRuntime = attrs
		} else {
			out.RestorableRuntimes = append(out.RestorableRuntimes, attrs)
		}
	}
	if incomplete != nil {
		return out, fmt.Errorf("%w: %w", errIncomplete, incomplete)
	}
	return out, nil
}

// versionAttrs returns the attributes of one version's runtime, whose
// sandbox_runtime_hash is hash: architecture and the hash for every class,
// the host CPU identity for a microVM runtime, and the runsc binary's
// cpu_features for a gVisor one.
func (s *workerSandboxSyncer) versionAttrs(ctx context.Context, class v1alpha1.SandboxClass, rec *sandboxAssetsRecord, hash string) (sandboxruntimes.Attributes, error) {
	attrs := sandboxruntimes.Attributes{
		hardware.AttrArchitecture:       runtime.GOARCH,
		hardware.AttrSandboxRuntimeHash: hash,
	}
	switch class {
	case v1alpha1.SandboxClassMicroVM:
		vendor, model, err := s.hostCPUIdentity()
		if err != nil {
			return nil, fmt.Errorf("while probing host CPU identity: %w", err)
		}
		attrs[hardware.AttrCPUVendor], attrs[hardware.AttrCPUModel] = vendor, model
	case v1alpha1.SandboxClassGvisor:
		mask, err := s.gvisorCPUFeatures(ctx, rec)
		if err != nil {
			return nil, err
		}
		attrs[hardware.AttrCPUFeatures] = mask
	default:
		return nil, fmt.Errorf("%w: unsupported sandbox class %q", errPermanent, class)
	}
	return attrs, nil
}

// cpuIdentity is the host CPU identity microVM runtimes report.
type cpuIdentity struct {
	vendor, model string
}

// hostCPUIdentity returns the host CPU's vendor and model, probing them the
// first time. A failed probe is tried again on the next sync.
func (s *workerSandboxSyncer) hostCPUIdentity() (vendor, model string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hostCPU == nil {
		if vendor, model, err = s.probeCPUIdentity(); err != nil {
			return "", "", err
		}
		s.hostCPU = &cpuIdentity{vendor: vendor, model: model}
	}
	return s.hostCPU.vendor, s.hostCPU.model, nil
}

// gvisorCPUFeatures returns the encoded cpu_features of rec's runsc binary.
// The first time this atelet sees the binary's digest it fetches the binary
// into the local asset cache and runs `<runsc> cpu-features`.
func (s *workerSandboxSyncer) gvisorCPUFeatures(ctx context.Context, rec *sandboxAssetsRecord) (string, error) {
	asset, ok := runscAssetOf(rec)
	if !ok {
		return "", fmt.Errorf("%w: gvisor version has no %q or %q asset", errPermanent, gvisorAssetName, runscAssetName)
	}
	digest := rec.Assets[asset].SHA256
	s.mu.Lock()
	mask, ok := s.cpuFeatures[digest]
	s.mu.Unlock()
	if ok {
		return mask, nil
	}
	// A download that hangs would hold a sync worker; prewarmTimeout bounds
	// the fetch for the same reason it bounds prewarming.
	ctx, cancel := context.WithTimeout(ctx, prewarmTimeout)
	defer cancel()
	// The probe has no actor to stage the binary for: it links it into a
	// scratch dir it removes again, and the cache keeps the entry.
	scratch, err := os.MkdirTemp(s.scratchDir, "cpu-features-")
	if err != nil {
		return "", fmt.Errorf("while creating cpu-features scratch dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(scratch) }()
	paths, err := s.assets.ensureSandboxAssets(ctx, rec, scratch)
	if err != nil {
		return "", err
	}
	names, err := s.runCPUFeatures(ctx, paths[asset])
	if err != nil {
		return "", err
	}
	mask, err = hardware.EncodeCPUFeatures(runtime.GOARCH, names)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errPermanent, err)
	}
	s.mu.Lock()
	s.cpuFeatures[digest] = mask
	s.mu.Unlock()
	return mask, nil
}

// runscAssetOf returns the name of the asset in rec that carries runsc, the
// most preferred of runscAssetNames that is present with a digest.
func runscAssetOf(rec *sandboxAssetsRecord) (string, bool) {
	for _, name := range runscAssetNames {
		if a, ok := rec.Assets[name]; ok && a.SHA256 != "" {
			return name, true
		}
	}
	return "", false
}

// cpuFeaturesTimeout bounds one `runsc cpu-features` run, which takes
// milliseconds. A runsc that hangs would hold a sync worker. A var so tests
// can shorten it.
var cpuFeaturesTimeout = 30 * time.Second

// runRunscCPUFeatures executes `<runscPath> cpu-features` and returns the
// comma-separated feature names it prints. The binary is content-addressed,
// so one that runs and fails, or prints nothing, would do so again and fails
// permanently; one that cannot be started, or is cut off by ctx, is retried.
func runRunscCPUFeatures(ctx context.Context, runscPath string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, cpuFeaturesTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, runscPath, "cpu-features")
	cmd.Env = []string{} // the probe needs nothing from atelet's environment
	out, err := cmd.Output()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case ctx.Err() != nil:
		// Before the ExitError case: a process killed at the deadline exits
		// with one too.
		return nil, fmt.Errorf("while running %s cpu-features: %w", runscPath, ctx.Err())
	case errors.As(err, &exitErr):
		if stderr := strings.TrimSpace(string(exitErr.Stderr)); stderr != "" {
			return nil, fmt.Errorf("%w: %s cpu-features: %w (%s)", errPermanent, runscPath, err, stderr)
		}
		return nil, fmt.Errorf("%w: %s cpu-features: %w", errPermanent, runscPath, err)
	default:
		return nil, fmt.Errorf("while starting %s cpu-features: %w", runscPath, err)
	}
	printed := strings.TrimSpace(string(out))
	if printed == "" {
		return nil, fmt.Errorf("%w: %s cpu-features printed no features", errPermanent, runscPath)
	}
	return strings.Split(printed, ","), nil
}

// versionRuntimeHash returns the runtime hash of one version. The CRD
// requires every pause image to be pinned by digest, which is the digest the
// image cache resolves the ref to, so the hash needs no pull. An unpinned one
// is errPermanent: only a spec change fixes it.
func versionRuntimeHash(rec *sandboxAssetsRecord) (string, error) {
	var pauseDigest string
	if rec.PauseImage != "" {
		ref, err := name.NewDigest(rec.PauseImage)
		if err != nil {
			return "", fmt.Errorf("%w: pause image %q is not pinned by digest: %w", errPermanent, rec.PauseImage, err)
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

// syncPod writes pod's sandbox runtimes file:
//   - plan: resolve the pool's config and its versions for this node
//   - build: hash and probe each version's runtime
//   - write: replace the file if its content changed
//
// A file missing restorable runtimes for a transient reason is written all
// the same, and the errIncomplete from build is returned so finish retries.
func (s *workerSandboxSyncer) syncPod(ctx context.Context, pod *corev1.Pod) error {
	plan, err := s.resolver.plan(pod)
	if err != nil {
		return err
	}
	want, buildErr := s.buildFile(ctx, plan)
	if buildErr != nil && !errors.Is(buildErr, errIncomplete) {
		return buildErr
	}
	if err := s.write(ctx, string(pod.UID), plan.config, want); err != nil {
		return err
	}
	return buildErr
}

func (s *workerSandboxSyncer) write(ctx context.Context, uid, config string, want *sandboxruntimes.File) error {
	if err := os.MkdirAll(s.podDir(uid), 0o700); err != nil {
		return fmt.Errorf("while creating worker directory: %w", err)
	}
	wrote, err := sandboxruntimes.Write(s.runtimesFile(uid), want)
	if err != nil {
		return err
	}
	if wrote {
		slog.InfoContext(ctx, "Wrote worker sandbox runtimes",
			slog.String("pod_uid", uid),
			slog.String("sandbox_config", config),
			slog.String("default_runtime_hash", want.DefaultRuntime[hardware.AttrSandboxRuntimeHash]),
			slog.Int("restorable_runtimes", len(want.RestorableRuntimes)))
	}
	return nil
}

func (s *workerSandboxSyncer) runtimesFile(uid string) string {
	return filepath.Join(s.podDir(uid), nodepath.SandboxRuntimesFileName)
}
