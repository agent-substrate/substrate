// Copyright 2026 The Agent Substrate Authors
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
//   - build: hash each version's asset digests into its sandbox_bundle_hash,
//     stage the default version's assets under the worker's directory and
//     verify each restorable version's in the cache, and probe what a
//     snapshot taken on the runtime must find again: the host CPU identity
//     for microVM, the runsc binary's CPU features for gVisor
//   - write: replace the file in the worker directory if its content changed
//   - prune: remove the directories of defaults the file no longer names
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

	corev1 "k8s.io/api/core/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/sandboxruntimes"
	atelisters "go.ate.dev/substrate/pkg/client/listers/ate/v1"
)

// errPermanent marks a failure that retrying cannot fix, so the syncer drops
// the item instead of backing off until the next event for the pod:
//   - the pod has no worker pool label
//   - the config's default version is missing or has no assets for this
//     node's architecture
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
	sandboxClass atev1.SandboxClass // the config's; decides which attributes its runtimes report
	versions     []versionPlan      // the default first
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
	workerPools atelisters.WorkerPoolLister
	configs     atelisters.SandboxConfigLister
}

// resolve returns the SandboxConfig of a worker pod on this node. Apart from
// a pod in no pool, every failure can succeed once an informer catches up or
// the atecontroller resolves the pool.
func (r *workerSandboxResolver) resolve(pod *corev1.Pod) (*atev1.SandboxConfig, error) {
	pool, err := r.pool(pod)
	if err != nil {
		return nil, err
	}
	return r.config(pool)
}

// pool returns the WorkerPool a worker pod on this node belongs to.
func (r *workerSandboxResolver) pool(pod *corev1.Pod) (*atev1.WorkerPool, error) {
	poolName := pod.Labels[workerPoolLabel]
	if poolName == "" {
		return nil, fmt.Errorf("%w: worker pod %s/%s has no %s label", errPermanent, pod.Namespace, pod.Name, workerPoolLabel)
	}
	pool, err := r.workerPools.WorkerPools(pod.Namespace).Get(poolName)
	if err != nil {
		return nil, fmt.Errorf("%w: while getting WorkerPool %s/%s: %w", errUnresolved, pod.Namespace, poolName, err)
	}
	return pool, nil
}

// config returns the SandboxConfig the workers of pool run.
func (r *workerSandboxResolver) config(pool *atev1.WorkerPool) (*atev1.SandboxConfig, error) {
	configName := pool.ResolvedSandboxConfig()
	if configName == "" {
		return nil, fmt.Errorf("%w: WorkerPool %s/%s has not resolved a SandboxConfig for sandbox class %q", errUnresolved, pool.Namespace, pool.Name, pool.Spec.DefaultSandboxClass())
	}
	cfg, err := r.configs.Get(configName)
	if err != nil {
		return nil, fmt.Errorf("%w: while getting SandboxConfig %q: %w", errUnresolved, configName, err)
	}
	return cfg, nil
}

// plan resolves pod's SandboxConfig and the versions of it this node can run,
// along with the object the sync is acting on (the resolved SandboxConfig, or
// the WorkerPool when its config has not resolved) for failure Events.
func (r *workerSandboxResolver) plan(pod *corev1.Pod) (*sandboxPlan, k8sruntime.Object, error) {
	pool, err := r.pool(pod)
	if err != nil {
		return nil, nil, err
	}
	cfg, err := r.config(pool)
	if err != nil {
		return nil, pool, err
	}
	plan, err := planConfig(cfg)
	return plan, cfg, err
}

// planPool is plan for a pool already in hand: the config its workers run
// and the versions of it this node can run.
func (r *workerSandboxResolver) planPool(pool *atev1.WorkerPool) (*sandboxPlan, error) {
	cfg, err := r.config(pool)
	if err != nil {
		return nil, err
	}
	return planConfig(cfg)
}

// planConfig returns the plan for cfg: its class and the versions of it this
// node can run.
func planConfig(cfg *atev1.SandboxConfig) (*sandboxPlan, error) {
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
func planVersions(cfg *atev1.SandboxConfig) ([]versionPlan, error) {
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
		if v.Name == def.Name || (v.State != nil && *v.State == atev1.SandboxVersionStateDisabled) {
			continue
		}
		if rec, ok := recordForArch(cfg, v); ok {
			vers = append(vers, versionPlan{name: v.Name, rec: rec})
		}
	}
	return vers, nil
}

// recordForArch projects the assets of one version of a SandboxConfig onto
// this node's architecture. It reports false when the version lists no assets
// for this architecture.
func recordForArch(cfg *atev1.SandboxConfig, ver *atev1.SandboxVersionConfig) (*sandboxAssetsRecord, bool) {
	files := ver.Assets[runtime.GOARCH]
	if len(files) == 0 {
		return nil, false
	}
	rec := &sandboxAssetsRecord{
		SandboxClass: string(cfg.Spec.SandboxClass),
		Assets:       make(map[string]assetEntry, len(files)),
	}
	for name, f := range files {
		rec.Assets[name] = assetEntry{URL: f.URL, SHA256: f.SHA256}
	}
	return rec, true
}

// hashedVersion is one version of a plan with its bundle hash.
type hashedVersion struct {
	versionPlan
	hash string
}

// hashVersions hashes each version of plan. The hashes come from the
// SandboxConfig spec alone, so it does no IO.
func hashVersions(plan *sandboxPlan) []hashedVersion {
	hashed := make([]hashedVersion, 0, len(plan.versions))
	for _, v := range plan.versions {
		hashed = append(hashed, hashedVersion{versionPlan: v, hash: bundleHash(v.rec)})
	}
	return hashed
}

// buildFile returns the file content naming the runtimes of the hashed
// versions, the default first, for the worker pod with the given UID. A
// version whose bundle hash equals an earlier one's is left out, so the file
// lists each runtime once. The default version's failure fails the sync,
// which leaves the existing file in place until a retry succeeds. A
// restorable version whose attributes cannot be built is left out; when the
// failure is transient the file is returned with an error wrapping
// errIncomplete, so the caller writes the runtimes that resolved right away
// and retries for the rest.
//
// TODO: write the file as soon as the default version's attributes are known
// and add restorable runtimes on a later pass. On a fresh node the first
// worker cannot register until every restorable version's assets have been
// fetched and probed, serially.
//
// Every build stages the default version's assets into its directory under
// the worker's (see defaultRuntimeDirsName), fetching what the cache lacks,
// so promoting an evicted version to default verifies it in the cache, and
// links it, before the previous default is unlinked; a re-sync with the same
// default costs a stat per asset. A restorable version is staged into a
// scratch directory, removed again, the first time this atelet process sees
// its bundle hash; Run and Restore fetch an evicted restorable version on
// demand. The first time the process sees a gVisor runsc digest it runs the
// binary for its cpu_features. A version with a bad sha256 fails
// verification without being cached or memoized, so retries re-download it
// until the SandboxConfig is fixed.
func (s *workerSandboxSyncer) buildFile(ctx context.Context, uid string, plan *sandboxPlan, hashed []hashedVersion) (*sandboxruntimes.File, error) {
	class := sandboxruntimes.Class{SandboxClass: string(plan.sandboxClass)}
	seen := make(map[string]bool, len(hashed))
	var incomplete error
	for _, v := range hashed {
		if seen[v.hash] {
			continue
		}
		attrs, err := s.versionAttrs(ctx, uid, plan.sandboxClass, v)
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
		seen[v.hash] = true
		if v.isDefault {
			class.DefaultRuntime = sandboxruntimes.Runtime{Version: attrs}
		} else {
			class.RestorableRuntimes = append(class.RestorableRuntimes, sandboxruntimes.Runtime{Version: attrs})
		}
	}
	// One class per pool today.
	out := &sandboxruntimes.File{SandboxClasses: []sandboxruntimes.Class{class}}
	if incomplete != nil {
		return out, fmt.Errorf("%w: %w", errIncomplete, incomplete)
	}
	return out, nil
}

// versionAttrs returns the attributes of v's runtime: architecture and the
// bundle hash for every class, the host CPU identity for a microVM runtime
// and the runsc binary's cpu_features for a gVisor one, with the version's
// assets staged for the worker pod with the given UID (see stageDefault and
// stageScratch).
func (s *workerSandboxSyncer) versionAttrs(ctx context.Context, uid string, class atev1.SandboxClass, v hashedVersion) (sandboxruntimes.Attributes, error) {
	attrs := sandboxruntimes.Attributes{
		hardware.AttrArchitecture:      runtime.GOARCH,
		hardware.AttrSandboxBundleHash: v.hash,
	}
	switch class {
	case atev1.SandboxClassMicroVM:
		vendor, model, err := s.hostCPUIdentity()
		if err != nil {
			return nil, fmt.Errorf("while probing host CPU identity: %w", err)
		}
		if err := s.ensureMicroVMAssets(ctx, uid, v); err != nil {
			return nil, err
		}
		attrs[hardware.AttrCPUVendor], attrs[hardware.AttrCPUModel] = vendor, model
	case atev1.SandboxClassGvisor:
		mask, err := s.gvisorCPUFeatures(ctx, uid, v)
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

// stageDefault stages the default version v into its directory under the
// worker pod's, through the cache, fetching what it lacks, so a default
// whose assets cannot be fetched or fail SHA256 verification is not
// advertised. The links stay and keep the assets cached while no actor runs
// them; once the directory is filled this costs a stat per asset.
func (s *workerSandboxSyncer) stageDefault(ctx context.Context, uid string, v hashedVersion) error {
	// Backstop for the sync worker this runs on; see probeFetchTimeout.
	ctx, cancel := context.WithTimeout(ctx, probeFetchTimeout)
	defer cancel()
	_, err := s.assets.ensureSandboxAssets(ctx, v.rec, s.defaultRuntimeDir(uid, v.hash))
	return err
}

// stageScratch stages rec's assets into a scratch directory, which done
// removes, and returns the staged paths; the cache keeps the entry. This
// verifies a restorable version's assets, and gives the gVisor probe a runsc
// linked for it alone.
func (s *workerSandboxSyncer) stageScratch(ctx context.Context, rec *sandboxAssetsRecord) (paths map[string]string, done func(), err error) {
	ctx, cancel := context.WithTimeout(ctx, probeFetchTimeout)
	defer cancel()
	scratch, err := os.MkdirTemp(s.scratchDir, "sandbox-assets-")
	if err != nil {
		return nil, nil, fmt.Errorf("while creating sandbox assets scratch dir: %w", err)
	}
	done = func() { _ = os.RemoveAll(scratch) }
	paths, err = s.assets.ensureSandboxAssets(ctx, rec, scratch)
	if err != nil {
		done()
		return nil, nil, err
	}
	return paths, done, nil
}

// ensureMicroVMAssets stages a microVM version: the default on every sync
// (see stageDefault), so promoting an evicted version to default verifies it
// in the cache before the previous default is unlinked; a restorable version
// once per atelet process (see stageScratch).
func (s *workerSandboxSyncer) ensureMicroVMAssets(ctx context.Context, uid string, v hashedVersion) error {
	s.mu.Lock()
	verified := s.verifiedBundles[v.hash]
	s.mu.Unlock()
	switch {
	case v.isDefault:
		if err := s.stageDefault(ctx, uid, v); err != nil {
			return err
		}
	case !verified:
		_, done, err := s.stageScratch(ctx, v.rec)
		if err != nil {
			return err
		}
		done()
	default:
		return nil
	}
	s.mu.Lock()
	s.verifiedBundles[v.hash] = true
	s.mu.Unlock()
	return nil
}

// gvisorCPUFeatures returns the encoded cpu_features of v's runsc binary. It
// stages the version like ensureMicroVMAssets, and runs `<runsc>
// cpu-features` the first time this atelet process sees the binary's digest,
// on a runsc linked into scratch for the probe alone, never on the one under
// the worker's directory. The memo is per digest, so a bundle that adds an
// asset beside a known runsc is staged for that asset without running runsc
// again.
func (s *workerSandboxSyncer) gvisorCPUFeatures(ctx context.Context, uid string, v hashedVersion) (string, error) {
	digest := v.rec.Assets[gvisorAssetName].SHA256
	if digest == "" {
		return "", fmt.Errorf("%w: gvisor version has no %q asset", errPermanent, gvisorAssetName)
	}
	s.mu.Lock()
	mask, probed := s.cpuFeatures[digest]
	verified := s.verifiedBundles[v.hash]
	s.mu.Unlock()
	if v.isDefault {
		if err := s.stageDefault(ctx, uid, v); err != nil {
			return "", err
		}
		verified = true
	}
	if !verified || !probed {
		paths, done, err := s.stageScratch(ctx, v.rec)
		if err != nil {
			return "", err
		}
		defer done()
		if !probed {
			names, err := s.runCPUFeatures(ctx, runscPathFor(paths))
			if err != nil {
				return "", err
			}
			if mask, err = hardware.EncodeCPUFeatures(runtime.GOARCH, names); err != nil {
				return "", fmt.Errorf("%w: %w", errPermanent, err)
			}
		}
	}
	s.mu.Lock()
	s.cpuFeatures[digest] = mask
	s.verifiedBundles[v.hash] = true
	s.mu.Unlock()
	return mask, nil
}

// probeFetchTimeout bounds staging a version's assets during buildFile (a
// gVisor version's runsc binary for its probe, or a microVM version's kernel,
// guest image, Cloud Hypervisor and virtiofsd), from the first cache lookup to
// the staged links. The asset cache bounds each individual download with its
// own detached --sandbox-asset-fetch-timeout, and the exec below has
// cpuFeaturesTimeout. This is the backstop for the whole version so a slow
// bucket or a download another caller started cannot hold one of the syncer's
// few workers for longer than this before the sync fails and retries. Twice
// the flag's default: one guest image at the flag's bound, then the three
// small binaries. Raising the flag past this makes a cold build time out and
// retry; its downloads continue detached, so the retry finds them cached or
// joins one still in flight.
const probeFetchTimeout = 10 * time.Minute

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

// bundleHash is the sandbox_bundle_hash of a version: a sha256 over one
// sorted "asset/<name>=<sha256>" line per asset. Version names and URLs are
// left out, so the same binaries under another name or in another bucket are
// the same runtime.
func bundleHash(rec *sandboxAssetsRecord) string {
	lines := make([]string, 0, len(rec.Assets))
	for name, a := range rec.Assets {
		lines = append(lines, "asset/"+name+"="+a.SHA256)
	}
	slices.Sort(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// syncPod writes pod's sandbox runtimes file:
//   - plan: resolve the pool's config and its versions for this node
//   - hash: hash each version's runtime, and make each resolvable by its
//     hash before the file can let the worker register it
//   - build: stage the default version into its directory under the pod's,
//     verify each restorable version's assets in the cache, and probe what
//     a snapshot taken on the runtime must find again
//   - write: replace the file if its content changed, then record what
//     the worker now advertises in the index
//   - prune: remove the directories of the defaults the file no longer
//     names, which unlinks them
//
// The default's directory is filled before the file names it and removed
// only after the file stops, so a worker's default stays cached while no
// actor runs it. A build that fails leaves the file, and so the previous
// default's directory, as they were; the directory of a default it staged
// before failing stays until a later sync writes the file and prunes it.
//
// A file missing restorable runtimes for a transient reason is written all
// the same, and the errIncomplete from build is returned so finish retries.
// The returned k8sruntime.Object is the resolved SandboxConfig (or WorkerPool
// when its config has not resolved), attached as `related` on failure Events.
func (s *workerSandboxSyncer) syncPod(ctx context.Context, pod *corev1.Pod) (k8sruntime.Object, error) {
	plan, related, err := s.resolver.plan(pod)
	if err != nil {
		return related, err
	}
	hashed := hashVersions(plan)
	// Resolvable, and on disk, before the file can let the worker register
	// any of them.
	if err := s.index.registerRuntimes(plan.config, hashed); err != nil {
		return related, err
	}
	uid := string(pod.UID)
	want, buildErr := s.buildFile(ctx, uid, plan, hashed)
	if buildErr != nil && !errors.Is(buildErr, errIncomplete) {
		return related, buildErr
	}
	if err := s.write(ctx, uid, plan.config, want); err != nil {
		return related, err
	}
	// What the worker advertises is what its file now says: a sync that
	// failed before the write left the previous entry, and one that wrote
	// a partial file names only the runtimes in it. One class per pool
	// today; a pool that runs several will yield one plan per class here.
	written := writtenVersions(hashed, want)
	if err := s.index.register(uid, advertisedClass{class: plan.sandboxClass, config: plan.config, hashed: written}); err != nil {
		return related, err
	}
	// Only now, with the file naming its defaults, each linked into its
	// directory by the build.
	var defaults []string
	for _, v := range written {
		if v.isDefault {
			defaults = append(defaults, v.hash)
		}
	}
	if err := s.pruneDefaultRuntimeDirs(uid, defaults); err != nil {
		return related, err
	}
	return related, buildErr
}

// writtenVersions returns the versions of hashed whose runtime file names,
// in hashed's order: every version whose attributes were built.
func writtenVersions(hashed []hashedVersion, file *sandboxruntimes.File) []hashedVersion {
	written := make(map[string]bool, len(hashed))
	for _, c := range file.SandboxClasses {
		written[c.DefaultRuntime.Version[hardware.AttrSandboxBundleHash]] = true
		for _, rt := range c.RestorableRuntimes {
			written[rt.Version[hardware.AttrSandboxBundleHash]] = true
		}
	}
	out := make([]hashedVersion, 0, len(hashed))
	for _, v := range hashed {
		if written[v.hash] {
			out = append(out, v)
		}
	}
	return out
}

// write replaces the pod's file if its content changed. The build created
// the pod's directory when it staged the default (see stageDefault).
func (s *workerSandboxSyncer) write(ctx context.Context, uid, config string, want *sandboxruntimes.File) error {
	wrote, err := sandboxruntimes.Write(s.runtimesFile(uid), want)
	if err != nil {
		return err
	}
	if wrote {
		for _, c := range want.SandboxClasses {
			slog.InfoContext(ctx, "Wrote worker sandbox runtimes",
				slog.String("pod_uid", uid),
				slog.String("sandbox_config", config),
				slog.String("sandbox_class", c.SandboxClass),
				slog.String("default_bundle_hash", c.DefaultRuntime.Version[hardware.AttrSandboxBundleHash]),
				slog.Int("restorable_runtimes", len(c.RestorableRuntimes)))
		}
	}
	return nil
}

func (s *workerSandboxSyncer) runtimesFile(uid string) string {
	return filepath.Join(s.podDir(uid), nodepath.SandboxRuntimesFileName)
}
