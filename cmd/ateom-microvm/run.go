//go:build linux

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
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/readyz"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/sizing"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// runningActor holds the live state for one actor's micro-VM. ateom owns the
// cloud-hypervisor process directly (booted by RunWorkload or relaunched by
// RestoreWorkload), so it tracks that process and its api-socket for teardown.
type runningActor struct {
	// baseID is the FROZEN base sandbox id propagated across this actor's restore
	// lineage. For a cold-run actor this is the actor's own id; for a restored
	// actor it is the id read from the snapshot's base-id file (the golden id,
	// propagated). CheckpointWorkload writes it back into the next snapshot's
	// base-id file so the chain survives suspend->resume->suspend.
	baseID string

	// ateom owns this CH process (booted at Prepare/Run or relaunched at Restore).
	chCmd *exec.Cmd
	// vfsdCmd is the virtiofsd serving the unified share (merged rootfs overlay,
	// durable-dir volumes, and CSI volumes). ateom owns it; teardownActor
	// kills it after the CH process.
	vfsdCmd *exec.Cmd
	// apiSocket is the CH api-socket for this ateom-owned VMM.
	apiSocket string

	// restoreSourceDir is the snapshot dir this actor was OnDemand-restored from
	// (CH demand-pages its guest RAM from it). Set when restored via OnDemand.
	// CheckpointWorkload overlays CH's new (sparse, faulted-only) snapshot onto this
	// base to produce a COMPLETE snapshot (CH's OnDemand snapshot alone drops the
	// un-faulted pages). Empty for cold-run actors (their snapshot is already complete).
	restoreSourceDir string

	// snapshotIsSelfContained is set when this actor was restored eagerly, which
	// reads every populated extent up front. Every page the snapshot had is then
	// resident, so cloud-hypervisor's next snapshot already holds all of it and
	// there is no delta to overlay onto restoreSourceDir.
	snapshotIsSelfContained bool

	// guestAgent is the kata-agent ttrpc client retained past boot. Two things
	// share it: the stdout/stderr forwarding goroutines (they pump the
	// container's output via ReadStdout/ReadStderr on this connection for the
	// actor's lifetime) and GetWorkloadStats (via s.guestStats, which points at
	// this same client). It is NOT closed when RunWorkload / RestoreWorkload
	// return — teardownActor closes it, which makes the in-flight
	// ReadStdout/ReadStderr calls fail and the forwarding goroutines exit
	// (io.EOF). nil if the post-boot dial failed (e.g. a best-effort
	// post-restore dial), which loses both log forwarding and guest stats for
	// this activation.
	guestAgent *kata.AgentClient

	// workloadIDs are the guest container ids of this actor's workloads, for the
	// SIGTERM the graceful shutdown propagates into the guest (see shutdown.go).
	workloadIDs []string
}

// baseIDFile is a tiny snapshot file (under the checkpoint/restore dir) holding
// the FROZEN base sandbox id — the id the guest's virtio-fs find-paths are pinned
// to (<baseID>/rootfs). It is the id the RO base was FIRST shared under (the golden
// actor's cold-run id) and is INVARIANT across every restore of that actor's
// lineage: the guest memory keeps referencing <baseID>/rootfs, while the snapshot
// config.json's socket paths get rewritten to the current actor UID on each restore.
// RestoreWorkload reads this to lay the reconstructed-from-image base at the path
// the guest expects. (The config.json socket id is the WRONG source — it equals the
// current id, not the frozen golden id, for any restored-then-checkpointed actor.)
const baseIDFile = "base-id"

// Asset names in RunWorkloadRequest.runtime_asset_paths (set by atelet's
// fetchRuntimeAssets, keyed by the ActorTemplate runtime asset names).
const (
	assetCH        = "cloud-hypervisor"
	assetKernel    = "kata-kernel"
	assetImage     = "kata-image"
	assetConfig    = "kata-config"
	assetVirtiofsd = "virtiofsd"
)

// kataAgentPath is where the kata guest image keeps the agent binary. buildVMConfig
// boots it as PID 1 (init=), so it is the guest's entire userspace.
const kataAgentPath = "/usr/bin/kata-agent"

// vmmMemReserveMiB is the DEFAULT guest RAM held back from the pod's memory limit
// for the cloud-hypervisor VMM + virtiofsd, which run as host processes in the same
// pod cgroup as the guest RAM; without a margin the pod OOMs. Overridable per
// deployment via --vmm-mem-reserve-mib (see AteomService.memReserveMiB).
//
// Measured on the worker pod's cgroup with one 256MiB-guest actor: the VMM stack's own
// cost is ~12MiB (anon 8.1 + kernel 3.7; the rest of cloud-hypervisor's RSS is the guest
// memfd, already accounted as guest RAM), and two virtiofsds are ~3MiB each. What needs
// the rest of the margin is transient: a pause/resume cycle took the cgroup from 94MiB
// to 153MiB, and the 57MiB difference was page cache from writing and reading the
// snapshot.
//
// That transient scales with snapshot size, so no fixed reserve is right for every guest
// size — this one is halved rather than cut to the ~32MiB the steady state would justify.
// The fix that would let it drop that far is keeping snapshot I/O out of the page cache
// (posix_fadvise(DONTNEED) after the checkpoint write and the restore read); until then
// the margin absorbs it, and a deployment running large guests can raise the flag.
const vmmMemReserveMiB = 128

// minGuestMemMiB is the floor for guest RAM (the declared limit minus the VMM
// reserve); a declared memory limit that leaves less is rejected at cold boot with a
// clear error instead of being silently honored (see resolveGuestMemMiB), since too
// little RAM makes the guest hang on boot rather than fail cleanly. Keep the admission
// floor on ActorTemplate.spec.resources in sync (it is this value + vmmMemReserveMiB).
//
// Measured against the counter demo on a guest booting the agent as PID 1: 32MiB never
// reaches Ready, 64MiB boots but idles with 1.1MB free (it only survives because page
// cache is reclaimable), and 128MiB idles with 43MiB free. So 128 is the smallest size
// with real headroom, not the smallest that boots — a workload heavier than a static Go
// binary needs more, and this floor cannot know how much.
const minGuestMemMiB = 128

// maxActorContainers is a sanity cap on containers per actor (all share the one
// micro-VM + virtiofsd). 25 is far above any real pod.
const maxActorContainers = 25

// workloadIDs returns the guest container ids for the actor's containers, in
// order. Recorded on runningActor so the SIGTERM handler knows which guest
// workloads to signal and wait on, and it must name what the guest actually
// runs: a container the agent does not know is rejected with InvalidContainerId,
// and graceful shutdown then gives up without ever reaching the workload.
//
// Containers run under their bare name.
func workloadIDs(ctrs []actorContainer) []string {
	ids := make([]string, 0, len(ctrs))
	for _, c := range ctrs {
		ids = append(ids, c.name)
	}
	return ids
}

// actorContainer is one of the actor's containers prepared for the shared micro-VM:
// its name (also the kata containerID + the merged rootfs's find-paths subdir), the
// host OCI bundle rootfs that backs the overlay lower, and its OCI spec. The writable
// upper is a host directory (see rootfsupper.go); the host kernel merges the two.
type actorContainer struct {
	name         string
	bundleRootfs string
	// spec is the container's OCI spec shaped for micro-VM execution.
	spec *specs.Spec
	// imageMounts are the image volumes this container mounts, and where.
	imageMounts []*ateompb.ImageVolumeMount
}

// resolvedRuntime holds the concrete binary/config paths for a request, taken
// from fetched runtime assets when present, else the process flags.
type resolvedRuntime struct {
	chBinary   string // path to the cloud-hypervisor binary
	configFile string // path to the kata configuration.toml
	virtiofsd  string // path to virtiofsd (overlay RO lower); "" => "virtiofsd" on PATH
}

// firstNonEmpty returns the first non-empty string, or "" if all are empty.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// resolveRuntime resolves the cloud-hypervisor binary + the kata config path from
// fetched assets, falling back to flags.
func (s *AteomService) resolveRuntime(paths map[string]string) resolvedRuntime {
	return resolvedRuntime{
		chBinary:   firstNonEmpty(paths[assetCH], s.chBinary),
		configFile: firstNonEmpty(paths[assetConfig], s.kataConfig),
		virtiofsd:  paths[assetVirtiofsd],
	}
}

// writeGuestResolvConf copies the worker pod's /etc/resolv.conf into a container's
// bundle rootfs (the overlay RO lower) so the guest gets cluster DNS: ateom drops
// atelet's resolv.conf bind and sends no CreateSandbox.Dns, so the guest can
// otherwise reach IPs but not resolve names.
//
// The rootfs is untrusted, so the write goes through os.Root and unlinks rather
// than truncates: an image-planted /etc or /etc/resolv.conf symlink would
// otherwise be followed and clobber that path on the worker pod as root.
func writeGuestResolvConf(rootfs string) error {
	content, err := os.ReadFile("/etc/resolv.conf")
	if err != nil {
		return fmt.Errorf("reading host resolv.conf: %w", err)
	}
	if len(content) == 0 {
		return fmt.Errorf("host /etc/resolv.conf is empty")
	}
	root, err := os.OpenRoot(rootfs)
	if err != nil {
		return fmt.Errorf("opening rootfs %q: %w", rootfs, err)
	}
	defer root.Close()
	if err := root.Mkdir("etc", 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("creating %q: %w", filepath.Join(rootfs, "etc"), err)
	}
	if err := root.Remove("etc/resolv.conf"); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("removing existing guest resolv.conf: %w", err)
	}
	f, err := root.OpenFile("etc/resolv.conf", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("creating guest resolv.conf: %w", err)
	}
	_, err = f.Write(content)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("writing guest resolv.conf: %w", err)
	}
	return nil
}

// RunWorkload starts an actor's containers in a cloud-hypervisor micro-VM. It
// consumes a VM booted by PrepareSandbox when one is available, otherwise it
// performs the same boot inline.
//
// ateom boots cloud-hypervisor directly (no kata shim) and gives each container a
// rootfs merged ON THE HOST: overlay(image lower + host-disk upper), served over the
// one kataShared virtio-fs share. It drives the kata clh boot (vm.create kernel+image+fs,
// add-net, vm.boot) and the post-boot setup the shim would otherwise do (agent
// CreateSandbox + guest network config) before having the kata-agent assemble and
// start each container.
//
// Contract with atelet:
//   - The runtime assets (guest kernel, guest OS image, cloud-hypervisor, virtiofsd,
//     base kata config) are on disk and passed as runtime asset paths.
//   - The OCI bundle (config.json + populated rootfs/) is prepared per container.
func (s *AteomService) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (resp *ateompb.RunWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.rejectIfDraining(); err != nil {
		return nil, err
	}

	// Register the boot so a SIGTERM arriving mid-cold-boot cancels it rather than
	// waiting out the whole thing holding lock.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.setActiveRPC(rpcRunWorkload, cancel)
	defer s.clearActiveRPC()

	var prepared *preparedSandbox
	if s.prepared != nil {
		if s.prepared.cleanupStarted {
			return nil, status.Error(codes.FailedPrecondition, "prepared sandbox cleanup is incomplete")
		}
		if !s.prepared.params.matches(sandboxParamsFromRun(req)) {
			return nil, status.Error(codes.FailedPrecondition, "RunWorkload does not match the prepared sandbox")
		}
		prepared = s.prepared
		s.prepared = nil
	} else if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}

	p := actorBootParams{
		actorRef:         resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()},
		actorUID:         req.GetActorUid(),
		templateAtespace: req.GetActorTemplateAtespace(),
		templateName:     req.GetActorTemplateName(),
		containers:       req.GetSpec().GetContainers(),
		assetPaths:       req.GetRuntimeAssetPaths(),
		egressGateway:    req.GetEgressGateway(),
		size:             sizing.FromLimits(req.GetCpuMilli(), req.GetMemoryBytes()),
		prepared:         prepared,
	}

	attribution := p.actorAttribution()
	s.actorLogger.EmitLifecycleLog(ctx, "Actor starting", attribution)

	// Retain the attribution before the boot rather than after it, so a sample
	// taken against a workload that dies mid-boot is still attributable. A cold
	// boot can take a while and can be retried, and an actor that never reaches
	// readyz is one whose usage is worth reporting rather than the one case that
	// reports nothing. The defer drops it again if the boot fails outright.
	// Matches ateom-gvisor's RunWorkload.
	s.activeActor.Store(&attribution)
	defer func() {
		if retErr != nil {
			s.activeActor.Store(nil)
		}
	}()

	if err := s.coldBootActorRetrying(ctx, p); err != nil {
		return nil, err
	}
	s.actorLogger.EmitLifecycleLog(ctx, "Actor started", attribution)
	slog.InfoContext(ctx, "Actor started (overlay rootfs)", slog.String("id", p.actorUID))
	return &ateompb.RunWorkloadResponse{}, nil
}

// actorBootParams is what a cold boot needs about an actor. It comes from a Run
// request, or from a Restore request whose snapshot scope covers only the
// durable-dir volumes (the workload itself cold-starts).
type actorBootParams struct {
	actorRef         resources.ActorRef
	actorUID         string
	templateAtespace string
	templateName     string
	containers       []*ateompb.Container
	assetPaths       map[string]string
	// egressGateway is nil unless actor TCP should be redirected through atunnel.
	egressGateway *ateompb.EgressGateway
	// size is the actor's declared limits (from the ActorTemplate), supplied on
	// the RunWorkload / RestoreWorkload RPC. It sizes the VM itself (vCPUs,
	// memory); a container's own cgroup limit comes from its declared resources.
	// Zero fields keep the kata defaults.
	size sizing.SandboxSize
	// prepared is non-nil when PrepareSandbox already booted the VM while atelet
	// prepared the application OCI bundles and, on restore, the checkpoint.
	// coldBootActor consumes it exactly once; a retry after a dead prepared VM
	// performs a full fresh boot.
	prepared *preparedSandbox
}

// actorAttribution regroups the actor fields that arrived on the Run/Restore
// request, for retention in AteomService.activeActor.
func (p actorBootParams) actorAttribution() resources.ActorAttribution {
	return resources.ActorAttribution{
		Ref:              p.actorRef,
		UID:              p.actorUID,
		TemplateAtespace: p.templateAtespace,
		TemplateName:     p.templateName,
	}
}

// coldBootAttempts is how many times a cold boot is tried when the micro-VM
// stops before any application container starts. Two: one retry covers a
// transient guest death, and beyond that the fault is not transient and the
// caller should hear about it.
const coldBootAttempts = 2

// coldBootActorRetrying cold-boots the actor, retrying if the micro-VM stopped
// before any application container started.
//
// Retrying is safe there and nowhere else: a guest that never reached its agent
// ran none of the actor's containers, so the attempt has no observable effect,
// and coldBootActor's failure path tears the whole thing down (VMM, virtiofsds,
// network, bundle mounts) before returning. It is also the only recovery — the
// dead VM does not come back, so the alternative is failing the actor's resume.
// Every retry is logged alongside the guest's boot diagnostics, so a guest that
// dies at boot is never silent.
func (s *AteomService) coldBootActorRetrying(ctx context.Context, p actorBootParams) error {
	for attempt := 1; ; attempt++ {
		err := s.coldBootActor(ctx, p)
		p.prepared = nil
		if err == nil || attempt >= coldBootAttempts || !errors.Is(err, errGuestStopped) {
			return err
		}
		slog.WarnContext(ctx, "Micro-VM stopped before application containers started; retrying cold boot",
			slog.String("id", p.actorUID), slog.Int("attempt", attempt), slog.Any("err", err))
	}
}

// coldBootActor completes a cold boot and starts the actor's containers,
// reusing a VM from PrepareSandbox when supplied and otherwise booting one
// inline. It registers the result in s.running. The caller holds s.lock and
// owns the lifecycle logging.
func (s *AteomService) coldBootActor(ctx context.Context, p actorBootParams) (retErr error) {
	actorUID := p.actorUID
	prepared := p.prepared

	// From this point coldBootActor owns a VM supplied by PrepareSandbox or one
	// returned by bootSandbox below. Install cleanup before any other operation:
	// egress setup and OCI-bundle validation can fail before the normal boot path.
	defer func() {
		if retErr == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if prepared != nil {
			if cleanupErr := s.cleanupPreparedSandbox(cleanupCtx, prepared); cleanupErr != nil {
				slog.WarnContext(cleanupCtx, "Failed to clean up microVM after cold boot failure", slog.Any("err", cleanupErr))
			}
		}
		// buildActorContainers may have composed bundle overlays before a fresh
		// boot was attempted. bootSandbox cleanup also reaches these mounts, but
		// this covers failures that happen before bootSandbox owns any resources.
		if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(actorUID)); err != nil {
			slog.WarnContext(cleanupCtx, "Failed to unmount bundle rootfs overlays after Run failure", slog.Any("err", err))
		}
	}()

	// All of the actor's containers share the one micro-VM (which is the pod
	// sandbox): each gets its own overlay rootfs and its own kata-agent
	// CreateContainer/StartContainer, driven below after the shared boot +
	// CreateSandbox + guest networking.
	containers := p.containers
	if len(containers) == 0 {
		return status.Error(codes.InvalidArgument, "actor spec has no containers")
	}
	if len(containers) > maxActorContainers {
		return status.Errorf(codes.Unimplemented, "ateom-microvm supports at most %d containers, got %d", maxActorContainers, len(containers))
	}

	// ateom builds the CH vm.create itself, so it needs the guest kernel + image
	// paths directly.
	paths := p.assetPaths
	kernel, image := paths[assetKernel], paths[assetImage]
	if kernel == "" || image == "" {
		return fmt.Errorf("ateom-microvm requires %q and %q asset paths", assetKernel, assetImage)
	}
	egress, err := s.prepareActorEgress(ctx, p.actorUID, p.egressGateway)
	if err != nil {
		return err
	}

	// Prepare each container's OCI spec + record its bundle rootfs (the overlay
	// lower the host merges under the container's writable upper).
	ctrs, err := s.buildActorContainers(actorUID, containers)
	if err != nil {
		return err
	}

	tStarted := time.Now()
	preparedEarly := prepared != nil
	if prepared == nil {
		prepared, err = s.bootSandbox(ctx, sandboxBootParams{
			actorUID:       actorUID,
			assetPaths:     paths,
			redirectEgress: p.egressGateway != nil,
			size:           p.size,
		})
		if err != nil {
			return err
		}
	}
	tSandbox := time.Now()

	// Reject limits the booted guest can never satisfy before any application
	// container reaches the agent. The envelope is captured from the exact
	// sizing used by bootSandbox, including for a VM prepared by an earlier RPC.
	if err := checkResourceEnvelope(ctrs, prepared.envelope); err != nil {
		return err
	}

	// virtiofsd and the guest are already running. Mount the application rootfs
	// overlays into its shared tree only after atelet has completed the bundles;
	// the guest first reads that tree in CreateSandbox below.
	if err := s.stageMergedRootfsMounts(ctx, actorUID, ctrs, containers); err != nil {
		return err
	}
	tRootfs := time.Now()

	// A VM prepared in a separate RPC can die while atelet is still pulling the
	// image. Cloud Hypervisor removes this socket when it exits; classify that as
	// the one safe retry case because no application container has run yet.
	vsockPath := kata.VsockSocketPath(actorUID)
	if _, err := os.Stat(vsockPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w (cloud-hypervisor removed %q): %v", errGuestStopped, vsockPath, err)
		}
		return fmt.Errorf("while checking kata-agent vsock socket %q: %w", vsockPath, err)
	}
	ac := prepared.actor.guestAgent
	if ac == nil {
		return fmt.Errorf("prepared micro-VM has no kata-agent client")
	}

	// Post-boot kata-agent setup: sandbox, guest networking, start each container.
	if err := s.startActorContainers(ctx, ac, actorUID, vsockPath, ctrs); err != nil {
		return err
	}
	tContainers := time.Now()

	// Block until every readyz-enabled container reports 200.
	if err := readyz.WaitAll(ctx, containers, ateomnet.ActorVethIP); err != nil {
		return fmt.Errorf("while waiting for container readyz: %w", err)
	}
	tReady := time.Now()

	slog.InfoContext(ctx, "Actor boot phases", slog.String("id", actorUID),
		slog.Bool("prepared_early", preparedEarly),
		slog.Duration("sandbox", tSandbox.Sub(tStarted)),
		slog.Duration("rootfs", tRootfs.Sub(tSandbox)),
		slog.Duration("containers", tContainers.Sub(tRootfs)),
		slog.Duration("readyz", tReady.Sub(tContainers)))

	if err := s.activateActorNetworking(p.actorRef.Atespace, p.actorRef.Name, egress); err != nil {
		return err
	}
	prepared.actor.workloadIDs = workloadIDs(ctrs)
	s.running[actorUID] = prepared.actor

	// Forward each container's stdout/stderr into the pod logs, keyed by the
	// container id (== the name; see StartRootfsContainer). The goroutines read
	// over ac for the actor's lifetime and exit (io.EOF) when teardownActor
	// closes ac.
	workloadIDs := make([]string, 0, len(ctrs))
	attribution := p.actorAttribution()
	for _, c := range ctrs {
		s.startActorLogForwarding(ac, attribution, c.name, c.name)
		workloadIDs = append(workloadIDs, c.name)
	}

	// Publish the guest to GetWorkloadStats, past every error return above: a
	// failing attempt closes ac on its way out (and coldBootActorRetrying may
	// then try the whole boot again), so a target published earlier would leave
	// the handler polling a connection nobody owns. Same client the forwarding
	// above reads over — ttrpc multiplexes, and teardownActor ends both.
	s.guestStats.Store(&guestStatsTarget{actorUID: actorUID, agent: ac, workloadIDs: workloadIDs})

	return nil
}

// buildActorContainers prepares each of the actor's containers for the shared
// micro-VM: it loads the OCI spec from the per-container bundle, injects guest DNS,
// and records the bundle rootfs that backs the overlay's RO lower. No host disk is
// mounted here — the merged overlays are assembled in stageMergedRootfs after the
// sandbox state is clean. Both RunWorkload and RestoreWorkload go through here.
func (s *AteomService) buildActorContainers(actorUID string, containers []*ateompb.Container) ([]actorContainer, error) {
	ctrs := make([]actorContainer, len(containers))
	for i, c := range containers {
		cn := c.GetName()
		bundle := ateompath.OCIBundlePath(actorUID, cn)
		spec, err := ocispec.Load(bundle)
		if err != nil {
			return nil, fmt.Errorf("while reading the OCI spec for %q: %w", cn, err)
		}
		if err := ocispec.ShapeMicroVM(spec, ocispec.MicroVMOptions{ActorUID: actorUID, ContainerID: cn}); err != nil {
			return nil, fmt.Errorf("while shaping the OCI spec for %q: %w", cn, err)
		}
		// Compose the bundle rootfs from the node's cached image layers (an
		// overlay mounted in this pod's namespace; no-op for bundles without an
		// overlay spec). Everything downstream — the resolv.conf write below,
		// the bind into virtiofsd's shared dir, the read-only remount — then
		// sees the composed tree, with host-side writes landing in the bundle's
		// private upper. The guest still builds its own writable upper on top.
		if err := imagecache.SetupBundleRootfs(bundle); err != nil {
			return nil, fmt.Errorf("while composing rootfs for %q: %w", cn, err)
		}
		bundleRootfs := filepath.Join(bundle, "rootfs")
		// Write cluster DNS into the lower before it's served over virtio-fs: ateom
		// drops atelet's resolv.conf bind and sends no CreateSandbox.Dns, so without
		// this the guest can reach IPs but not resolve names. Doing it here covers both
		// run and restore (both reconstruct the lower from the bundle).
		if err := writeGuestResolvConf(bundleRootfs); err != nil {
			return nil, fmt.Errorf("while writing guest resolv.conf for %q: %w", cn, err)
		}
		ctrs[i] = actorContainer{
			name:         cn,
			bundleRootfs: bundleRootfs,
			spec:         spec,
			imageMounts:  c.GetImageVolumeMounts(),
		}
	}
	return ctrs, nil
}

// stageMergedRootfsMounts assembles each container's merged rootfs on the host
// at virtiofsd's find-paths location and stages the actor's volumes beneath the
// same unified share. It is separate from startRootfsShare so PrepareSandbox can
// boot before the OCI bundles exist, then add all mounts before CreateSandbox.
func (s *AteomService) stageMergedRootfsMounts(ctx context.Context, id string, ctrs []actorContainer, containers []*ateompb.Container) error {
	upperBase := rootfsUpperDir(id)
	for _, c := range ctrs {
		if err := kata.StageMergedRootfs(ctx, c.bundleRootfs, upperBase, id, c.name); err != nil {
			return fmt.Errorf("while staging merged rootfs for %q: %w", c.name, err)
		}
		for _, vm := range c.imageMounts {
			src := ateompath.ImageVolumeMountPath(id, c.name, vm.GetVolumeName())
			if err := kata.StageImageVolume(ctx, src, id, c.name, vm.GetVolumeName()); err != nil {
				return fmt.Errorf("while staging image volume %q for %q: %w", vm.GetVolumeName(), c.name, err)
			}
		}
	}
	if hasDurableVolumes(containers) {
		if err := s.stageDurableVolumes(ctx, id); err != nil {
			return fmt.Errorf("while staging durable-dir volumes: %w", err)
		}
	}
	if hasCsiVolumes(containers) {
		if err := s.stageCsiVolumes(ctx, id); err != nil {
			return fmt.Errorf("while staging CSI volumes: %w", err)
		}
	}
	if hasSystemInfoVolumes(containers) {
		if err := s.stageSystemInfoVolumes(ctx, id); err != nil {
			return fmt.Errorf("while staging system-info volumes: %w", err)
		}
	}
	return nil
}

// startRootfsShare starts the one virtiofsd serving an actor's shared rootfs
// tree. The tree may still be empty: mounts added later beneath this rshared
// path propagate into virtiofsd's mount namespace.
func (s *AteomService) startRootfsShare(ctx context.Context, rr resolvedRuntime, id string) (*exec.Cmd, error) {
	if err := os.MkdirAll(kata.SharedDir(id), 0o755); err != nil {
		return nil, fmt.Errorf("while creating virtiofsd shared directory: %w", err)
	}
	vfsdLog, err := os.OpenFile(virtiofsdLogPath(id), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("while opening virtiofsd log: %w", err)
	}
	defer vfsdLog.Close()
	vfsdCmd, err := kata.StartVirtiofsd(ctx, kata.VirtiofsdOptions{
		Binary:     rr.virtiofsd,
		SocketPath: kata.VirtiofsdSocketPath(id),
		SharedDir:  kata.SharedDir(id),
		Log:        vfsdLog,
	})
	if err != nil {
		return nil, fmt.Errorf("while starting virtiofsd: %w", err)
	}
	return vfsdCmd, nil
}

// stageMergedRootfs preserves the restore path's original ordering: assemble
// all rootfs mounts first, then start virtiofsd. The returned process outlives
// this call and is owned by the caller.
func (s *AteomService) stageMergedRootfs(ctx context.Context, rr resolvedRuntime, id string, ctrs []actorContainer, containers []*ateompb.Container) (*exec.Cmd, error) {
	if err := s.stageMergedRootfsMounts(ctx, id, ctrs, containers); err != nil {
		return nil, err
	}
	return s.startRootfsShare(ctx, rr, id)
}

// guestConfig reads guest sizing + agent kernel params from the resolved kata
// config, enabling the debug console (vsock 1026) for in-guest diagnostics and,
// with kataDebug, raising the agent log level.
func (s *AteomService) guestConfig(rr resolvedRuntime) (memMiB, vcpus int, kparams string, err error) {
	var cfgBytes []byte
	if rr.configFile != "" {
		cfgBytes, _ = os.ReadFile(rr.configFile)
	}
	cfg, err := kata.ParseConfig(cfgBytes, 2048, 1)
	if err != nil {
		return 0, 0, "", fmt.Errorf("while parsing kata config: %w", err)
	}
	kparams = kata.WithDebugConsole(cfg.KernelParams)
	if s.kataDebug {
		kparams = kata.WithAgentDebug(kparams)
	}
	return cfg.MemoryMiB, cfg.VCPUs, kparams, nil
}

// resolveGuestMemMiB returns the micro-VM guest RAM (MiB) for an actor's declared
// memory limit. declaredBytes == 0 means "unset" and returns fallbackMiB (the
// kata-config default). Otherwise the guest gets the declared memory minus the VMM
// reserve; if that leaves less than a bootable minimum it errors — naming the limit,
// the reserve, and the minimum — instead of silently reverting to the (larger)
// fallback, which would boot the actor bigger than the worker was sized for and OOM
// the pod (see vmmMemReserveMiB, minGuestMemMiB, and internal/sizing).
func resolveGuestMemMiB(declaredBytes int64, reserveMiB, fallbackMiB int) (int, error) {
	if declaredBytes <= 0 {
		return fallbackMiB, nil
	}
	declaredMiB := int(declaredBytes / (1024 * 1024))
	m := declaredMiB - reserveMiB
	if m < minGuestMemMiB {
		return 0, fmt.Errorf("actor memory limit %dMiB is too small for a micro-VM: "+
			"the %dMiB VMM reserve leaves %dMiB, below the %dMiB guest minimum",
			declaredMiB, reserveMiB, m, minGuestMemMiB)
	}
	return m, nil
}

// agentInit reports whether to boot the kata agent as the guest's PID 1, from what
// the VMM just told us about itself over vmm.ping.
//
// Booting the agent directly skips systemd entirely, which is most of what the guest
// reads at boot and most of what its snapshot carries. The catch is that it also drops
// chronyd (kata-containers.target wants it), and chronyd is what repairs the guest
// clock after a resume — so this is only safe on a VMM that advances the guest clock
// across a restore itself. On an older or unreadable version, boot systemd instead and
// keep the guest correct at the cost of the memory.
func agentInit(ctx context.Context, info ch.VMMInfo) bool {
	if info.AdvancesGuestClockOnRestore() {
		return true
	}
	slog.InfoContext(ctx, "VMM does not advance the guest clock on restore; booting systemd to keep chronyd",
		slog.String("vmm_version", info.Version), slog.String("vmm_build_version", info.BuildVersion))
	return false
}

// initParams returns the kernel cmdline parameters that select the guest's PID 1.
// The systemd path must name kata's target (else the guest powers off ~6s in) and
// mask systemd-networkd, since the agent owns eth0.
func initParams(agentInit bool) string {
	if agentInit {
		return "init=" + kataAgentPath
	}
	return "systemd.unit=kata-containers.target " +
		"systemd.mask=systemd-networkd.service systemd.mask=systemd-networkd.socket"
}

// buildVMConfig assembles the cloud-hypervisor VmConfig. The console is arch-specific:
// ttyAMA0 on arm64, ttyS0 on amd64. /dev/vda is the RO guest image; the actor rootfs's RO
// lower is the virtio-fs device on PCI segment 1 (hence num_pci_segments=2), with no
// actor disks.
//
// init=kataAgentPath boots the kata agent as PID 1 instead of systemd. The agent detects
// that it is PID 1 and does the init work itself: it mounts /proc, /sys, devtmpfs /dev,
// /dev/shm, /dev/pts, tmpfs /run and the cgroup hierarchy, then serves ttrpc over vsock.
// Nothing else in the guest image is ours to run — the workload is a container the agent
// starts — so systemd only cost us. Measured on the counter demo, dropping it took the
// guest's boot-time reads from this disk from 58.6MiB to 35.0MiB, the snapshot from 145MiB
// to 106.6MiB at the same guest RAM, and a cold boot from 15.9s to 10.3s: the agent is
// PID 1 rather than a unit systemd reaches several seconds in, so ateom stops waiting for
// it (the dial phase goes 10.4s -> 4.7s).
//
// Dropping systemd also drops chronyd (kata-containers.target wants it), which is what
// used to repair the guest clock after a resume. That is safe only from cloud-hypervisor
// v53, which advances the guest clock across a restore itself; on v52 a restored guest
// stays frozen at the instant it was snapshotted.
//
// The disk-backed rootfs upper share (see rootfsupper.go) is always present.
//
// The guest console is a virtio-console (hvc0), not the emulated UART. Every byte
// written to an 8250/pl011 traps to the VMM, and a kata guest prints ~340 lines
// before the agent listens, so the UART — not the kernel's work — dominated cold
// boot: measured host-launch to the agent's ttrpc accept, 1.24s -> 0.39s on a GKE
// amd64 node and 21.6s -> 1.9s on a nested-virt arm64 kind node. What that costs is
// the earliest messages: hvc0 only exists once virtio-console probes, so the memory
// map, CPU features and ACPI lines never reach the log. kataDebug adds the UART back
// with earlycon (and pays the ~800ms) for diagnosing a guest that dies before then.
func buildVMConfig(id, kernel, image, kparams, consoleLog string, memMiB, vcpus int, agentInit, debug bool) ch.VmConfig {
	cmdline := "root=/dev/vda1 rootflags=data=ordered,errors=remount-ro ro rootfstype=ext4 " +
		"panic=1 no_timer_check noreplace-smp console=hvc0 " +
		initParams(agentInit)
	if kparams != "" {
		cmdline += " " + kparams
	}
	serial := &ch.ConsoleConfig{Mode: "Off"}
	if debug {
		cmdline += " " + earlyconParam()
		serial = &ch.ConsoleConfig{Mode: "File", File: kata.SerialLogPath(id)}
	}
	return ch.VmConfig{
		Cpus:    ch.CpusConfig{BootVcpus: int32(vcpus), MaxVcpus: int32(vcpus)},
		Memory:  ch.MemoryConfig{Size: int64(memMiB) * 1024 * 1024, Shared: true},
		Payload: ch.PayloadConfig{Kernel: kernel, Cmdline: cmdline},
		Disks: []ch.DiskConfig{
			{Path: image, Readonly: true, ImageType: "Raw", NumQueues: int32(vcpus), QueueSize: 1024},
		},
		Fs:       buildFsConfigs(id),
		Platform: &ch.PlatformConfig{NumPciSegments: 2},
		Rng:      &ch.RngConfig{Src: "/dev/urandom"},
		Console:  &ch.ConsoleConfig{Mode: "File", File: consoleLog},
		Serial:   serial,
		Vsock:    &ch.VsockConfig{Cid: 3, Socket: kata.VsockSocketPath(id)},
	}
}

// earlyconParam points the kernel's early console at the UART cloud-hypervisor
// emulates before virtio-console exists: an ISA port on x86, MMIO on arm64.
func earlyconParam() string {
	if runtime.GOARCH == "arm64" {
		return "earlycon=pl011,mmio,0x09000000"
	}
	return "earlycon=uart,io,0x3f8,115200"
}

// buildFsConfigs returns the VM's virtio-fs device: the unified share hosting
// container rootfs trees, durable volumes, CSI volumes, and system-info
// volumes. Sits on PCI segment 1 (the segment buildVMConfig reserves for
// virtio-fs).
func buildFsConfigs(id string) []ch.FsConfig {
	return []ch.FsConfig{{
		Tag: kata.FsTag, Socket: kata.VirtiofsdSocketPath(id),
		NumQueues: 1, QueueSize: 1024, PciSegment: 1,
	}}
}

// startActorContainers performs the post-boot kata-agent setup the shim normally
// does at boot: establish the sandbox once (mounting the kataShared virtio-fs base),
// configure guest networking (eth0 IP/MAC/MTU + routes) once, then start each
// container on its own overlay rootfs. On failure it dumps guest diagnostics.
func (s *AteomService) startActorContainers(ctx context.Context, ac *kata.AgentClient, id, vsockPath string, ctrs []actorContainer) error {
	// Establish the agent sandbox + the kataShared virtio-fs mount (every
	// container's merged rootfs, durable volumes, CSI volumes, and system-info
	// volumes). All containers share it, so use the first container's hostname.
	tStart := time.Now()
	sbCtx, sbCancel := context.WithTimeout(ctx, 20*time.Second)
	err := ac.CreateSandboxForActor(sbCtx, kata.CreateSandboxOpts{
		SandboxID: id,
		Hostname:  ctrs[0].spec.Hostname,
	})
	sbCancel()
	if err != nil {
		return fmt.Errorf("while creating agent sandbox: %w", err)
	}
	tSandbox := time.Now()

	// Configure guest networking (the shim's job): eth0 IP/MAC/MTU, routes, ARP.
	mtu := uint64(s.actorVethMTU(ctx))
	netCtx, netCancel := context.WithTimeout(ctx, 20*time.Second)
	err = s.configureGuestNetwork(netCtx, ac, mtu)
	netCancel()
	if err != nil {
		dump := kata.DebugConsoleDump(ctx, vsockPath, "ip addr 2>&1; echo '== route =='; ip route 2>&1; echo '== neigh =='; ip neigh 2>&1")
		slog.ErrorContext(ctx, "guest network config failed; dump", slog.String("dump", dump))
		return fmt.Errorf("while configuring guest network: %w", err)
	}

	tNetwork := time.Now()

	for _, c := range ctrs {
		if err := startRootfsContainer(ctx, ac, vsockPath, c); err != nil {
			return err
		}
	}
	slog.InfoContext(ctx, "Agent setup phases", slog.String("id", id),
		slog.Duration("sandbox", tSandbox.Sub(tStart)),
		slog.Duration("network", tNetwork.Sub(tSandbox)),
		slog.Duration("containers", time.Since(tNetwork)),
		slog.Int("container_count", len(ctrs)))
	return nil
}

// startRootfsContainer brings up one container on its host-merged rootfs (the
// stock kata flow: create + start against shared/<name>/rootfs). On failure it
// dumps the guest's view of the shared tree.
//
// Its spec binds every declared volume at its mount path.
func startRootfsContainer(ctx context.Context, ac *kata.AgentClient, vsockPath string, c actorContainer) error {
	cCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err := ac.StartRootfsContainer(cCtx, c.name, c.spec)
	cancel()
	if err != nil {
		dump := kata.DebugConsoleDump(ctx, vsockPath,
			"echo '== shared/containers =='; ls -la /run/kata-containers/shared/containers/ 2>&1 | head -40; "+
				"echo '== rootfs =='; ls /run/kata-containers/shared/containers/"+c.name+"/rootfs/ 2>&1 | head; "+
				"echo '== mounts =='; grep -E 'kata|virtiofs' /proc/mounts 2>&1")
		slog.ErrorContext(ctx, "rootfs container failed; dump", slog.String("container", c.name), slog.String("dump", dump))
		return fmt.Errorf("while starting rootfs container %q: %w", c.name, err)
	}
	return nil
}

// startActorLogForwarding spawns two goroutines that pump the actor container's
// stdout and stderr (read over the kata-agent ttrpc client ac via repeated
// ReadStdout/ReadStderr) through the shared actorlog forwarder, which annotates
// each line with the actor's ate.dev/* labels and writes it to the pod's stdout.
//
// The streams are keyed by streamID == the kata containerID==execID (the overlay
// workload id); lines are tagged with actorName + containerName
// (ate.actor.container.name) so a multi-container actor demultiplexes.
// The reader contexts are context.Background() — the goroutines are NOT bound to the
// RPC that started them; they terminate when ac is closed (by teardownActor), which
// makes the in-flight ReadStdout/ReadStderr fail and the StreamReader return io.EOF,
// ending WrapContainerLogs. This keeps the agent connection (which ttrpc allows
// concurrent Calls on) alive for forwarding while guaranteeing no goroutine outlives
// the connection.
func (s *AteomService) startActorLogForwarding(ac *kata.AgentClient, a resources.ActorAttribution, streamID, containerName string) {
	go s.actorLogger.WrapContainerLogs(kata.NewStdioReader(context.Background(), ac, streamID, streamID, false), a, containerName)
	go s.actorLogger.WrapContainerLogs(kata.NewStdioReader(context.Background(), ac, streamID, streamID, true), a, containerName)
}

// errGuestStopped reports that the micro-VM stopped before an application
// container started. Callers can safely retry because there are no workload
// side effects yet.
var errGuestStopped = errors.New("micro-VM stopped before application containers started")

// Bounds on the kata-agent dial poll. What they set is the window between the agent
// starting to listen and us noticing: an attempt already in flight when the agent
// comes up cannot succeed (cloud-hypervisor has answered that CONNECT), so the wait
// is the attempt plus the interval.
//
// They were 5s and 500ms, i.e. one attempt a second. What dominates this phase is
// the guest not listening yet — measured at 1.37s on GKE — so the poll should cost
// a fraction of that, not add half a second of its own.
const (
	agentDialAttemptTimeout = 300 * time.Millisecond
	agentDialInterval       = 20 * time.Millisecond
)

// dialAgentRetry polls DialAgent until the kata-agent answers the hybrid-vsock
// CONNECT (the socket file exists as soon as cloud-hypervisor boots, but the agent
// only listens once the guest's init reaches it) or the overall timeout elapses.
//
// Poll fast. A failed attempt is cheap — cloud-hypervisor answers the CONNECT of a
// port nothing is listening on straight away — while a slow poll adds its whole
// interval to every cold boot, for nothing.
//
// A dial that fails with ENOENT ends the poll immediately as errGuestStopped:
// callers wait for the socket to appear before dialing, and cloud-hypervisor
// unlinks it when the VM stops (virtio-vsock device shutdown), so a socket that
// has gone missing means the guest died. Polling on would only spend the rest
// of the timeout to report a bare "no such file or directory".
func dialAgentRetry(ctx context.Context, vsockPath string, timeout time.Duration) (*kata.AgentClient, error) {
	deadline := time.Now().Add(timeout)
	start := time.Now()
	var lastErr error
	for attempt := 1; ; attempt++ {
		dctx, cancel := context.WithTimeout(ctx, agentDialAttemptTimeout)
		ac, err := kata.DialAgent(dctx, vsockPath)
		cancel()
		if err == nil {
			slog.InfoContext(ctx, "kata-agent answered", slog.Int("attempts", attempt),
				slog.Duration("elapsed", time.Since(start)))
			return ac, nil
		}
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w (cloud-hypervisor removed %q): %w", errGuestStopped, vsockPath, err)
		}
		if lastErr == nil {
			// The first failure is the interesting one: it says why the agent is not
			// answering yet. Later attempts repeat it until it succeeds.
			slog.DebugContext(ctx, "kata-agent not answering yet", slog.Any("err", err),
				slog.Duration("attempt_took", time.Since(start)))
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, lastErr
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(agentDialInterval):
		}
	}
}

// logGuestBootDiagnostics dumps what the host recorded about a guest that never
// reached the kata-agent: the console tail, where a guest-side panic or an early
// power-off shows up, and each virtiofsd's log — cloud-hypervisor stops the VM
// when a vhost-user backend dies, and that leaves the console silent.
func logGuestBootDiagnostics(ctx context.Context, actorUID, consoleLog string) {
	for _, l := range []struct{ name, path string }{
		{"console", consoleLog},
		{"serial", kata.SerialLogPath(actorUID)},
		{"virtiofsd", virtiofsdLogPath(actorUID)},
	} {
		b, err := os.ReadFile(l.path)
		if err != nil || len(b) == 0 {
			continue
		}
		slog.ErrorContext(ctx, "agent dial failed; guest boot diagnostics",
			slog.String("log", l.name), slog.String("tail", tailString(string(b), 3000)))
	}
}

// virtiofsdLogPath is where the overlay RO lower's virtiofsd logs, under the
// actor's VM dir alongside the sockets and the guest console.
func virtiofsdLogPath(id string) string { return filepath.Join(kata.VMDir(id), "virtiofsd.log") }

// tailString returns the last n bytes of s (for logging a serial-console tail).
func tailString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// configureGuestNetwork replicates the kata shim's guest network setup over the
// agent: configure eth0 (IP/MAC/MTU), install the connected + default routes, and
// pin the gateway's ARP entry to its fixed MAC (so a restored guest's frozen
// neighbor entry stays valid).
func (s *AteomService) configureGuestNetwork(ctx context.Context, ac *kata.AgentClient, mtu uint64) error {
	if err := ac.UpdateInterface(ctx, &agentpb.Interface{
		Device: ateomnet.ActorVethName,
		Name:   ateomnet.ActorVethName,
		HwAddr: actorGuestMAC,
		Mtu:    mtu,
		IPAddresses: []*agentpb.IPAddress{
			{Family: agentpb.IPFamily_v4, Address: ateomnet.ActorVethIP, Mask: "30"},
		},
	}); err != nil {
		return err
	}
	if err := ac.UpdateRoutes(ctx, []*agentpb.Route{
		{Dest: ateomnet.ActorVethSubnet, Device: ateomnet.ActorVethName, Scope: uint32(unix.RT_SCOPE_LINK), Family: agentpb.IPFamily_v4},
		{Dest: "", Gateway: ateomnet.ActorVethGateway, Device: ateomnet.ActorVethName, Family: agentpb.IPFamily_v4},
	}); err != nil {
		return err
	}
	return ac.AddARPNeighbors(ctx, []*agentpb.ARPNeighbor{{
		ToIPAddress: &agentpb.IPAddress{Family: agentpb.IPFamily_v4, Address: ateomnet.ActorVethGateway},
		Device:      ateomnet.ActorVethName,
		Lladdr:      hostVethMAC,
		State:       0x80, // NUD_PERMANENT
	}})
}

// waitForFile polls for path to exist, up to d. Used to wait for the kata-agent
// hybrid-vsock socket the shim creates during VM boot before dialing it.
func waitForFile(path string, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// slogWriter adapts an io.Writer to slog at info level, capturing the
// cloud-hypervisor process's stdout/stderr into the worker logs.
type slogWriter struct{ ctx context.Context }

func (w slogWriter) Write(p []byte) (int, error) {
	slog.InfoContext(w.ctx, "cloud-hypervisor", slog.String("out", string(p)))
	return len(p), nil
}
