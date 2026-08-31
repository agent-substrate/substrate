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

	// ateom owns this CH process (booted at Run or relaunched at Restore).
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
func writeGuestResolvConf(rootfs string) error {
	content, err := os.ReadFile("/etc/resolv.conf")
	if err != nil {
		return fmt.Errorf("reading host resolv.conf: %w", err)
	}
	if len(content) == 0 {
		return fmt.Errorf("host /etc/resolv.conf is empty")
	}
	etc := filepath.Join(rootfs, "etc")
	if err := os.MkdirAll(etc, 0o755); err != nil {
		return fmt.Errorf("creating %q: %w", etc, err)
	}
	if err := os.WriteFile(filepath.Join(etc, "resolv.conf"), content, 0o644); err != nil {
		return fmt.Errorf("writing guest resolv.conf: %w", err)
	}
	return nil
}

// RunWorkload boots the actor as a cloud-hypervisor micro-VM and starts its containers.
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

	if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}

	p := actorBootParams{
		actorRef:      resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()},
		actorUID:      req.GetActorUid(),
		templateNS:    req.GetActorTemplateNamespace(),
		templateName:  req.GetActorTemplateName(),
		containers:    req.GetSpec().GetContainers(),
		assetPaths:    req.GetRuntimeAssetPaths(),
		egressGateway: req.GetEgressGateway(),
		size:          sizing.FromLimits(req.GetCpuMilli(), req.GetMemoryBytes()),
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
	actorRef     resources.ActorRef
	actorUID     string
	templateNS   string
	templateName string
	containers   []*ateompb.Container
	assetPaths   map[string]string
	// egressGateway is nil unless actor TCP should be redirected through atunnel.
	egressGateway *ateompb.EgressGateway
	// size is the actor's declared limits (from the ActorTemplate), supplied on
	// the RunWorkload / RestoreWorkload RPC. It sizes the VM itself (vCPUs,
	// memory); a container's own cgroup limit comes from its declared resources.
	// Zero fields keep the kata defaults.
	size sizing.SandboxSize
}

// actorAttribution regroups the actor fields that arrived on the Run/Restore
// request, for retention in AteomService.activeActor.
func (p actorBootParams) actorAttribution() resources.ActorAttribution {
	return resources.ActorAttribution{
		Ref:               p.actorRef,
		UID:               p.actorUID,
		TemplateNamespace: p.templateNS,
		TemplateName:      p.templateName,
	}
}

// coldBootAttempts is how many times a cold boot is tried when the micro-VM
// stops before the kata-agent answers. Two: one retry covers a transient guest
// death (a contended host makes the guest's boot pathologically slow, and a
// boot that stalls long enough is torn down guest-side), and beyond that the
// fault is not transient and the caller should hear about it.
const coldBootAttempts = 2

// coldBootActorRetrying cold-boots the actor, retrying if the micro-VM stopped
// before the kata-agent answered.
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
		if err == nil || attempt >= coldBootAttempts || !errors.Is(err, errGuestStopped) {
			return err
		}
		slog.WarnContext(ctx, "Micro-VM stopped before the kata-agent answered; retrying cold boot",
			slog.String("id", p.actorUID), slog.Int("attempt", attempt), slog.Any("err", err))
	}
}

// coldBootActor boots the actor's micro-VM from scratch and starts its
// containers, registering the result in s.running. The caller holds s.lock and
// owns the lifecycle logging.
func (s *AteomService) coldBootActor(ctx context.Context, p actorBootParams) (retErr error) {
	actorUID := p.actorUID

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
	rr := s.resolveRuntime(paths)
	egress, err := s.prepareActorEgress(ctx, p.actorUID, p.egressGateway)
	if err != nil {
		return err
	}

	// Networking (host side): per-activation veth into the interior netns. The
	// tap + TC mirror is built below (after the VM exists) so its FDs are fresh.
	if err := ateomnet.SetupActorNetwork(ctx, ateomnet.NetworkConfig{
		InteriorNetNS:      s.interiorNetNS,
		HostVethHWAddr:     hostVethHWAddr,
		SweepInteriorLinks: true,
		EgressRedirectPort: s.egressRedirectPort(p.egressGateway != nil),
	}); err != nil {
		return fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if cleanupErr := s.deactivateActorNetworking(cleanupCtx); cleanupErr != nil {
				slog.WarnContext(cleanupCtx, "Failed to deactivate actor networking after Run failure", slog.Any("err", cleanupErr))
			}
			if cleanupErr := ateomnet.CleanupActorNetwork(cleanupCtx, s.interiorNetNS); cleanupErr != nil {
				slog.WarnContext(cleanupCtx, "Failed to clean up actor network after Run failure", slog.Any("err", cleanupErr))
			}
			// Detach any bundle rootfs overlays mounted by buildActorContainers
			// before the failure, mirroring teardownActor's cleanup.
			if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(actorUID)); err != nil {
				slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays after Run failure", slog.Any("err", err))
			}
		}
	}()

	// Guest sizing + agent kernel params from the kata config.
	memMiB, vcpus, kparams, err := s.guestConfig(rr)
	if err != nil {
		return err
	}

	// Right-size the VM to the actor's declared limits (see internal/sizing),
	// keeping the kata-config values above as the fallback when a limit is unset.
	// vCPUs round up; VM RAM reserves a fixed margin for the VMM + virtiofsd, which
	// share the pod cgroup with the guest RAM. A declared memory limit the reserve
	// leaves too small to boot is rejected (resolveGuestMemMiB) rather than silently
	// falling back to the larger kata default. NB: a FULL-scope snapshot restore
	// reuses the size baked into the snapshot (restoreFullScope), so resizing an
	// existing actor takes effect on its next cold boot.
	sz := p.size
	if v := sz.VCPUs(); v > 0 {
		vcpus = v
	}
	memMiB, err = resolveGuestMemMiB(sz.MemoryBytes, s.memReserveMiB, memMiB)
	if err != nil {
		return err
	}

	// Prepare each container's OCI spec + record its bundle rootfs (the overlay
	// lower the host merges under the container's writable upper).
	ctrs, err := s.buildActorContainers(actorUID, containers)
	if err != nil {
		return err
	}

	// Reject limits the guest can never satisfy, before the containers reach the
	// agent. Restore does not repeat this: it resumes a snapshotted VM, and the
	// template spec is immutable, so an actor's limits were validated when its
	// golden was built.
	if err := checkResourceEnvelope(ctrs, guestEnvelope{
		memMiB:        memMiB,
		vcpus:         vcpus,
		declaredBytes: sz.MemoryBytes,
		reserveMiB:    s.memReserveMiB,
	}); err != nil {
		return err
	}

	// Clean stale per-sandbox state + create the runtime dir for the sockets.
	kata.CleanupSandboxState(ctx, actorUID)
	if err := os.MkdirAll(kata.VMDir(actorUID), 0o700); err != nil {
		return fmt.Errorf("while creating VM dir: %w", err)
	}

	// A cold boot starts from the bare image: give it a pristine host upper dir
	// (atelet's actor-dir reset does not know this directory; see rootfsupper.go).
	if err := resetRootfsUpperDir(actorUID); err != nil {
		return err
	}

	// Assemble each container's merged rootfs on the host (overlay of image lower +
	// host upper, mounted into the shared dir) + durable-dir and CSI volumes (if any),
	// and start the ONE virtiofsd that serves them all. CH connects to it at vm.create
	// and demand-pages for the actor's lifetime, so ateom owns the process (killed in teardownActor).
	vfsdCmd, err := s.stageMergedRootfs(ctx, rr, actorUID, ctrs, containers)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil && vfsdCmd.Process != nil {
			_ = vfsdCmd.Process.Kill()
			_, _ = vfsdCmd.Process.Wait()
		}
	}()

	// Launch a bare VMM (CH + api-socket); ateom owns this process for teardown.
	apiSocket := filepath.Join(kata.VMDir(actorUID), "clh-api.sock")
	chCmd, client, err := ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
		Binary:    rr.chBinary,
		APISocket: apiSocket,
		Stdout:    slogWriter{ctx},
		Stderr:    slogWriter{ctx},
	})
	if err != nil {
		return fmt.Errorf("while launching VMM: %w", err)
	}
	defer func() {
		if retErr != nil && chCmd.Process != nil {
			_ = chCmd.Process.Kill()
			_, _ = chCmd.Process.Wait()
		}
	}()

	// Assemble the CH VmConfig (kata-compatible cmdline, RO kata image on /dev/vda +
	// the virtio-fs device; no actor virtio-blk disks — rootfs writes land in the
	// host-side overlay upper through the shared mount). The console log is also read
	// on a failed agent dial below, so keep it here.
	consoleLog := kata.ConsoleLogPath(actorUID)
	vmCfg := buildVMConfig(actorUID, kernel, image, kparams, consoleLog, memMiB, vcpus,
		agentInit(ctx, client.Info()), s.kataDebug)
	if err := client.CreateVM(ctx, vmCfg); err != nil {
		return fmt.Errorf("while creating VM: %w", err)
	}

	// Network device: build the tap + TC mirror against the actor veth and add a
	// virtio-net to the created (pre-boot) VM with the tap FDs (SCM_RIGHTS).
	tapFiles, err := s.setupRestoreTap(ctx, "tap0_kata", 1)
	if err != nil {
		return fmt.Errorf("while building tap: %w", err)
	}
	defer func() {
		for _, f := range tapFiles {
			_ = f.Close() // CH dups adopted FDs; ours always close.
		}
	}()
	var fds []int
	for _, f := range tapFiles {
		fds = append(fds, int(f.Fd()))
	}
	if err := client.AddNetWithFDs(ctx, actorGuestMAC, 2*len(tapFiles), fds); err != nil {
		return fmt.Errorf("while adding net device: %w", err)
	}

	// Boot.
	if err := client.BootVM(ctx); err != nil {
		return fmt.Errorf("while booting VM: %w", err)
	}
	slog.InfoContext(ctx, "Micro-VM booted", slog.String("id", actorUID), slog.String("api", apiSocket))

	// Dial the kata-agent over hybrid-vsock. The agent only starts listening once
	// the guest's init reaches kata-containers.target — well after CH creates the
	// vsock socket file — so poll the CONNECT until it answers (as the kata shim
	// does), rather than dialing once.
	tBooted := time.Now()
	vsockPath := kata.VsockSocketPath(actorUID)
	if !waitForFile(vsockPath, 15*time.Second) {
		return fmt.Errorf("kata-agent vsock socket %q did not appear", vsockPath)
	}
	tVsock := time.Now()
	ac, err := dialAgentRetry(ctx, vsockPath, 60*time.Second)
	if err != nil {
		logGuestBootDiagnostics(ctx, actorUID, consoleLog)
		return fmt.Errorf("while dialing kata-agent: %w", err)
	}
	tDialed := time.Now()
	// The agent client must stay open past this RPC: the stdout/stderr forwarding
	// goroutines (started below) read over it for the actor's lifetime. It is stored
	// on the runningActor and closed by teardownActor. Close it here only if Run
	// fails after this point (no runningActor recorded).
	defer func() {
		if retErr != nil {
			_ = ac.Close()
		}
	}()

	// Post-boot kata-agent setup: sandbox, guest networking, start each container.
	if err := s.startActorContainers(ctx, ac, actorUID, vsockPath, ctrs); err != nil {
		return err
	}
	tContainers := time.Now()

	// Block until every readyz-enabled container reports 200.
	if err := readyz.WaitAll(ctx, containers, ateomnet.ActorVethIP); err != nil {
		return fmt.Errorf("while waiting for container readyz: %w", err)
	}

	// Everything from BootVM onward, split. ateom used to log only the total, which
	// hid where a cold boot actually goes: it is not the guest booting.
	slog.InfoContext(ctx, "Actor boot phases", slog.String("id", actorUID),
		slog.Duration("vsock_wait", tVsock.Sub(tBooted)),
		slog.Duration("agent_dial", tDialed.Sub(tVsock)),
		slog.Duration("containers", tContainers.Sub(tDialed)),
		slog.Duration("readyz", time.Since(tContainers)),
		slog.Duration("since_boot", time.Since(tBooted)))

	ra := &runningActor{chCmd: chCmd, vfsdCmd: vfsdCmd, apiSocket: apiSocket, baseID: actorUID, guestAgent: ac, workloadIDs: workloadIDs(ctrs)}
	if err := s.activateActorNetworking(p.actorRef.Atespace, p.actorRef.Name, egress); err != nil {
		return err
	}
	s.running[actorUID] = ra

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

// stageMergedRootfs assembles each container's merged rootfs on the host
// (overlay: image lower + the actor's rootfs-upper dirs) at virtiofsd's
// find-paths location (SharedDir(id)/<cid>/rootfs), stages durable-dir volumes,
// CSI volumes, and system-info volumes (if any) under SharedDir(id)/durable,
// SharedDir(id)/csi, and SharedDir(id)/system-info,
// then starts the ONE virtiofsd that serves them all. Must run AFTER CleanupSandboxState (which
// wipes SharedDir) and resetRootfsUpperDir/untarRootfsUpper (which own the
// upper contents). The returned virtiofsd cmd outlives this call (CH
// demand-pages from it); the caller owns it (tracked on runningActor, killed
// in teardownActor).
func (s *AteomService) stageMergedRootfs(ctx context.Context, rr resolvedRuntime, id string, ctrs []actorContainer, containers []*ateompb.Container) (*exec.Cmd, error) {
	upperBase := rootfsUpperDir(id)
	for _, c := range ctrs {
		if err := kata.StageMergedRootfs(ctx, c.bundleRootfs, upperBase, id, c.name); err != nil {
			return nil, fmt.Errorf("while staging merged rootfs for %q: %w", c.name, err)
		}
		for _, vm := range c.imageMounts {
			src := ateompath.ImageVolumeMountPath(id, c.name, vm.GetVolumeName())
			if err := kata.StageImageVolume(ctx, src, id, c.name, vm.GetVolumeName()); err != nil {
				return nil, fmt.Errorf("while staging image volume %q for %q: %w", vm.GetVolumeName(), c.name, err)
			}
		}
	}
	if hasDurableVolumes(containers) {
		if err := s.stageDurableVolumes(ctx, id); err != nil {
			return nil, fmt.Errorf("while staging durable-dir volumes: %w", err)
		}
	}
	if hasCsiVolumes(containers) {
		if err := s.stageCsiVolumes(ctx, id); err != nil {
			return nil, fmt.Errorf("while staging CSI volumes: %w", err)
		}
	}
	if hasSystemInfoVolumes(containers) {
		if err := s.stageSystemInfoVolumes(ctx, id); err != nil {
			return nil, fmt.Errorf("while staging system-info volumes: %w", err)
		}
	}
	vfsdLog, _ := os.OpenFile(virtiofsdLogPath(id), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
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

// errGuestStopped reports that the micro-VM stopped before the kata-agent
// answered. Callers that can start over (a cold boot has no observable side
// effects until the agent runs the containers) retry on it.
var errGuestStopped = errors.New("micro-VM stopped before the kata-agent answered")

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
