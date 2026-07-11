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
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/readyz"
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
	// vfsdCmd is the virtiofsd serving the overlay RO lower (the CH fs device
	// demand-pages from it for the actor's lifetime). ateom owns it; teardownActor
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

	// logAgent is the kata-agent ttrpc client kept open for the lifetime of the
	// stdout/stderr forwarding goroutines (they pump the container's output via
	// ReadStdout/ReadStderr on this connection). It is NOT closed when RunWorkload /
	// RestoreWorkload return — teardownActor closes it, which makes the in-flight
	// ReadStdout/ReadStderr calls fail and the forwarding goroutines exit (io.EOF).
	// nil if forwarding was not started (e.g. a best-effort post-restore dial failed).
	logAgent *kata.AgentClient
}

// baseIDFile is a tiny snapshot file (under the checkpoint/restore dir) holding
// the FROZEN base sandbox id — the id the guest's virtio-fs find-paths are pinned
// to (<baseID>/rootfs). It is the id the RO base was FIRST shared under (the golden
// actor's cold-run id) and is INVARIANT across every restore of that actor's
// lineage: the guest memory keeps referencing <baseID>/rootfs, while the snapshot
// config.json's socket paths get rewritten to the current actor name on each restore.
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

// maxActorContainers is a sanity cap on containers per actor (all share the one
// micro-VM + virtiofsd). 25 is far above any real pod.
const maxActorContainers = 25

// overlayWorkloadID is the kata containerID of a container's overlay WORKLOAD,
// distinct from its carrier container (the carrier keeps the bare container name so
// the agent binds the RO base to /run/kata-containers/<name>/rootfs; the workload
// overlays on top). Stable across the restore lineage (container names don't change).
//
// The "_ovl" separator is deliberately a character that is invalid in a Kubernetes
// container name (DNS-1123 labels are [a-z0-9-]): the carrier id is the bare name, so a
// workload id can never equal a carrier id (a bare name has no "_") nor another workload
// id (names are unique within an actor) — even for containers named "x" and "x-ovl". A
// "-ovl" suffix would let "x"'s workload id collide with the "x-ovl" carrier id.
func overlayWorkloadID(name string) string { return name + "_ovl" }

// actorContainer is one of the actor's containers prepared for the shared micro-VM:
// its name (also the kata containerID + the overlay lower's find-paths subdir), the
// host OCI bundle rootfs that backs the RO lower, and its OCI spec. The writable
// overlay upper is a guest tmpfs (OverlayUpperBase(name)), so there is no host disk.
type actorContainer struct {
	name         string
	bundleRootfs string
	spec         *specs.Spec
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
// ateom boots cloud-hypervisor directly (no kata shim) and gives each container an
// overlay rootfs: its OCI image read-only over virtio-fs (the lower) plus a guest
// tmpfs (the writable upper). It drives the kata clh boot (vm.create kernel+image+fs,
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

	atespace := req.GetAtespace()
	name := req.GetActorName()
	templateNS := req.GetActorTemplateNamespace()
	templateName := req.GetActorTemplateName()

	s.actorLogger.EmitLifecycleLog("Actor starting", atespace, name, templateNS, templateName)

	// All of the actor's containers share the one micro-VM (which is the pod
	// sandbox): each gets its own overlay rootfs and its own kata-agent
	// CreateContainer/StartContainer, driven below after the shared boot +
	// CreateSandbox + guest networking.
	containers := req.GetSpec().GetContainers()
	if len(containers) == 0 {
		return nil, status.Error(codes.InvalidArgument, "actor spec has no containers")
	}
	if len(containers) > maxActorContainers {
		return nil, status.Errorf(codes.Unimplemented, "ateom-microvm supports at most %d containers, got %d", maxActorContainers, len(containers))
	}

	// ateom builds the CH vm.create itself, so it needs the guest kernel + image
	// paths directly.
	paths := req.GetRuntimeAssetPaths()
	kernel, image := paths[assetKernel], paths[assetImage]
	if kernel == "" || image == "" {
		return nil, fmt.Errorf("ateom-microvm requires %q and %q asset paths", assetKernel, assetImage)
	}
	rr := s.resolveRuntime(paths)

	// Networking (host side): per-activation veth into the interior netns. The
	// tap + TC mirror is built below (after the VM exists) so its FDs are fresh.
	if err := s.setupActorNetwork(ctx); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			if cleanupErr := s.cleanupActorNetwork(ctx); cleanupErr != nil {
				slog.WarnContext(ctx, "Failed to clean up actor network after Run failure", slog.Any("err", cleanupErr))
			}
		}
	}()

	// Prepare each container's OCI spec + record its bundle rootfs (the overlay RO
	// lower). No host disk — the rootfs is overlay(virtio-fs lower + guest-tmpfs upper).
	ctrs, err := s.buildActorContainers(atespace, name, containers)
	if err != nil {
		return nil, err
	}

	// Guest sizing + agent kernel params from the kata config.
	memMiB, vcpus, kparams, err := s.guestConfig(rr)
	if err != nil {
		return nil, err
	}

	// Clean stale per-sandbox state + create the runtime dir for the sockets.
	kata.CleanupSandboxState(ctx, name)
	if err := os.MkdirAll(kata.VMDir(name), 0o700); err != nil {
		return nil, fmt.Errorf("while creating VM dir: %w", err)
	}

	// Stage the overlay RO lowers (bind each image into the shared dir) + start the
	// virtiofsd that serves them. CH connects to it at vm.create and demand-pages for
	// the actor's lifetime, so ateom owns the process (killed in teardownActor).
	vfsdCmd, err := s.stageOverlayLowers(ctx, rr, name, ctrs)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil && vfsdCmd.Process != nil {
			_ = vfsdCmd.Process.Kill()
			_, _ = vfsdCmd.Process.Wait()
		}
	}()

	// Launch a bare VMM (CH + api-socket); ateom owns this process for teardown.
	apiSocket := filepath.Join(kata.VMDir(name), "clh-api.sock")
	chCmd, client, err := ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
		Binary:    rr.chBinary,
		APISocket: apiSocket,
		Stdout:    slogWriter{ctx},
		Stderr:    slogWriter{ctx},
	})
	if err != nil {
		return nil, fmt.Errorf("while launching VMM: %w", err)
	}
	defer func() {
		if retErr != nil && chCmd.Process != nil {
			_ = chCmd.Process.Kill()
			_, _ = chCmd.Process.Wait()
		}
	}()

	// Assemble the CH VmConfig (kata-compatible cmdline, RO kata image on /dev/vda +
	// the virtio-fs device for the overlay RO lower; no actor virtio-blk disks — the
	// writable upper is a guest tmpfs). serialLog is also read on a failed agent dial
	// below, so keep it here.
	serialLog := filepath.Join(kata.VMDir(name), "serial.log")
	vmCfg := buildVMConfig(name, kernel, image, kparams, serialLog, memMiB, vcpus)
	if err := client.CreateVM(ctx, vmCfg); err != nil {
		return nil, fmt.Errorf("while creating VM: %w", err)
	}

	// Network device: build the tap + TC mirror against the actor veth and add a
	// virtio-net to the created (pre-boot) VM with the tap FDs (SCM_RIGHTS).
	tapFiles, err := s.setupRestoreTap(ctx, "tap0_kata", 1)
	if err != nil {
		return nil, fmt.Errorf("while building tap: %w", err)
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
		return nil, fmt.Errorf("while adding net device: %w", err)
	}

	// Boot.
	if err := client.BootVM(ctx); err != nil {
		return nil, fmt.Errorf("while booting VM: %w", err)
	}
	slog.InfoContext(ctx, "Micro-VM booted", slog.String("id", name), slog.String("api", apiSocket))

	// Dial the kata-agent over hybrid-vsock. The agent only starts listening once
	// the guest's init reaches kata-containers.target — well after CH creates the
	// vsock socket file — so poll the CONNECT until it answers (as the kata shim
	// does), rather than dialing once.
	vsockPath := kata.VsockSocketPath(name)
	if !waitForFile(vsockPath, 15*time.Second) {
		return nil, fmt.Errorf("kata-agent vsock socket %q did not appear", vsockPath)
	}
	ac, err := dialAgentRetry(ctx, vsockPath, 60*time.Second)
	if err != nil {
		if b, rerr := os.ReadFile(serialLog); rerr == nil {
			slog.ErrorContext(ctx, "agent dial failed; guest serial tail", slog.String("serial", tailString(string(b), 3000)))
		}
		return nil, fmt.Errorf("while dialing kata-agent: %w", err)
	}
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
	if err := s.startActorContainers(ctx, ac, name, vsockPath, ctrs); err != nil {
		return nil, err
	}

	// Block until every readyz-enabled container reports 200.
	if err := readyz.WaitAll(ctx, containers, actorVethIP); err != nil {
		return nil, fmt.Errorf("while waiting for container readyz: %w", err)
	}

	ra := &runningActor{chCmd: chCmd, vfsdCmd: vfsdCmd, apiSocket: apiSocket, baseID: name, logAgent: ac}
	s.running[name] = ra

	// Forward each container's stdout/stderr into the pod logs. The overlay workload's
	// container/exec id is <name>_ovl (see startOverlayContainer), so key the streams by
	// that and tag with the display container name. The goroutines read over ac for the
	// actor's lifetime and exit (io.EOF) when teardownActor closes ac.
	for _, c := range ctrs {
		s.startActorLogForwarding(ac, atespace, name, templateNS, templateName, overlayWorkloadID(c.name), c.name)
	}

	s.actorLogger.EmitLifecycleLog("Actor started", atespace, name, templateNS, templateName)
	slog.InfoContext(ctx, "Actor started (overlay rootfs)", slog.String("id", name))
	return &ateompb.RunWorkloadResponse{}, nil
}

// buildActorContainers prepares each of the actor's containers for the shared
// micro-VM: it loads the OCI spec from the per-container bundle, injects guest DNS,
// and records the bundle rootfs that backs the overlay's RO lower. No host disk is
// built — the rootfs is overlay(virtio-fs RO lower + guest-tmpfs upper); the lowers
// are bound into virtiofsd's shared dir in stageOverlayLowers after the sandbox state
// is clean. Both RunWorkload and RestoreWorkload go through here.
func (s *AteomService) buildActorContainers(atespace, id string, containers []*ateompb.Container) ([]actorContainer, error) {
	netnsPath := ateompath.AteomNetNSPath(s.podUID)
	ctrs := make([]actorContainer, len(containers))
	for i, c := range containers {
		cn := c.GetName()
		bundle := ateompath.OCIBundlePath(atespace, id, cn)
		spec, err := ensureKataCompatibleSpec(bundle, id, netnsPath)
		if err != nil {
			return nil, fmt.Errorf("while preparing kata OCI spec for %q: %w", cn, err)
		}
		bundleRootfs := filepath.Join(bundle, "rootfs")
		// Write cluster DNS into the lower before it's served over virtio-fs: ateom
		// drops atelet's resolv.conf bind and sends no CreateSandbox.Dns, so without
		// this the guest can reach IPs but not resolve names. Doing it here covers both
		// run and restore (both reconstruct the lower from the bundle).
		if err := writeGuestResolvConf(bundleRootfs); err != nil {
			return nil, fmt.Errorf("while writing guest resolv.conf for %q: %w", cn, err)
		}
		ctrs[i] = actorContainer{name: cn, bundleRootfs: bundleRootfs, spec: spec}
	}
	return ctrs, nil
}

// stageOverlayLowers makes each container's RO lower available to virtiofsd by
// bind-mounting its OCI image rootfs into virtiofsd's find-paths location
// (SharedDir(id)/<cid>/rootfs), then starts the one virtiofsd that serves them all.
// Must run AFTER CleanupSandboxState (which wipes SharedDir) and the VM dir exists.
// The returned virtiofsd cmd outlives this call (CH demand-pages from it); the caller
// owns it (tracked on runningActor, killed in teardownActor).
func (s *AteomService) stageOverlayLowers(ctx context.Context, rr resolvedRuntime, id string, ctrs []actorContainer) (*exec.Cmd, error) {
	for _, c := range ctrs {
		if err := kata.ReconstructSharedDirFromImage(ctx, c.bundleRootfs, id, c.name); err != nil {
			return nil, fmt.Errorf("while staging overlay lower for %q: %w", c.name, err)
		}
	}
	vfsdLog, _ := os.OpenFile(filepath.Join(kata.VMDir(id), "virtiofsd.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
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

// buildVMConfig assembles the cloud-hypervisor VmConfig. The kernel cmdline replicates
// kata's clh boot cmdline; beyond the base params it must set
// systemd.unit=kata-containers.target (else the guest powers off ~6s in) and mask
// systemd-networkd (the agent owns eth0). The console is arch-specific: ttyAMA0 on
// arm64, ttyS0 on amd64. /dev/vda is the RO guest image; the actor rootfs's RO lower is
// the virtio-fs device on PCI segment 1 (hence num_pci_segments=2), with no actor disks.
func buildVMConfig(id, kernel, image, kparams, serialLog string, memMiB, vcpus int) ch.VmConfig {
	console := "ttyS0"
	if runtime.GOARCH == "arm64" {
		console = "ttyAMA0"
	}
	cmdline := "root=/dev/vda1 rootflags=data=ordered,errors=remount-ro ro rootfstype=ext4 " +
		"panic=1 no_timer_check noreplace-smp console=" + console + ",115200n8 " +
		"systemd.unit=kata-containers.target systemd.mask=systemd-networkd.service systemd.mask=systemd-networkd.socket"
	if kparams != "" {
		cmdline += " " + kparams
	}
	return ch.VmConfig{
		Cpus:    ch.CpusConfig{BootVcpus: int32(vcpus), MaxVcpus: int32(vcpus)},
		Memory:  ch.MemoryConfig{Size: int64(memMiB) * 1024 * 1024, Shared: true},
		Payload: ch.PayloadConfig{Kernel: kernel, Cmdline: cmdline},
		Disks: []ch.DiskConfig{
			{Path: image, Readonly: true, ImageType: "Raw", NumQueues: int32(vcpus), QueueSize: 1024},
		},
		Fs: []ch.FsConfig{{
			Tag: kata.FsTag, Socket: kata.VirtiofsdSocketPath(id),
			NumQueues: 1, QueueSize: 1024, PciSegment: 1,
		}},
		Platform: &ch.PlatformConfig{NumPciSegments: 2},
		Rng:      &ch.RngConfig{Src: "/dev/urandom"},
		Serial:   &ch.ConsoleConfig{Mode: "File", File: serialLog},
		Vsock:    &ch.VsockConfig{Cid: 3, Socket: kata.VsockSocketPath(id)},
	}
}

// startActorContainers performs the post-boot kata-agent setup the shim normally
// does at boot: establish the sandbox once (mounting the kataShared virtio-fs base),
// configure guest networking (eth0 IP/MAC/MTU + routes) once, then start each
// container on its own overlay rootfs. On failure it dumps guest diagnostics.
func (s *AteomService) startActorContainers(ctx context.Context, ac *kata.AgentClient, id, vsockPath string, ctrs []actorContainer) error {
	// Establish the agent sandbox + the kataShared virtio-fs mount (the RO base for
	// every container's overlay lower). All containers share it, so use the first
	// container's hostname.
	sbCtx, sbCancel := context.WithTimeout(ctx, 20*time.Second)
	err := ac.CreateSandboxForActor(sbCtx, id, ctrs[0].spec.Hostname)
	sbCancel()
	if err != nil {
		return fmt.Errorf("while creating agent sandbox: %w", err)
	}

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

	for _, c := range ctrs {
		if err := startOverlayContainer(ctx, ac, vsockPath, c); err != nil {
			return err
		}
	}
	return nil
}

// startOverlayContainer brings up one container's rootfs as overlay(virtio-fs RO
// lower + guest-tmpfs upper): a carrier container (id == name) eager-binds the RO base
// to /run/kata-containers/<name>/rootfs, then the workload (id == <name>_ovl) overlays
// it with a tmpfs upper. On failure it dumps the guest overlay state.
func startOverlayContainer(ctx context.Context, ac *kata.AgentClient, vsockPath string, c actorContainer) error {
	carrierCtx, carrierCancel := context.WithTimeout(ctx, 30*time.Second)
	err := ac.CreateCarrier(carrierCtx, c.name, c.spec)
	carrierCancel()
	if err != nil {
		dump := kata.DebugConsoleDump(ctx, vsockPath, "echo '== shared/containers =='; ls -la /run/kata-containers/shared/containers/ 2>&1 | head -40")
		slog.ErrorContext(ctx, "carrier create failed; dump", slog.String("container", c.name), slog.String("dump", dump))
		return fmt.Errorf("while creating carrier %q: %w", c.name, err)
	}

	upperBase := kata.OverlayUpperBase(c.name)
	wlCtx, wlCancel := context.WithTimeout(ctx, 30*time.Second)
	err = ac.StartOverlayWorkload(wlCtx, c.name, overlayWorkloadID(c.name), upperBase, c.spec)
	wlCancel()
	if err != nil {
		dump := kata.DebugConsoleDump(ctx, vsockPath,
			"echo '== upper =='; ls -la "+upperBase+" 2>&1; echo '== lower =='; ls /run/kata-containers/"+c.name+"/rootfs/ 2>&1 | head; "+
				"echo '== mounts =='; grep -E 'kata|overlay' /proc/mounts 2>&1")
		slog.ErrorContext(ctx, "overlay workload failed; dump", slog.String("container", c.name), slog.String("dump", dump))
		return fmt.Errorf("while starting overlay workload %q: %w", c.name, err)
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
// (ate.dev/container_name) so a multi-container actor demultiplexes.
// The reader contexts are context.Background() — the goroutines are NOT bound to the
// RPC that started them; they terminate when ac is closed (by teardownActor), which
// makes the in-flight ReadStdout/ReadStderr fail and the StreamReader return io.EOF,
// ending WrapContainerLogs. This keeps the agent connection (which ttrpc allows
// concurrent Calls on) alive for forwarding while guaranteeing no goroutine outlives
// the connection.
func (s *AteomService) startActorLogForwarding(ac *kata.AgentClient, atespace, actorName, actorTemplateNamespace, actorTemplateName, streamID, containerName string) {
	go s.actorLogger.WrapContainerLogs(kata.NewStdioReader(context.Background(), ac, streamID, streamID, false), atespace, actorName, actorTemplateNamespace, actorTemplateName, containerName)
	go s.actorLogger.WrapContainerLogs(kata.NewStdioReader(context.Background(), ac, streamID, streamID, true), atespace, actorName, actorTemplateNamespace, actorTemplateName, containerName)
}

// dialAgentRetry polls DialAgent until the kata-agent answers the hybrid-vsock
// CONNECT (the socket file exists at boot, but the agent only listens once the
// guest reaches kata-containers.target) or the overall timeout elapses. Each
// attempt is capped at 5s (usually it fails fast with connection-refused while
// the agent isn't listening yet; the cap only bounds a rare hung dial), then
// waits 500ms before retrying — so steady-state polling is ~every 500ms, not 5s.
func dialAgentRetry(ctx context.Context, vsockPath string, timeout time.Duration) (*kata.AgentClient, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		ac, err := kata.DialAgent(dctx, vsockPath)
		cancel()
		if err == nil {
			return ac, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, lastErr
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

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
		Device: actorVethName,
		Name:   actorVethName,
		HwAddr: actorGuestMAC,
		Mtu:    mtu,
		IPAddresses: []*agentpb.IPAddress{
			{Family: agentpb.IPFamily_v4, Address: actorVethIP, Mask: "30"},
		},
	}); err != nil {
		return err
	}
	if err := ac.UpdateRoutes(ctx, []*agentpb.Route{
		{Dest: actorVethSubnet, Device: actorVethName, Scope: uint32(unix.RT_SCOPE_LINK), Family: agentpb.IPFamily_v4},
		{Dest: "", Gateway: actorVethGateway, Device: actorVethName, Family: agentpb.IPFamily_v4},
	}); err != nil {
		return err
	}
	return ac.AddARPNeighbors(ctx, []*agentpb.ARPNeighbor{{
		ToIPAddress: &agentpb.IPAddress{Family: agentpb.IPFamily_v4, Address: actorVethGateway},
		Device:      actorVethName,
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
