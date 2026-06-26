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
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// runningActor holds the live state for one actor's micro-VM. ateom owns the
// cloud-hypervisor process directly (booted by RunWorkload or relaunched by
// RestoreWorkload), so it tracks that process and its api-socket for teardown.
type runningActor struct {
	containerName string

	// baseID is the FROZEN base sandbox id propagated across this actor's restore
	// lineage. For a cold-run actor this is the actor's own id; for a restored
	// actor it is the id read from the snapshot's base-id file (the golden id,
	// propagated). CheckpointWorkload writes it back into the next snapshot's
	// base-id file so the chain survives suspend->resume->suspend.
	baseID string

	// ateom owns this CH process (booted at Run or relaunched at Restore).
	chCmd *exec.Cmd
	// apiSocket is the CH api-socket for this ateom-owned VMM.
	apiSocket string

	// restoreSourceDir is the snapshot dir this actor was OnDemand-restored from
	// (the base CH is demand-paging from). Set only on the owned-boot virtio-blk
	// path when restored via OnDemand. CheckpointWorkload overlays CH's new (sparse,
	// faulted-only) snapshot onto this base to produce a COMPLETE snapshot (CH's
	// OnDemand snapshot alone drops the un-faulted pages). Empty for cold-run actors
	// (their snapshot is already complete).
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
// config.json's socket paths get rewritten to the current actor id on each restore.
// RestoreWorkload reads this to lay the reconstructed-from-image base at the path
// the guest expects. (The config.json socket id is the WRONG source — it equals the
// current id, not the frozen golden id, for any restored-then-checkpointed actor.)
const baseIDFile = "base-id"

// Asset names in RunWorkloadRequest.runtime_asset_paths (set by atelet's
// fetchRuntimeAssets, keyed by the ActorTemplate runtime asset names).
const (
	assetCH     = "cloud-hypervisor"
	assetKernel = "kata-kernel"
	assetImage  = "kata-image"
	assetConfig = "kata-config"
)

// actorRootfsDiskName is the actor's writable rootfs disk file under the actor
// dir; it is the /dev/vdb backing path recorded in the snapshot config.json and
// reopened verbatim on restore.
const actorRootfsDiskName = "actor-rootfs.ext4"

// goldenRootfsDiskName is the verbatim copy of the actor's /dev/vdb disk AS-OF the
// golden snapshot, kept under the actor dir. reset-to-golden recreates /dev/vdb
// from it on restore (byte-identical to what the snapshot's guest RAM/ext4 cache
// expects), discarding the actor's later rootfs writes — gVisor semantics.
const goldenRootfsDiskName = "golden-rootfs.ext4"

// fileMissing reports whether path does not exist.
func fileMissing(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}

// copyDiskFile copies a (sparse) disk image verbatim, preserving holes so the
// (mostly-empty) ext4 image doesn't materialize its scratch blocks. Used to
// save/restore the golden rootfs disk template.
func copyDiskFile(ctx context.Context, src, dst string) error {
	tmp := dst + ".tmp"
	_ = os.Remove(tmp)
	if out, err := exec.CommandContext(ctx, "cp", "--sparse=always", src, tmp).CombinedOutput(); err != nil {
		return fmt.Errorf("cp %s -> %s: %w: %s", src, tmp, err, out)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmp, dst, err)
	}
	return nil
}

// resolvedRuntime holds the concrete binary/config paths for a request, taken
// from fetched runtime assets when present, else the process flags.
type resolvedRuntime struct {
	chBinary   string // path to the cloud-hypervisor binary
	configFile string // path to the kata configuration.toml
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
	}
}

// writeGuestResolvConf copies the worker pod's /etc/resolv.conf into the bundle
// rootfs (before it's packed into the ext4 disk) so the guest gets cluster DNS:
// ateom drops atelet's resolv.conf bind and sends no CreateSandbox.Dns, so the
// guest can otherwise reach IPs but not resolve names.
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

// RunWorkload boots the actor as a cloud-hypervisor micro-VM that ateom owns.
//
// ateom boots cloud-hypervisor itself — no kata shim — and gives the actor a
// writable boot-time virtio-blk disk (/dev/vdb, built from the OCI bundle rootfs)
// as its container rootfs. Rootfs data lives on that host-backed disk rather than
// a guest tmpfs overlay-upper, so the CH snapshot is memory-only with no balloon
// needed to reclaim a RAM-backed upper. It replicates the kata clh boot (vm.create
// kernel+image, add-net, vm.boot) and the shim's post-boot work (agent
// CreateSandbox + guest network config) before driving the kata-agent to start the
// blk-rootfs container.
//
// Contract with atelet (mirrors ateom-gvisor):
//   - The runtime assets (guest kernel, guest OS image, cloud-hypervisor, base
//     kata config) are on disk and passed as runtime asset paths.
//   - The OCI bundle (config.json + populated rootfs/) is prepared per container.
func (s *AteomService) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (resp *ateompb.RunWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ns := req.GetActorTemplateNamespace()
	name := req.GetActorTemplateName()
	id := req.GetActorId()

	s.actorLogger.EmitLifecycleLog("Actor starting", id, name, ns)

	// KNOWN GAP vs the gVisor runtime: it runs multiple containers per actor; this
	// runtime is single-container for now. Multi-container is a mechanical extension
	// (one boot-time virtio-blk rootfs disk + agent CreateContainer per container,
	// sharing the one guest/sandbox) and is tracked as follow-up work.
	containers := req.GetSpec().GetContainers()
	if len(containers) != 1 {
		return nil, status.Errorf(codes.Unimplemented, "ateom-microvm supports exactly one container, got %d", len(containers))
	}
	containerName := containers[0].GetName()

	// Owned-boot builds the CH vm.create itself, so it needs the guest kernel +
	// image paths directly.
	paths := req.GetRuntimeAssetPaths()
	kernel, image := paths[assetKernel], paths[assetImage]
	if kernel == "" || image == "" {
		return nil, fmt.Errorf("owned-boot requires %q and %q asset paths", assetKernel, assetImage)
	}
	actorDir := ateompath.ActorPath(ns, name, id)
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

	bundle := ateompath.OCIBundlePath(ns, name, id, containerName)
	spec, err := ensureKataCompatibleSpec(bundle, id, ateompath.AteomNetNSPath(s.podUID))
	if err != nil {
		return nil, fmt.Errorf("while preparing kata OCI spec: %w", err)
	}

	// Build the actor's writable rootfs as a raw ext4 virtio-blk disk from the
	// atelet-populated OCI bundle rootfs. This becomes /dev/vdb.
	diskPath := filepath.Join(actorDir, actorRootfsDiskName)
	if err := writeGuestResolvConf(filepath.Join(bundle, "rootfs")); err != nil {
		return nil, fmt.Errorf("while writing guest resolv.conf: %w", err)
	}
	if err := kata.BuildExt4Image(ctx, filepath.Join(bundle, "rootfs"), diskPath); err != nil {
		return nil, fmt.Errorf("while building actor rootfs disk: %w", err)
	}

	// Guest sizing + agent kernel params from the kata config.
	memMiB, vcpus, kparams, err := s.guestConfig(rr)
	if err != nil {
		return nil, err
	}

	// Clean stale per-sandbox state + create the runtime dir for the sockets.
	kata.CleanupSandboxState(ctx, id)
	if err := os.MkdirAll(kata.VMDir(id), 0o700); err != nil {
		return nil, fmt.Errorf("while creating VM dir: %w", err)
	}

	// Launch a bare VMM (CH + api-socket); ateom owns this process for teardown.
	apiSocket := filepath.Join(kata.VMDir(id), "clh-api.sock")
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

	// Assemble the CH VmConfig (kata-compatible cmdline, RO image on /dev/vda +
	// writable rootfs on /dev/vdb). serialLog is also read on a failed agent dial
	// below, so keep it here.
	serialLog := filepath.Join(kata.VMDir(id), "serial.log")
	vmCfg := buildVMConfig(id, kernel, image, diskPath, kparams, serialLog, memMiB, vcpus)
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
	slog.InfoContext(ctx, "Micro-VM booted (owned-boot)", slog.String("id", id), slog.String("api", apiSocket))

	// Dial the kata-agent over hybrid-vsock. The agent only starts listening once
	// the guest's init reaches kata-containers.target — well after CH creates the
	// vsock socket file — so poll the CONNECT until it answers (as the kata shim
	// does), rather than dialing once.
	vsockPath := kata.VsockSocketPath(id)
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

	// Post-boot kata-agent setup: sandbox, guest networking, start the container.
	if err := s.startActorContainer(ctx, ac, id, vsockPath, spec); err != nil {
		return nil, err
	}

	ra := &runningActor{chCmd: chCmd, apiSocket: apiSocket, containerName: containerName, baseID: id, logAgent: ac}
	s.running[id] = ra

	// Forward the actor container's stdout/stderr into the pod logs (parity with
	// ateom-gvisor). StartBlkWorkload uses containerID==execID==id, so the agent
	// keys the streams by id. The goroutines read over ac for the actor's lifetime
	// and exit (io.EOF) when teardownActor closes ac.
	s.startActorLogForwarding(ac, id, name, ns, containerName)

	s.actorLogger.EmitLifecycleLog("Actor started", id, name, ns)
	slog.InfoContext(ctx, "Actor started (owned-boot, virtio-blk rootfs)", slog.String("id", id))
	return &ateompb.RunWorkloadResponse{}, nil
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

// buildVMConfig assembles the cloud-hypervisor VmConfig for the owned boot. The
// kernel cmdline replicates kata's clh boot cmdline (verified against a live kata
// snapshot's payload.cmdline): beyond the root/clh base params it MUST include
// systemd.unit=kata-containers.target (else systemd boots the default target and
// powers off — the guest exits ~6s in) and mask systemd-networkd (the agent owns
// eth0). The console is ARCH-SPECIFIC: ttyAMA0 (PL011) on arm64, ttyS0 (8250) on
// amd64 — the wrong one => "unable to open an initial console". The config's
// kernel_params are appended; serial is captured to serialLog for boot debugging.
// The RO guest image is /dev/vda, the writable rootfs /dev/vdb.
func buildVMConfig(id, kernel, image, diskPath, kparams, serialLog string, memMiB, vcpus int) ch.VmConfig {
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
			{Path: diskPath, Readonly: false, ImageType: "Raw", NumQueues: int32(vcpus), QueueSize: 1024},
		},
		Rng:    &ch.RngConfig{Src: "/dev/urandom"},
		Serial: &ch.ConsoleConfig{Mode: "File", File: serialLog},
		Vsock:  &ch.VsockConfig{Cid: 3, Socket: kata.VsockSocketPath(id)},
	}
}

// startActorContainer performs the post-boot kata-agent setup the shim normally
// does at boot: establish the sandbox, configure guest networking (eth0
// IP/MAC/MTU + routes), and start the actor container on its /dev/vdb rootfs. On
// failure it dumps guest diagnostics over the debug console.
func (s *AteomService) startActorContainer(ctx context.Context, ac *kata.AgentClient, id, vsockPath string, spec *specs.Spec) error {
	// Establish the agent sandbox (the shim normally does this at boot).
	sbCtx, sbCancel := context.WithTimeout(ctx, 20*time.Second)
	err := ac.CreateSandbox(sbCtx, &agentpb.CreateSandboxRequest{Hostname: spec.Hostname, SandboxId: id})
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

	// Start the actor with its rootfs on /dev/vdb (single blk storage).
	wlCtx, wlCancel := context.WithTimeout(ctx, 30*time.Second)
	err = ac.StartBlkWorkload(wlCtx, id, "/dev/vdb", spec)
	wlCancel()
	if err != nil {
		dump := kata.DebugConsoleDump(ctx, vsockPath,
			"echo '== /dev/vdb =='; ls -l /dev/vdb 2>&1; blkid /dev/vdb 2>&1; "+
				"echo '== mounts =='; grep kata /proc/mounts 2>&1")
		slog.ErrorContext(ctx, "blk workload failed; dump", slog.String("dump", dump))
		return fmt.Errorf("while starting blk workload: %w", err)
	}
	return nil
}

// startActorLogForwarding spawns two goroutines that pump the actor container's
// stdout and stderr (read over the kata-agent ttrpc client ac via repeated
// ReadStdout/ReadStderr) through the shared actorlog forwarder, which annotates
// each line with the actor's ate.dev/* labels and writes it to the pod's stdout.
//
// The streams are keyed by containerID==execID==id (the value StartBlkWorkload
// passed); lines are tagged with the container name (ate.dev/container_name). The
// reader contexts are context.Background() — the goroutines are NOT bound to the RPC
// that started them; they terminate when ac is closed (by teardownActor), which
// makes the in-flight ReadStdout/ReadStderr fail and the StreamReader return
// io.EOF, ending WrapContainerLogs. This keeps the agent connection (which ttrpc
// allows concurrent Calls on) alive for forwarding while guaranteeing no goroutine
// outlives the connection.
func (s *AteomService) startActorLogForwarding(ac *kata.AgentClient, id, name, ns, containerName string) {
	go s.actorLogger.WrapContainerLogs(kata.NewStdioReader(context.Background(), ac, id, id, false), id, name, ns, containerName)
	go s.actorLogger.WrapContainerLogs(kata.NewStdioReader(context.Background(), ac, id, id, true), id, name, ns, containerName)
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
