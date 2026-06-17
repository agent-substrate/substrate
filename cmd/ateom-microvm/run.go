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
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	ctrtypes "github.com/containerd/containerd/api/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// runningActor holds the live state for one actor's micro-VM.
//
// Two ownership modes:
//   - kata-shim-owned (from RunWorkload): shim != nil; kata owns the CH process.
//   - ateom-owned (from RestoreWorkload): shim == nil; ateom relaunched CH +
//     virtiofsd directly and owns those processes.
type runningActor struct {
	containerName string

	// kata-shim-owned
	shim *kata.Shim

	// ateom-owned (post-restore)
	chCmd   *exec.Cmd
	vfsdCmd *exec.Cmd
	// apiSocket is the CH api-socket for an ateom-owned (restored) VMM; empty
	// for kata-shim-owned actors (use kata.CLHSocketPath then).
	apiSocket string
}

// sharedDirTar is the file name (under the checkpoint/restore dir) holding the
// captured virtio-fs shared-dir contents shipped alongside the CH snapshot.
const sharedDirTar = "shared-dir.tar"

// Asset names in RunWorkloadRequest.runtime_asset_paths (set by atelet's
// fetchRuntimeAssets, keyed by the ActorTemplate runtime asset names).
const (
	assetShim      = "kata-shim"
	assetCH        = "cloud-hypervisor"
	assetVirtiofsd = "virtiofsd"
	assetKernel    = "kata-kernel"
	assetImage     = "kata-image"
	assetConfig    = "kata-config"
)

// resolvedRuntime holds the concrete binary/config paths for a request, taken
// from fetched runtime assets when present, else the process flags.
type resolvedRuntime struct {
	shim       string
	ch         string
	virtiofsd  string
	configFile string
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// resolveRuntime determines the kata shim / cloud-hypervisor / virtiofsd binaries
// and the kata config for a request. atelet fetches these content-addressed and
// passes their local paths; when a base config + kernel + image are present we
// render a configuration.toml pointing at the fetched paths (fetch-not-bake).
// Falls back to the process flags for anything not supplied.
func (s *AteomService) resolveRuntime(actorDir string, paths map[string]string) (resolvedRuntime, error) {
	r := resolvedRuntime{
		shim:       firstNonEmpty(paths[assetShim], s.shimBinary),
		ch:         firstNonEmpty(paths[assetCH], s.chBinary),
		virtiofsd:  firstNonEmpty(paths[assetVirtiofsd], s.virtiofsdBinary),
		configFile: s.kataConfig,
	}
	base, kernel, image := paths[assetConfig], paths[assetKernel], paths[assetImage]
	if base != "" && kernel != "" && image != "" {
		baseBytes, err := os.ReadFile(base)
		if err != nil {
			return r, fmt.Errorf("reading base kata config %q: %w", base, err)
		}
		// TODO: make guest VM memory size a first-class ActorTemplate field and
		// rewrite default_memory here (like the asset paths) instead of requiring
		// per-size configuration.toml asset variants. Suspend/resume latency
		// scales directly with guest memory (CH's snapshot scans/writes the whole
		// memfd): on GKE pd-balanced disks, a 512MiB guest measured ~3.4s suspend
		// / ~1.6s resume vs ~18s / ~4.4s at kata's stock default_memory=2048.
		rendered, err := kata.RenderConfig(baseBytes, kata.ConfigAssets{
			Kernel: kernel, Image: image, Hypervisor: r.ch, Virtiofsd: r.virtiofsd,
		})
		if err != nil {
			return r, fmt.Errorf("rendering kata config: %w", err)
		}
		if s.kataDebug {
			// Verbose kata: hypervisor/agent/runtime debug on, guest console (with
			// the kata-agent's logs) forwarded into the shim log -> pod logs.
			rendered = kata.EnableDebug(rendered)
		}
		cfgPath := filepath.Join(actorDir, "configuration.toml")
		if err := os.MkdirAll(actorDir, 0o700); err != nil {
			return r, fmt.Errorf("creating actor dir: %w", err)
		}
		if err := os.WriteFile(cfgPath, rendered, 0o600); err != nil {
			return r, fmt.Errorf("writing rendered kata config: %w", err)
		}
		r.configFile = cfgPath
	}
	return r, nil
}

// RunWorkload boots the actor as a kata + cloud-hypervisor micro-VM.
//
// Contract with atelet (mirrors ateom-gvisor):
//   - The runtime assets (kata shim, guest kernel, rootfs, cloud-hypervisor,
//     virtiofsd) are on disk and configured.
//   - The OCI bundle (config.json + populated rootfs/) is prepared per container.
//
// ateom drives the kata shim v2 ttrpc Task API directly (no containerd daemon):
// Bootstrap (start action) -> Create (boots the CH VM) -> Start. The CH api
// socket then lives at kata.CLHSocketPath(id), which CheckpointWorkload drives.
//
// Proven end-to-end by TestKataLifecycle: boot + run + pause + resume of a
// busybox container in a CH micro-VM, no containerd.
func (s *AteomService) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (resp *ateompb.RunWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ns := req.GetActorTemplateNamespace()
	name := req.GetActorTemplateName()
	id := req.GetActorId()

	containers := req.GetSpec().GetContainers()
	if len(containers) != 1 {
		// POC: one container per sandbox. Multi-container pods are future work.
		return nil, status.Errorf(codes.Unimplemented, "ateom-microvm supports exactly one container, got %d", len(containers))
	}
	containerName := containers[0].GetName()

	// Networking (mirrors ateom-gvisor's veth model): build the per-activation
	// veth into the interior netns and point kata at it; kata wires the guest
	// to the stable actor address via its tap + TC mirror.
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
	if err := ensureKataCompatibleSpec(bundle, id, ateompath.AteomNetNSPath(s.podUID)); err != nil {
		return nil, fmt.Errorf("while preparing kata OCI spec: %w", err)
	}

	actorDir := ateompath.ActorPath(ns, name, id)
	rr, err := s.resolveRuntime(actorDir, req.GetRuntimeAssetPaths())
	if err != nil {
		return nil, fmt.Errorf("while resolving runtime assets: %w", err)
	}
	shim := &kata.Shim{
		Binary:    rr.shim,
		ID:        id,
		Bundle:    bundle,
		Namespace: s.namespace,
		// No containerd: these only need to be stable, unique paths. The shim
		// derives its ttrpc socket from (GRPCAddress, Namespace, ID) and would
		// publish events to TTRPCAddress (best-effort, logged if unreachable).
		GRPCAddress:  filepath.Join(actorDir, "shim-containerd.sock"),
		TTRPCAddress: filepath.Join(actorDir, "shim-containerd.sock.ttrpc"),
		ConfigFile:   rr.configFile,
		Diagnostics:  slogWriter{ctx},
		Debug:        s.kataDebug,
	}

	// Clear any leftover kata host-side state for this (deterministic) sandbox id
	// from a prior failed/torn-down attempt, so the shim's share + virtiofsd
	// socket setup doesn't collide ("address already in use" / "directory not
	// empty"). Since we drive the shim directly, failed Creates don't self-clean.
	kata.CleanupSandboxState(id)

	slog.InfoContext(ctx, "Bootstrapping kata shim", slog.String("id", id), slog.String("bundle", bundle))
	if err := shim.Bootstrap(ctx); err != nil {
		return nil, fmt.Errorf("while bootstrapping kata shim: %w", err)
	}
	// On any failure after Bootstrap, tear the shim down so the pod isn't wedged.
	defer func() {
		if retErr != nil {
			_ = shim.Shutdown(ctx, true)
			_ = shim.Close()
			_ = shim.CleanupAction(ctx)
		}
	}()

	// Pass the rootfs as a bind mount, mirroring what containerd's snapshotter
	// provides: the mount SOURCE must differ from the target (<bundle>/rootfs),
	// so kata's shim bind-mounts source->target and virtio-fs-shares the target.
	// atelet populates <bundle>/rootfs directly; a self-bind (source==target) is
	// not shared, leaving the guest rootfs empty -> agent createContainer ENOENT.
	// Relocate the populated rootfs to a sibling source dir (once per bundle) and
	// point kata at it, with an empty <bundle>/rootfs as the mount target.
	rootfsDir := filepath.Join(bundle, "rootfs")
	rootfsSrc := filepath.Join(bundle, "rootfs-src")
	if fi, statErr := os.Stat(rootfsSrc); statErr != nil || !fi.IsDir() {
		if err := os.Rename(rootfsDir, rootfsSrc); err != nil {
			return nil, fmt.Errorf("relocating populated rootfs to source dir: %w", err)
		}
	}
	if err := os.MkdirAll(rootfsDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating rootfs mount target: %w", err)
	}
	if _, err := shim.Create(ctx, kata.CreateOptions{
		Rootfs: []*ctrtypes.Mount{{
			Type:    "bind",
			Source:  rootfsSrc,
			Options: []string{"bind", "rw"},
		}},
	}); err != nil {
		return nil, fmt.Errorf("while creating container/VM: %w", err)
	}
	slog.InfoContext(ctx, "Micro-VM created", slog.String("clh_socket", kata.CLHSocketPath(id)))

	if _, err := shim.Start(ctx); err != nil {
		return nil, fmt.Errorf("while starting container: %w", err)
	}

	s.running[id] = &runningActor{shim: shim, containerName: containerName}
	slog.InfoContext(ctx, "Actor started", slog.String("id", id))
	return &ateompb.RunWorkloadResponse{}, nil
}

// ensureKataCompatibleSpec augments the bundle's config.json with the fields
// kata's OCI conversion requires but atelet's (gVisor-oriented) spec omits.
// Without linux.resources, kata's ContainerConfig nil-derefs and the shim
// crashes. This shaper is a bridge; a future atelet change should emit
// runtime-appropriate specs so it can retire.
func ensureKataCompatibleSpec(bundle, id, netnsPath string) error {
	specPath := filepath.Join(bundle, "config.json")
	b, err := os.ReadFile(specPath)
	if err != nil {
		return fmt.Errorf("reading %q: %w", specPath, err)
	}
	var spec specs.Spec
	if err := json.Unmarshal(b, &spec); err != nil {
		return fmt.Errorf("parsing %q: %w", specPath, err)
	}

	if spec.Linux == nil {
		spec.Linux = &specs.Linux{}
	}
	if spec.Linux.Resources == nil {
		spec.Linux.Resources = defaultKataResources()
	}
	if spec.Linux.CgroupsPath == "" {
		spec.Linux.CgroupsPath = "/ateomchv/" + id
	}

	// atelet's spec carries gVisor pause-model CRI annotations
	// (container-type=container, sandbox-id=pause). kata reads those and waits
	// for a separate "pause" sandbox that we never create, failing with "the
	// sandbox hasn't been created". Strip them so kata treats this single
	// container as its own sandbox (creates the VM), as in the integration tests.
	for k := range spec.Annotations {
		if strings.HasPrefix(k, "io.kubernetes.cri.") {
			delete(spec.Annotations, k)
		}
	}

	// Point the network namespace at our interior netns (which holds the pod's
	// eth0); kata finds eth0 there and wires it to the VM's virtio-net.
	netnsSet := false
	for i := range spec.Linux.Namespaces {
		if spec.Linux.Namespaces[i].Type == specs.NetworkNamespace {
			spec.Linux.Namespaces[i].Path = netnsPath
			netnsSet = true
		}
	}
	if !netnsSet {
		spec.Linux.Namespaces = append(spec.Linux.Namespaces, specs.LinuxNamespace{
			Type: specs.NetworkNamespace, Path: netnsPath,
		})
	}

	// Replace atelet's gVisor-oriented mounts (minimal /dev tmpfs, a
	// /etc/resolv.conf host bind that ENOENTs against the distroless rootfs) with
	// the exact set `ctr run --runtime io.containerd.kata.v2` emits, which kata's
	// agent accepts. (POC shaper; pod DNS integration is future work.)
	spec.Mounts = defaultKataMounts()

	out, err := json.MarshalIndent(&spec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling spec: %w", err)
	}
	if err := os.WriteFile(specPath, out, 0o600); err != nil {
		return fmt.Errorf("writing %q: %w", specPath, err)
	}
	return nil
}

// defaultKataMounts mirrors the mount set `ctr run --runtime io.containerd.kata.v2`
// produces (the proven-good shape for the kata agent).
func defaultKataMounts() []specs.Mount {
	return []specs.Mount{
		{Destination: "/proc", Type: "proc", Source: "proc", Options: []string{"nosuid", "noexec", "nodev"}},
		{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
		{Destination: "/dev/pts", Type: "devpts", Source: "devpts", Options: []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"}},
		{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
		{Destination: "/dev/mqueue", Type: "mqueue", Source: "mqueue", Options: []string{"nosuid", "noexec", "nodev"}},
		{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
		{Destination: "/run", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
	}
}

// defaultKataResources mirrors the device allowlist + cpu shares that
// `ctr run --runtime io.containerd.kata.v2` emits (the proven-good shape).
func defaultKataResources() *specs.LinuxResources {
	dev := func(t string, major, minor int64, access string) specs.LinuxDeviceCgroup {
		d := specs.LinuxDeviceCgroup{Allow: true, Type: t, Access: access}
		if major != 0 {
			d.Major = &major
		}
		if minor >= 0 {
			d.Minor = &minor
		}
		return d
	}
	shares := uint64(1024)
	return &specs.LinuxResources{
		Devices: []specs.LinuxDeviceCgroup{
			{Allow: false, Access: "rwm"},
			dev("c", 1, 3, "rwm"),    // /dev/null
			dev("c", 1, 8, "rwm"),    // /dev/random
			dev("c", 1, 7, "rwm"),    // /dev/full
			dev("c", 5, 0, "rwm"),    // /dev/tty
			dev("c", 1, 5, "rwm"),    // /dev/zero
			dev("c", 1, 9, "rwm"),    // /dev/urandom
			dev("c", 5, 1, "rwm"),    // /dev/console
			dev("c", 136, -1, "rwm"), // pts
			dev("c", 5, 2, "rwm"),    // /dev/ptmx
		},
		CPU: &specs.LinuxCPU{Shares: &shares},
	}
}

// CheckpointWorkload suspends the actor and writes a portable CH snapshot.
//
// Contract with atelet (mirrors ateom-gvisor): after we return, atelet uploads
// the checkpoint dir to object storage, then tears down bundles and resets the
// actor dir.
//
// ateom drives CH's REST api-socket (the one kata created at CLHSocketPath(id)):
// pause -> snapshot file://<CheckpointStateDir> (config.json + state.json +
// sparse memory-ranges) -> tear the sandbox down (shim + VMM). Driving CH's
// REST API under a kata-owned VMM does not corrupt shim state.
func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ns := req.GetActorTemplateNamespace()
	name := req.GetActorTemplateName()
	id := req.GetActorId()

	// kata-shim-owned actors expose CH at kata's socket; restored (ateom-owned)
	// actors at the socket we launched the VMM on.
	ra := s.running[id]
	chSocket := kata.CLHSocketPath(id)
	if ra != nil && ra.apiSocket != "" {
		chSocket = ra.apiSocket
	}
	client := ch.NewClient(chSocket)
	if err := client.WaitReady(ctx, 10*time.Second); err != nil {
		return nil, fmt.Errorf("while waiting for CH api-socket: %w", err)
	}
	tPause := time.Now()
	if err := client.Pause(ctx); err != nil {
		return nil, fmt.Errorf("while pausing guest: %w", err)
	}
	dPause := time.Since(tPause)

	checkpointDir := ateompath.CheckpointStateDir(ns, name, id)
	// Start from a clean dir so CH's snapshot files are the only contents.
	if err := os.RemoveAll(checkpointDir); err != nil {
		return nil, fmt.Errorf("while clearing checkpoint dir: %w", err)
	}
	if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
		return nil, fmt.Errorf("while creating checkpoint dir: %w", err)
	}

	// Capture the virtio-fs shared dir (container rootfs) BEFORE snapshot/
	// teardown — find-paths restore re-opens these by relative path. For a
	// kata-shim-owned actor it lives in the shim's mount namespace (nsenter);
	// for a restored (ateom-owned) actor we built it ourselves in our own
	// namespace (ReconstructSharedDir), so tar it directly.
	tCapture := time.Now()
	if ra != nil && ra.shim != nil {
		if err := ra.shim.CaptureSharedDir(ctx, filepath.Join(checkpointDir, sharedDirTar)); err != nil {
			return nil, fmt.Errorf("while capturing virtio-fs shared dir: %w", err)
		}
	} else if _, statErr := os.Stat(kata.SharedDir(id)); statErr == nil {
		if err := kata.CaptureSharedDirLocal(ctx, id, filepath.Join(checkpointDir, sharedDirTar)); err != nil {
			return nil, fmt.Errorf("while capturing local shared dir: %w", err)
		}
	} else {
		slog.WarnContext(ctx, "No shared dir found for actor; skipping shared-dir capture", slog.String("id", id))
	}
	dCapture := time.Since(tCapture)

	slog.InfoContext(ctx, "Snapshotting guest", slog.String("id", id), slog.String("dir", checkpointDir))
	tSnapshot := time.Now()
	if err := client.Snapshot(ctx, checkpointDir); err != nil {
		return nil, fmt.Errorf("while snapshotting guest: %w", err)
	}
	dSnapshot := time.Since(tSnapshot)

	// Report exactly the files we wrote so atelet ships precisely the CH snapshot
	// (config.json + state.json + memory-ranges + shared-dir.tar), not gVisor's
	// fixed set.
	snapshotFiles, err := listFiles(checkpointDir)
	if err != nil {
		return nil, fmt.Errorf("while listing snapshot files: %w", err)
	}

	// Tear down: the actor returns to "available". Best-effort; the snapshot is
	// already on disk for atelet to ship.
	tTeardown := time.Now()
	s.teardownActor(ctx, id, ra, client)
	dTeardown := time.Since(tTeardown)
	delete(s.running, id)

	// Tear down the per-activation actor network (mirrors gVisor).
	if err := s.cleanupActorNetwork(ctx); err != nil {
		slog.WarnContext(ctx, "Failed to clean up actor network after checkpoint", slog.Any("err", err))
	}

	slog.InfoContext(ctx, "Actor checkpointed", slog.String("id", id), slog.Any("snapshot_files", snapshotFiles),
		slog.Duration("pause", dPause), slog.Duration("capture", dCapture),
		slog.Duration("snapshot", dSnapshot), slog.Duration("teardown", dTeardown))
	return &ateompb.CheckpointWorkloadResponse{SnapshotFiles: snapshotFiles}, nil
}

// listFiles returns the (relative) names of regular files directly under dir.
func listFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if e.Type().IsRegular() {
			files = append(files, e.Name())
		}
	}
	return files, nil
}

// teardownActor stops the CH VMM and the kata shim for an actor. Best-effort:
// the snapshot is already on disk, so this only needs to release resources. ra
// may be nil (e.g. ateom restarted and lost in-memory state).
//
// Order matters for kata-shim-owned actors: kill the shim process BEFORE
// destroying the VM. If the CH VM is shut down first, the shim's wait goroutine
// observes the task vanish and runs kata's container-stop path, which signals the
// (now-gone) guest agent over vsock and panics on a nil agent connection
// (kata_agent.go signalProcess). Killing the shim first avoids that path
// entirely; CleanupSandboxState then sweeps the orphaned VMM/virtiofsd + host
// state without needing kata's (buggy) graceful teardown.
func (s *AteomService) teardownActor(ctx context.Context, id string, ra *runningActor, client *ch.Client) {
	if ra != nil && ra.shim != nil {
		// SIGKILL the foreground shim server + close ttrpc, before the VM goes.
		_ = ra.shim.Close()
	}

	if client != nil {
		tShutdown := time.Now()
		shutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := client.Shutdown(shutCtx); err != nil {
			slog.WarnContext(ctx, "CH shutdown failed (continuing teardown)", slog.Any("err", err))
		}
		cancel()
		slog.InfoContext(ctx, "CH API shutdown done", slog.Duration("took", time.Since(tShutdown)))
	}

	if ra != nil {
		// ateom-owned (post-restore): kill the CH + virtiofsd we launched.
		if ra.chCmd != nil && ra.chCmd.Process != nil {
			_ = ra.chCmd.Process.Kill()
			_, _ = ra.chCmd.Process.Wait()
		}
		if ra.vfsdCmd != nil && ra.vfsdCmd.Process != nil {
			_ = ra.vfsdCmd.Process.Kill()
			_, _ = ra.vfsdCmd.Process.Wait()
		}
	}

	// Sweep kata's host-side state + any orphaned per-sandbox processes (e.g. a
	// shim-owned actor's now-parentless virtiofsd). This is ateom's own cleanup
	// (process kill + unmount + rm); it never calls into the kata agent, so it
	// can't hit the teardown panic above.
	kata.CleanupSandboxState(id)
}

// RestoreWorkload restores the actor on a (possibly different) pod by relaunching
// cloud-hypervisor directly from the downloaded snapshot — bypassing the kata
// shim — and resuming.
//
// Contract with atelet: the snapshot dir (config.json + state.json +
// memory-ranges + shared-dir.tar) has been downloaded to RestoreStateDir.
//
// Steps:
//  1. Reconstruct the virtio-fs shared dir (container rootfs) from shared-dir.tar
//     so find-paths can re-open the inodes the snapshot references.
//  2. Start virtiofsd on the vhost-user socket the snapshot expects.
//  3. Relaunch CH with --restore source_url=file://<dir> (ch.Restore), which
//     reconnects to virtiofsd, recreates vsock, and reloads guest RAM (paused).
//  4. Resume.
//
// The snapshot's config.json references kernel + guest-OS image at static node
// paths (present on any kata node) and per-sandbox sockets under VMDir(id); this
// POC restores under the same id, so those paths line up. ateom then owns the CH.
func (s *AteomService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (resp *ateompb.RestoreWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ns := req.GetActorTemplateNamespace()
	name := req.GetActorTemplateName()
	id := req.GetActorId()

	restoreDir := ateompath.RestoreStateDir(ns, name, id)

	// Resolve the cloud-hypervisor + virtiofsd binaries (fetched assets or flags).
	rr, err := s.resolveRuntime(ateompath.ActorPath(ns, name, id), req.GetRuntimeAssetPaths())
	if err != nil {
		return nil, fmt.Errorf("while resolving runtime assets: %w", err)
	}

	// Clear leftover per-sandbox state/processes from prior failed attempts
	// (stale virtiofsd holding the vhost-user socket, etc.).
	kata.CleanupSandboxState(id)

	// The snapshot was taken under the SOURCE actor's id (e.g. the golden
	// actor); its config.json references that id's socket paths. Rewrite them to
	// this actor's VMDir so the sockets we create are the ones CH reopens.
	if err := rewriteSnapshotSocketPaths(restoreDir, id); err != nil {
		return nil, fmt.Errorf("while rewriting snapshot socket paths: %w", err)
	}

	// 1. Reconstruct the virtio-fs shared dir from the shipped tar.
	tarPath := filepath.Join(restoreDir, sharedDirTar)
	if _, err := os.Stat(tarPath); err != nil {
		return nil, fmt.Errorf("missing shared-dir tar %q: %w", tarPath, err)
	}
	if err := kata.ReconstructSharedDir(ctx, tarPath, id); err != nil {
		return nil, fmt.Errorf("while reconstructing shared dir: %w", err)
	}

	// kata's per-sandbox runtime dir holds the sockets the snapshot references.
	if err := os.MkdirAll(kata.VMDir(id), 0o700); err != nil {
		return nil, fmt.Errorf("while creating VM dir: %w", err)
	}

	// 2. Networking: rebuild the per-activation actor veth, then recreate
	// kata's tap + TC mirror against it. The snapshot's virtio-net device is
	// fd-backed, so CH requires fresh tap FDs on restore (net_fds). The guest's
	// frozen network config (stable actor address, gateway with a fixed MAC)
	// remains valid as-is — no in-guest reconfiguration.
	if err := s.setupActorNetwork(ctx); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			if cleanupErr := s.cleanupActorNetwork(ctx); cleanupErr != nil {
				slog.WarnContext(ctx, "Failed to clean up actor network after Restore failure", slog.Any("err", cleanupErr))
			}
		}
	}()
	netDevs, err := ch.SnapshotNetDevices(restoreDir)
	if err != nil {
		return nil, fmt.Errorf("while reading snapshot net devices: %w", err)
	}
	var restoredNets []ch.RestoredNet
	var tapFiles []*os.File
	defer func() {
		// CH dups the FDs it adopts (SCM_RIGHTS), so ours close unconditionally.
		for _, f := range tapFiles {
			_ = f.Close()
		}
	}()
	for i, nd := range netDevs {
		files, terr := s.setupRestoreTap(ctx, fmt.Sprintf("tap%d_kata", i), nd.QueuePairs)
		if terr != nil {
			return nil, fmt.Errorf("while building restore tap for %s: %w", nd.ID, terr)
		}
		tapFiles = append(tapFiles, files...)
		rn := ch.RestoredNet{ID: nd.ID}
		for _, f := range files {
			rn.FDs = append(rn.FDs, int(f.Fd()))
		}
		restoredNets = append(restoredNets, rn)
	}

	// 3. Start virtiofsd on the vhost-user socket from config.json.
	vfsdCmd, err := kata.StartVirtiofsd(ctx, kata.VirtiofsdOptions{
		Binary:     rr.virtiofsd,
		SocketPath: kata.VirtiofsdSocketPath(id),
		SharedDir:  kata.SharedDir(id),
		Log:        slogWriter{ctx},
	})
	if err != nil {
		return nil, fmt.Errorf("while starting virtiofsd: %w", err)
	}
	defer func() {
		if retErr != nil && vfsdCmd.Process != nil {
			_ = vfsdCmd.Process.Kill()
		}
	}()

	// 4. Launch a bare VMM and restore with the tap FDs attached (SCM_RIGHTS).
	apiSocket := filepath.Join(kata.VMDir(id), "clh-api-restore.sock")
	slog.InfoContext(ctx, "Restoring CH from snapshot", slog.String("id", id), slog.String("dir", restoreDir))
	chCmd, client, err := ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
		Binary:    rr.ch,
		APISocket: apiSocket,
		Stdout:    slogWriter{ctx},
		Stderr:    slogWriter{ctx},
	})
	if err != nil {
		return nil, fmt.Errorf("while launching VMM for restore: %w", err)
	}
	defer func() {
		if retErr != nil && chCmd.Process != nil {
			_ = chCmd.Process.Kill()
		}
	}()
	if err := client.RestoreWithNetFDs(ctx, restoreDir, restoredNets); err != nil {
		return nil, fmt.Errorf("while restoring VM with net FDs: %w", err)
	}

	// 5. Resume (restore comes back paused).
	if err := client.Resume(ctx); err != nil {
		return nil, fmt.Errorf("while resuming restored guest: %w", err)
	}

	s.running[id] = &runningActor{chCmd: chCmd, vfsdCmd: vfsdCmd, apiSocket: apiSocket}
	slog.InfoContext(ctx, "Actor restored", slog.String("id", id))
	return &ateompb.RestoreWorkloadResponse{}, nil
}

// rewriteSnapshotSocketPaths updates a CH snapshot config.json's per-sandbox
// socket paths (virtio-fs vhost-user socket, hybrid-vsock socket) from the
// source actor's VMDir to the restoring actor's VMDir. Disk/kernel paths are
// content-addressed static files and identical on every node.
func rewriteSnapshotSocketPaths(snapshotDir, id string) error {
	cfgPath := filepath.Join(snapshotDir, "config.json")
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		return err
	}
	var cfg map[string]any
	if err := json.Unmarshal(b, &cfg); err != nil {
		return fmt.Errorf("parsing %q: %w", cfgPath, err)
	}
	if fsList, ok := cfg["fs"].([]any); ok {
		for _, f := range fsList {
			if fm, ok := f.(map[string]any); ok {
				fm["socket"] = kata.VirtiofsdSocketPath(id)
			}
		}
	}
	if vsock, ok := cfg["vsock"].(map[string]any); ok {
		vsock["socket"] = kata.VsockSocketPath(id)
	}
	out, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(cfgPath, out, 0o600)
}

// slogWriter adapts an io.Writer to slog at info level, for the kata shim's
// start/delete diagnostics.
type slogWriter struct{ ctx context.Context }

func (w slogWriter) Write(p []byte) (int, error) {
	slog.InfoContext(w.ctx, "kata shim", slog.String("out", string(p)))
	return len(p), nil
}
