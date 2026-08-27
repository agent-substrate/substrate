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
	"path/filepath"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/readyz"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/sizing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// restoreMemMode picks how cloud-hypervisor should load guest RAM, from what the VMM
// just told us about itself over vmm.ping.
//
// OnDemand is what we want: it faults pages in as the guest touches them, so an idle
// restored actor holds its working set rather than its whole snapshot — on the counter
// demo, 16MiB against 158MiB. Eager gives that up, reading every populated extent up
// front.
//
// It is still the right choice on a VMM that prefaults, where OnDemand is not merely
// wasteful but unusable: the prefault storm starves the guest and its readiness probe
// never passes.
func restoreMemMode(ctx context.Context, info ch.VMMInfo) string {
	if !info.PrefaultsUnconditionally() {
		return ch.MemRestoreOnDemand
	}
	if info.Version == "" && info.BuildVersion == "" {
		// Unknown version: eager works everywhere, so prefer a bigger idle footprint
		// over an actor that cannot start. Say so, because that cost is invisible.
		slog.WarnContext(ctx, "cloud-hypervisor did not report a version; restoring eagerly",
			slog.String("mode", ch.MemRestoreEager))
	}
	return ch.MemRestoreEager
}

// RestoreWorkload brings the actor back from a snapshot, on a possibly different
// pod. What that means depends on the scope the snapshot was taken with:
//
//   - FULL: relaunch cloud-hypervisor from the snapshot and resume the guest
//     (restoreFullScope).
//   - DATA: there is no guest to resume — re-materialize the durable-dir volumes and
//     cold-boot the actor, which starts its containers afresh from the OCI image.
//   - DATA_ON_GOLDEN: atelet staged a combined set into RestoreStateDir — the
//     guest files (memory + VM state) from the template's golden snapshot plus
//     the durable-dir tar from the actor's own snapshot — so this restores
//     exactly like FULL: the golden guest resumes over the actor's data.
//
// Contract with atelet: the snapshot's files have been downloaded to RestoreStateDir,
// and the durable-dir volume directories re-created (empty).
func (s *AteomService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (resp *ateompb.RestoreWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.rejectIfDraining(); err != nil {
		return nil, err
	}

	// Same as RunWorkload: a restore is a boot, and graceful shutdown cancels it
	// rather than queueing behind it.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.setActiveRPC(rpcRestoreWorkload, cancel)
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
	restoreDir := ateompath.RestoreStateDir(p.actorUID)
	durableDir := ateompath.DurableDirVolumeMountsDir(p.actorUID)
	tStart := time.Now()

	attribution := p.actorAttribution()
	s.actorLogger.EmitLifecycleLog(ctx, "Actor restoring", attribution)

	// Same as RunWorkload: retain before the restore, drop again if it fails. A
	// Full-scope resume reaches "executing" in a different way than a cold boot
	// does, but the window between accepting the actor and serving it is the same
	// window, and a poll landing in it should name the actor either way.
	s.activeActor.Store(&attribution)
	defer func() {
		if retErr != nil {
			s.activeActor.Store(nil)
		}
	}()

	// Restore the durable-dir volumes before anything can observe them: for Full
	// that means before the share's virtiofsd starts, for Data before the workload
	// cold-starts. The snapshot must carry them — the actor declares the volume, and
	// every scope captures it.
	if hasDurableVolumes(p.containers) {
		if err := untarDurableVolumes(durableDir, restoreDir); err != nil {
			return nil, err
		}
	}

	switch scope := req.GetScope(); scope {
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN:
		// DATA_ON_GOLDEN: the restore dir holds the golden snapshot's guest
		// files, and the untar above re-materialized the ACTOR's durable-dir
		// data, so resuming the golden guest picks up the actor's data through
		// the durable virtio-fs share.
		if err := s.restoreFullScope(ctx, p, restoreDir, tStart); err != nil {
			return nil, err
		}
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
		// A Data snapshot holds no guest state, so this is a cold boot that
		// happens to start with the volumes already populated. readyz gating comes
		// with the cold-boot path, so the actor is serving when we return.
		if err := s.coldBootActorRetrying(ctx, p); err != nil {
			return nil, err
		}
		slog.InfoContext(ctx, "Actor restored (durable-dir volumes, cold boot)",
			slog.String("id", p.actorUID), slog.Duration("total", time.Since(tStart)))
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported snapshot scope: %v", scope)
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor restored", attribution)
	return &ateompb.RestoreWorkloadResponse{}, nil
}

// restoreFullScope restores a whole-guest snapshot: relaunch cloud-hypervisor
// directly from it and resume.
//
// Each container's rootfs is a host-merged overlay (image lower + host upper). Steps:
// rewrite the snapshot config's per-VMDir paths (vsock + serial + fs sockets) to this
// actor's; re-materialize the uppers from rootfs-upper.tar (in the background,
// overlapped with bundle preparation) and re-mount the merged trees at the frozen
// find-paths paths; start the virtiofsd serving them; rebuild the tap (the snapshot's
// virtio-net is fd-backed → fresh net_fds); relaunch CH with --restore (OnDemand),
// and resume. Guest RAM — the actor's in-memory state and the frozen network config —
// comes back from the memory snapshot; the durable-dir volumes were restored by the
// caller from their tar.
func (s *AteomService) restoreFullScope(ctx context.Context, p actorBootParams, restoreDir string, tStart time.Time) (retErr error) {
	actorUID := p.actorUID

	rr := s.resolveRuntime(p.assetPaths)
	egress, err := s.prepareActorEgress(ctx, p.actorUID, p.egressGateway)
	if err != nil {
		return err
	}
	kata.CleanupSandboxState(ctx, actorUID)

	// Repoint the snapshot's vsock socket to this actor's VMDir (the disk + kernel
	// paths are content-addressed/per-actor and already line up on the same node).
	if err := rewriteSnapshotSocketPaths(restoreDir, actorUID); err != nil {
		return fmt.Errorf("while rewriting snapshot socket paths: %w", err)
	}
	srcID := actorUID
	if b, rerr := os.ReadFile(filepath.Join(restoreDir, baseIDFile)); rerr == nil {
		if v := strings.TrimSpace(string(b)); v != "" {
			srcID = v
		}
	}
	if err := os.MkdirAll(kata.VMDir(actorUID), 0o700); err != nil {
		return fmt.Errorf("while creating VM dir: %w", err)
	}
	tPrep := time.Now()

	// Merged-rootfs snapshots carry the upper as rootfs-upper.tar (the tar's
	// presence is what says which model the guest expects). Start
	// re-materializing the upper contents NOW, in the background: the untar
	// scales with the actor's data and is joined right before the host overlay
	// mounts need it, so it hides behind the bundle preparation below. Legacy
	// guest-tmpfs-upper snapshots have no tar: their upper rides inside the
	// restored guest memory and the share presents the bare image instead.
	//
	// An error return between here and the join MUST drain the goroutine (the
	// deferred receive below): returning with the untar still writing would let
	// a retried restore's own untar race it inside the same directory.
	untarDone := make(chan error, 1)
	untarJoined := false
	go func() {
		untarDone <- untarRootfsUpper(rootfsUpperDir(actorUID), restoreDir)
	}()
	defer func() {
		if !untarJoined {
			<-untarDone
		}
	}()

	// Reconstruct each container's rootfs at the frozen find-paths location
	// SharedDir(id)/<cid>/rootfs from the LOCAL OCI bundle (atelet re-unpacked
	// the golden image) and start the one virtiofsd serving the tree. The fs
	// sockets in the snapshot config are repointed to this VMDir by
	// rewriteSnapshotSocketPaths above. Cross-node consistency relies on a
	// deterministic unpack of the same image at the same <cid>/rootfs path
	// (plus, for merged rootfs, the upper re-materialized from the tar).
	containers := p.containers
	if len(containers) == 0 {
		return status.Error(codes.InvalidArgument, "actor spec has no containers")
	}
	if len(containers) > maxActorContainers {
		return status.Errorf(codes.Unimplemented, "ateom-microvm supports at most %d containers, got %d", maxActorContainers, len(containers))
	}
	// The VM's RAM comes from the snapshot, so a limit the current VMM reserve can
	// no longer satisfy (e.g. --vmm-mem-reserve-mib was raised after the snapshot
	// was taken) has to fail here rather than silently pair the guest with a cgroup
	// limit larger than its RAM.
	guestSize, err := s.guestSize(p.size)
	if err != nil {
		return err
	}
	ctrs, err := s.buildActorContainers(actorUID, containers, guestSize)
	if err != nil {
		return err
	}
	tBundles := time.Now()
	// The overlay mounts need the upper on disk: join the background untar (still
	// overlapped with the bundle preparation above), then assemble the merged trees
	// and serve them.
	untarErr := <-untarDone
	untarJoined = true
	if untarErr != nil {
		return untarErr
	}
	tUpper := time.Now()
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

	tLowers := time.Now()
	tDurable := tLowers

	// Networking: rebuild the per-activation veth + tap; the snapshot's virtio-net
	// is fd-backed, so CH needs fresh tap FDs (net_fds) on restore.
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
				slog.WarnContext(cleanupCtx, "Failed to deactivate actor networking after Restore failure", slog.Any("err", cleanupErr))
			}
			if cleanupErr := ateomnet.CleanupActorNetwork(cleanupCtx, s.interiorNetNS); cleanupErr != nil {
				slog.WarnContext(cleanupCtx, "Failed to clean up actor network after Restore failure", slog.Any("err", cleanupErr))
			}
			// Detach any bundle rootfs overlays mounted by buildActorContainers
			// before the failure, mirroring teardownActor's cleanup.
			if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(actorUID)); err != nil {
				slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays after Restore failure", slog.Any("err", err))
			}
		}
	}()
	netDevs, err := ch.SnapshotNetDevices(restoreDir)
	if err != nil {
		return fmt.Errorf("while reading snapshot net devices: %w", err)
	}
	var restoredNets []ch.RestoredNet
	var tapFiles []*os.File
	defer func() {
		for _, f := range tapFiles {
			_ = f.Close()
		}
	}()
	for i, nd := range netDevs {
		files, terr := s.setupRestoreTap(ctx, fmt.Sprintf("tap%d_kata", i), nd.QueuePairs)
		if terr != nil {
			return fmt.Errorf("while building restore tap for %s: %w", nd.ID, terr)
		}
		tapFiles = append(tapFiles, files...)
		rn := ch.RestoredNet{ID: nd.ID}
		for _, f := range files {
			rn.FDs = append(rn.FDs, int(f.Fd()))
		}
		restoredNets = append(restoredNets, rn)
	}

	// Relaunch CH and restore with the tap FDs attached (SCM_RIGHTS). CH reopens
	// /dev/vda (image) + each /dev/vd{b+i} (actor rootfs) from the snapshot config paths.
	apiSocket := filepath.Join(kata.VMDir(actorUID), "clh-api-restore.sock")
	tTap := time.Now()
	chCmd, client, err := ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
		Binary: rr.chBinary, APISocket: apiSocket, Stdout: slogWriter{ctx}, Stderr: slogWriter{ctx},
	})
	if err != nil {
		return fmt.Errorf("while launching VMM for restore: %w", err)
	}
	defer func() {
		if retErr != nil && chCmd.Process != nil {
			_ = chCmd.Process.Kill()
		}
	}()
	// How guest RAM comes back depends on the VMM (see restoreMemMode), and the rest
	// of the actor's lifecycle follows from that choice:
	//
	//   - OnDemand: cloud-hypervisor demand-pages from restoreDir for the VM's whole
	//     lifetime, so it must stay put, and the snapshot it writes later holds only
	//     the pages faulted in meanwhile — CheckpointWorkload overlays that delta onto
	//     this source to rebuild a complete one.
	//   - Eager: every populated extent is read here and now. Nothing pages from the
	//     source afterwards and nothing merges against it, so it is dropped below and
	//     the next snapshot stands on its own.
	tLaunch := time.Now()
	memMode := restoreMemMode(ctx, client.Info())
	slog.InfoContext(ctx, "restoring guest memory",
		slog.String("mode", memMode), slog.String("vmm_version", client.Info().Version))
	if err := client.RestoreWithNetFDs(ctx, restoreDir, restoredNets, memMode); err != nil {
		return fmt.Errorf("while restoring VM with net FDs: %w", err)
	}
	tVMRestore := time.Now()
	if err := client.Resume(ctx); err != nil {
		return fmt.Errorf("while resuming restored guest: %w", err)
	}
	tResume := time.Now()

	// Block until every readyz-enabled container reports 200.
	if err := readyz.WaitAll(ctx, containers, ateomnet.ActorVethIP); err != nil {
		return fmt.Errorf("while waiting for container readyz: %w", err)
	}

	// Where a resume goes. Like the boot phases, this used to be a single total,
	// which hid that a first (cold) restore and a later (warm) one differ by more
	// than 5x on the same actor. upper/lowers is the host reassembling the rootfs;
	// vm_restore is cloud-hypervisor reading guest RAM back.
	slog.InfoContext(ctx, "Actor restore phases", slog.String("id", actorUID),
		slog.Duration("prep", tPrep.Sub(tStart)),
		slog.Duration("bundles", tBundles.Sub(tPrep)),
		slog.Duration("upper_join", tUpper.Sub(tBundles)),
		slog.Duration("lowers", tLowers.Sub(tUpper)),
		slog.Duration("durable", tDurable.Sub(tLowers)),
		slog.Duration("tap", tTap.Sub(tDurable)),
		slog.Duration("vmm_launch", tLaunch.Sub(tTap)),
		slog.Duration("vm_restore", tVMRestore.Sub(tLaunch)),
		slog.Duration("resume", tResume.Sub(tVMRestore)),
		slog.Duration("readyz", time.Since(tResume)),
		slog.Duration("total", time.Since(tStart)))

	// An eager restore has read the whole snapshot into guest memory, and nothing
	// merges against it afterwards, so the staged copy is dead weight from here on —
	// a second ~160MiB per running actor on top of the checkpoint it will write.
	// Drop the memory image but keep the directory: atelet re-stages it wholesale
	// before any later restore, and the small files beside it stay cheap to keep.
	if memMode == ch.MemRestoreEager {
		staged := filepath.Join(restoreDir, "memory-ranges")
		if err := os.Remove(staged); err != nil && !os.IsNotExist(err) {
			// Not fatal: it only costs disk until the actor is torn down.
			slog.WarnContext(ctx, "could not drop the staged memory image", "error", err)
		} else {
			slog.InfoContext(ctx, "dropped the staged memory image (eager restore needs no merge base)")
		}
	}

	ra := &runningActor{
		chCmd: chCmd, vfsdCmd: vfsdCmd,
		apiSocket: apiSocket, baseID: srcID, restoreSourceDir: restoreDir,
		snapshotIsSelfContained: memMode == ch.MemRestoreEager,
		// Signaling an id the agent does not know fails the whole graceful
		// shutdown with InvalidContainerId, so these must be what the guest runs.
		workloadIDs: workloadIDs(ctrs),
	}

	// Re-attach stdout/stderr forwarding for each container: the restored guest's
	// containers + kata-agent are alive, so a fresh dial over this actor's vsock
	// resumes ReadStdout/ReadStderr. Best-effort — a failed dial must not fail the
	// restore (the actor is already running); forwarding is just skipped.
	vsockPath := kata.VsockSocketPath(actorUID)
	guestAC, dialErr := dialAgentRetry(ctx, vsockPath, 15*time.Second)
	if dialErr != nil {
		slog.WarnContext(ctx, "post-restore agent dial failed; actor log forwarding and guest stats disabled for this restore",
			slog.String("id", actorUID), slog.Any("err", dialErr))
	} else {
		ra.guestAgent = guestAC
		attribution := p.actorAttribution()
		for _, c := range containers {
			s.startActorLogForwarding(guestAC, attribution, c.GetName(), c.GetName())
		}
	}

	if err := s.activateActorNetworking(p.actorRef.Atespace, p.actorRef.Name, egress); err != nil {
		return err
	}
	s.running[actorUID] = ra

	// Publish the guest to GetWorkloadStats, past the last error return above
	// for the same reason as in coldBootActor. Skipped when the dial failed:
	// telemetry rides on the forwarding connection, so that activation answers
	// FAILED_PRECONDITION until its next checkpoint. Not worth a second dial of
	// its own — whatever kept the agent from answering a 15s retry loop would
	// keep it from answering that one too.
	if ra.guestAgent != nil {
		s.guestStats.Store(&guestStatsTarget{actorUID: actorUID, agent: ra.guestAgent, workloadIDs: ra.workloadIDs})
	}

	slog.InfoContext(ctx, "Actor restored (overlay rootfs)",
		slog.String("id", actorUID), slog.Duration("total", time.Since(tStart)))
	return nil
}

// rewriteSnapshotSocketPaths repoints the snapshot config.json's per-VMDir paths from
// the source actor's VMDir to the restoring actor's: the hybrid-vsock socket, the
// File serial console, and each virtio-fs socket, so the sockets/files we create are
// the ones CH reopens. The kernel and /dev/vda kata image are content-addressed static
// files with identical paths on every node, so they need no rewrite, and the overlay
// has no per-actor disk to repoint.
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
	if vsock, ok := cfg["vsock"].(map[string]any); ok {
		vsock["socket"] = kata.VsockSocketPath(id)
	}
	// ateom captures the guest console to a file under the source actor's VMDir
	// (virtio-console normally, plus the UART in debug mode). On restore those paths
	// are stale (they point at the golden/source pod's VMDir), so CH's
	// CreateConsoleDevice fails (No such file or directory). Repoint them at this
	// actor's VMDir.
	for key, path := range map[string]string{
		"console": kata.ConsoleLogPath(id),
		"serial":  kata.SerialLogPath(id),
	} {
		dev, ok := cfg[key].(map[string]any)
		if !ok {
			continue
		}
		if mode, _ := dev["mode"].(string); mode == "File" {
			dev["file"] = path
		}
	}
	// The virtio-fs share is served by its per-VMDir virtiofsd socket; the
	// snapshot recorded the golden actor's, so repoint it at this actor's VMDir.
	if fss, ok := cfg["fs"].([]any); ok {
		for _, f := range fss {
			fm, ok := f.(map[string]any)
			if !ok {
				return fmt.Errorf("snapshot config %q has a malformed fs device", cfgPath)
			}
			switch tag, _ := fm["tag"].(string); tag {
			case kata.FsTag:
				fm["socket"] = kata.VirtiofsdSocketPath(id)
			case "ateDurable":
				// Legacy multi-virtiofs snapshot backward compatibility.
				fm["socket"] = kata.DurableVirtiofsdSocketPath(id)
			case "ateCSI":
				// Legacy multi-virtiofs snapshot backward compatibility.
				fm["socket"] = kata.CsiVirtiofsdSocketPath(id)
			default:
				return fmt.Errorf("snapshot config %q has fs device with unknown tag %q", cfgPath, tag)
			}
		}
	}
	out, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	if err := os.WriteFile(cfgPath, out, 0o600); err != nil {
		return err
	}
	return nil
}
