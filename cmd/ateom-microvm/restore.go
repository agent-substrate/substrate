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
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateomphaselog"
	"github.com/agent-substrate/substrate/internal/ateomstats"

	"github.com/agent-substrate/substrate/internal/ateomnet"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/sizing"
	"github.com/agent-substrate/substrate/internal/wakeupprobe"
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
// wasteful but unusable: the prefault storm starves the guest and its wakeup probe
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

// reseedGuestCRNG mixes fresh, per-restore entropy into the restored guest's kernel
// CRNG through the kata-agent (see the call site in restoreMemoryFidelity for why a restore
// needs this). The nonce is a throwaway; its only job is to differ between restores so
// that clones of one snapshot diverge instead of producing identical randomness. The
// caller owns ac: it stays open for log forwarding and guest stats.
func reseedGuestCRNG(ctx context.Context, ac *kata.AgentClient) error {
	nonce, err := newReseedNonce()
	if err != nil {
		return err
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return ac.ReseedRandomDev(rctx, nonce)
}

// newReseedNonce returns 32 bytes of fresh entropy to mix into the guest CRNG.
func newReseedNonce() ([]byte, error) {
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generating reseed nonce: %w", err)
	}
	return nonce, nil
}

// RestoreWorkload brings the actor back from a snapshot, on a possibly different
// pod. What that means depends on the fidelity the snapshot was taken with:
//
//   - MEMORY: relaunch cloud-hypervisor from the snapshot and resume the guest
//     (restoreMemoryFidelity).
//   - VOLUMES: there is no guest to resume — re-materialize the durable-dir
//     volumes and cold-boot the actor, which starts its containers afresh from
//     the OCI image.
//
// Contract with atelet: the snapshot's files are in ActorDirs.restore_dir,
// and the durable-dir volume directories re-created (empty).
func (s *AteomService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (resp *ateompb.RestoreWorkloadResponse, retErr error) {
	if err := validateActorDirs(req.GetActorDirs()); err != nil {
		return nil, err
	}
	if err := validateFidelity(req.GetFidelity()); err != nil {
		return nil, err
	}
	if err := validateRuntimeAssetPaths(req.GetRuntimeAssetPaths()); err != nil {
		return nil, err
	}
	if !s.locks.Lock(ctx, req.GetActorUid()) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(req.GetActorUid())

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Register for startup cancellation before checking for shutdown.
	release, err := s.beginRPC(req.GetActorUid(), rpcRestoreWorkload, cancel)
	if err != nil {
		return nil, err
	}
	defer release()

	if err := s.tunnel.Deactivate(ctx, ateomstats.ActorAttributionFromRequest(req)); err != nil {
		return nil, err
	}

	p := actorBootParams{
		actorRef:         resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()},
		actorUID:         req.GetActorUid(),
		actorDirs:        req.GetActorDirs(),
		templateAtespace: req.GetActorTemplateAtespace(),
		templateName:     req.GetActorTemplateName(),
		containers:       req.GetSpec().GetContainers(),
		assetPaths:       req.GetRuntimeAssetPaths(),
		egressGateway:    req.GetEgressGateway(),
		size:             sizing.FromLimits(req.GetCpuMilli(), req.GetMemoryBytes()),
	}
	restoreDir := p.actorDirs.GetRestoreDir()
	durableDir := p.actorDirs.GetDurableDirVolumeMountsDir()
	tStart := time.Now()

	attribution := p.actorAttribution()
	s.actorLogger.EmitLifecycleLog(ctx, "Actor restoring", attribution)

	if err := s.endPreviousActivation(ctx, attribution.UID, req.GetActorDirs()); err != nil {
		return nil, err
	}
	// Publish attribution before restore, so a stats read during it is
	// attributed. A VOLUMES restore cold-boots, so its CPU counts from zero; the other
	// scopes resume the guest's counters, so theirs counts from the first
	// reading.
	resumesGuest := req.GetFidelity() != ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	if _, err := s.hostActor(ctx, attribution, resumesGuest); err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			// Detached: the RPC's context may be what failed it.
			cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cleanupCancel()
			_ = s.unhostActor(cleanupCtx, attribution.UID)
		}
	}()

	// Restore the durable-dir volumes before anything can observe them: for MEMORY
	// that means before the share's virtiofsd starts, for VOLUMES before the workload
	// cold-starts.
	if hasDurableVolumes(p.containers) {
		if err := untarDurableVolumes(durableDir, restoreDir, durableVolumeNames(p.containers)); err != nil {
			return nil, err
		}
	}

	switch scope := req.GetFidelity(); scope {
	case ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY:
		if err := s.restoreMemoryFidelity(ctx, p, scope, restoreDir, req.GetPreserveRestoreDir(), tStart); err != nil {
			return nil, err
		}
	case ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		// A VOLUMES snapshot holds no guest state, so this is a cold boot that
		// happens to start with the volumes already populated. wakeup probe gating comes
		// with the cold-boot path, so the actor is serving when we return.
		if err := s.coldBootActorRetrying(ctx, p); err != nil {
			return nil, err
		}
		dTotal := time.Since(tStart)
		slog.InfoContext(ctx, "Actor restored (durable-dir volumes, cold boot)",
			slog.String("id", p.actorUID), slog.Duration("total", dTotal))
		// A cold boot has none of the memory-restore phases, so the total is the
		// only observation on its record.
		ateomphaselog.LogSnapshotPhases(ctx, "Restore timing breakdown", attribution, scope,
			ateomphaselog.RestoreDurationKey, nil, []ateomphaselog.Phase{{Name: phaseTotal, D: dTotal}})
	default:
		return nil, apierror.InvalidArgument("unsupported snapshot fidelity: %v", scope)
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor restored", attribution)
	return &ateompb.RestoreWorkloadResponse{}, nil
}

// restoreMemoryFidelity restores a whole-guest snapshot: relaunch cloud-hypervisor
// directly from it and resume.
//
// Each container's rootfs is a host-merged overlay (image lower + host upper). Steps:
// rewrite the snapshot config's per-VMDir paths (vsock + serial + fs sockets) to this
// actor's; re-materialize the uppers from the per-container tars (in the background,
// overlapped with bundle preparation) and re-mount the merged trees at the frozen
// find-paths paths; start the virtiofsd serving them; rebuild the tap (the snapshot's
// virtio-net is fd-backed → fresh net_fds); relaunch CH with --restore (OnDemand),
// and resume. Guest RAM — the actor's in-memory state and the frozen network config —
// comes back from the memory snapshot; the durable-dir volumes were restored by the
// caller from their tar.
func (s *AteomService) restoreMemoryFidelity(ctx context.Context, p actorBootParams, scope ateompb.SnapshotFidelity, restoreDir string, preserveRestoreDir bool, tStart time.Time) (retErr error) {
	actorUID := p.actorUID

	rr := s.resolveRuntime(p.assetPaths)
	egress, err := s.tunnel.PrepareEgress(ctx, p.attribution(), p.egressGateway)
	if err != nil {
		return err
	}
	s.cleanupSandboxState(ctx, actorUID)

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

	// MEMORY snapshots carry each container's upper as a tar (rootfsUpperTarFile).
	// Start re-materializing the upper contents NOW, in the background: the
	// untar scales with the actor's data and is joined right before the host
	// overlay mounts need it, so it hides behind the bundle preparation below.
	//
	// An error return between here and the join MUST drain the goroutine (the
	// deferred receive below): returning with the untar still writing would let
	// a retried restore's own untar race it inside the same directory.
	untarDone := make(chan error, 1)
	untarJoined := false
	go func() {
		untarDone <- untarRootfsUpper(rootfsUpperDir(p.actorDirs), restoreDir, containerNames(p.containers))
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
		return apierror.InvalidArgument("actor spec has no containers")
	}
	if len(containers) > maxActorContainers {
		return apierror.Unimplemented("ateom-microvm supports at most %d containers, got %d", maxActorContainers, len(containers))
	}
	ctrs, err := s.buildActorContainers(p.actorUID, p.actorDirs, containers)
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
	leaf, err := s.actorLeaf(actorUID, p.size)
	if err != nil {
		return err
	}
	defer leaf.Close()
	vfsdCmd, err := s.stageMergedRootfs(ctx, rr, actorUID, p.actorDirs, ctrs, containers, leaf.SysProcAttr())
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

	// Networking: rebuild the actor's namespace; the snapshot's virtio-net is
	// fd-backed, so CH needs fresh tap FDs (net_fds) on restore. The caller
	// unhosts on failure, once the defers here have stopped the actor's
	// processes.
	defer func() {
		if retErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if cleanupErr := s.tunnel.Deactivate(cleanupCtx, p.attribution()); cleanupErr != nil {
				slog.WarnContext(cleanupCtx, "Failed to deactivate actor networking after Restore failure", slog.Any("err", cleanupErr))
			}
			// Detach any bundle rootfs overlays mounted by buildActorContainers
			// before the failure, mirroring teardownActor's cleanup.
			if err := imagecache.UnmountAllUnder(p.actorDirs.GetOciBundleDir()); err != nil {
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
		files, terr := setupActorTap(ctx, s.sandboxNetNS(actorUID), fmt.Sprintf("tap%d_kata", i), nd.QueuePairs)
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

	// Relaunch CH and restore with the tap FDs attached (SCM_RIGHTS). CH reopens the
	// guest image from the snapshot config's path, as whichever device that config
	// names: virtio-pmem, or the virtio-blk disk of a snapshot taken before the image
	// moved to pmem (those keep restoring as they were taken).
	apiSocket := filepath.Join(kata.VMDir(actorUID), "clh-api-restore.sock")
	tTap := time.Now()
	chCmd, client, err := ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
		Binary: rr.chBinary, APISocket: apiSocket, Stdout: slogWriter{ctx}, Stderr: slogWriter{ctx},
		SysProcAttr: leaf.SysProcAttr(),
	})
	if err != nil {
		return fmt.Errorf("while launching VMM for restore: %w", err)
	}
	defer func() {
		if retErr != nil && chCmd.Process != nil {
			_ = chCmd.Process.Kill()
			_, _ = chCmd.Process.Wait()
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

	// One kata-agent connection serves this whole activation: the CRNG reseed below,
	// then log forwarding and guest stats. As on cold boot, not reaching the agent
	// fails the restore. A failing restore closes the connection on its way out.
	guestAC, err := dialAgentRetry(ctx, kata.VsockSocketPath(actorUID), 15*time.Second)
	if err != nil {
		return fmt.Errorf("while dialing kata-agent after resume: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = guestAC.Close()
		}
	}()

	// Reseed the guest CRNG before the workload gets far. A restored guest resumes with
	// the entropy pool frozen in the snapshot, so every actor restored from one snapshot
	// (golden cold-start, tag clones) would share an identical CRNG state and emit the
	// same "random" values. Cloud Hypervisor has no VmGenID device to signal the guest,
	// so we feed fresh per-restore entropy through the kata-agent's ReseedRandomDev, which
	// mixes it into /dev/random and reseeds. A failed reseed fails the restore, since the
	// actor would otherwise run with randomness it shares with its clones.
	//
	// The reseed runs just after Resume, so a workload that reads randomness in its first
	// instants after resume can still see the frozen state. Fully closing that needs the
	// workload frozen across the reseed, or a VMM VmGenID that acts before the vCPUs resume.
	//
	// TODO: switch to a Cloud Hypervisor VmGenID device once it exists. clh has no such
	// device today; Firecracker and QEMU do. With one, the VMM changes the generation id
	// and notifies the guest before unpausing the vCPUs, so a >=5.18 kernel reseeds its
	// CRNG on its own with no host round-trip, no per-restore RPC, and no post-Resume
	// race. At that point this agent-driven reseed can be dropped.
	if err := reseedGuestCRNG(ctx, guestAC); err != nil {
		return fmt.Errorf("while reseeding guest CRNG: %w", err)
	}

	// Block until every wakeup-probe-enabled container reports 200.
	if err := wakeupprobe.WaitAll(ctx, containers, ateomnet.ActorVethIP, wakeupprobe.DialFunc(s.sandboxDialer(actorUID))); err != nil {
		return fmt.Errorf("while waiting for container wakeup probe: %w", err)
	}

	// Where a resume goes. Like the boot phases, this used to be a single total,
	// which hid that a first (cold) restore and a later (warm) one differ by more
	// than 5x on the same actor. upper/lowers is the host reassembling the rootfs;
	// vm_restore is cloud-hypervisor reading guest RAM back.
	dWakeupProbe := time.Since(tResume)
	dTotal := time.Since(tStart)
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
		slog.Duration("wakeup_probe", dWakeupProbe),
		slog.Duration("total", dTotal))
	// The joinable per-actor record the benchmarking tooling aggregates. The
	// durable delta is not carried: tDurable is pinned to tLowers today, so it
	// would always be the zero a record skips.
	ateomphaselog.LogSnapshotPhases(ctx, "Restore timing breakdown", p.actorAttribution(), scope,
		ateomphaselog.RestoreDurationKey, nil, []ateomphaselog.Phase{
			{Name: phasePrep, D: tPrep.Sub(tStart)},
			{Name: phaseBundles, D: tBundles.Sub(tPrep)},
			{Name: phaseUpperJoin, D: tUpper.Sub(tBundles)},
			{Name: phaseLowers, D: tLowers.Sub(tUpper)},
			{Name: phaseTap, D: tTap.Sub(tDurable)},
			{Name: phaseVMMLaunch, D: tLaunch.Sub(tTap)},
			{Name: phaseVMRestore, D: tVMRestore.Sub(tLaunch)},
			{Name: phaseResume, D: tResume.Sub(tVMRestore)},
			{Name: phaseWakeupProbe, D: dWakeupProbe},
			{Name: phaseTotal, D: dTotal},
		})

	ra := &runningActor{
		chCmd: chCmd, vfsdCmd: vfsdCmd,
		apiSocket: apiSocket, baseID: srcID, restoreSourceDir: restoreDir,
		preserveRestoreSource:   preserveRestoreDir,
		snapshotIsSelfContained: memMode == ch.MemRestoreEager,
		// Signaling an id the agent does not know fails the whole graceful
		// shutdown with InvalidContainerId, so these must be what the guest runs.
		workloadIDs: workloadIDs(ctrs),
		guestAgent:  guestAC,
	}

	// Re-attach stdout/stderr forwarding for each container over the agent
	// connection dialed after resume: the restored guest's containers are alive, so
	// ReadStdout/ReadStderr pick up where they left off.
	attribution := p.actorAttribution()
	for _, c := range containers {
		s.startActorLogForwarding(guestAC, attribution, c.GetName(), c.GetName())
	}

	if err := s.tunnel.Activate(p.attribution(), s.sandboxDialer(p.actorUID), egress); err != nil {
		return err
	}
	s.setRunningVM(actorUID, ra)

	// After the last error return, so a failed restore stays retryable.
	maybeDropStagedMemoryImage(ctx, restoreDir, memMode, preserveRestoreDir)

	// Publish the guest to GetWorkloadStats, past the last error return above
	// for the same reason as in coldBootActor. Same client the forwarding above
	// reads over.
	s.setGuestStats(actorUID, &guestStatsTarget{actorUID: actorUID, agent: guestAC, workloadIDs: ra.workloadIDs})
	// Looked up while the caller holds the actor's lock, so it is this
	// activation.
	if hosted := s.lookupActor(actorUID); hosted != nil {
		go s.recordInitial(context.WithoutCancel(ctx), hosted)
	}

	slog.InfoContext(ctx, "Actor restored (overlay rootfs)",
		slog.String("id", actorUID), slog.Duration("total", time.Since(tStart)))
	return nil
}

// maybeDropStagedMemoryImage deletes memory-ranges after an eager restore (it is
// fully in guest memory and nothing merges against it), unless restoreDir is a
// preserved snapshot.
func maybeDropStagedMemoryImage(ctx context.Context, restoreDir, memMode string, preserveRestoreDir bool) {
	if memMode != ch.MemRestoreEager || preserveRestoreDir {
		return
	}
	staged := filepath.Join(restoreDir, "memory-ranges")
	if err := os.Remove(staged); err != nil && !os.IsNotExist(err) {
		// Not fatal: it only costs disk until the actor is torn down.
		slog.WarnContext(ctx, "could not drop the staged memory image", "error", err)
	} else {
		slog.InfoContext(ctx, "dropped the staged memory image (eager restore needs no merge base)")
	}
}

// rewriteSnapshotSocketPaths repoints the snapshot config.json's per-VMDir paths from
// the source actor's VMDir to the restoring actor's: the hybrid-vsock socket, the
// File serial console, and each virtio-fs socket, so the sockets/files we create are
// the ones CH reopens. The kernel and the kata image (virtio-pmem, or virtio-blk in
// older snapshots) are content-addressed static files with identical paths on every
// node, so they need no rewrite, and the overlay has no per-actor disk to repoint.
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
	changed := false
	if vsock, ok := cfg["vsock"].(map[string]any); ok {
		want := kata.VsockSocketPath(id)
		if got, _ := vsock["socket"].(string); got != want {
			vsock["socket"] = want
			changed = true
		}
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
			if got, _ := dev["file"].(string); got != path {
				dev["file"] = path
				changed = true
			}
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
				want := kata.VirtiofsdSocketPath(id)
				if got, _ := fm["socket"].(string); got != want {
					fm["socket"] = want
					changed = true
				}
			default:
				return fmt.Errorf("snapshot config %q has fs device with unknown tag %q", cfgPath, tag)
			}
		}
	}
	if !changed {
		// Same-actor resume: nothing to rewrite, so leave a preserved snapshot untouched.
		return nil
	}
	out, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	// Temp file + rename, not O_TRUNC, which would write through a shared inode.
	tmp, err := os.CreateTemp(snapshotDir, ".config.json.tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op once the rename succeeds
	if _, err := tmp.Write(out); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), cfgPath)
}
