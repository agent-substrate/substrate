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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateworker/phaselog"
	"go.ate.dev/substrate/internal/workerstats"

	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/ateworker/cgroup"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/internal/ateworkervalidation"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/sizing"
	"go.ate.dev/substrate/internal/wakeupprobe"
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

// reseedTimeout bounds ReseedRandomDev. It needs to cover a whole resume wave, not a
// single quiet guest.
const reseedTimeout = 60 * time.Second

// randomDevReseeder is the part of *kata.AgentClient that reseedGuestCRNG
// uses, so tests can fake it.
type randomDevReseeder interface {
	ReseedRandomDev(ctx context.Context, data []byte) error
}

// reseedGuestCRNG mixes fresh, per-restore entropy into the restored guest's kernel
// CRNG through the kata-agent (see the call site in restoreMemoryFidelity for why a restore
// needs this). The nonce is a throwaway; its only job is to differ between restores so
// that clones of one snapshot diverge instead of producing identical randomness. The
// caller owns ac: it stays open for log forwarding and guest stats.
func reseedGuestCRNG(ctx context.Context, ac randomDevReseeder) error {
	nonce, err := newReseedNonce()
	if err != nil {
		return err
	}
	rctx, cancel := context.WithTimeout(ctx, reseedTimeout)
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
//   - ROOTFS: as VOLUMES, but each container's rootfs upper is re-materialized
//     from its tar first, so the cold boot sees the rootfs writes the actor had
//     made.
//   - VOLUMES: there is no guest to resume — re-materialize the durable-dir
//     volumes and cold-boot the actor, which starts its containers afresh from
//     the OCI image.
//
// Contract with atelet: the snapshot's files are in ActorDirs.restore_dir,
// and the durable-dir volume directories re-created (empty).
func (s *AteWorkerService) RestoreWorkload(ctx context.Context, req *ateworkerpb.RestoreWorkloadRequest) (resp *ateworkerpb.RestoreWorkloadResponse, retErr error) {
	if err := ateworkervalidation.ValidateRestoreWorkloadRequest(ctx, req); err != nil {
		return nil, err
	}
	epoch, err := s.epoch.Require()
	if err != nil {
		return nil, err
	}
	if err := validateFidelity(req.GetFidelity()); err != nil {
		return nil, err
	}
	if err := validateRuntimeAssetPaths(req.GetActorUid(), req.GetActorDirs(), req.GetRuntimeAssetPaths()); err != nil {
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

	if err := s.tunnel.Deactivate(ctx, workerstats.ActorAttributionFromRequest(req)); err != nil {
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
		size:             sizing.FromLimits(req.GetResources().GetLimits()),
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
	// attributed. Only a MEMORY restore resumes the guest and its CPU counters,
	// so only it counts from the first reading; the lower fidelities cold-boot
	// a fresh guest whose CPU counts from zero.
	resumesGuest := includesMemory(req.GetFidelity())
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
	// that means before the share's virtiofsd starts, for the lower fidelities
	// before the workload cold-starts.
	if hasDurableVolumes(p.containers) {
		if err := untarDurableVolumes(durableDir, restoreDir, durableVolumeNames(p.containers)); err != nil {
			return nil, err
		}
	}

	switch scope := req.GetFidelity(); scope {
	case ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY:
		if err := s.restoreMemoryFidelity(ctx, p, scope, restoreDir, req.GetPreserveRestoreDir(), tStart); err != nil {
			return nil, err
		}
	case ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS:
		// A ROOTFS snapshot holds no guest state either, but it carries each
		// container's rootfs upper. The cold boot re-materializes the uppers
		// from the snapshot instead of starting from a pristine dir, overlapped
		// with its bundle preparation like the MEMORY path does.
		p.rootfsUpperSnapshotDir = restoreDir
		if err := s.coldBootActorRetrying(ctx, p); err != nil {
			return nil, err
		}
		dTotal := time.Since(tStart)
		slog.InfoContext(ctx, "Actor restored (rootfs upper + durable-dir volumes, cold boot)",
			slog.String("id", p.actorUID), slog.Duration("total", dTotal))
		phaselog.LogSnapshotPhases(ctx, "Restore timing breakdown", attribution, scope,
			phaselog.RestoreDurationKey, nil, []phaselog.Phase{{Name: phaseTotal, D: dTotal}})
	case ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
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
		phaselog.LogSnapshotPhases(ctx, "Restore timing breakdown", attribution, scope,
			phaselog.RestoreDurationKey, nil, []phaselog.Phase{{Name: phaseTotal, D: dTotal}})
	default:
		return nil, apierror.InvalidArgument("unsupported snapshot fidelity: %v", scope)
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor restored", attribution)
	return &ateworkerpb.RestoreWorkloadResponse{WorkerEpoch: epoch}, nil
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
func (s *AteWorkerService) restoreMemoryFidelity(ctx context.Context, p actorBootParams, scope ateworkerpb.SnapshotFidelity, restoreDir string, preserveRestoreDir bool, tStart time.Time) (retErr error) {
	actorUID := p.actorUID
	// Checked before anything is prepared or cleaned up: the snapshot's
	// kernel and guest image paths are repointed at these below.
	kernel, image := p.assetPaths[assetKernel], p.assetPaths[assetImage]
	if kernel == "" || image == "" {
		return fmt.Errorf("ateworker-microvm requires %q and %q asset paths", assetKernel, assetImage)
	}

	rr := s.resolveRuntime(p.assetPaths)
	egress, err := s.tunnel.PrepareEgress(ctx, p.attribution(), p.egressGateway)
	if err != nil {
		return err
	}
	// On failure, undo everything below that outlives the processes. This defer
	// MUST stay registered first (right after PrepareEgress): defers run last
	// in, first out, so it runs after the defers below have drained the
	// background untar and stopped cloud-hypervisor and virtiofsd. Only then is
	// it safe to sweep the sandbox state (which detaches the merged rootfs
	// overlays over the upper dir) and remove the upper dir; moving a defer
	// above this one would remove the upper under a live untar or overlay.
	defer func() {
		if retErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if cleanupErr := s.tunnel.Deactivate(cleanupCtx, p.attribution()); cleanupErr != nil {
				slog.WarnContext(cleanupCtx, "Failed to deactivate actor networking after Restore failure", slog.Any("err", cleanupErr))
			}
			s.cleanupFailedRestoreRootfs(cleanupCtx, actorUID, p.actorDirs)
		}
	}()
	s.cleanupSandboxState(ctx, actorUID)

	// Repoint the snapshot's per-actor paths to this actor's: its VMDir
	// sockets and files, and the kernel and guest image atelet staged under
	// this actor's own directory.
	if err := rewriteSnapshotPaths(restoreDir, actorUID, kernel, image); err != nil {
		return fmt.Errorf("while rewriting snapshot paths: %w", err)
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
	// rewriteSnapshotPaths above. Cross-node consistency relies on a
	// deterministic unpack of the same image at the same <cid>/rootfs path
	// (plus, for merged rootfs, the upper re-materialized from the tar).
	containers := p.containers
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
	leaf, err := cgroup.OpenActorLeaf(actorUID, p.size.MilliCPU)
	if err != nil {
		return err
	}
	defer leaf.Close()
	vfsdProc, err := s.stageMergedRootfs(ctx, rr, actorUID, p.actorDirs, ctrs, containers, leaf.SysProcAttr())
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			vfsdProc.Stop()
		}
	}()

	tLowers := time.Now()
	tDurable := tLowers

	// Networking: rebuild the actor's namespace; the snapshot's virtio-net is
	// fd-backed, so CH needs fresh tap FDs (net_fds) on restore. The caller
	// unhosts on failure, once the defers here have stopped the actor's
	// processes.
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

	// Relaunch CH and restore with the tap FDs attached (SCM_RIGHTS).
	apiSocket := filepath.Join(kata.VMDir(actorUID), "clh-api-restore.sock")
	tTap := time.Now()
	chProc, client, err := ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
		Binary: rr.chBinary, APISocket: apiSocket, Stdout: slogWriter{ctx}, Stderr: slogWriter{ctx},
		SysProcAttr: leaf.SysProcAttr(),
	})
	if err != nil {
		return fmt.Errorf("while launching VMM for restore: %w", err)
	}
	defer func() {
		if retErr != nil {
			chProc.Stop()
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
	// An eager restore has read the whole snapshot into guest memory, and nothing
	// merges against it afterwards, so drop the staged memory image and evict its
	// page cache before resuming the VM so guest RAM and snapshot page cache do
	// not overlap during Resume and wakeup probe gating.
	//
	// When the restore dir is not preserved, dropping ahead of Resume means a
	// failure after this point costs a re-download of the snapshot on retry.
	maybeDropStagedMemoryImage(ctx, restoreDir, memMode, preserveRestoreDir)
	dropActorSnapshotCacheAsync(restoreDir, memMode == ch.MemRestoreEager)
	if err := client.Resume(ctx); err != nil {
		return restoreGuestReadyError(ctx, "resuming restored guest", err)
	}
	tResume := time.Now()

	// One kata-agent connection serves this whole activation: the CRNG reseed below,
	// then log forwarding and guest stats. As on cold boot, not reaching the agent
	// fails the restore. A failing restore closes the connection on its way out.
	guestAC, err := dialAgentRetry(ctx, kata.VsockSocketPath(actorUID), 15*time.Second)
	if err != nil {
		return restoreGuestReadyError(ctx, "dialing kata-agent after resume", err)
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
	//
	// Egress opens only once the reseed succeeds, so nothing leaves the host drawn
	// from CRNG state the actor shares with the snapshot's other clones, and a
	// failed reseed is still safe to retry (see restoreGuestReadyError). The
	// restored workload is past its boot, so it needs egress only from the wakeup
	// probe on. Ingress follows the wakeup probe, as in coldBootActor.
	if err := reseedGuestCRNG(ctx, guestAC); err != nil {
		return restoreGuestReadyError(ctx, "reseeding guest CRNG", err)
	}
	if err := s.tunnel.ActivateEgress(p.attribution(), egress); err != nil {
		return err
	}
	tReseeded := time.Now()

	// Block until every wakeup-probe-enabled container reports 200.
	if err := wakeupprobe.WaitAll(ctx, containers, actornet.ActorVethIP, wakeupprobe.DialFunc(s.sandboxDialer(actorUID))); err != nil {
		return fmt.Errorf("while waiting for container wakeup probe: %w", err)
	}

	// Where a resume goes. Like the boot phases, this used to be a single total,
	// which hid that a first (cold) restore and a later (warm) one differ by more
	// than 5x on the same actor. upper/lowers is the host reassembling the rootfs;
	// vm_restore is cloud-hypervisor reading guest RAM back; reseed is the kata-agent dial
	// and the CRNG reseed, the head of wakeup_probe.
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
		slog.Duration("reseed", tReseeded.Sub(tResume)),
		slog.Duration("wakeup_probe", dWakeupProbe),
		slog.Duration("total", dTotal))
	// The joinable per-actor record the benchmarking tooling aggregates. The
	// durable delta is not carried: tDurable is pinned to tLowers today, so it
	// would always be the zero a record skips.
	phaselog.LogSnapshotPhases(ctx, "Restore timing breakdown", p.actorAttribution(), scope,
		phaselog.RestoreDurationKey, nil, []phaselog.Phase{
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
		chProc: chProc, vfsdProc: vfsdProc,
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
	for _, c := range ctrs {
		s.startActorLogForwarding(guestAC, attribution, c.id, c.name)
	}

	if err := s.tunnel.ActivateIngress(p.attribution(), s.sandboxDialer(p.actorUID)); err != nil {
		return err
	}
	s.setRunningVM(actorUID, ra)
	go s.watchVM(p.attribution(), p.actorDirs, ra)

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

// maybeDropStagedMemoryImage deletes the staged memory-ranges file and
// already-extracted tar archives after an eager restore (it is fully in guest
// memory and nothing merges against it), unless restoreDir is a preserved
// snapshot.
func maybeDropStagedMemoryImage(ctx context.Context, restoreDir, memMode string, preserveRestoreDir bool) {
	if memMode != ch.MemRestoreEager || preserveRestoreDir {
		return
	}
	dropStagedMemoryImage(ctx, restoreDir)
}

// cleanupFailedRestoreRootfs removes what a failed memory restore assembled on
// the host for the actor's rootfs, mirroring teardownActor: it sweeps the
// sandbox state (detaching the merged rootfs overlays), removes the rootfs
// upper dir re-materialized from the snapshot, and detaches the bundle rootfs
// overlays mounted by buildActorContainers. The caller must have stopped the
// actor's VMM and virtiofsd and drained the upper dir's untar first.
func (s *AteWorkerService) cleanupFailedRestoreRootfs(ctx context.Context, actorUID string, actorDirs *ateworkerpb.ActorDirs) {
	s.cleanupSandboxState(ctx, actorUID)
	if err := os.RemoveAll(rootfsUpperDir(actorDirs)); err != nil {
		slog.WarnContext(ctx, "Failed to remove rootfs upper dir after Restore failure", slog.String("actorUID", actorUID), slog.Any("err", err))
	}
	if err := imagecache.UnmountAllUnder(actorDirs.GetOciBundleDir()); err != nil {
		slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays after Restore failure", slog.String("actorUID", actorUID), slog.Any("err", err))
	}
}

// restoreGuestReadyError classifies a failure to resume the restored micro-VM or
// reach its kata-agent before the workload serves any inbound traffic. Because
// the restoreMemoryFidelity and RestoreWorkload defers tear down the VMM,
// virtiofsd, rootfs mounts, network, and hosted-actor slot on the way out, the
// failed attempt leaves no worker-local state behind and is safe for the caller
// to retry (re-staging the snapshot if it was dropped above). When the caller's
// own context ended, wrap ctx.Err() so the server reports the context status
// instead.
//
// The guest did run, though: after Resume, it and the workload in it run while
// the agent is dialed and the CRNG reseeded (up to about 15s plus
// reseedTimeout). Nothing routes inbound traffic to it yet (not even the wakeup
// probe), and atunnel's egress proxy closes its outbound TCP connections until
// tunnel.ActivateEgress, which restoreMemoryFidelity calls after the reseed for
// that reason, but a background task in the guest can query the DNS relay or
// write to its filesystems. The retry re-extracts the rootfs upper and
// durable-dir volumes from the snapshot, whereas a CSI volume is outside the
// snapshot and keeps any writes made during that window while guest memory and
// its page cache resume from the snapshot; we accept that narrow window over
// crashing the actor on a transient host stall. When the reseed is what failed,
// the guest also ran on CRNG state shared with the snapshot's other clones.
//
// A guest found stopped while dialing the agent (errGuestStopped:
// cloud-hypervisor exited, e.g. a guest kernel panic after resume or a corrupt
// memory image) is not reported as Unavailable: the same snapshot would likely
// stop the guest again on every retry. coldBootActorRetrying likewise caps
// retries of that case, at coldBootAttempts. A stop that surfaces in Resume or
// the reseed instead is reported as Unavailable, and the caller's limit on
// consecutive retryable failures bounds it.
func restoreGuestReadyError(ctx context.Context, step string, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("while %s: %v: %w", step, err, ctx.Err())
	}
	if errors.Is(err, errGuestStopped) {
		return fmt.Errorf("while %s: %w", step, err)
	}
	return apierror.Unavailable("while %s: %w", step, err)
}

// rewriteSnapshotPaths repoints the snapshot config.json's per-actor paths from
// the source actor's to the restoring actor's. The file is CH's own: at
// snapshot time it serializes the config the VM booted with, and at restore
// it reopens every path in it, so each path must exist on this node for
// this actor. That covers the hybrid-vsock socket, the File serial console,
// and each virtio-fs socket under the VMDir, and the kernel and the guest
// image, which atelet stages under each actor's own directory and removes
// with the actor, so the source actor's copies may be gone. How atelet
// identifies the runtime (asset URLs today, a SandboxConfig hash later)
// changes none of this. The overlay has no per-actor disk to repoint.
func rewriteSnapshotPaths(snapshotDir, id, kernel, image string) error {
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
	// ateworker captures the guest console to a file under the source actor's VMDir
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
	// Every VM this worker boots has a payload with the kernel and a pmem
	// device with the guest image (see buildVMConfig), and both paths are
	// per-actor, so a snapshot config without them is broken: skipping the
	// rewrite would hand CH the source actor's paths.
	payload, ok := cfg["payload"].(map[string]any)
	if !ok {
		return fmt.Errorf("snapshot config %q has no payload", cfgPath)
	}
	if got, _ := payload["kernel"].(string); got != kernel {
		payload["kernel"] = kernel
		changed = true
	}
	pmems, _ := cfg["pmem"].([]any)
	guestImage := false
	for _, pm := range pmems {
		pmm, ok := pm.(map[string]any)
		if !ok {
			return fmt.Errorf("snapshot config %q has a malformed pmem device", cfgPath)
		}
		switch pmID, _ := pmm["id"].(string); pmID {
		case guestImagePmemID:
			guestImage = true
			if got, _ := pmm["file"].(string); got != image {
				pmm["file"] = image
				changed = true
			}
		default:
			return fmt.Errorf("snapshot config %q has pmem device with unknown id %q", cfgPath, pmID)
		}
	}
	if !guestImage {
		return fmt.Errorf("snapshot config %q has no %q pmem device", cfgPath, guestImagePmemID)
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
