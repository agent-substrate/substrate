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
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateomphaselog"
	"github.com/agent-substrate/substrate/internal/ateomstats"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"golang.org/x/sync/errgroup"
)

// CheckpointWorkload suspends the actor and writes a portable snapshot.
//
// Contract with atelet: after we return, atelet uploads the checkpoint dir to object
// storage, then tears down bundles and resets the actor dir.
//
// What the snapshot holds depends on the requested scope:
//
//   - FULL: the whole guest. ateom drives the CH REST api-socket: pause -> snapshot
//     file://<checkpoint_dir> (config.json + state.json + sparse memory-ranges)
//     -> tear the VMM down. Each container's rootfs is overlay(virtio-fs RO lower +
//     disk-backed upper): the upper is host-backed like the durable-dir volumes and
//     ships alongside as its own tar (see rootfsupper.go); process memory persists
//     via the memory snapshot. The RO lower is reconstructed from the OCI image at
//     restore, so it never ships. Durable-dir volumes ship alongside as per-volume tars.
//   - DATA: the durable-dir volumes only, as those same tars. The guest is discarded, so
//     the actor cold-starts on restore with its volumes re-materialized.
//
// Either way the guest is paused first, which is what makes the tar coherent: the
// durable share is served write-through, so every completed guest write is already on
// the host and no further ones can arrive.
//
// Allow checkpointing even if the pod is shutting down. This will allow actors
// (or the harness) to suspend on shutdown.
func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (_ *ateompb.CheckpointWorkloadResponse, err error) {
	if err := validateActorDirs(req.GetActorDirs()); err != nil {
		return nil, err
	}
	if !s.locks.Lock(ctx, req.GetActorUid()) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(req.GetActorUid())

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer s.inFlight.Add(req.GetActorUid(), rpcCheckpointWorkload, nil)()

	// Per-phase timing, recorded on the way out so a failed checkpoint still
	// reports the phases it completed, and the failing step its elapsed time.
	// Phases left at zero never ran. The snapshot, durable_dir and rootfs_upper
	// captures run concurrently on the paused guest, so those three are
	// independent observations rather than a partition of the total.
	tStart := time.Now()
	var dPrep, dPause, dSnapshot, dDurable, dUpper, dTeardown time.Duration
	attribution := ateomstats.ActorAttributionFromRequest(req)
	scope := req.GetScope()
	defer func() {
		ateomphaselog.LogSnapshotPhases(ctx, "Checkpoint timing breakdown", attribution, scope,
			ateomphaselog.CheckpointDurationKey, err, []ateomphaselog.Phase{
				{Name: phasePrep, D: dPrep},
				{Name: phasePause, D: dPause},
				{Name: phaseSnapshot, D: dSnapshot},
				{Name: phaseDurableDir, D: dDurable},
				{Name: phaseRootfsUpper, D: dUpper},
				{Name: phaseTeardown, D: dTeardown},
				{Name: phaseTotal, D: time.Since(tStart)},
			})
	}()

	if err := s.tunnel.Deactivate(ctx, attribution); err != nil {
		return nil, err
	}

	actorUID := req.GetActorUid()
	actorDirs := req.GetActorDirs()

	s.actorLogger.EmitLifecycleLog(ctx, "Actor checkpointing", attribution)

	// Check what the request asks for BEFORE touching the guest: these are
	// properties of the request, and pausing first would leave the actor
	// suspended mid-flight for a call that could never have succeeded.
	//
	// Durable-dir volumes are host-backed, so they are captured the same way
	// under either scope — and are the ONLY thing a Data-scope snapshot
	// captures.
	durable := hasDurableVolumes(req.GetSpec().GetContainers())
	csi := hasCsiVolumes(req.GetSpec().GetContainers())
	switch scope {
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL:
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
		// TODO: Revisit handling for CSI volumes since snapshots are currently quietly ignored.
		if !durable && !csi {
			return nil, apierror.FailedPrecondition(
				"no durable-dir or CSI volumes found for a Data-scope snapshot")
		}
	default:
		return nil, apierror.InvalidArgument("unsupported snapshot scope: %v", scope)
	}

	// Captured now: the checkpoint unhosts the actor, and the final record
	// waits for the teardown.
	hosted := s.lookupActor(actorUID)

	// The actor's CH was booted by RunWorkload or relaunched by RestoreWorkload;
	// either way ateom owns it and tracks its api-socket.
	ra := s.runningVM(actorUID)
	chSocket := kata.CLHSocketPath(actorUID)
	if ra != nil && ra.apiSocket != "" {
		chSocket = ra.apiSocket
	}
	client := ch.NewClient(chSocket)
	if _, err := client.WaitReady(ctx, 10*time.Second); err != nil {
		return nil, fmt.Errorf("while waiting for CH api-socket: %w", err)
	}
	// Read before the pause: a paused guest cannot answer.
	if hosted != nil {
		s.readFinal(ctx, hosted)
	}

	tPause := time.Now()
	dPrep = tPause.Sub(tStart)
	pauseErr := client.Pause(ctx)
	dPause = time.Since(tPause)
	if pauseErr != nil {
		return nil, fmt.Errorf("while pausing guest: %w", pauseErr)
	}

	checkpointDir := actorDirs.GetCheckpointDir()
	// Start from a clean dir so CH's snapshot files are the only contents.
	if err := os.RemoveAll(checkpointDir); err != nil {
		return nil, fmt.Errorf("while clearing checkpoint dir %q: %w", checkpointDir, err)
	}
	if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
		return nil, fmt.Errorf("while creating checkpoint dir %q: %w", checkpointDir, err)
	}

	// Capture the snapshot's pieces CONCURRENTLY: the CH snapshot, the
	// durable-dir tars, and the rootfs upper tars read independent data from a
	// quiesced guest and write distinct files into checkpointDir, so the paused
	// window costs the slowest of them rather than their sum (the tars scale
	// with the actor's data; suspend latency is the metric that matters).
	//
	//   - CH snapshot (Full only): the guest memory + VM state. A Data snapshot
	//     deliberately captures no VM state — no memory image, and no base-id,
	//     since nothing will reattach to the frozen virtio-fs lower: at restore
	//     the actor cold-boots from the OCI image.
	//   - Durable-dir tars (any scope, when declared): host-backed, so pausing
	//     the write-through share makes the tars coherent.
	//   - Rootfs upper tars (Full only): host-backed like the durable volumes —
	//     the memory snapshot does not carry rootfs writes. Under Data the
	//     workload cold-starts on restore, discarding rootfs state.
	var durableFiles []string
	g, gctx := errgroup.WithContext(ctx)
	if scope == ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL {
		g.Go(func() error {
			t := time.Now()
			d, err := s.snapshotVMState(gctx, client, ra, actorUID, checkpointDir)
			if err != nil {
				d = time.Since(t)
			}
			dSnapshot = d
			return err
		})
	}
	if durable {
		g.Go(func() error {
			t := time.Now()
			var err error
			durableFiles, err = tarDurableVolumes(gctx, actorDirs.GetDurableDirVolumeMountsDir(), checkpointDir, durableVolumeNames(req.GetSpec().GetContainers()))
			dDurable = time.Since(t)
			return err
		})
	}
	if scope == ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL {
		g.Go(func() error {
			t := time.Now()
			err := tarRootfsUpper(gctx, rootfsUpperDir(actorDirs), checkpointDir, containerNames(req.GetSpec().GetContainers()))
			dUpper = time.Since(t)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Report exactly the files we wrote so atelet ships precisely this snapshot: for
	// Full, the CH snapshot (config.json + state.json + memory-ranges + base-id) plus
	// any durable-dir tars; for Data, those tars alone.
	snapshotFiles, err := listFiles(checkpointDir)
	if err != nil {
		return nil, fmt.Errorf("while listing snapshot files: %w", err)
	}

	// Tear down: the actor returns to "available". Best-effort; the snapshot is
	// already on disk for atelet to ship.
	tTeardown := time.Now()
	if err := s.terminateWorkload(ctx, attribution, actorDirs); err != nil {
		slog.WarnContext(ctx, "failed to terminate workload after checkpoint",
			slog.String("actor", attribution.Ref.String()),
			slog.String("actorUID", actorUID),
			slog.Any("err", err))
	}
	dTeardown = time.Since(tTeardown)

	s.actorLogger.EmitLifecycleLog(ctx, "Actor checkpointed", attribution)
	slog.InfoContext(ctx, "Actor checkpointed", slog.String("id", actorUID), slog.Any("snapshot_files", snapshotFiles),
		slog.String("scope", scope.String()), slog.Duration("pause", dPause),
		slog.Duration("snapshot", dSnapshot),
		// The tars run while the guest is paused, CONCURRENTLY with the CH
		// snapshot: the paused window costs max(snapshot, durable_dir,
		// rootfs_upper), and the tar durations scale with the actor's data.
		slog.Duration("durable_dir", dDurable), slog.Duration("rootfs_upper", dUpper),
		slog.Duration("teardown", dTeardown))
	s.recordFinalIfEnded(ctx, hosted)
	return &ateompb.CheckpointWorkloadResponse{SnapshotFiles: snapshotFiles, DataSnapshotFiles: durableFiles}, nil
}

// snapshotVMState captures the paused guest into checkpointDir: the CH snapshot
// (config.json + state.json + memory-ranges) plus the base-id the restore side
// needs, and returns how long the snapshot itself took.
func (s *AteomService) snapshotVMState(ctx context.Context, client *ch.Client, ra *runningActor, actorUID, checkpointDir string) (time.Duration, error) {
	// Record the FROZEN base id (the id the guest's virtio-fs find-paths are pinned
	// to, <baseID>/rootfs). For a cold-run actor this is its own id; for a restored
	// actor it is the golden id propagated via ra.baseID (set from the snapshot we
	// restored from). RestoreWorkload reads this to lay the
	// reconstructed-from-image base at the path the guest expects. We can NOT derive
	// it from config.json (its socket paths get rewritten to the current id on every
	// restore, losing the invariant golden id).
	baseID := actorUID
	if ra != nil && ra.baseID != "" {
		baseID = ra.baseID
	}
	if err := os.WriteFile(filepath.Join(checkpointDir, baseIDFile), []byte(baseID), 0o600); err != nil {
		return 0, fmt.Errorf("while writing %s: %w", baseIDFile, err)
	}

	slog.InfoContext(ctx, "Snapshotting guest", slog.String("id", actorUID), slog.String("dir", checkpointDir))
	tSnapshot := time.Now()
	if err := client.Snapshot(ctx, checkpointDir); err != nil {
		return 0, fmt.Errorf("while snapshotting guest: %w", err)
	}
	dSnapshot := time.Since(tSnapshot)

	// Diff-snapshot completion for an OnDemand-restored actor: CH's snapshot here is
	// sparse — only the pages faulted in since the OnDemand restore — so on its own
	// it's INCOMPLETE (the un-faulted pages were being demand-paged from the restore
	// source). Overlay it onto that source to rebuild a COMPLETE memory-ranges, so the
	// snapshot is self-contained and re-restorable. (A cold-run actor has no restore
	// source and its snapshot is already complete — no merge.)
	if ra != nil && ra.snapshotIsSelfContained {
		// Eager restore already pulled every populated extent into guest memory, so
		// what cloud-hypervisor just wrote is the whole guest, not a delta. Merging
		// would copy the entire resident set onto the restore source for nothing.
		slog.InfoContext(ctx, "Snapshot is self-contained (eager restore); skipping merge",
			slog.String("id", actorUID))
	} else if ra != nil && ra.restoreSourceDir != "" {
		tMerge := time.Now()
		if err := mergeOnDemandDelta(ctx, ra.restoreSourceDir, checkpointDir, ra.preserveRestoreSource); err != nil {
			return 0, fmt.Errorf("while merging OnDemand delta into restore source: %w", err)
		}
		slog.InfoContext(ctx, "Merged OnDemand delta into base (complete snapshot)",
			slog.String("id", actorUID), slog.Duration("merge", time.Since(tMerge)))
	}

	// The RO lower never ships (reconstructed from the OCI image at restore).
	// The disk-backed upper ships as its own tar from CheckpointWorkload; a
	return dSnapshot, nil
}

// mergeOnDemandDelta merges the OnDemand delta in checkpointDir with the base in
// restoreSourceDir. A preserved base is copied, never modified in place.
func mergeOnDemandDelta(ctx context.Context, restoreSourceDir, checkpointDir string, preserveRestoreSource bool) error {
	base := filepath.Join(restoreSourceDir, "memory-ranges")
	delta := filepath.Join(checkpointDir, "memory-ranges")
	if preserveRestoreSource {
		return ch.MergeSparseOverlay(ctx, base, delta, delta)
	}
	return ch.MergeDeltaIntoBase(ctx, base, delta)
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

// teardownActor stops the ateom-owned CH VMM for an actor. ra may be
// nil (e.g. ateom restarted and lost in-memory state).
func (s *AteomService) teardownActor(ctx context.Context, id string, actorDirs *ateompb.ActorDirs, ra *runningActor, client *ch.Client) error {
	// Stop offering the guest to GetWorkloadStats first, before anything below
	// makes it stop answering. Clearing it here rather than alongside the
	// attribution is what keeps a poll that lands mid-teardown on the
	// FAILED_PRECONDITION path ("no numbers right now") instead of surfacing a
	// closed connection as a failed read.
	s.setGuestStats(id, nil)

	var errs []error
	if client != nil {
		tShutdown := time.Now()
		shutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := client.Shutdown(shutCtx); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				slog.WarnContext(ctx, "CH shutdown returned error during teardown", slog.Any("err", err))
			}
		}
		cancel()
		slog.InfoContext(ctx, "CH API shutdown done", slog.Duration("took", time.Since(tShutdown)))
	}

	if ra != nil {
		// Close the kata-agent client kept open for stdout/stderr forwarding. This
		// fails the forwarding goroutines' in-flight ReadStdout/ReadStderr calls, so
		// they return io.EOF and exit (no goroutine leak). Guarded so a second
		// teardown / a never-forwarded actor is a no-op.
		s.actorsMu.Lock()
		agent := ra.guestAgent
		ra.guestAgent = nil
		s.actorsMu.Unlock()
		if agent != nil {
			_ = agent.Close()
		}

		// Kill the CH process ateom launched.
		if ra.chCmd != nil && ra.chCmd.Process != nil {
			_ = ra.chCmd.Process.Kill()
			_, _ = ra.chCmd.Process.Wait()
		}
		// Kill the virtiofsd (after CH, its only client).
		if ra.vfsdCmd != nil && ra.vfsdCmd.Process != nil {
			_ = ra.vfsdCmd.Process.Kill()
			_, _ = ra.vfsdCmd.Process.Wait()
		}
	}

	// Sweep any leftover per-sandbox host-side state + orphaned per-sandbox
	// processes. This is ateom's own cleanup (process kill + unmount + rm) —
	// it also drops the merged rootfs overlay mounts, which MUST come before
	// the upper-dir removal below (removing a live overlay's upperdir would
	// corrupt the mount rather than delete the files).
	s.cleanupSandboxState(ctx, id)

	// Remove the rootfs upper dir: ateom owns it — atelet's actor-dir reset
	// doesn't know it — and its absence is what marks a worker as holding no
	// disk-backed upper. Runs after the checkpoint tar, which is already on disk.
	if err := os.RemoveAll(rootfsUpperDir(actorDirs)); err != nil {
		slog.WarnContext(ctx, "Failed to remove rootfs upper dir", slog.String("actorUID", id), slog.Any("err", err))
	}

	// Detach the bundle rootfs overlays composed in buildActorContainers, so
	// atelet's bundle wipe doesn't strand live mounts in this namespace.
	if err := imagecache.UnmountAllUnder(actorDirs.GetOciBundleDir()); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("while unmounting bundle rootfs overlays: %w", err))
		}
	}
	return errors.Join(errs...)
}

// TerminateWorkload stops the running actor, tears down its VMM, and cleans up
// networking and overlays.
func (s *AteomService) TerminateWorkload(ctx context.Context, req *ateompb.TerminateWorkloadRequest) (*ateompb.TerminateWorkloadResponse, error) {
	if err := validateActorDirs(req.GetActorDirs()); err != nil {
		return nil, err
	}
	if !s.locks.Lock(ctx, req.GetActorUid()) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(req.GetActorUid())

	attribution := ateomstats.ActorAttributionFromRequest(req)

	hosted := s.lookupActor(attribution.UID)
	if hosted != nil {
		s.readFinal(ctx, hosted)
	}
	err := s.terminateWorkload(ctx, attribution, req.GetActorDirs())
	s.recordFinalIfEnded(ctx, hosted)
	if err != nil {
		return nil, fmt.Errorf("failed to terminate workload: %w", err)
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor terminated", attribution)

	return &ateompb.TerminateWorkloadResponse{}, nil
}

// endPreviousActivation ends the activation a re-host is about to replace. A VM
// still running for the actor would be dropped from tracking by the re-host and
// left running, so it is stopped, read first for the final record. This happens
// after a checkpoint that failed before its teardown, or on a retried Run or
// Restore. Ending the activation also stops a late recordInitial from writing.
func (s *AteomService) endPreviousActivation(ctx context.Context, actorUID string, actorDirs *ateompb.ActorDirs) error {
	old := s.lookupActor(actorUID)
	if s.runningVM(actorUID) != nil {
		if old != nil {
			s.readFinal(ctx, old)
		}
		if err := s.stopActorVM(ctx, actorUID, actorDirs); err != nil {
			return fmt.Errorf("while stopping the actor's previous micro-VM: %w", err)
		}
	}
	if old != nil {
		s.recordFinal(ctx, old)
	}
	return nil
}

// stopActorVM tears down the actor's micro-VM, if any, keeping it hosted.
func (s *AteomService) stopActorVM(ctx context.Context, actorUID string, actorDirs *ateompb.ActorDirs) error {
	ra := s.runningVM(actorUID)
	chSocket := kata.CLHSocketPath(actorUID)
	if ra != nil && ra.apiSocket != "" {
		chSocket = ra.apiSocket
	}
	return s.teardownActor(ctx, actorUID, actorDirs, ra, ch.NewClient(chSocket))
}

func (s *AteomService) terminateWorkload(ctx context.Context, actor resources.ActorAttribution, actorDirs *ateompb.ActorDirs) error {
	var errs []error
	if err := s.tunnel.Deactivate(ctx, actor); err != nil {
		errs = append(errs, fmt.Errorf("while deactivating actor networking: %w", err))
	}

	actorUID := actor.UID
	if err := s.stopActorVM(ctx, actorUID, actorDirs); err != nil {
		errs = append(errs, fmt.Errorf("while tearing down actor: %w", err))
	}
	// Remove attribution after teardown; a failed checkpoint may leave the VM running.
	if err := s.unhostActor(ctx, actorUID); err != nil {
		errs = append(errs, fmt.Errorf("while cleaning up actor network: %w", err))
	}
	return errors.Join(errs...)
}
