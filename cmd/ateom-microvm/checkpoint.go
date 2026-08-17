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
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CheckpointWorkload suspends the actor and writes a portable snapshot.
//
// Contract with atelet: after we return, atelet uploads the checkpoint dir to object
// storage, then tears down bundles and resets the actor dir.
//
// What the snapshot holds depends on the requested scope:
//
//   - FULL: the whole guest. ateom drives the CH REST api-socket: pause -> snapshot
//     file://<CheckpointStateDir> (config.json + state.json + sparse memory-ranges)
//     -> tear the VMM down. Each container's rootfs is overlay(virtio-fs RO lower +
//     disk-backed upper): the upper is host-backed like the durable-dir volumes and
//     ships alongside as its own tar (see rootfsupper.go); process memory persists
//     via the memory snapshot. The RO lower is reconstructed from the OCI image at
//     restore, so it never ships. Durable-dir volumes ship alongside as a tar.
//   - DATA: the durable-dir volumes only, as that same tar. The guest is discarded, so
//     the actor cold-starts on restore with its volumes re-materialized.
//
// Either way the guest is paused first, which is what makes the tar coherent: the
// durable share is served write-through, so every completed guest write is already on
// the host and no further ones can arrive.
//
// Allow checkpointing even if the pod is shutting down. This will allow actors
// (or the harness) to suspend on shutdown.
func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.setActiveRPC(rpcCheckpointWorkload, cancel)
	defer s.clearActiveRPC()

	if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}

	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}
	actorUID := req.GetActorUid()
	templateNS := req.GetActorTemplateNamespace()
	templateName := req.GetActorTemplateName()

	s.actorLogger.EmitLifecycleLog("Actor checkpointing", actorRef, actorUID, templateNS, templateName)

	// Check what the request asks for BEFORE touching the guest: these are
	// properties of the request, and pausing first would leave the actor
	// suspended mid-flight for a call that could never have succeeded.
	//
	// Durable-dir volumes are host-backed, so they are captured the same way
	// under either scope — and are the ONLY thing a Data-scope snapshot
	// captures. DATA_ON_GOLDEN is restore-only (a DataOnGolden commit arrives
	// here as plain DATA) and lands in the default rejection.
	durable := hasDurableVolumes(req.GetSpec().GetContainers())
	scope := req.GetScope()
	switch scope {
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL:
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
		if !durable {
			return nil, status.Error(codes.FailedPrecondition,
				"no durable-dir volumes found for a Data-scope snapshot")
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported snapshot scope: %v", scope)
	}

	// The actor's CH was booted by RunWorkload or relaunched by RestoreWorkload;
	// either way ateom owns it and tracks its api-socket.
	ra := s.running[actorUID]
	chSocket := kata.CLHSocketPath(actorUID)
	if ra != nil && ra.apiSocket != "" {
		chSocket = ra.apiSocket
	}
	client := ch.NewClient(chSocket)
	if _, err := client.WaitReady(ctx, 10*time.Second); err != nil {
		return nil, fmt.Errorf("while waiting for CH api-socket: %w", err)
	}

	tPause := time.Now()
	if err := client.Pause(ctx); err != nil {
		return nil, fmt.Errorf("while pausing guest: %w", err)
	}
	dPause := time.Since(tPause)

	checkpointDir := ateompath.CheckpointStateDir(actorUID)
	// Start from a clean dir so CH's snapshot files are the only contents.
	if err := os.RemoveAll(checkpointDir); err != nil {
		return nil, fmt.Errorf("while clearing checkpoint dir %q: %w", checkpointDir, err)
	}
	if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
		return nil, fmt.Errorf("while creating checkpoint dir %q: %w", checkpointDir, err)
	}

	// Capture the snapshot's pieces CONCURRENTLY: the CH snapshot, the
	// durable-dir tar, and the rootfs upper tar read independent data from a
	// quiesced guest and write distinct files into checkpointDir, so the paused
	// window costs the slowest of them rather than their sum (the tars scale
	// with the actor's data; suspend latency is the metric that matters).
	//
	//   - CH snapshot (Full only): the guest memory + VM state. A Data snapshot
	//     deliberately captures no VM state — no memory image, and no base-id,
	//     since nothing will reattach to the frozen virtio-fs lower: at restore
	//     the actor cold-boots from the OCI image (or, under an OnGolden data
	//     resume policy, is combined with the golden snapshot's guest state).
	//   - Durable-dir tar (any scope, when declared): host-backed, so pausing
	//     the write-through share makes the tar coherent.
	//   - Rootfs upper tar (Full only): host-backed like the durable volumes —
	//     the memory snapshot does not carry rootfs writes. Under Data the
	//     workload cold-starts on restore, discarding rootfs state. Gated on
	//     the host dir a disk-upper boot creates (actorHasDiskUpper) so a
	//     legacy actor restored from a tmpfs-upper snapshot checkpoints
	//     correctly (its upper is inside the memory image).
	var dSnapshot, dDurable, dUpper time.Duration
	g, gctx := errgroup.WithContext(ctx)
	if scope == ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL {
		g.Go(func() error {
			var err error
			dSnapshot, err = s.snapshotVMState(gctx, client, ra, actorUID, checkpointDir)
			return err
		})
	}
	if durable {
		g.Go(func() error {
			t := time.Now()
			if err := tarDurableVolumes(gctx, ateompath.DurableDirVolumeMountsDir(actorUID), checkpointDir); err != nil {
				return err
			}
			dDurable = time.Since(t)
			return nil
		})
	}
	if scope == ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL && actorHasDiskUpper(actorUID) {
		g.Go(func() error {
			t := time.Now()
			if err := tarRootfsUpper(gctx, rootfsUpperDir(actorUID), checkpointDir); err != nil {
				return err
			}
			dUpper = time.Since(t)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Report exactly the files we wrote so atelet ships precisely this snapshot: for
	// Full, the CH snapshot (config.json + state.json + memory-ranges + base-id) plus
	// any durable-dir tar; for Data, that tar alone.
	snapshotFiles, err := listFiles(checkpointDir)
	if err != nil {
		return nil, fmt.Errorf("while listing snapshot files: %w", err)
	}

	// Tear down: the actor returns to "available". Best-effort; the snapshot is
	// already on disk for atelet to ship.
	tTeardown := time.Now()
	s.teardownActor(ctx, actorUID, ra, client)
	dTeardown := time.Since(tTeardown)
	delete(s.running, actorUID)

	// The guest is gone as of the teardown above, so the ateom is back to
	// "available": there is nothing left to measure, and holding the attribution
	// would let a later GetWorkloadStats report a checkpointed actor as though it
	// were still running.
	//
	// Nothing above this point clears it, unlike the gVisor ateom, which clears
	// as soon as its checkpoint call has taken the sandbox down. Here the guest
	// is only paused until this teardown, so a checkpoint that failed earlier has
	// left it present, and reporting its usage is then the honest answer. This is
	// the same point at which the running entry goes away, which is what keeps
	// the two views of "is an actor here" from disagreeing.
	s.activeActor.Store(nil)

	// Tear down the per-activation actor network.
	if err := ateomnet.CleanupActorNetwork(ctx, s.interiorNetNS); err != nil {
		slog.WarnContext(ctx, "Failed to clean up actor network after checkpoint", slog.Any("err", err))
	}

	s.actorLogger.EmitLifecycleLog("Actor checkpointed", actorRef, actorUID, templateNS, templateName)
	slog.InfoContext(ctx, "Actor checkpointed", slog.String("id", actorUID), slog.Any("snapshot_files", snapshotFiles),
		slog.String("scope", scope.String()), slog.Duration("pause", dPause),
		slog.Duration("snapshot", dSnapshot),
		// The tars run while the guest is paused, CONCURRENTLY with the CH
		// snapshot: the paused window costs max(snapshot, durable_dir,
		// rootfs_upper), and the tar durations scale with the actor's data.
		slog.Duration("durable_dir", dDurable), slog.Duration("rootfs_upper", dUpper),
		slog.Duration("teardown", dTeardown))
	return &ateompb.CheckpointWorkloadResponse{SnapshotFiles: snapshotFiles}, nil
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
		base := filepath.Join(ra.restoreSourceDir, "memory-ranges")
		delta := filepath.Join(checkpointDir, "memory-ranges")
		tMerge := time.Now()
		// Reuse base's on-disk working set (rename + overlay) instead of copying it —
		// CH is paused and about to be torn down, and base is discarded after. See
		// MergeDeltaIntoBase. (Falls back to the copying merge across filesystems.)
		if err := ch.MergeDeltaIntoBase(ctx, base, delta); err != nil {
			return 0, fmt.Errorf("while merging OnDemand delta into restore source: %w", err)
		}
		slog.InfoContext(ctx, "Merged OnDemand delta into base (complete snapshot)",
			slog.String("id", actorUID), slog.Duration("merge", time.Since(tMerge)))
	}

	// The RO lower never ships (reconstructed from the OCI image at restore).
	// The disk-backed upper ships as its own tar from CheckpointWorkload; a
	// legacy tmpfs upper is already inside the memory snapshot above.
	return dSnapshot, nil
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

// teardownActor stops the ateom-owned CH VMM for an actor. Best-effort: the
// snapshot is already on disk, so this only needs to release resources. ra may be
// nil (e.g. ateom restarted and lost in-memory state).
func (s *AteomService) teardownActor(ctx context.Context, id string, ra *runningActor, client *ch.Client) {
	// Stop offering the guest to GetWorkloadStats first, before anything below
	// makes it stop answering. Clearing it here rather than alongside the
	// attribution is what keeps a poll that lands mid-teardown on the
	// FAILED_PRECONDITION path ("no numbers right now") instead of surfacing a
	// closed connection as a failed read.
	s.guestStats.Store(nil)

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
		// Close the kata-agent client kept open for stdout/stderr forwarding. This
		// fails the forwarding goroutines' in-flight ReadStdout/ReadStderr calls, so
		// they return io.EOF and exit (no goroutine leak). Guarded so a second
		// teardown / a never-forwarded actor is a no-op.
		if ra.guestAgent != nil {
			_ = ra.guestAgent.Close()
			ra.guestAgent = nil
		}

		// Kill the CH process ateom launched.
		if ra.chCmd != nil && ra.chCmd.Process != nil {
			_ = ra.chCmd.Process.Kill()
			_, _ = ra.chCmd.Process.Wait()
		}
		// Kill the virtiofsds (after CH, their only client): the merged rootfs
		// share's and, when the actor has durable-dir volumes, the durable share's.
		for _, cmd := range []*exec.Cmd{ra.vfsdCmd, ra.durableVfsdCmd} {
			if cmd != nil && cmd.Process != nil {
				_ = cmd.Process.Kill()
				_, _ = cmd.Process.Wait()
			}
		}
	}

	// Sweep any leftover per-sandbox host-side state + orphaned per-sandbox
	// processes. This is ateom's own cleanup (process kill + unmount + rm) —
	// it also drops the merged rootfs overlay mounts, which MUST come before
	// the upper-dir removal below (removing a live overlay's upperdir would
	// corrupt the mount rather than delete the files).
	kata.CleanupSandboxState(ctx, id)

	// Remove the rootfs upper dir: ateom owns it — atelet's actor-dir reset
	// doesn't know it — and its absence is what marks a worker as holding no
	// disk-backed upper (actorHasDiskUpper). Runs after the checkpoint tar,
	// which is already on disk. A no-op for legacy tmpfs-upper actors.
	if err := os.RemoveAll(rootfsUpperDir(id)); err != nil {
		slog.WarnContext(ctx, "Failed to remove rootfs upper dir", slog.String("actorUID", id), slog.Any("err", err))
	}

	// Detach the bundle rootfs overlays composed in buildActorContainers, so
	// atelet's bundle wipe doesn't strand live mounts in this namespace.
	// Best-effort like the rest of teardown.
	if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(id)); err != nil {
		slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays", slog.String("actorUID", id), slog.Any("err", err))
	}
}
