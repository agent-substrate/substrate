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
	"sort"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/ateomstats"
	"github.com/agent-substrate/substrate/internal/checkpointmarker"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"google.golang.org/grpc/codes"
)

// Allow checkpointing even if the pod is shutting down. This will allow actors
// (or the harness) to suspend on shutdown.
func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.setActiveRPC(rpcCheckpointWorkload, cancel)
	defer s.clearActiveRPC()

	attribution := ateomstats.ActorAttributionFromRequest(req)

	// Replay a previously completed checkpoint for this actor if available.
	if rec, ok, err := checkpointmarker.Read(req.GetActorUid(), req.GetScope().String()); err != nil {
		return nil, err
	} else if ok {
		slog.InfoContext(ctx, "Checkpoint already completed for this actor; replaying its result",
			"actor", attribution.Ref,
			"actorUID", req.GetActorUid(),
			"snapshotFiles", rec.SnapshotFiles)
		// Finish any pending workload termination, unless the ateom now holds a different actor.
		if held := s.activeActor.Load(); held != nil && held.UID != req.GetActorUid() {
			slog.WarnContext(ctx, "Not running the post-checkpoint teardown: this ateom now holds a different actor",
				slog.String("id", req.GetActorUid()), slog.String("active_actor_uid", held.UID))
			return &ateompb.CheckpointWorkloadResponse{SnapshotFiles: rec.SnapshotFiles}, nil
		}
		if err := s.terminateWorkload(ctx, attribution.Ref, req.GetActorUid(), req.GetRunscPath(), req.GetSpec().GetContainers()); err != nil {
			slog.WarnContext(ctx, "Failed to terminate workload while replaying checkpoint",
				slog.String("actorUID", req.GetActorUid()), slog.Any("err", err))
		}
		s.activeSession = nil
		return &ateompb.CheckpointWorkloadResponse{SnapshotFiles: rec.SnapshotFiles}, nil
	}

	if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor checkpointing", attribution)

	// Contract with atelet:
	//
	//   * After we exit, atelet will upload checkpoint to GCS
	//   * After we exit, atelet will tear down OCI bundles and reset the actor directory.

	// Checkpoint only saves state; no sizing is applied, so size is left zero.
	rcmd := &runsc{
		path:     req.GetRunscPath(),
		actorUID: req.GetActorUid(),
	}

	checkpointPath := ateompath.CheckpointStateDir(req.GetActorUid())
	// Start from a clean directory so retried attempts do not mix with stale
	// or partially-written snapshot files from previous runs.
	if err := os.RemoveAll(checkpointPath); err != nil {
		return nil, fmt.Errorf("while clearing checkpoint directory: %w", err)
	}
	if err := os.MkdirAll(checkpointPath, 0o700); err != nil {
		return nil, fmt.Errorf("while creating checkpoint directory: %w", err)
	}

	// Always take durable-dir snapshot if at least one container has a durable-dir volume mount.
	// TODO(dberkov): this is a temporary workaround until gVisor supports taking durable-dir snapshots in a single request with the process snapshot.
	switch req.GetScope() {
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
		var ddv []string
		for _, ctr := range req.GetSpec().GetContainers() {
			for _, m := range ctr.GetDurableDirVolumeMounts() {
				ddv = append(ddv, m.GetMountPath())
			}
		}
		if len(ddv) == 0 {
			return nil, fmt.Errorf("no durable-dir volumes found for DATA snapshot")
		}
		if err := rcmd.cmdFsCheckpoint(ctx, "pause", checkpointPath, ddv); err != nil {
			return nil, classifyCheckpointFailure(ctx, rcmd, fmt.Errorf("while fscheckpointing durable-dir %q: %w", ddv[0], err))
		}
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL:
		// Checkpoint pause container (root of the sandbox)
		if err := rcmd.cmdCheckpoint(ctx, "pause", checkpointPath); err != nil {
			return nil, classifyCheckpointFailure(ctx, rcmd, fmt.Errorf("while checkpointing pause: %w", err))
		}
	default:
		return nil, fmt.Errorf("unsupported snapshot scope: %v", req.GetScope())
	}

	// The sandbox is gone as of the checkpoint above, so the ateom is back to
	// "available" from here on: there is nothing left to measure, and holding
	// the attribution would let a later GetWorkloadStats report a checkpointed
	// actor as though it were still running.
	//
	// Cleared here rather than at the end of the function because everything
	// below is bookkeeping over a dead sandbox and can still fail (listing the
	// snapshot files returns an error), which would otherwise leave the
	// attribution behind. Conversely nothing above this point clears it: a
	// checkpoint that failed may well have left the workload running, and
	// reporting its usage is then the honest answer.
	s.activeActor.Store(nil)

	// Report exactly the files runsc wrote so atelet ships precisely this set
	// (checkpoint.img plus any pages images), rather than a hardcoded list.
	snapshotFiles, err := listSnapshotFiles(checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("while listing checkpoint files: %w", err)
	}

	// Record checkpoint completion before answering. If writing the marker fails,
	// log and continue since the snapshot files are already complete on disk.
	if err := checkpointmarker.Write(req.GetActorUid(), req.GetScope().String(), snapshotFiles); err != nil {
		slog.ErrorContext(ctx, "Failed to record the checkpoint completion marker; answering anyway, but a lost response can no longer be replayed",
			"actor", attribution.Ref,
			"actorUID", req.GetActorUid(),
			"snapshotFiles", snapshotFiles,
			"err", err)
	}

	// Cleanup the containers after checkpointing.
	// This is best-effort cleanup for actor containers that may have been left behind after checkpointing.
	if err := s.terminateWorkload(ctx, attribution.Ref, attribution.UID, req.GetRunscPath(), req.GetSpec().GetContainers()); err != nil {
		slog.WarnContext(ctx, "failed to terminate workload after checkpoint",
			slog.String("actor", attribution.Ref.String()),
			slog.String("actorUID", attribution.UID),
			slog.Any("err", err))
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor checkpointed", attribution)
	s.activeSession = nil

	return &ateompb.CheckpointWorkloadResponse{SnapshotFiles: snapshotFiles}, nil
}

// stateProbeTimeout bounds probing runsc state during failure classification.
const stateProbeTimeout = 15 * time.Second

// classifyCheckpointFailure inspects runsc container state after a failure to
// distinguish retriable transient errors from unrecoverable errors (where the
// sandbox was destroyed and cannot be retried).
func classifyCheckpointFailure(ctx context.Context, rcmd *runsc, err error) error {
	// Probe with a separate timeout so an expired caller context is not
	// mistaken for a missing sandbox.
	probeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stateProbeTimeout)
	defer cancel()

	out, stateErr := rcmd.cmdStateOutput(probeCtx, "pause")
	if stateErr == nil {
		return err
	}
	if !sandboxNotFound(out) {
		slog.WarnContext(ctx, "Checkpoint failed and the sandbox state could not be determined; leaving the failure retriable",
			"actorUID", rcmd.actorUID, "stateErr", stateErr, "runscOutput", string(out), "err", err)
		return err
	}
	slog.WarnContext(ctx, "Checkpoint failed and the sandbox is gone; the actor's state is unrecoverable",
		"actorUID", rcmd.actorUID, "stateErr", stateErr, "err", err)
	return ateerrors.NewGRPCError(ctx, codes.DataLoss, ateerrors.ReasonInvalidCheckpointResult, ateerrors.ActorCrashedMetadata(),
		fmt.Errorf("%w: checkpoint failed and no sandbox remains to retry against: %w", ateerrors.ReasonInvalidCheckpointResult, err))
}

// sandboxNotFound checks if runsc output explicitly indicates the container does not exist.
func sandboxNotFound(runscOutput []byte) bool {
	for line := range strings.Lines(string(runscOutput)) {
		msg, ok := strings.CutPrefix(strings.TrimSpace(line), "error:")
		if !ok {
			continue
		}
		if strings.Contains(strings.ToLower(msg), "does not exist") {
			return true
		}
	}
	return false
}

// listSnapshotFiles returns the (relative) names of regular files directly under dir.
func listSnapshotFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		// ateom's own completion marker shares the directory but is
		// bookkeeping, not snapshot content, so it never joins the set.
		if e.Type().IsRegular() && e.Name() != ateompath.CheckpointDoneFileName {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	return files, nil
}
