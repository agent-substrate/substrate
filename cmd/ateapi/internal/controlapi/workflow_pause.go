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

package controlapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PauseActor executes the workflow to pause a running actor. Idempotent:
// a re-entered workflow fast-forwards past the steps a previous attempt
// completed, deriving progress from the persisted actor alone.
func (w *ActorWorkflow) PauseActor(ctx context.Context, actorRef resources.ActorRef) (_ *ateapipb.Actor, err error) {
	start := time.Now()
	var actor *ateapipb.Actor
	var actorTemplate *atev1alpha1.ActorTemplate
	var wireSnapshotScope string
	// Set just before finalize; nil until then, so earlier exits label
	// themselves from the record they hold.
	var finalAttrs []attribute.KeyValue

	defer func() {
		attrs := finalAttrs
		if attrs == nil {
			attrs = lifecycleOpAttrs(actor, actorTemplate, "", wireSnapshotScope)
		}
		w.instruments.recordLifecycleOp(ctx, ateattr.OperationPause, start, err, attrs...)
	}()

	leaseCtx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	actor, actorTemplate, err = w.loadActorForPause(leaseCtx, actorRef)
	if err != nil {
		return nil, err
	}
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_PAUSED {
		// Fully paused already: FinalizePaused commits PAUSED and the cleared
		// worker assignment in a single update, so there is nothing left to do.
		// This success reports no pool, and cannot: the previous attempt
		// released the worker, so the record names none (#957).
		return actor, nil
	}
	var marked *ateapipb.Actor
	if marked, err = w.ensureMarkedPausing(leaseCtx, actorRef, actor); err != nil {
		return nil, err
	}
	actor = marked
	if wireSnapshotScope, err = w.ensureAteletPaused(leaseCtx, actorRef, actor, actorTemplate); err != nil {
		return nil, err
	}
	// TODO: There is no difference between suspend and pause for now, but we
	// could optimize pause by not detaching. We would need to make sure Resume
	// is idempotent.
	if err = w.ensureVolumesDetached(leaseCtx, actor, actorTemplate, "DetachVolumesForPause", ateattr.OperationPause); err != nil {
		return nil, err
	}
	// FinalizePaused clears the WorkerAssignment the labels read, so snapshot
	// them here, as crash.go does for the crash counter.
	finalAttrs = lifecycleOpAttrs(actor, actorTemplate, "", wireSnapshotScope)
	var finalized *ateapipb.Actor
	if finalized, err = w.ensurePausedFinalized(leaseCtx, actorRef, actorTemplate); err != nil {
		return nil, err
	}
	actor = finalized
	return actor, nil
}

// loadActorForPause fetches the current actor record and its template.
func (w *ActorWorkflow) loadActorForPause(ctx context.Context, actorRef resources.ActorRef) (_ *ateapipb.Actor, _ *atev1alpha1.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "LoadActorForPause")
	defer func() { err = done(err) }()

	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, nil, err
	}
	actorTemplate, err := w.actorTemplateLister.ActorTemplates(actor.GetActorTemplateNamespace()).Get(actor.GetActorTemplateName())
	if err != nil {
		return nil, nil, fmt.Errorf("while getting ActorTemplate: %w", err)
	}
	return actor, actorTemplate, nil
}

// ensureMarkedPausing transitions a RUNNING actor to PAUSING, minting the
// local snapshot name. Skips when a previous attempt already marked the
// actor; the persisted name then stays authoritative for the rest of the
// workflow.
func (w *ActorWorkflow) ensureMarkedPausing(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "MarkPausing")
	defer func() { err = done(err) }()

	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_PAUSING {
		markSkipped(ctx, "actor already PAUSING")
		return actor, nil
	}
	// The pause edge only exists from RUNNING.
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		return nil, status.Errorf(codes.FailedPrecondition, "MarkPausing prerequisite not met for Actor: %s (got: %v, want %s)", actorRef, actor.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_RUNNING)
	}
	// By design a golden actor cannot be paused — it can only be suspended
	// (committed).
	if actorRef.Atespace == resources.GoldenActorAtespace {
		return nil, status.Errorf(codes.FailedPrecondition, "actors in atespace %q are golden actors, which cannot be paused", actorRef.Atespace)
	}

	snapshotName := resources.NewSnapshotName()
	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_PAUSING
		toUpdate.Status.InProgressLocalSnapshotName = snapshotName
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, err
	}
	return storedActor, nil
}

// ensureAteletPaused checkpoints the workload locally on the worker node
// under the actor's persisted snapshot name. This is the atelet reentrancy
// seam (#372): the request is keyed by the actor UID, the worker pod UID, and
// the once-minted snapshot name, so a re-entered workflow re-sends the same
// semantic request; once atelet's Checkpoint is idempotent on those keys this
// step becomes fully reentrant with no changes here.
func (w *ActorWorkflow) ensureAteletPaused(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate) (wireSnapshotScope string, err error) {
	ctx, done := stepSpan(ctx, "CallAteletPause")
	defer func() { err = done(err) }()

	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment == nil {
		// Missing active worker pod reference in PAUSING state indicates corrupted store state.
		if err := crashActor(ctx, w.store, actorRef, ateattr.OperationPause, ateattr.ReasonCorruptedAssignment); err != nil {
			slog.ErrorContext(ctx, "Failed to crash actor", slog.String("err", err.Error()))
		}
		return "", status.Errorf(codes.FailedPrecondition, "CallAteletPause prerequisite not met for Actor: %s. No worker assignment", actorRef)
	}

	ateletConn, err := w.dialer.DialForWorker(assignment.GetWorkerNamespace(), assignment.GetWorkerPod())
	if err != nil {
		if errors.Is(err, ErrWorkerPodNotFound) {
			slog.ErrorContext(ctx, "Worker pod gone before checkpoint, crashing actor", "namespace", assignment.GetWorkerNamespace(), "pod", assignment.GetWorkerPod(), "in_progress_local_snapshot_name", actor.GetStatus().GetInProgressLocalSnapshotName())
			if err := crashActor(ctx, w.store, actorRef, ateattr.OperationPause, ateattr.ReasonWorkerPodGone); err != nil {
				slog.ErrorContext(ctx, "Failed to crash actor", slog.String("err", err.Error()))
			}
			return "", fmt.Errorf("actor is CRASHED because its worker pod is gone and no snapshot was written")
		}
		return "", fmt.Errorf("while getting atelet conn for worker pod: %w", err)
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

	workloadSpec, err := workloadSpecFromActorTemplate(actorTemplate, actor)
	if err != nil {
		return "", err
	}

	// Checkpoint does not carry the sandbox config: atelet uses the version the
	// actor is currently running (recorded on-node at Run/Restore) and pins it
	// into the snapshot manifest.
	req := &ateletpb.CheckpointRequest{
		TargetAteomUid:         assignment.GetWorkerPodUid(),
		Atespace:               actor.GetMetadata().GetAtespace(),
		ActorName:              actor.GetMetadata().GetName(),
		ActorTemplateNamespace: actor.GetActorTemplateNamespace(),
		ActorTemplateName:      actor.GetActorTemplateName(),
		Spec:                   workloadSpec,
		Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
		Config: &ateletpb.CheckpointRequest_LocalConfig{
			LocalConfig: &ateletpb.LocalCheckpointConfiguration{
				SnapshotName: actor.GetStatus().GetInProgressLocalSnapshotName(),
			},
		},
		Scope:    toAteletSnapshotScope(actorTemplate.Spec.SnapshotsConfig.OnPause),
		ActorUid: actor.GetMetadata().Uid,
	}
	wireSnapshotScope = ateattr.SnapshotScopeValue(req.Scope)

	_, err = client.Checkpoint(ctx, req)
	return wireSnapshotScope, maybeCrashActor(ctx, w.store, actorRef, err, "while checkpointing workload", ateattr.OperationPause)
}

// ensurePausedFinalized releases the actor's worker (only when it is still
// owned by this actor), records where the local snapshot lives, and commits
// PAUSED with the assignment cleared in a single update — or CRASHED when the
// worker's node name was lost, since a local snapshot on an unknown node can
// never be resumed. It re-reads the actor first so an out-of-band transition
// (e.g. the syncer crashing the actor after its worker died) is not
// overwritten: with no assignment left there is nothing to finalize.
func (w *ActorWorkflow) ensurePausedFinalized(ctx context.Context, actorRef resources.ActorRef, actorTemplate *atev1alpha1.ActorTemplate) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "FinalizePaused")
	defer func() { err = done(err) }()

	latestActor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, err
	}

	// 1. Free the worker (if it hasn't been freed yet)
	if assignment := latestActor.GetStatus().GetWorkerAssignment(); assignment != nil {
		worker, err := w.store.GetWorker(ctx, assignment.GetWorker().GetName())
		nodeName := ""
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("while getting worker for release: %w", err)
			}
			slog.Warn("Worker already gone during finalize pause, skipping release", "worker", assignment.GetWorkerPod())
		} else {
			nodeName = worker.GetNodeName()
			// Only free it if it still belongs to us

			if wass := worker.GetStatus().GetAssignment(); wass != nil {
				if wass.GetActorUid() == latestActor.GetMetadata().GetUid() {
					_, err := w.store.UpdateWorker(ctx, worker.GetMetadata().GetName(), store.PreconditionFrom(worker), func(toUpdate *ateapipb.Worker) error {
						toUpdate.Status.Assignment = nil
						return nil
					})
					if err != nil {
						if errors.Is(err, store.ErrVersionConflict) {
							return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
						}
						return nil, err
					}
				}
			}
		}

		// 2. Clear the actor's assignment, now that the worker is freed
		latestActor, err = w.store.GetActor(ctx, actorRef)
		if err != nil {
			return nil, err
		}
		wasAlreadyCrashed := latestActor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED
		newState := ateapipb.ActorState_ACTOR_STATE_PAUSED
		if nodeName == "" {
			// Without a node name we cannot record where the local snapshot lives,
			// so the actor can never be resumed (the scheduler would search for a
			// worker on an unknown node forever). Crash it instead of leaving it
			// stuck in PAUSED.
			slog.ErrorContext(ctx, "Node name not found during finalize pause, crashing actor", slog.Any("actor", actorRef))
			newState = ateapipb.ActorState_ACTOR_STATE_CRASHED
		}
		contentScope := toActorSnapshotContentScope(actorTemplate.Spec.SnapshotsConfig.OnPause)
		sandboxClass := ""
		if worker != nil {
			sandboxClass = worker.GetSandboxClass()
		}
		// Snapshot crash attributes before pod and pool pointers are cleared below.
		latestActor.Status.State = newState
		crashAttrs := ateattr.ActorMetricAttributes(latestActor, sandboxClass, ateattr.OperationPause, ateattr.ReasonCorruptedAssignment)

		storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(latestActor), func(toUpdate *ateapipb.Actor) error {
			toUpdate.Status.State = newState
			// TODO(dberkov) - what if InProgressLocalSnapshotName is empty? That shouldn't be possible.
			if toUpdate.GetStatus().GetInProgressLocalSnapshotName() != "" {
				localInfo := &ateapipb.LocalSnapshotInfo{
					SnapshotName: toUpdate.GetStatus().GetInProgressLocalSnapshotName(),
					ContentScope: contentScope,
				}
				if newState != ateapipb.ActorState_ACTOR_STATE_CRASHED {
					localInfo.NodeVmsWithLocalSnapshots = []string{nodeName}
				}
				toUpdate.Status.LocalSnapshotInfo = localInfo
				toUpdate.Status.InProgressLocalSnapshotName = ""
			}
			toUpdate.Status.WorkerAssignment = nil
			return nil
		})
		if err == nil && storedActor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED && !wasAlreadyCrashed {
			recordActorCrash(ctx, crashAttrs)
		}
		if err != nil {
			if errors.Is(err, store.ErrVersionConflict) {
				return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
			}
			return nil, err
		}
		latestActor = storedActor
	}

	return latestActor, nil
}
