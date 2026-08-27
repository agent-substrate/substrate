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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
)

// resumeSnapshotSource is the boot source resolved once by loadActorForResume
// and passed by value to the restore step — never mutated after resolution.
type resumeSnapshotSource struct {
	// SnapshotURI is the storage location of the durable snapshot to restore
	// from: the actor's own latest snapshot when one exists, the template's
	// golden snapshot otherwise. Zero means cold boot from the spec (unless
	// the actor holds a local snapshot, which takes precedence at restore).
	SnapshotURI resources.SnapshotURI
	Scope       ateapipb.SnapshotContentScope
	// GoldenSnapshotURI is the storage location of the ActorTemplate's golden
	// snapshot. Populated only when the template's onResume configuration
	// selects the golden snapshot as the boot source for the pending restore:
	// restore then combines the golden snapshot with the actor's data.
	GoldenSnapshotURI resources.SnapshotURI
}

// restoreTelemetry labels the restore operation for the resume lifecycle
// metric. WireSnapshotScope describes the restore requested, not the stored
// snapshot's scope: a data snapshot restored on golden goes out as
// data_on_golden.
type restoreTelemetry struct {
	SnapshotKind      string
	WireSnapshotScope string
}

// ResumeActor executes the workflow to resume a suspended actor. Idempotent:
// a re-entered workflow fast-forwards past the steps a previous attempt
// completed, deriving progress from the persisted actor alone.
func (w *ActorWorkflow) ResumeActor(ctx context.Context, actorRef resources.ActorRef, boot bool) (_ *ateapipb.Actor, resumed bool, err error) {
	start := time.Now()
	var actor *ateapipb.Actor
	var actorTemplate *atev1alpha1.ActorTemplate
	var tele restoreTelemetry
	var wasRunning bool

	// Recorded before the lock so lock contention still counts as an attempt.
	// Clean already-running no-ops are skipped: the router resumes per routed
	// request, and recording those would sample at router QPS and bury
	// cold-resume latency.
	defer func() {
		if err == nil && wasRunning {
			return
		}
		w.instruments.recordLifecycleOp(ctx, ateattr.OperationResume, start, err,
			lifecycleOpAttrs(actor, actorTemplate, tele.SnapshotKind, tele.WireSnapshotScope)...)
	}()

	lockCtx, lock, err := w.acquireActorLock(ctx, actorRef)
	if err != nil {
		return nil, false, err
	}
	defer lock.Close()

	var src resumeSnapshotSource
	actor, actorTemplate, src, err = w.loadActorForResume(lockCtx, actorRef, boot)
	if err != nil {
		return nil, false, err
	}
	if wasRunning = actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING; wasRunning {
		return actor, false, nil
	}
	var created *ateapipb.Actor
	if created, err = w.ensureVolumesCreated(lockCtx, actorRef, actor, actorTemplate); err != nil {
		return nil, false, err
	}
	actor = created
	var worker *ateapipb.Worker
	var assigned *ateapipb.Actor
	if assigned, worker, err = w.ensureWorkerAssigned(lockCtx, actorRef, actor, actorTemplate); err != nil {
		return nil, false, err
	}
	actor = assigned
	if err = w.ensureVolumesAttached(lockCtx, actor, worker, actorTemplate); err != nil {
		return nil, false, err
	}
	if tele, err = w.ensureAteletRestored(lockCtx, actorRef, actor, actorTemplate, src); err != nil {
		return nil, false, err
	}
	var running *ateapipb.Actor
	if running, err = w.finalizeRunning(lockCtx, actorRef); err != nil {
		return nil, false, err
	}
	actor = running
	return actor, true, nil
}

// validateGoldenSnapshotScope rejects a golden snapshot that does not carry
// the guest state (memory + fs delta) a restore needs. Golden actors always
// commit Full (commitSnapshotScope), so this only trips on golden snapshots
// taken before that rule existed — surface a clear error instead of shipping
// a restore request atelet would reject (or that would boot an empty guest).
func validateGoldenSnapshotScope(snapshot *ateapipb.ActorSnapshot) error {
	switch snapshot.GetStatus().GetContentScope() {
	case ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED,
		ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL:
		return nil
	default:
		return status.Errorf(codes.FailedPrecondition,
			"ActorTemplate golden snapshot %q was taken with scope %s, not Full; regenerate the golden snapshot",
			snapshot.GetMetadata().GetName(), snapshot.GetStatus().GetContentScope())
	}
}

// loadActorForResume fetches the current actor record and its template, and
// resolves the boot source for the pending restore.
func (w *ActorWorkflow) loadActorForResume(ctx context.Context, actorRef resources.ActorRef, boot bool) (_ *ateapipb.Actor, _ *atev1alpha1.ActorTemplate, _ resumeSnapshotSource, err error) {
	ctx, done := stepSpan(ctx, "LoadActorForResume")
	defer func() { err = done(err) }()

	var src resumeSnapshotSource
	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil, src, status.Errorf(codes.NotFound, "Actor %s not found", actorRef)
		}
		return nil, nil, src, fmt.Errorf("while getting actor from DB: %w", err)
	}

	// If the actor is already running, there is no pending restore to prepare
	// for. Short-circuit immediately to avoid unnecessary store reads for snapshots
	// and template resolution on the hot resume path.
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING {
		return actor, nil, src, nil
	}

	actorTemplate, err := w.actorTemplateLister.ActorTemplates(actor.GetActorTemplateNamespace()).Get(actor.GetActorTemplateName())
	if err != nil {
		return nil, nil, src, fmt.Errorf("while getting ActorTemplate: %w", err)
	}
	if ref := actor.GetStatus().GetLatestSnapshot(); ref != nil {
		snapshot, err := w.store.GetActorSnapshot(ctx, ref.GetAtespace(), ref.GetName())
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil, src, status.Error(codes.DataLoss, "ActorSnapshot data is missing")
		}
		if err != nil {
			return nil, nil, src, fmt.Errorf("while getting ActorSnapshot: %w", err)
		}
		if src.SnapshotURI, err = resources.ParseSnapshotURI(snapshot.GetStatus().GetSnapshotUri()); err != nil {
			return nil, nil, src, status.Errorf(codes.DataLoss, "ActorSnapshot %s/%s: %v", ref.GetAtespace(), ref.GetName(), err)
		}
		src.Scope = snapshot.GetStatus().GetContentScope()
	} else if actorTemplate.Status.GoldenSnapshot != "" && !boot {
		snapshot, err := w.store.GetActorSnapshot(ctx, resources.GoldenActorAtespace, actorTemplate.Status.GoldenSnapshot)
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil, src, status.Error(codes.DataLoss, "ActorTemplate golden snapshot data is missing")
		}
		if err != nil {
			return nil, nil, src, fmt.Errorf("while getting golden ActorSnapshot: %w", err)
		}
		if err := validateGoldenSnapshotScope(snapshot); err != nil {
			return nil, nil, src, err
		}
		if src.SnapshotURI, err = resources.ParseSnapshotURI(snapshot.GetStatus().GetSnapshotUri()); err != nil {
			return nil, nil, src, status.Errorf(codes.DataLoss, "golden ActorSnapshot %s: %v", actorTemplate.Status.GoldenSnapshot, err)
		}
		src.Scope = snapshot.GetStatus().GetContentScope()
	}

	// The template's onResume configuration selects the boot source for the
	// pending restore. When it names the golden snapshot, resolve the golden
	// snapshot's location so the restore can combine the golden's guest
	// state with the actor's data. The pending
	// restore is data-only when the actor is paused with a Data pause scope
	// (the local snapshot takes precedence at restore), or when its durable
	// snapshot holds Data. Valid Full snapshots restore from their own
	// content and ignore the policy.
	if actorTemplate.Spec.SnapshotsConfig.OnResume.FromData == atev1alpha1.ResumeSourceGolden {
		dataOnly := false
		if actor.GetStatus().GetLocalSnapshotInfo() != nil {
			dataOnly = actorTemplate.Spec.SnapshotsConfig.OnPause == atev1alpha1.SnapshotScopeData
		} else if actor.GetStatus().GetLatestSnapshot() != nil {
			dataOnly = src.Scope == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
		}
		if dataOnly {
			if actorTemplate.Status.GoldenSnapshot == "" {
				return nil, nil, src, status.Error(codes.FailedPrecondition, "a Golden data resume requires the ActorTemplate golden snapshot, which is not available")
			}
			goldenSnapshot, err := w.store.GetActorSnapshot(ctx, resources.GoldenActorAtespace, actorTemplate.Status.GoldenSnapshot)
			if errors.Is(err, store.ErrNotFound) {
				return nil, nil, src, status.Error(codes.DataLoss, "ActorTemplate golden snapshot data is missing")
			}
			if err != nil {
				return nil, nil, src, fmt.Errorf("while getting golden ActorSnapshot: %w", err)
			}
			if err := validateGoldenSnapshotScope(goldenSnapshot); err != nil {
				return nil, nil, src, err
			}
			if src.GoldenSnapshotURI, err = resources.ParseSnapshotURI(goldenSnapshot.GetStatus().GetSnapshotUri()); err != nil {
				return nil, nil, src, status.Errorf(codes.DataLoss, "golden ActorSnapshot %s: %v", actorTemplate.Status.GoldenSnapshot, err)
			}
		}
	}

	return actor, actorTemplate, src, nil
}

// ensureVolumesCreated provisions any initial actor volumes that are in
// PENDING state, persisting the resulting volume state (even when creation
// partially failed, so progress is not lost) and returning the stored copy.
func (w *ActorWorkflow) ensureVolumesCreated(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "CreateVolumes")
	defer func() { err = done(err) }()

	pending := false
	for _, vol := range actor.GetStatus().GetActorVolumes() {
		if vol.GetStatus() == ateapipb.ExternalVolume_STATUS_PENDING {
			pending = true
			break
		}
	}
	if !pending {
		markSkipped(ctx, "no volumes awaiting creation")
		return actor, nil
	}

	volumes, createErr := createActorVolumes(ctx, w.pluginRegistry, w.storageClassLister, actor.GetMetadata().GetUid(), actorTemplate, actor.GetStatus().GetActorVolumes())
	// createActorVolumes reports the state it got to even when it fails, so both
	// paths persist the same field.
	updatePrecondition := store.PreconditionFrom(actor)
	persistVolumes := func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.ActorVolumes = volumes
		return nil
	}
	if createErr != nil {
		// Even if volume creation failed, we still want to persist any updated volume state.
		if _, updateErr := w.store.UpdateActor(ctx, actorRef, updatePrecondition, persistVolumes); updateErr != nil {
			slog.ErrorContext(ctx, "failed to update actor volumes on volume creation failure in resume", slog.Any("error", updateErr))
		}
		return nil, createErr
	}
	storedActor, updateErr := w.store.UpdateActor(ctx, actorRef, updatePrecondition, persistVolumes)
	if updateErr != nil {
		if errors.Is(updateErr, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while updating actor after volume creation: %w", updateErr)
	}
	return storedActor, nil
}

// ensureWorkerAssigned leaves the actor RESUMING with a validated, live,
// owned, and eligible worker.
//
// A RESUMING actor was assigned by a previous attempt: its persisted
// assignment is revalidated (worker still exists, not draining, still owned
// by this actor UID, still eligible for the actor's constraints) and reused;
// a stale assignment crashes the actor. A SUSPENDED or PAUSED actor goes
// through scheduling, claiming the worker and persisting RESUMING with the
// assignment. A version conflict there is retried under a bounded backoff
// only after re-reading the actor and revalidating it can still be resumed —
// the conflicting writer may have crashed, drained, or deleted it.
func (w *ActorWorkflow) ensureWorkerAssigned(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate) (_ *ateapipb.Actor, _ *ateapipb.Worker, err error) {
	ctx, done := stepSpan(ctx, "AssignWorker")
	defer func() { err = done(err) }()

	switch actor.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_RESUMING:
		worker, err := w.validateAssignedWorker(ctx, actorRef, actor, actorTemplate)
		if err != nil {
			return nil, nil, err
		}
		markSkipped(ctx, "actor already RESUMING with a valid worker assignment")
		return actor, worker, nil
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_PAUSED:
	default:
		return nil, nil, status.Errorf(codes.FailedPrecondition, "AssignWorker prerequisite not met for Actor: %s (got: %v, want %s or %s)", actorRef, actor.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_PAUSED)
	}

	backoff := wait.Backoff{
		Steps:    5,
		Duration: 10 * time.Millisecond,
		Factor:   2.0,
		Jitter:   1.0,
	}
	var assignedActor *ateapipb.Actor
	var assignedWorker *ateapipb.Worker
	err = wait.ExponentialBackoff(backoff, func() (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		attemptActor, attemptWorker, attemptErr := w.assignWorkerAttempt(ctx, actorRef, actor, actorTemplate)
		if attemptErr == nil {
			assignedActor, assignedWorker = attemptActor, attemptWorker
			return true, nil
		}
		if errors.Is(attemptErr, store.ErrVersionConflict) {
			if attemptActor != nil {
				actor = attemptActor // retry with the refreshed actor
			}
			return false, nil
		}
		return false, attemptErr
	})
	if err != nil {
		return nil, nil, err
	}
	return assignedActor, assignedWorker, nil
}

// validateAssignedWorker checks a RESUMING actor's persisted assignment
// against the current worker record. Every invalid outcome crashes the actor:
// a RESUMING actor whose worker vanished, drained, was reassigned, or is no
// longer eligible can never make progress on its own.
func (w *ActorWorkflow) validateAssignedWorker(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate) (*ateapipb.Worker, error) {
	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment == nil {
		slog.ErrorContext(ctx, "expected a worker assignment on a RESUMING actor, found none")

		// Crash the actor if its worker assignment is missing. We should never be in this state.
		if cerr := crashActor(ctx, w.store, actorRef, ateattr.OperationResume, ateattr.ReasonCorruptedAssignment); cerr != nil {
			return nil, cerr
		}
		return nil, status.Errorf(codes.Aborted, "actor %s crashed", actorRef)
	}

	worker, err := w.store.GetWorker(ctx, assignment.GetWorker().GetName())
	if err != nil {
		// Crash the actor if it was assigned to a deleted pod.
		if errors.Is(err, store.ErrNotFound) {
			if cerr := crashActor(ctx, w.store, actorRef, ateattr.OperationResume, ateattr.ReasonWorkerPodGone); cerr != nil {
				return nil, cerr
			}
			return nil, status.Errorf(codes.Aborted, "actor %s crashed", actorRef)
		}
		return nil, fmt.Errorf("failed to get already assigned worker for actor %w", err)
	}
	if worker.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_DRAINING {
		slog.InfoContext(ctx, "Assigned worker is draining; crashing actor",
			slog.String("actor", actorRef.String()),
			slog.String("worker", worker.GetWorkerNamespace()+"/"+worker.GetWorkerPod()))
		if cerr := crashActor(ctx, w.store, actorRef, ateattr.OperationResume, ateattr.ReasonWorkerReassigned); cerr != nil {
			return nil, cerr
		}
		return nil, status.Errorf(codes.Aborted, "actor %s crashed", actorRef.String())
	}
	// Verify the worker is still assigned to the same Actor.
	if worker.GetStatus().GetAssignment().GetActorUid() != actor.GetMetadata().GetUid() {
		slog.ErrorContext(ctx, "crashing actor because its assigned worker no longer belongs to it",
			slog.String("worker", worker.GetWorkerPod()),
			slog.Any("assignment", worker.GetStatus().GetAssignment()))
		if cerr := crashActor(ctx, w.store, actorRef, ateattr.OperationResume, ateattr.ReasonWorkerReassigned); cerr != nil {
			return nil, fmt.Errorf("while crashing actor: %w", cerr)
		}
		return nil, status.Errorf(codes.Aborted, "actor %s crashed", actorRef)
	}
	constraints, err := schedulingConstraints(actor, actorTemplate)
	if err != nil {
		return nil, err
	}
	if !w.scheduler.Applies(worker, constraints) {
		slog.ErrorContext(ctx, "crashing actor because previously assigned worker is not eligible anymore")
		release := proto.Clone(worker).(*ateapipb.Worker)
		release.Status.Assignment = nil
		// If that worker's pool is no longer eligible (e.g. the actor's
		// worker_selector was updated after the failed attempt), release it back
		// to the free pool instead of leaving it claimed forever — nothing else
		// reclaims a healthy worker whose actor moved on to a different pool.
		if err := w.store.UpdateWorker(ctx, release, release.GetMetadata().GetVersion()); err != nil {
			return nil, fmt.Errorf("while releasing stale worker assignment: %w", err)
		}
		if cerr := crashActor(ctx, w.store, actorRef, ateattr.OperationResume, ateattr.ReasonCorruptedAssignment); cerr != nil {
			return nil, fmt.Errorf("while crashing actor: %w", cerr)
		}
		return nil, status.Errorf(codes.Aborted, "actor %s crashed", actorRef)
	}
	return worker, nil
}

// schedulerRecordable excludes retried version conflicts: the assignment loop
// re-runs attempts transparently on store.ErrVersionConflict, so counting
// those attempts would inflate the error rate and double-count the eventual
// success.
func schedulerRecordable(err error) bool {
	return !errors.Is(err, store.ErrVersionConflict)
}

// assignWorkerAttempt makes one attempt at claiming a worker for the actor
// and persisting RESUMING with the assignment. On a version conflict it
// re-reads the actor: if the fresh copy can still be resumed the refreshed
// actor is returned along with the conflict so the caller retries with clean
// inputs; any other status aborts the resume.
func (w *ActorWorkflow) assignWorkerAttempt(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate) (_ *ateapipb.Actor, _ *ateapipb.Worker, err error) {
	start := time.Now()
	outcome := ateattr.SchedulerOutcomeError
	poolNamespace := ""
	pool := ""
	class := ""
	if actorTemplate != nil {
		class = string(actorTemplate.Spec.SandboxClass)
	}
	defer func() {
		if schedulerRecordable(err) {
			w.instruments.recordSchedulerAssignment(ctx, start, outcome, poolNamespace, pool, class, err)
		}
	}()

	workers, err := w.workerCache.Workers()
	if err != nil {
		return nil, nil, fmt.Errorf("while listing workers: %w", err)
	}

	constraints, err := schedulingConstraints(actor, actorTemplate)
	if err != nil {
		return nil, nil, err
	}

	var assignedWorker *ateapipb.Worker

	// Check if we already have a worker assigned from a previous failed attempt.
	// This can happen if ateapi crashed after updating worker with actor assignment,
	// but has not yet updated the actor.
	for _, worker := range workers {
		if worker.GetStatus().GetAssignment() == nil {
			continue
		}
		if worker.GetStatus().GetAssignment().GetActorUid() != actor.GetMetadata().GetUid() {
			continue
		}
		if w.scheduler.Applies(worker, constraints) {
			assignedWorker = worker
			break
		}
		// Workers() returns pointers directly from the cache so we need to clone before
		// mutating so that the cache is not corrupted if UpdateWorker fails.
		releaseWorker := proto.Clone(worker).(*ateapipb.Worker)
		releaseWorker.Status.Assignment = nil
		// The claimed worker is no longer eligible (e.g. the actor's
		// worker_selector changed after the failed attempt); release it back
		// to the free pool — nothing else reclaims a healthy worker whose
		// actor moved on to a different pool. Best effort in the background.
		go func(release *ateapipb.Worker) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := w.store.UpdateWorker(bgCtx, release, release.GetMetadata().GetVersion()); err != nil {
				slog.ErrorContext(bgCtx, "Failed to release stale worker assignment",
					slog.String("worker", release.GetWorkerNamespace()+"/"+release.GetWorkerPod()),
					slog.Any("err", err))
			}
		}(releaseWorker)
	}
	if assignedWorker == nil {
		pickedWorker, err := w.scheduler.Schedule(ctx, constraints)
		if err != nil {
			if errors.Is(err, scheduling.ErrNoCapacity) {
				outcome = ateattr.SchedulerOutcomeNoFreeWorker
				return nil, nil, status.Errorf(codes.ResourceExhausted, "no free workers available")
			}
			return nil, nil, err
		}

		assignedWorker = pickedWorker
		slog.InfoContext(ctx, "Picked worker", slog.Any("worker", pickedWorker.String()))
	}

	// Workers() returns pointers directly from the cache so we need to clone before
	// mutating so that the cache is not corrupted if UpdateWorker fails.
	assignedWorker = proto.Clone(assignedWorker).(*ateapipb.Worker)
	assignedWorker.Status.Assignment = &ateapipb.ActorAssignment{
		ActorTemplate: &ateapipb.KubeNamespacedObjectRef{
			Namespace: actor.GetActorTemplateNamespace(),
			Name:      actor.GetActorTemplateName(),
		},
		Actor: &ateapipb.ObjectRef{
			Atespace: actor.GetMetadata().GetAtespace(),
			Name:     actor.GetMetadata().GetName(),
		},
		ActorUid: actor.GetMetadata().GetUid(),
	}

	if err := w.store.UpdateWorker(ctx, assignedWorker, assignedWorker.GetMetadata().GetVersion()); err != nil {
		return nil, nil, err
	}

	newAssignment := workerAssignmentFrom(assignedWorker)
	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_RESUMING
		toUpdate.Status.WorkerAssignment = newAssignment
		return nil
	})
	if err != nil {
		if !errors.Is(err, store.ErrVersionConflict) {
			return nil, nil, err
		}
		// refresh the version of actor to avoid always failure in rest retries.
		fresh, gerr := w.store.GetActor(ctx, actorRef)
		if gerr != nil {
			slog.WarnContext(ctx, "Failed to refresh actor after assignment conflict", slog.Any("err", gerr))
			return nil, nil, err
		}
		switch fresh.GetStatus().GetState() {
		case ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_PAUSED:
			slog.InfoContext(ctx, "Retrying assignment due to actor version conflict", slog.Any("actor", actorRef))
			return fresh, nil, err
		default:
			return nil, nil, status.Errorf(codes.Aborted, "actor %s is %s and can no longer be resumed", actorRef, fresh.GetStatus().GetState())
		}
	}
	poolNamespace = assignedWorker.GetWorkerNamespace()
	pool = assignedWorker.GetWorkerPool()
	outcome = ateattr.SchedulerOutcomeAssigned
	return storedActor, assignedWorker, nil
}

func workerAssignmentFrom(w *ateapipb.Worker) *ateapipb.WorkerAssignment {
	return &ateapipb.WorkerAssignment{
		Worker:          &ateapipb.ObjectRef{Name: w.GetMetadata().GetName()},
		WorkerNamespace: w.GetWorkerNamespace(),
		WorkerPool:      w.GetWorkerPool(),
		WorkerPod:       w.GetWorkerPod(),
		WorkerPodUid:    w.GetWorkerPodUid(),
		WorkerPodIp:     w.GetIp(),
	}
}

// actorResourceLimits returns the actor's declared CPU (millicores) and memory
// (bytes) limits from its ActorTemplate, or 0 for a dimension the template did
// not set. These size the sandbox (supplied over the actor RPCs) and gate
// scheduling (a worker must have >= capacity).
func actorResourceLimits(tmpl *atev1alpha1.ActorTemplate) (cpuMilli, memBytes int64) {
	res := tmpl.Spec.Resources
	if res == nil {
		return 0, 0
	}
	if c := res.Limits.Cpu(); c != nil {
		cpuMilli = c.MilliValue()
	}
	if m := res.Limits.Memory(); m != nil {
		memBytes = m.Value()
	}
	return cpuMilli, memBytes
}

func schedulingConstraints(actor *ateapipb.Actor, tmpl *atev1alpha1.ActorTemplate) (scheduling.Constraints, error) {
	cpuMilli, memBytes := actorResourceLimits(tmpl)
	c := scheduling.Constraints{
		SandboxClass:  string(tmpl.Spec.SandboxClass),
		ActorSelector: labels.SelectorFromSet(labels.Set(actor.GetWorkerSelector().GetMatchLabels())),
		RequiredNodes: actor.GetStatus().GetLocalSnapshotInfo().GetNodeVmsWithLocalSnapshots(),
		CPUMilli:      cpuMilli,
		MemoryBytes:   memBytes,
	}
	if tmpl.Spec.WorkerSelector != nil {
		sel, err := metav1.LabelSelectorAsSelector(tmpl.Spec.WorkerSelector)
		if err != nil {
			return scheduling.Constraints{}, fmt.Errorf("invalid template worker selector: %w", err)
		}
		c.TemplateSelector = sel
	}
	return c, nil
}

// ensureVolumesAttached attaches the actor's mounted external volumes to the
// assigned worker's node. Attachment is idempotent, so a re-entered workflow
// safely runs it again.
// TODO replace re-execution with a proper check on the volumes' attach state.
func (w *ActorWorkflow) ensureVolumesAttached(ctx context.Context, actor *ateapipb.Actor, worker *ateapipb.Worker, actorTemplate *atev1alpha1.ActorTemplate) (err error) {
	ctx, done := stepSpan(ctx, "AttachVolumes")
	defer func() { err = done(err) }()

	node := worker.GetNodeName()
	if node == "" {
		return fmt.Errorf("assigned worker has no node name")
	}

	ref := &ateapipb.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()}
	for _, vol := range getMountedActorVolumes(ctx, ref, actor.GetStatus().GetActorVolumes(), actorTemplate) {
		slog.InfoContext(ctx, "Attaching volume to node", slog.String("volume_id", vol.GetStorageVolumeId()), slog.String("node", node))
		plugin, err := w.pluginRegistry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			return fmt.Errorf("failed to get volume plugin for %q: %w", vol.GetVolumeType(), err)
		}
		if err := plugin.AttachVolume(ctx, vol.GetStorageVolumeId(), node); err != nil {
			return fmt.Errorf("failed to attach volume %q to node %q: %w", vol.GetStorageVolumeId(), node, err)
		}
	}
	return nil
}

// ensureAteletRestored brings the workload up on the assigned worker:
// restoring the actor's local snapshot when one exists, else its durable (or
// golden) snapshot, else cold-booting from the template spec. This is the
// atelet reentrancy seam (#372): the request is keyed by the actor UID and
// the worker pod UID, so a re-entered workflow re-sends the same semantic
// request; once atelet's Restore/Run are idempotent on those keys this step
// becomes fully reentrant with no changes here.
func (w *ActorWorkflow) ensureAteletRestored(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate, src resumeSnapshotSource) (tele restoreTelemetry, err error) {
	ctx, done := stepSpan(ctx, "CallAteletRestore")
	defer func() { err = done(err) }()

	assignment := actor.GetStatus().GetWorkerAssignment()
	ateletConn, err := w.dialer.DialForWorker(assignment.GetWorkerNamespace(), assignment.GetWorkerPod())
	if err != nil {
		return tele, err
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

	workloadSpec, err := workloadSpecFromActorTemplate(actorTemplate, actor)
	if err != nil {
		return tele, err
	}
	egressGateway := w.egressGateway()

	// The actor's declared limits ride the RPC down to the sandbox so it is sized
	// to the actor (replacing the worker-pod downward-API approach).
	cpuMilli, memBytes := actorResourceLimits(actorTemplate)

	if local := actor.GetStatus().GetLocalSnapshotInfo(); local != nil {
		slog.InfoContext(ctx, "Actor has snapshot; Restoring from snapshot")
		tele.SnapshotKind = ateattr.SnapshotKindLocal

		req := &ateletpb.RestoreRequest{
			TargetAteomUid:         assignment.GetWorkerPodUid(),
			Atespace:               actor.GetMetadata().GetAtespace(),
			ActorName:              actor.GetMetadata().GetName(),
			ActorTemplateNamespace: actor.GetActorTemplateNamespace(),
			ActorTemplateName:      actor.GetActorTemplateName(),
			Spec:                   workloadSpec,
			ActorUid:               actor.GetMetadata().Uid,
			EgressGateway:          egressGateway,
			CpuMilli:               cpuMilli,
			MemoryBytes:            memBytes,
		}
		req.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
		req.Config = &ateletpb.RestoreRequest_LocalConfig{
			LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: local.GetSnapshotName()},
		}
		// The wire scope describes the restore OPERATION. When the template's
		// onResume configuration selected the golden snapshot as the boot
		// source, loadActorForResume resolved the golden URI, and the
		// pause snapshot restores as DATA_ON_GOLDEN — atelet combines the
		// golden snapshot's guest state with the actor's data. Otherwise the
		// scope mirrors what the pause captured.
		req.Scope = toAteletSnapshotScope(actorTemplate.Spec.SnapshotsConfig.OnPause)
		if !src.GoldenSnapshotURI.IsZero() {
			req.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN
			req.GoldenSnapshotUri = src.GoldenSnapshotURI.String()
		}
		tele.WireSnapshotScope = ateattr.SnapshotScopeValue(req.Scope)

		_, err = client.Restore(ctx, req)
		return tele, maybeCrashActor(ctx, w.store, actorRef, err, "while restoring workload", ateattr.OperationResume)
	} else if !src.SnapshotURI.IsZero() {
		slog.InfoContext(ctx, "Actor has durable snapshot; Restoring from snapshot")
		// Mirrors loadActorForResume's source resolution: the durable URI is
		// the actor's own snapshot when one exists, the golden otherwise.
		tele.SnapshotKind = ateattr.SnapshotKindGolden
		if actor.GetStatus().GetLatestSnapshot() != nil {
			tele.SnapshotKind = ateattr.SnapshotKindLatest
		}

		// Same wire-scope derivation as the local branch above: the snapshot
		// restores as DATA_ON_GOLDEN when the golden URI was resolved per the
		// template's onResume configuration.
		scope := actorSnapshotContentScopeToAtelet(src.Scope)
		if !src.GoldenSnapshotURI.IsZero() {
			scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN
		}
		tele.WireSnapshotScope = ateattr.SnapshotScopeValue(scope)
		req := &ateletpb.RestoreRequest{
			TargetAteomUid:         assignment.GetWorkerPodUid(),
			Atespace:               actor.GetMetadata().GetAtespace(),
			ActorName:              actor.GetMetadata().GetName(),
			ActorTemplateNamespace: actor.GetActorTemplateNamespace(),
			ActorTemplateName:      actor.GetActorTemplateName(),
			Spec:                   workloadSpec,
			Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
			Config: &ateletpb.RestoreRequest_ExternalConfig{
				ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
					SnapshotUri: src.SnapshotURI.String(),
				},
			},
			Scope: scope,
			// Empty unless this is a Golden data resume.
			GoldenSnapshotUri: src.GoldenSnapshotURI.String(),
			ActorUid:          actor.GetMetadata().Uid,
			EgressGateway:     egressGateway,
			CpuMilli:          cpuMilli,
			MemoryBytes:       memBytes,
		}
		_, err = client.Restore(ctx, req)
		return tele, maybeCrashActor(ctx, w.store, actorRef, err, "while restoring durable snapshot", ateattr.OperationResume)
	} else {
		slog.InfoContext(ctx, "Actor has no snapshot; ActorTemplate has no golden snapshot; Booting from ActorTemplate spec")
		tele.SnapshotKind = ateattr.SnapshotKindBoot

		// Booting from scratch: resolve the sandbox binaries from the pool's
		// SandboxConfig and send them so atelet can fetch and record them.
		// (Restores above are self-describing via the snapshot manifest.)
		sandboxAssets, err := resolveSandboxAssets(w.workerPoolLister, w.sandboxConfigLister, assignment.GetWorkerNamespace(), assignment.GetWorkerPool())
		if err != nil {
			return tele, fmt.Errorf("while resolving sandbox assets: %w", err)
		}

		req := &ateletpb.RunRequest{
			TargetAteomUid:         assignment.GetWorkerPodUid(),
			Atespace:               actor.GetMetadata().GetAtespace(),
			ActorName:              actor.GetMetadata().GetName(),
			ActorTemplateNamespace: actor.GetActorTemplateNamespace(),
			ActorTemplateName:      actor.GetActorTemplateName(),
			SandboxAssets:          sandboxAssets,
			Spec:                   workloadSpec,
			ActorUid:               actor.GetMetadata().Uid,
			EgressGateway:          egressGateway,
			CpuMilli:               cpuMilli,
			MemoryBytes:            memBytes,
		}
		_, err = client.Run(ctx, req)
		return tele, maybeCrashActor(ctx, w.store, actorRef, err, "while creating workload from spec", ateattr.OperationResume)
	}
}

func (w *ActorWorkflow) egressGateway() *ateletpb.EgressGateway {
	if w.egressGatewayAddress == "" {
		return nil
	}
	return &ateletpb.EgressGateway{Address: w.egressGatewayAddress}
}

// finalizeRunning re-reads the actor for a fresh version and commits RUNNING.
func (w *ActorWorkflow) finalizeRunning(ctx context.Context, actorRef resources.ActorRef) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "FinalizeRunning")
	defer func() { err = done(err) }()

	latestActor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, err
	}

	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(latestActor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
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
