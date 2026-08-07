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
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

// ResumeInput holds the immutable parameters requested by the client.
type ResumeInput struct {
	ActorRef resources.ActorRef
	Boot     bool
}

// ResumeState holds the mutable state loaded and modified during execution.
type ResumeState struct {
	Actor            *ateapipb.Actor
	Worker           *ateapipb.Worker
	ActorTemplate    *atev1alpha1.ActorTemplate
	WasRunning       bool
	SnapshotLocation string
	SnapshotScope    ateapipb.SnapshotContentScope
	SnapshotKind     string
	// WireSnapshotScope labels the restore requested, not the stored snapshot's
	// SnapshotScope: a data snapshot restored on golden goes out as data_on_golden.
	WireSnapshotScope string
	// GoldenSnapshotLocation is the storage location of the ActorTemplate's
	// golden snapshot. Populated only when the template's onResume
	// configuration selects the golden snapshot as the boot source for the
	// pending restore: restore then combines the golden snapshot with the
	// actor's data.
	GoldenSnapshotLocation string
}

// validateGoldenSnapshotScope rejects a golden snapshot that does not carry
// the guest state (memory + fs delta) a restore needs. Golden actors always
// commit Full (commitSnapshotScope), so this only trips on golden snapshots
// taken before that rule existed — surface a clear error instead of shipping
// a restore request atelet would reject (or that would boot an empty guest).
func validateGoldenSnapshotScope(snapshot *ateapipb.ActorSnapshot) error {
	switch snapshot.GetContentScope() {
	case ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED,
		ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL:
		return nil
	default:
		return status.Errorf(codes.FailedPrecondition,
			"ActorTemplate golden snapshot %q was taken with scope %s, not Full; regenerate the golden snapshot",
			snapshot.GetMetadata().GetName(), snapshot.GetContentScope())
	}
}

type LoadActorForResumeStep struct {
	store               store.Interface
	actorTemplateLister listersv1alpha1.ActorTemplateLister
}

func (s *LoadActorForResumeStep) Name() string { return "LoadActorForResume" }
func (s *LoadActorForResumeStep) IsComplete(ctx context.Context, input *ResumeInput, state *ResumeState) (bool, error) {
	// Always run this step to get the latest state from the DB
	return false, nil
}
func (s *LoadActorForResumeStep) CheckPrerequisite(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	return nil
}
func (s *LoadActorForResumeStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	actor, err := s.store.GetActor(ctx, input.ActorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return status.Errorf(codes.NotFound, "Actor %s not found", input.ActorRef)
		}
		return fmt.Errorf("while getting actor from DB: %w", err)
	}
	state.Actor = actor
	state.WasRunning = (actor.GetStatus() == ateapipb.Actor_STATUS_RUNNING)

	actorTemplate, err := s.actorTemplateLister.ActorTemplates(actor.GetActorTemplateNamespace()).Get(actor.GetActorTemplateName())
	if err != nil {
		return fmt.Errorf("while getting ActorTemplate: %w", err)
	}
	state.ActorTemplate = actorTemplate
	if ref := actor.GetLatestSnapshot(); ref != nil {
		snapshot, location, err := s.store.GetActorSnapshot(ctx, ref.GetAtespace(), ref.GetName())
		if errors.Is(err, store.ErrNotFound) {
			return status.Error(codes.DataLoss, "ActorSnapshot data is missing")
		}
		if err != nil {
			return fmt.Errorf("while getting ActorSnapshot: %w", err)
		}
		state.SnapshotLocation = location
		state.SnapshotScope = snapshot.GetContentScope()
	} else if actorTemplate.Status.GoldenSnapshot != "" && !input.Boot {
		snapshot, location, err := s.store.GetActorSnapshot(ctx, resources.GoldenActorAtespace, actorTemplate.Status.GoldenSnapshot)
		if errors.Is(err, store.ErrNotFound) {
			return status.Error(codes.DataLoss, "ActorTemplate golden snapshot data is missing")
		}
		if err != nil {
			return fmt.Errorf("while getting golden ActorSnapshot: %w", err)
		}
		if err := validateGoldenSnapshotScope(snapshot); err != nil {
			return err
		}
		state.SnapshotLocation = location
		state.SnapshotScope = snapshot.GetContentScope()
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
		if actor.GetLocalSnapshotInfo() != nil {
			dataOnly = actorTemplate.Spec.SnapshotsConfig.OnPause == atev1alpha1.SnapshotScopeData
		} else if actor.GetLatestSnapshot() != nil {
			dataOnly = state.SnapshotScope == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
		}
		if dataOnly {
			if actorTemplate.Status.GoldenSnapshot == "" {
				return status.Error(codes.FailedPrecondition, "a Golden data resume requires the ActorTemplate golden snapshot, which is not available")
			}
			goldenSnapshot, goldenLocation, err := s.store.GetActorSnapshot(ctx, resources.GoldenActorAtespace, actorTemplate.Status.GoldenSnapshot)
			if errors.Is(err, store.ErrNotFound) {
				return status.Error(codes.DataLoss, "ActorTemplate golden snapshot data is missing")
			}
			if err != nil {
				return fmt.Errorf("while getting golden ActorSnapshot: %w", err)
			}
			if err := validateGoldenSnapshotScope(goldenSnapshot); err != nil {
				return err
			}
			state.GoldenSnapshotLocation = goldenLocation
		}
	}

	// If the Actor is in Resuming state, it means a previous attempt crashed after AssignWorkerStep.
	// We don't need to repeat the AssignWorkerStep, load the Worker now.
	if actor.Status == ateapipb.Actor_STATUS_RESUMING {
		assignment := actor.GetWorkerAssignment()
		if assignment == nil {
			slog.ErrorContext(ctx, "expected a worker assignment on a RESUMING actor, found none")

			// Crash the actor if its worker assignment is missing. We should never be in this state.
			if cerr := crashActor(ctx, s.store, input.ActorRef, ateattr.OperationResume, ateattr.ReasonCorruptedAssignment); cerr != nil {
				return cerr
			}
			return status.Errorf(codes.Aborted, "actor %s crashed", input.ActorRef)
		}

		wk, err := s.store.GetWorker(ctx, assignment.GetWorkerNamespace(), assignment.GetWorkerPool(), assignment.GetWorkerPod())
		if err != nil {
			// Crash the actor if it was assigned to a deleted pod.
			if errors.Is(err, store.ErrNotFound) {
				if cerr := crashActor(ctx, s.store, input.ActorRef, ateattr.OperationResume, ateattr.ReasonWorkerPodGone); cerr != nil {
					return cerr
				}
				return status.Errorf(codes.Aborted, "actor %s crashed", input.ActorRef)
			}
			return fmt.Errorf("failed to get already assigned worker for actor %w", err)
		}
		if wk.GetState() == ateapipb.Worker_STATE_DRAINING {
			slog.InfoContext(ctx, "Assigned worker is draining; crashing actor",
				slog.String("actor", input.ActorRef.String()),
				slog.String("worker", wk.GetWorkerNamespace()+"/"+wk.GetWorkerPod()))
			if cerr := crashActor(ctx, s.store, input.ActorRef, ateattr.OperationResume, ateattr.ReasonWorkerReassigned); cerr != nil {

				return cerr
			}
			return status.Errorf(codes.Aborted, "actor %s crashed", input.ActorRef.String())
		}
		state.Worker = wk
	}
	return nil
}

func (s *LoadActorForResumeStep) RetryBackoff() *wait.Backoff { return nil }

// CreateVolumesStep provisions any initial actor volumes that are in PENDING state.
type CreateVolumesStep struct {
	store store.Interface
}

func (s *CreateVolumesStep) Name() string { return "CreateVolumes" }

func (s *CreateVolumesStep) IsComplete(ctx context.Context, input *ResumeInput, state *ResumeState) (bool, error) {
	for _, vol := range state.Actor.GetActorVolumes() {
		if vol.GetStatus() == ateapipb.ExternalVolume_STATUS_PENDING {
			return false, nil
		}
	}
	return true, nil
}

func (s *CreateVolumesStep) CheckPrerequisite(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	if state.Actor == nil {
		return fmt.Errorf("actor is required for CreateVolumesStep")
	}
	if state.ActorTemplate == nil {
		return fmt.Errorf("actorTemplate is required for CreateVolumesStep")
	}
	return nil
}

func (s *CreateVolumesStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	volumes, err := createActorVolumes(ctx, state.Actor.GetMetadata().GetUid(), state.ActorTemplate, state.Actor.GetActorVolumes())
	state.Actor.ActorVolumes = volumes
	if err != nil {
		// Even if volume creation failed, we still want to persist any updated volume state.
		if updated, updateErr := s.store.UpdateActor(ctx, state.Actor, state.Actor.GetMetadata().GetVersion()); updateErr != nil {
			slog.ErrorContext(ctx, "failed to update actor volumes on volume creation failure in resume", slog.Any("error", updateErr))
		} else {
			state.Actor = updated
		}
		return err
	}
	updated, updateErr := s.store.UpdateActor(ctx, state.Actor, state.Actor.GetMetadata().GetVersion())
	if updateErr != nil {
		return fmt.Errorf("while updating actor after volume creation: %w", updateErr)
	}
	state.Actor = updated
	return nil
}

func (s *CreateVolumesStep) RetryBackoff() *wait.Backoff { return nil }

type AssignWorkerStep struct {
	store       store.Interface
	workerCache *workercache.Cache
	scheduler   scheduling.Scheduler
	instruments *Instruments
}

func (s *AssignWorkerStep) Name() string { return "AssignWorker" }

func (s *AssignWorkerStep) IsComplete(ctx context.Context, input *ResumeInput, state *ResumeState) (bool, error) {
	// RESUMING means a previous attempt already assigned a worker (loaded by
	// LoadActorForResumeStep); RUNNING is past this step entirely.
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_RESUMING || state.Actor.GetStatus() == ateapipb.Actor_STATUS_RUNNING, nil
}
func (s *AssignWorkerStep) CheckPrerequisite(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	switch state.Actor.GetStatus() {
	case ateapipb.Actor_STATUS_SUSPENDED, ateapipb.Actor_STATUS_PAUSED:
		return nil
	default:
		return status.Errorf(codes.FailedPrecondition, "AssignWorkerStep prerequisite not met for Actor: %s (got: %v, want %s or %s)", input.ActorRef, state.Actor.GetStatus(), ateapipb.Actor_STATUS_SUSPENDED, ateapipb.Actor_STATUS_PAUSED)
	}
}

// schedulerRecordable excludes retried version conflicts: runStep re-runs Execute
// transparently on store.ErrVersionConflict, so counting those attempts would
// inflate the error rate and double-count the eventual success.
func schedulerRecordable(err error) bool {
	return !errors.Is(err, store.ErrVersionConflict)
}

func (s *AssignWorkerStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) (err error) {
	start := time.Now()
	outcome := ateattr.SchedulerOutcomeError
	pool := ""
	class := ""
	if state.ActorTemplate != nil {
		class = string(state.ActorTemplate.Spec.SandboxClass)
	}
	defer func() {
		if schedulerRecordable(err) {
			s.instruments.recordSchedulerAssignment(ctx, start, outcome, pool, class, err)
		}
	}()

	workers, err := s.workerCache.Workers()
	if err != nil {
		return fmt.Errorf("while listing workers: %w", err)
	}

	constraints, err := schedulingConstraints(state.Actor, state.ActorTemplate)
	if err != nil {
		return err
	}

	var assignedWorker *ateapipb.Worker

	// Check if we already have a worker assigned from a previous failed attempt.
	// This can happen if ateapi crashed after updating worker with actor assignment,
	// but has not yet updated the actor.
	for _, worker := range workers {
		if worker.Assignment == nil {
			continue
		}
		if worker.Assignment.GetActorUid() != state.Actor.GetMetadata().GetUid() {
			continue
		}
		if s.scheduler.Applies(worker, constraints) {
			assignedWorker = worker
			break
		}
		// Workers() returns pointers directly from the cache so we need to clone before
		// mutating so that the cache is not corrupted if UpdateWorker fails.
		releaseWorker := proto.Clone(worker).(*ateapipb.Worker)
		releaseWorker.Assignment = nil
		// The claimed worker is no longer eligible (e.g. the actor's
		// worker_selector changed after the failed attempt); release it back
		// to the free pool — nothing else reclaims a healthy worker whose
		// actor moved on to a different pool. Best effort in the background.
		go func(release *ateapipb.Worker) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := s.store.UpdateWorker(bgCtx, release, release.Version); err != nil {
				slog.ErrorContext(bgCtx, "Failed to release stale worker assignment",
					slog.String("worker", release.GetWorkerNamespace()+"/"+release.GetWorkerPod()),
					slog.Any("err", err))
			}
		}(releaseWorker)
	}
	if assignedWorker == nil {
		pickedWorker, err := s.scheduler.Schedule(ctx, constraints)
		if err != nil {
			if errors.Is(err, scheduling.ErrNoCapacity) {
				outcome = ateattr.SchedulerOutcomeNoFreeWorker
				return status.Errorf(codes.FailedPrecondition, "no free workers available")
			}
			return err
		}

		assignedWorker = pickedWorker
		slog.InfoContext(ctx, "Picked worker", slog.Any("worker", pickedWorker.String()))
	}

	// Workers() returns pointers directly from the cache so we need to clone before
	// mutating so that the cache is not corrupted if UpdateWorker fails.
	assignedWorker = proto.Clone(assignedWorker).(*ateapipb.Worker)
	assignedWorker.Assignment = &ateapipb.Assignment{
		ActorTemplate: &ateapipb.KubeNamespacedObjectRef{
			Namespace: state.Actor.GetActorTemplateNamespace(),
			Name:      state.Actor.GetActorTemplateName(),
		},
		Actor: &ateapipb.ObjectRef{
			Atespace: state.Actor.GetMetadata().GetAtespace(),
			Name:     state.Actor.GetMetadata().GetName(),
		},
		ActorUid: state.Actor.GetMetadata().GetUid(),
	}

	if err := s.store.UpdateWorker(ctx, assignedWorker, assignedWorker.Version); err != nil {
		return err
	}

	state.Actor.Status = ateapipb.Actor_STATUS_RESUMING
	state.Actor.WorkerAssignment = workerAssignmentFrom(assignedWorker)

	updatedActor, err := s.store.UpdateActor(ctx, state.Actor, state.Actor.GetMetadata().GetVersion())
	if err != nil {
		if !errors.Is(err, store.ErrVersionConflict) {
			return err
		}
		// refresh the version of actor to avoid always failure in rest retries.
		fresh, gerr := s.store.GetActor(ctx, input.ActorRef)
		if gerr != nil {
			slog.WarnContext(ctx, "Failed to refresh actor after assignment conflict", slog.Any("err", gerr))
			return err
		}
		switch fresh.GetStatus() {
		case ateapipb.Actor_STATUS_SUSPENDED, ateapipb.Actor_STATUS_PAUSED:
			slog.InfoContext(ctx, "Retrying assignment due to actor version conflict", slog.Any("actor", input.ActorRef))
			state.Actor = fresh
			return err
		default:
			return status.Errorf(codes.Aborted, "actor %s is %s and can no longer be resumed", input.ActorRef, fresh.GetStatus())
		}
	}
	state.Actor = updatedActor
	state.Worker = assignedWorker
	pool = assignedWorker.GetWorkerPool()
	outcome = ateattr.SchedulerOutcomeAssigned
	return nil
}

func workerAssignmentFrom(w *ateapipb.Worker) *ateapipb.WorkerAssignment {
	return &ateapipb.WorkerAssignment{
		WorkerNamespace: w.GetWorkerNamespace(),
		WorkerPool:      w.GetWorkerPool(),
		WorkerPod:       w.GetWorkerPod(),
		WorkerPodUid:    w.GetWorkerPodUid(),
		WorkerPodIp:     w.GetIp(),
	}
}

func schedulingConstraints(actor *ateapipb.Actor, tmpl *atev1alpha1.ActorTemplate) (scheduling.Constraints, error) {
	c := scheduling.Constraints{
		SandboxClass:  string(tmpl.Spec.SandboxClass),
		ActorSelector: labels.SelectorFromSet(labels.Set(actor.GetWorkerSelector().GetMatchLabels())),
		RequiredNodes: actor.GetLocalSnapshotInfo().GetNodeVmsWithLocalSnapshots(),
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

func (s *AssignWorkerStep) RetryBackoff() *wait.Backoff {
	return &wait.Backoff{
		Steps:    5,
		Duration: 10 * time.Millisecond,
		Factor:   2.0,
		Jitter:   1.0,
	}
}

type AttachVolumesStep struct {
	store store.Interface
}

func (s *AttachVolumesStep) Name() string { return "AttachVolumes" }

func (s *AttachVolumesStep) IsComplete(ctx context.Context, input *ResumeInput, state *ResumeState) (bool, error) {
	// TODO replace with a proper check on the volumes.
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_RUNNING, nil
}

func (s *AttachVolumesStep) CheckPrerequisite(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	return nil
}

func (s *AttachVolumesStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	assignment := state.Actor.GetWorkerAssignment()
	if assignment == nil {
		return fmt.Errorf("actor has no assigned worker pod")
	}

	worker, err := s.store.GetWorker(ctx, assignment.GetWorkerNamespace(), assignment.GetWorkerPool(), assignment.GetWorkerPod())
	if err != nil {
		return fmt.Errorf("failed to get worker for volume attachment: %w", err)
	}

	node := worker.GetNodeName()
	if node == "" {
		return fmt.Errorf("assigned worker has no node name")
	}

	ref := &ateapipb.ObjectRef{Atespace: state.Actor.GetMetadata().GetAtespace(), Name: state.Actor.GetMetadata().GetName()}
	for _, vol := range getMountedActorVolumes(ctx, ref, state.Actor.GetActorVolumes(), state.ActorTemplate) {
		slog.InfoContext(ctx, "Attaching volume to node", slog.String("volume_id", vol.GetStorageVolumeId()), slog.String("node", node))
		err := getVolumePlugin().AttachVolume(ctx, vol.GetStorageVolumeId(), node)
		if err != nil {
			return fmt.Errorf("failed to attach volume %q to node %q: %w", vol.GetStorageVolumeId(), node, err)
		}
	}
	return nil
}

func (s *AttachVolumesStep) RetryBackoff() *wait.Backoff { return nil }

type CallAteletRestoreStep struct {
	store                store.Interface
	dialer               *AteletDialer
	kubeClient           kubernetes.Interface
	secretCache          *envSecretCache
	workerPoolLister     listersv1alpha1.WorkerPoolLister
	sandboxConfigLister  listersv1alpha1.SandboxConfigLister
	scheduler            scheduling.Scheduler
	egressGatewayAddress string
}

func (s *CallAteletRestoreStep) Name() string { return "CallAteletRestore" }
func (s *CallAteletRestoreStep) IsComplete(ctx context.Context, input *ResumeInput, state *ResumeState) (bool, error) {
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_RUNNING, nil
}
func (s *CallAteletRestoreStep) CheckPrerequisite(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	if state.Actor.GetStatus() != ateapipb.Actor_STATUS_RESUMING {
		return status.Errorf(codes.FailedPrecondition, "CallAteletRestoreStep prerequisite not met for Actor: %s (got: %v, want %s)", input.ActorRef, state.Actor.GetStatus(), ateapipb.Actor_STATUS_RESUMING)
	}
	if state.Worker == nil {
		return status.Errorf(codes.FailedPrecondition, "Assigned worker is nil")
	}
	// Verify if the worker is still assigned to the same Actor.
	assignedActorUID := state.Worker.GetAssignment().GetActorUid()
	if assignedActorUID != state.Actor.GetMetadata().GetUid() {
		slog.ErrorContext(ctx, "crashing actor because its assigned worker no longer belongs to it",
			slog.String("worker", state.Worker.GetWorkerPod()),
			slog.Any("assignment", state.Worker.GetAssignment()))
		if cerr := crashActor(ctx, s.store, input.ActorRef, ateattr.OperationResume, ateattr.ReasonWorkerReassigned); cerr != nil {
			return fmt.Errorf("while crashing actor: %w", cerr)
		}
		return status.Errorf(codes.Aborted, "actor %s crashed", input.ActorRef)
	}
	constraints, err := schedulingConstraints(state.Actor, state.ActorTemplate)
	if err != nil {
		return err
	}
	if !s.scheduler.Applies(state.Worker, constraints) {
		slog.ErrorContext(ctx, "crashing actor because previously assigned worker is not eligible anymore")
		release := proto.Clone(state.Worker).(*ateapipb.Worker)
		release.Assignment = nil
		// If that worker's pool is no longer eligible (e.g. the actor's
		// worker_selector was updated after the failed attempt), release it back
		// to the free pool instead of leaving it claimed forever — nothing else
		// reclaims a healthy worker whose actor moved on to a different pool.
		if err := s.store.UpdateWorker(ctx, release, release.Version); err != nil {
			return fmt.Errorf("while releasing stale worker assignment: %w", err)
		}
		if cerr := crashActor(ctx, s.store, input.ActorRef, ateattr.OperationResume, ateattr.ReasonCorruptedAssignment); cerr != nil {
			return fmt.Errorf("while crashing actor: %w", cerr)
		}
		return status.Errorf(codes.Aborted, "actor %s crashed", input.ActorRef)
	}
	return nil
}
func (s *CallAteletRestoreStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	assignment := state.Actor.GetWorkerAssignment()
	ateletConn, err := s.dialer.DialForWorker(assignment.GetWorkerNamespace(), assignment.GetWorkerPod())
	if err != nil {
		return err
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

	workloadSpec, err := workloadSpecFromActorTemplateWithEnv(ctx, s.kubeClient, s.secretCache, state.ActorTemplate, state.Actor)
	if err != nil {
		return err
	}
	egressGateway := s.egressGateway()

	if local := state.Actor.GetLocalSnapshotInfo(); local != nil {
		slog.InfoContext(ctx, "Actor has snapshot; Restoring from snapshot")
		state.SnapshotKind = ateattr.SnapshotKindLocal

		req := &ateletpb.RestoreRequest{
			TargetAteomUid:         assignment.GetWorkerPodUid(),
			Atespace:               state.Actor.GetMetadata().GetAtespace(),
			ActorName:              state.Actor.GetMetadata().GetName(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			Spec:                   workloadSpec,
			ActorUid:               state.Actor.GetMetadata().Uid,
			EgressGateway:          egressGateway,
		}
		req.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
		req.Config = &ateletpb.RestoreRequest_LocalConfig{
			LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotPrefix: local.GetSnapshotPrefix()},
		}
		// The wire scope describes the restore OPERATION. When the template's
		// onResume configuration selected the golden snapshot as the boot
		// source, LoadActorForResume resolved the golden location, and the
		// pause snapshot restores as DATA_ON_GOLDEN — atelet combines the
		// golden snapshot's guest state with the actor's data. Otherwise the
		// scope mirrors what the pause captured.
		req.Scope = toAteletSnapshotScope(state.ActorTemplate.Spec.SnapshotsConfig.OnPause)
		if state.GoldenSnapshotLocation != "" {
			req.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN
			req.GoldenSnapshotUriPrefix = state.GoldenSnapshotLocation
		}
		state.WireSnapshotScope = ateattr.SnapshotScopeValue(req.Scope)

		_, err = client.Restore(ctx, req)
		return maybeCrashActor(ctx, s.store, input.ActorRef, err, "while restoring workload", ateattr.OperationResume)
	} else if state.SnapshotLocation != "" {
		slog.InfoContext(ctx, "Actor has durable snapshot; Restoring from snapshot")
		// Mirrors LoadActorForResume's source resolution: the durable location
		// is the actor's own snapshot when one exists, the golden otherwise.
		state.SnapshotKind = ateattr.SnapshotKindGolden
		if state.Actor.GetLatestSnapshot() != nil {
			state.SnapshotKind = ateattr.SnapshotKindLatest
		}

		// Same wire-scope derivation as the local branch above: the snapshot
		// restores as DATA_ON_GOLDEN when the golden location was resolved
		// per the template's onResume configuration.
		scope := actorSnapshotContentScopeToAtelet(state.SnapshotScope)
		if state.GoldenSnapshotLocation != "" {
			scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN
		}
		state.WireSnapshotScope = ateattr.SnapshotScopeValue(scope)
		req := &ateletpb.RestoreRequest{
			TargetAteomUid:         assignment.GetWorkerPodUid(),
			Atespace:               state.Actor.GetMetadata().GetAtespace(),
			ActorName:              state.Actor.GetMetadata().GetName(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			Spec:                   workloadSpec,
			Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
			Config: &ateletpb.RestoreRequest_ExternalConfig{
				ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
					SnapshotUriPrefix: state.SnapshotLocation,
				},
			},
			Scope: scope,
			// Empty unless this is a Golden data resume.
			GoldenSnapshotUriPrefix: state.GoldenSnapshotLocation,
			ActorUid:                state.Actor.GetMetadata().Uid,
			EgressGateway:           egressGateway,
		}
		_, err = client.Restore(ctx, req)
		return maybeCrashActor(ctx, s.store, input.ActorRef, err, "while restoring durable snapshot", ateattr.OperationResume)

	} else {
		slog.InfoContext(ctx, "Actor has no snapshot; ActorTemplate has no golden snapshot; Booting from ActorTemplate spec")
		state.SnapshotKind = ateattr.SnapshotKindBoot

		// Booting from scratch: resolve the sandbox binaries from the pool's
		// SandboxConfig and send them so atelet can fetch and record them.
		// (Restores above are self-describing via the snapshot manifest.)
		sandboxAssets, err := resolveSandboxAssets(s.workerPoolLister, s.sandboxConfigLister, assignment.GetWorkerNamespace(), assignment.GetWorkerPool())
		if err != nil {
			return fmt.Errorf("while resolving sandbox assets: %w", err)
		}

		req := &ateletpb.RunRequest{
			TargetAteomUid:         assignment.GetWorkerPodUid(),
			Atespace:               state.Actor.GetMetadata().GetAtespace(),
			ActorName:              state.Actor.GetMetadata().GetName(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			SandboxAssets:          sandboxAssets,
			Spec:                   workloadSpec,
			ActorUid:               state.Actor.GetMetadata().Uid,
			EgressGateway:          egressGateway,
		}
		_, err = client.Run(ctx, req)
		return maybeCrashActor(ctx, s.store, input.ActorRef, err, "while creating workload from spec", ateattr.OperationResume)
	}
	// Unreachable
}

func (s *CallAteletRestoreStep) RetryBackoff() *wait.Backoff { return nil }

func (s *CallAteletRestoreStep) egressGateway() *ateletpb.EgressGateway {
	if s.egressGatewayAddress == "" {
		return nil
	}
	return &ateletpb.EgressGateway{Address: s.egressGatewayAddress}
}

type FinalizeRunningStep struct {
	store store.Interface
}

func (s *FinalizeRunningStep) Name() string { return "FinalizeRunning" }
func (s *FinalizeRunningStep) IsComplete(ctx context.Context, input *ResumeInput, state *ResumeState) (bool, error) {
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_RUNNING, nil
}
func (s *FinalizeRunningStep) CheckPrerequisite(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	if state.Actor.GetStatus() != ateapipb.Actor_STATUS_RESUMING {
		return status.Errorf(codes.FailedPrecondition, "FinalizeRunningStep prerequisite not met for Actor: %s (got: %v, want %s)", input.ActorRef, state.Actor.GetStatus(), ateapipb.Actor_STATUS_RESUMING)
	}
	return nil
}
func (s *FinalizeRunningStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	latestActor, err := s.store.GetActor(ctx, input.ActorRef)
	if err != nil {
		return err
	}

	latestActor.Status = ateapipb.Actor_STATUS_RUNNING
	updatedActor, err := s.store.UpdateActor(ctx, latestActor, latestActor.GetMetadata().GetVersion())
	if err != nil {
		return err
	}
	state.Actor = updatedActor
	return nil
}

func (s *FinalizeRunningStep) RetryBackoff() *wait.Backoff { return nil }
