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
	Actor         *ateapipb.Actor
	Worker        *ateapipb.Worker
	ActorTemplate *atev1alpha1.ActorTemplate
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

	actorTemplate, err := s.actorTemplateLister.ActorTemplates(actor.GetActorTemplateNamespace()).Get(actor.GetActorTemplateName())
	if err != nil {
		return fmt.Errorf("while getting ActorTemplate: %w", err)
	}
	state.ActorTemplate = actorTemplate

	// If the Actor is in Resuming state, it means a previous attempt crashed after AssignWorkerStep.
	// We don't need to repeat the AssignWorkerStep, load the Worker now.
	if actor.Status == ateapipb.Actor_STATUS_RESUMING {
		allPopulated := actor.AteomPodUid != "" && actor.WorkerPoolName != "" && actor.AteomPodName != ""
		if !allPopulated {
			slog.ErrorContext(ctx, "expected all of AteomPodUid, WorkerPoolName and AteomPodName to be populated, found",
				slog.String("AteomPodUid", actor.AteomPodUid),
				slog.String("WorkerPoolName", actor.WorkerPoolName),
				slog.String("AteomPodName", actor.AteomPodName))

			// Crash the actor if its worker assignment is corrupted. We should never be in this state.
			if cerr := crashActor(ctx, s.store, input.ActorRef); cerr != nil {
				return cerr
			}
			return status.Errorf(codes.Aborted, "actor %s crashed", input.ActorRef)
		}

		wk, err := s.store.GetWorker(ctx, actor.AteomPodNamespace, actor.WorkerPoolName, actor.AteomPodName)
		if err != nil {
			// Crash the actor if it was assigned to a deleted pod.
			if errors.Is(err, store.ErrNotFound) {
				if cerr := crashActor(ctx, s.store, input.ActorRef); cerr != nil {
					return cerr
				}
				return status.Errorf(codes.Aborted, "actor %s crashed", input.ActorRef)
			}
			return fmt.Errorf("failed to get already assigned worker for actor %w", err)
		}
		state.Worker = wk
	}
	return nil
}

func (s *LoadActorForResumeStep) RetryBackoff() *wait.Backoff { return nil }

type AssignWorkerStep struct {
	store       store.Interface
	workerCache *workercache.Cache
	scheduler   scheduling.Scheduler
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

func (s *AssignWorkerStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
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
		if resources.ActorRefFromObjectRef(worker.Assignment.Actor) != input.ActorRef {
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
		Actor: input.ActorRef.ToObjectRef(),
	}

	if err := s.store.UpdateWorker(ctx, assignedWorker, assignedWorker.Version); err != nil {
		return err
	}

	state.Actor.Status = ateapipb.Actor_STATUS_RESUMING
	state.Actor.AteomPodNamespace = assignedWorker.GetWorkerNamespace()
	state.Actor.AteomPodName = assignedWorker.GetWorkerPod()
	state.Actor.AteomPodIp = assignedWorker.GetIp()
	state.Actor.AteomPodUid = assignedWorker.GetWorkerPodUid()
	state.Actor.WorkerPoolName = assignedWorker.GetWorkerPool()

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
	return nil
}

func schedulingConstraints(actor *ateapipb.Actor, tmpl *atev1alpha1.ActorTemplate) (scheduling.Constraints, error) {
	c := scheduling.Constraints{
		SandboxClass:  string(tmpl.Spec.SandboxClass),
		ActorSelector: labels.SelectorFromSet(labels.Set(actor.GetWorkerSelector().GetMatchLabels())),
		RequiredNodes: actor.GetLatestSnapshotInfo().GetLocal().GetNodeVmsWithLocalSnapshots(),
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
	if state.Actor.GetAteomPodNamespace() == "" {
		return fmt.Errorf("actor has no assigned worker pod")
	}

	worker, err := s.store.GetWorker(ctx, state.Actor.GetAteomPodNamespace(), state.Actor.GetWorkerPoolName(), state.Actor.GetAteomPodName())
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
	store               store.Interface
	dialer              *AteletDialer
	kubeClient          kubernetes.Interface
	secretCache         *envSecretCache
	workerPoolLister    listersv1alpha1.WorkerPoolLister
	sandboxConfigLister listersv1alpha1.SandboxConfigLister
	scheduler           scheduling.Scheduler
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
	assigned := state.Worker.GetAssignment().GetActor()
	if resources.ActorRefFromObjectRef(assigned) != input.ActorRef {
		slog.ErrorContext(ctx, "crashing actor because its assigned worker no longer belongs to it",
			slog.String("worker", state.Worker.GetWorkerPod()),
			slog.Any("assignment", state.Worker.GetAssignment()))
		if cerr := crashActor(ctx, s.store, input.ActorRef); cerr != nil {
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
		if cerr := crashActor(ctx, s.store, input.ActorRef); cerr != nil {
			return fmt.Errorf("while crashing actor: %w", cerr)
		}
		return status.Errorf(codes.Aborted, "actor %s crashed", input.ActorRef)
	}
	return nil
}
func (s *CallAteletRestoreStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	ateletConn, err := s.dialer.DialForWorker(state.Actor.GetAteomPodNamespace(), state.Actor.GetAteomPodName())
	if err != nil {
		return err
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

	workloadSpec, err := workloadSpecFromActorTemplateWithEnv(ctx, s.kubeClient, s.secretCache, state.ActorTemplate, state.Actor)
	if err != nil {
		return err
	}

	if data := state.Actor.GetLatestSnapshotInfo().GetData(); data != nil {
		slog.InfoContext(ctx, "Actor has snapshot; Restoring from snapshot")

		req := &ateletpb.RestoreRequest{
			TargetAteomUid:         state.Actor.GetAteomPodUid(),
			Atespace:               state.Actor.GetMetadata().GetAtespace(),
			ActorName:              state.Actor.GetMetadata().GetName(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			Spec:                   workloadSpec,
			ActorUid:               state.Actor.GetMetadata().Uid,
		}
		switch d := data.(type) {
		case *ateapipb.SnapshotInfo_Local:
			req.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			req.Config = &ateletpb.RestoreRequest_LocalConfig{
				LocalConfig: &ateletpb.LocalCheckpointConfiguration{
					SnapshotPrefix: d.Local.GetSnapshotPrefix(),
				},
			}
			req.Scope = toAteletSnapshotScope(state.ActorTemplate.Spec.SnapshotsConfig.OnPause)
		case *ateapipb.SnapshotInfo_External:
			req.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL
			req.Config = &ateletpb.RestoreRequest_ExternalConfig{
				ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
					SnapshotUriPrefix: d.External.GetSnapshotUriPrefix(),
				},
			}
			req.Scope = toAteletSnapshotScope(state.ActorTemplate.Spec.SnapshotsConfig.OnCommit)
		default:
			return fmt.Errorf("unsupported snapshot type: %T", data)
		}

		_, err = client.Restore(ctx, req)
		return maybeCrashActor(ctx, s.store, input.ActorRef, err, "while restoring workload")
	} else if state.ActorTemplate.Status.GoldenSnapshot != "" && !input.Boot {
		slog.InfoContext(ctx, "Actor has no snapshot; ActorTemplate has golden snapshot; Restoring from golden snapshot")

		snapshot := state.ActorTemplate.Status.GoldenSnapshot

		req := &ateletpb.RestoreRequest{
			TargetAteomUid:         state.Actor.GetAteomPodUid(),
			Atespace:               state.Actor.GetMetadata().GetAtespace(),
			ActorName:              state.Actor.GetMetadata().GetName(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			Spec:                   workloadSpec,
			Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
			Config: &ateletpb.RestoreRequest_ExternalConfig{
				ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
					SnapshotUriPrefix: snapshot,
				},
			},
			Scope:    toAteletSnapshotScope(state.ActorTemplate.Spec.SnapshotsConfig.OnCommit),
			ActorUid: state.Actor.GetMetadata().Uid,
		}
		_, err = client.Restore(ctx, req)
		return maybeCrashActor(ctx, s.store, input.ActorRef, err, "while creating workload from golden snapshot")
	} else {
		slog.InfoContext(ctx, "Actor has no snapshot; ActorTemplate has no golden snapshot; Booting from ActorTemplate spec")

		// Booting from scratch: resolve the sandbox binaries from the pool's
		// SandboxConfig and send them so atelet can fetch and record them.
		// (Restores above are self-describing via the snapshot manifest.)
		sandboxAssets, err := resolveSandboxAssets(s.workerPoolLister, s.sandboxConfigLister, state.Actor.GetAteomPodNamespace(), state.Actor.GetWorkerPoolName())
		if err != nil {
			return fmt.Errorf("while resolving sandbox assets: %w", err)
		}

		req := &ateletpb.RunRequest{
			TargetAteomUid:         state.Actor.GetAteomPodUid(),
			Atespace:               state.Actor.GetMetadata().GetAtespace(),
			ActorName:              state.Actor.GetMetadata().GetName(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			SandboxAssets:          sandboxAssets,
			Spec:                   workloadSpec,
			ActorUid:               state.Actor.GetMetadata().Uid,
		}
		_, err = client.Run(ctx, req)
		return maybeCrashActor(ctx, s.store, input.ActorRef, err, "while creating workload from spec")
	}
	// Unreachable
}

func (s *CallAteletRestoreStep) RetryBackoff() *wait.Backoff { return nil }

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
