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
	"math/rand"
	"slices"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

// ResumeInput holds the immutable parameters requested by the client.
type ResumeInput struct {
	ActorID string
	Boot    bool
}

// ResumeState holds the mutable state loaded and modified during execution.
type ResumeState struct {
	Actor         *ateapipb.Actor
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
func (s *LoadActorForResumeStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	actor, err := s.store.GetActor(ctx, input.ActorID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return status.Errorf(codes.NotFound, "Actor %s not found", input.ActorID)
		}
		return fmt.Errorf("while getting actor from DB: %w", err)
	}
	state.Actor = actor

	actorTemplate, err := s.actorTemplateLister.ActorTemplates(actor.GetActorTemplateNamespace()).Get(actor.GetActorTemplateName())
	if err != nil {
		return fmt.Errorf("while getting ActorTemplate: %w", err)
	}
	state.ActorTemplate = actorTemplate

	return nil
}

func (s *LoadActorForResumeStep) RetryBackoff() *wait.Backoff { return nil }

func eligibleWorkerPools(pools []*atev1alpha1.WorkerPool, templateClass atev1alpha1.SandboxClass, templateSelector *metav1.LabelSelector, actorSelector *ateapipb.Selector) (map[types.NamespacedName]struct{}, error) {
	templateSel := labels.Everything()
	if templateSelector != nil {
		sel, err := metav1.LabelSelectorAsSelector(templateSelector)
		if err != nil {
			return nil, fmt.Errorf("invalid template worker selector: %w", err)
		}
		templateSel = sel
	}

	actorSel := labels.SelectorFromSet(labels.Set(actorSelector.GetMatchLabels()))

	eligible := make(map[types.NamespacedName]struct{})
	for _, pool := range pools {
		// Snapshots are not portable across sandbox classes, so the pool's class
		// must match the template's. This is a hard gate AND'd with the label
		// selectors below. Both classes are populated by the CRD default (gvisor),
		// so we compare them directly.
		if pool.Spec.SandboxClass != templateClass {
			continue
		}
		set := labels.Set(pool.GetLabels())
		if templateSel.Matches(set) && actorSel.Matches(set) {
			eligible[types.NamespacedName{Namespace: pool.GetNamespace(), Name: pool.GetName()}] = struct{}{}
		}
	}
	return eligible, nil
}

type AssignWorkerStep struct {
	store            store.Interface
	workerCache      *workercache.Cache
	workerPoolLister listersv1alpha1.WorkerPoolLister
}

func (s *AssignWorkerStep) Name() string { return "AssignWorker" }

func (s *AssignWorkerStep) IsComplete(ctx context.Context, input *ResumeInput, state *ResumeState) (bool, error) {
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_RUNNING, nil
}
func (s *AssignWorkerStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	pools, err := s.workerPoolLister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("while listing worker pools: %w", err)
	}
	eligible, err := eligibleWorkerPools(pools, state.ActorTemplate.Spec.SandboxClass, state.ActorTemplate.Spec.WorkerSelector, state.Actor.GetWorkerSelector())
	if err != nil {
		return fmt.Errorf("while computing eligible worker pools: %w", err)
	}
	if len(eligible) == 0 {
		return status.Errorf(codes.FailedPrecondition, "no worker pool matches the template's sandboxClass and the template/actor selectors")
	}

	workers, err := s.workerCache.Workers()
	if err != nil {
		return fmt.Errorf("while listing workers: %w", err)
	}

	var assignedWorker *ateapipb.Worker

	// Check if we already have a worker assigned from a previous failed attempt.
	// If that worker's pool is no longer eligible (e.g. the actor's
	// worker_selector was updated after the failed attempt), release it back
	// to the free pool instead of leaving it claimed forever — nothing else
	// reclaims a healthy worker whose actor moved on to a different pool.
	for _, worker := range workers {
		if worker.GetActorId() != input.ActorID {
			continue
		}
		if _, ok := eligible[types.NamespacedName{Namespace: worker.GetWorkerNamespace(), Name: worker.GetWorkerPool()}]; ok {
			assignedWorker = worker
			break
		}
		// Workers() returns pointers directly from the cache so we need to clone before
		// mutating so that the cache is not corrupted if UpdateWorker fails.
		release := proto.Clone(worker).(*ateapipb.Worker)
		release.ActorId = ""
		release.ActorNamespace = ""
		release.ActorTemplate = ""
		if err := s.store.UpdateWorker(ctx, release, release.Version); err != nil {
			return fmt.Errorf("while releasing stale worker assignment: %w", err)
		}
	}

	// If not, find a free one using randomized shuffling
	if assignedWorker == nil {
		pickedWorker := s.findFreeWorker(workers, eligible, state.Actor.GetLatestSnapshotInfo().GetLocal().GetNodeVmsWithLocalSnapshots())
		if pickedWorker == nil {
			return status.Errorf(codes.FailedPrecondition, "no free workers available")
		}

		assignedWorker = pickedWorker
		slog.InfoContext(ctx, "Picked worker", slog.Any("worker", pickedWorker.String()))
	}

	// Workers() returns pointers directly from the cache so we need to clone before
	// mutating so that the cache is not corrupted if UpdateWorker fails.
	assignedWorker = proto.Clone(assignedWorker).(*ateapipb.Worker)
	assignedWorker.ActorId = input.ActorID
	assignedWorker.ActorNamespace = state.Actor.GetActorTemplateNamespace()
	assignedWorker.ActorTemplate = state.Actor.GetActorTemplateName()

	if err := s.store.UpdateWorker(ctx, assignedWorker, assignedWorker.Version); err != nil {
		return err
	}

	state.Actor.Status = ateapipb.Actor_STATUS_RESUMING
	state.Actor.AteomPodNamespace = assignedWorker.GetWorkerNamespace()
	state.Actor.AteomPodName = assignedWorker.GetWorkerPod()
	state.Actor.AteomPodIp = assignedWorker.GetIp()
	state.Actor.AteomPodUid = assignedWorker.GetWorkerPodUid()
	state.Actor.WorkerPoolName = assignedWorker.GetWorkerPool()

	if err := s.store.UpdateActor(ctx, state.Actor, state.Actor.GetVersion()); err != nil {
		return err
	}
	return nil
}

func (s *AssignWorkerStep) RetryBackoff() *wait.Backoff {
	return &wait.Backoff{
		Steps:    5,
		Duration: 10 * time.Millisecond,
		Factor:   2.0,
		Jitter:   1.0,
	}
}

func (s *AssignWorkerStep) findFreeWorker(workers []*ateapipb.Worker, eligible map[types.NamespacedName]struct{}, nodesRestrictions []string) *ateapipb.Worker {
	var freeWorkers []*ateapipb.Worker
	for _, worker := range workers {
		if worker.GetActorId() != "" {
			continue
		}
		if _, ok := eligible[types.NamespacedName{Namespace: worker.GetWorkerNamespace(), Name: worker.GetWorkerPool()}]; !ok {
			continue
		}
		if len(nodesRestrictions) == 0 || slices.Contains(nodesRestrictions, worker.GetNodeName()) {
			freeWorkers = append(freeWorkers, worker)
		}
	}

	if len(freeWorkers) > 0 {
		rand.Shuffle(len(freeWorkers), func(i, j int) {
			freeWorkers[i], freeWorkers[j] = freeWorkers[j], freeWorkers[i]
		})
		return freeWorkers[0]
	}
	return nil
}

type CallAteletRestoreStep struct {
	dialer              *AteletDialer
	kubeClient          kubernetes.Interface
	secretCache         *envSecretCache
	workerPoolLister    listersv1alpha1.WorkerPoolLister
	sandboxConfigLister listersv1alpha1.SandboxConfigLister
}

func (s *CallAteletRestoreStep) Name() string { return "CallAteletRestore" }
func (s *CallAteletRestoreStep) IsComplete(ctx context.Context, input *ResumeInput, state *ResumeState) (bool, error) {
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_RUNNING, nil
}
func (s *CallAteletRestoreStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	ateletConn, err := s.dialer.DialForWorker(state.Actor.GetAteomPodNamespace(), state.Actor.GetAteomPodName())
	if err != nil {
		return err
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

	workloadSpec, err := workloadSpecFromActorTemplate(ctx, s.kubeClient, s.secretCache, state.ActorTemplate)
	if err != nil {
		return err
	}

	if state.Actor.GetLatestSnapshotInfo().GetType() != ateapipb.SnapshotType_SNAPSHOT_TYPE_UNSPECIFIED {
		slog.InfoContext(ctx, "Actor has snapshot; Restoring from snapshot")

		req := &ateletpb.RestoreRequest{
			TargetAteomUid:         state.Actor.GetAteomPodUid(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			ActorId:                state.Actor.GetActorId(),
			Spec:                   workloadSpec,
		}
		switch state.Actor.GetLatestSnapshotInfo().GetType() {
		case ateapipb.SnapshotType_SNAPSHOT_TYPE_LOCAL:
			req.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			req.Config = &ateletpb.RestoreRequest_LocalConfig{
				LocalConfig: &ateletpb.LocalCheckpointConfiguration{
					SnapshotPrefix: state.Actor.GetLatestSnapshotInfo().GetLocal().SnapshotPrefix,
				},
			}
		case ateapipb.SnapshotType_SNAPSHOT_TYPE_EXTERNAL:
			req.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL
			req.Config = &ateletpb.RestoreRequest_ExternalConfig{
				ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
					SnapshotUriPrefix: state.Actor.GetLatestSnapshotInfo().GetExternal().SnapshotUriPrefix,
				},
			}
		default:
			return fmt.Errorf("unsupported snapshot type: %v", state.Actor.GetLatestSnapshotInfo().GetType())
		}

		_, err = client.Restore(ctx, req)
		if err != nil {
			return fmt.Errorf("while restoring workload: %w", err)
		}
		return nil
	} else if state.ActorTemplate.Status.GoldenSnapshot != "" && !input.Boot {
		slog.InfoContext(ctx, "Actor has no snapshot; ActorTemplate has golden snapshot; Restoring from golden snapshot")

		snapshot := state.ActorTemplate.Status.GoldenSnapshot

		req := &ateletpb.RestoreRequest{
			TargetAteomUid:         state.Actor.GetAteomPodUid(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			ActorId:                state.Actor.GetActorId(),
			Spec:                   workloadSpec,
			Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
			Config: &ateletpb.RestoreRequest_ExternalConfig{
				ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
					SnapshotUriPrefix: snapshot,
				},
			},
		}
		_, err = client.Restore(ctx, req)
		if err != nil {
			return fmt.Errorf("while creating workload from golden snapshot: %w", err)
		}
		return nil
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
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			ActorId:                state.Actor.GetActorId(),
			SandboxAssets:          sandboxAssets,
			Spec:                   workloadSpec,
		}
		_, err = client.Run(ctx, req)
		if err != nil {
			return fmt.Errorf("while creating workload from spec: %w", err)
		}

		return nil
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
func (s *FinalizeRunningStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	latestActor, err := s.store.GetActor(ctx, input.ActorID)
	if err != nil {
		return err
	}

	latestActor.Status = ateapipb.Actor_STATUS_RUNNING
	err = s.store.UpdateActor(ctx, latestActor, latestActor.GetVersion())
	if err == nil {
		state.Actor = latestActor
	}
	return err
}

func (s *FinalizeRunningStep) RetryBackoff() *wait.Backoff { return nil }
