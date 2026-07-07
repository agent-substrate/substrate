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
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

// ResumeInput holds the immutable parameters requested by the client.
type ResumeInput struct {
	ActorID  string
	Atespace string
	Boot     bool
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
	actor, err := s.store.GetActor(ctx, input.Atespace, input.ActorID)
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

func isWorkerEligibleForActor(worker *ateapipb.Worker, templateClass atev1alpha1.SandboxClass, templateSelector *metav1.LabelSelector, actorSelector *ateapipb.Selector) (bool, error) {
	// Snapshots are not portable across sandbox classes, so the worker's class
	// must match the template's. Both classes are populated by the CRD default
	// (gvisor), so we compare them directly.
	if worker.GetSandboxClass() != string(templateClass) {
		return false, nil
	}

	templateSel := labels.Everything()
	if templateSelector != nil {
		sel, err := metav1.LabelSelectorAsSelector(templateSelector)
		if err != nil {
			return false, fmt.Errorf("invalid template worker selector: %w", err)
		}
		templateSel = sel
	}

	actorSel := labels.SelectorFromSet(labels.Set(actorSelector.GetMatchLabels()))

	set := labels.Set(worker.GetLabels())
	return templateSel.Matches(set) && actorSel.Matches(set), nil
}

type AssignWorkerStep struct {
	store       store.Interface
	workerCache *workercache.Cache
}

func (s *AssignWorkerStep) Name() string { return "AssignWorker" }

func (s *AssignWorkerStep) IsComplete(ctx context.Context, input *ResumeInput, state *ResumeState) (bool, error) {
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_RUNNING, nil
}
func (s *AssignWorkerStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
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
		if worker.Assignment == nil {
			continue
		}
		if worker.Assignment.Actor.Name != input.ActorID {
			continue
		}
		eligible, err := isWorkerEligibleForActor(worker, state.ActorTemplate.Spec.SandboxClass, state.ActorTemplate.Spec.WorkerSelector, state.Actor.GetWorkerSelector())
		if err != nil {
			return fmt.Errorf("while checking worker eligibility: %w", err)
		}
		if eligible {
			assignedWorker = worker
			break
		}
		// Workers() returns pointers directly from the cache so we need to clone before
		// mutating so that the cache is not corrupted if UpdateWorker fails.
		release := proto.Clone(worker).(*ateapipb.Worker)
		release.Assignment = nil
		if err := s.store.UpdateWorker(ctx, release, release.Version); err != nil {
			return fmt.Errorf("while releasing stale worker assignment: %w", err)
		}
	}

	// If not, find a free one using randomized shuffling
	if assignedWorker == nil {
		pickedWorker, err := s.findFreeWorker(workers, state.ActorTemplate.Spec.SandboxClass, state.ActorTemplate.Spec.WorkerSelector, state.Actor.GetWorkerSelector(), state.Actor.GetLatestSnapshotInfo().GetLocal().GetNodeVmsWithLocalSnapshots())
		if err != nil {
			return err
		}
		if pickedWorker == nil {
			return status.Errorf(codes.FailedPrecondition, "no free workers available")
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
		Actor: &ateapipb.ActorRef{
			Name:     input.ActorID,
			Atespace: state.Actor.GetAtespace(),
		},
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

func (s *AssignWorkerStep) findFreeWorker(
	workers []*ateapipb.Worker,
	templateClass atev1alpha1.SandboxClass,
	templateSelector *metav1.LabelSelector,
	actorSelector *ateapipb.Selector,
	nodesRestrictions []string,
) (*ateapipb.Worker, error) {
	var freeWorkers []*ateapipb.Worker
	for _, worker := range workers {
		if worker.Assignment != nil {
			continue
		}
		eligible, err := isWorkerEligibleForActor(worker, templateClass, templateSelector, actorSelector)
		if err != nil {
			return nil, err
		}
		if !eligible {
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
		return freeWorkers[0], nil
	}
	return nil, nil
}

type CallAteletRestoreStep struct {
	store               store.Interface
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

	workloadSpec, err := workloadSpecFromActorTemplateWithEnv(ctx, s.kubeClient, s.secretCache, state.ActorTemplate)
	if err != nil {
		return err
	}

	if data := state.Actor.GetLatestSnapshotInfo().GetData(); data != nil {
		slog.InfoContext(ctx, "Actor has snapshot; Restoring from snapshot")

		req := &ateletpb.RestoreRequest{
			TargetAteomUid:         state.Actor.GetAteomPodUid(),
			Atespace:               state.Actor.GetAtespace(),
			ActorId:                state.Actor.GetActorId(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			Spec:                   workloadSpec,
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
		return maybeCrashActor(ctx, s.store, input.Atespace, input.ActorID, err, "while restoring workload")
	} else if state.ActorTemplate.Status.GoldenSnapshot != "" && !input.Boot {
		slog.InfoContext(ctx, "Actor has no snapshot; ActorTemplate has golden snapshot; Restoring from golden snapshot")

		snapshot := state.ActorTemplate.Status.GoldenSnapshot

		req := &ateletpb.RestoreRequest{
			TargetAteomUid:         state.Actor.GetAteomPodUid(),
			Atespace:               state.Actor.GetAtespace(),
			ActorId:                state.Actor.GetActorId(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			Spec:                   workloadSpec,
			Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
			Config: &ateletpb.RestoreRequest_ExternalConfig{
				ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
					SnapshotUriPrefix: snapshot,
				},
			},
			Scope: toAteletSnapshotScope(state.ActorTemplate.Spec.SnapshotsConfig.OnCommit),
		}
		_, err = client.Restore(ctx, req)
		return maybeCrashActor(ctx, s.store, input.Atespace, input.ActorID, err, "while creating workload from golden snapshot")
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
			Atespace:               state.Actor.GetAtespace(),
			ActorId:                state.Actor.GetActorId(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			SandboxAssets:          sandboxAssets,
			Spec:                   workloadSpec,
		}
		_, err = client.Run(ctx, req)
		return maybeCrashActor(ctx, s.store, input.Atespace, input.ActorID, err, "while creating workload from spec")
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
	latestActor, err := s.store.GetActor(ctx, input.Atespace, input.ActorID)
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
