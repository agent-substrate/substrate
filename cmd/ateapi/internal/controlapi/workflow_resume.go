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
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
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

type AssignWorkerStep struct {
	store store.Interface
}

func (s *AssignWorkerStep) Name() string { return "AssignWorker" }

func (s *AssignWorkerStep) IsComplete(ctx context.Context, input *ResumeInput, state *ResumeState) (bool, error) {
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_RUNNING, nil
}
func (s *AssignWorkerStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	workers, err := s.store.ListWorkers(ctx)
	if err != nil {
		return fmt.Errorf("while listing workers: %w", err)
	}

	var assignedWorker *ateapipb.Worker

	// Check if we already have a worker assigned from a previous failed attempt
	for _, worker := range workers {
		if worker.GetActorId() == input.ActorID && worker.GetWorkerPool() == state.ActorTemplate.Spec.WorkerPoolRef.Name && worker.GetWorkerNamespace() == state.ActorTemplate.Spec.WorkerPoolRef.Namespace {
			assignedWorker = worker
			break
		}
	}

	// If not, find a free one using randomized shuffling
	if assignedWorker == nil {
		pickedWorker := s.findFreeWorker(workers, state.ActorTemplate.Spec.WorkerPoolRef.Namespace, state.ActorTemplate.Spec.WorkerPoolRef.Name)
		if pickedWorker == nil {
			return status.Errorf(codes.FailedPrecondition, "no free workers available")
		}

		assignedWorker = pickedWorker
		slog.InfoContext(ctx, "Picked worker", slog.Any("worker", pickedWorker.String()))
	}

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

func (s *AssignWorkerStep) findFreeWorker(workers []*ateapipb.Worker, workerPoolNamespace, workerPoolName string) *ateapipb.Worker {
	var freeWorkers []*ateapipb.Worker
	for _, worker := range workers {
		if worker.GetActorId() == "" && worker.GetWorkerPool() == workerPoolName && worker.GetWorkerNamespace() == workerPoolNamespace {
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
	dialer *AteletDialer
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

	workloadSpec := &ateletpb.WorkloadSpec{
		PauseImage: state.ActorTemplate.Spec.PauseImage,
	}
	for _, ctr := range state.ActorTemplate.Spec.Containers {
		ateletCtr := &ateletpb.Container{
			Name:    ctr.Name,
			Image:   ctr.Image,
			Command: ctr.Command,
		}
		for _, env := range ctr.Env {
			ateletEnv := &ateletpb.EnvEntry{
				Name:  env.Name,
				Value: env.Value,
			}
			ateletCtr.Env = append(ateletCtr.Env, ateletEnv)
		}
		workloadSpec.Containers = append(workloadSpec.Containers, ateletCtr)
	}

	runscCfg := &ateletpb.RunscConfig{}
	if state.ActorTemplate.Spec.Runsc.AMD64 != nil {
		runscCfg.Amd64 = &ateletpb.RunscPlatformConfig{
			Sha256Hash: state.ActorTemplate.Spec.Runsc.AMD64.SHA256Hash,
			Url:        state.ActorTemplate.Spec.Runsc.AMD64.URL,
		}
	}
	if state.ActorTemplate.Spec.Runsc.ARM64 != nil {
		runscCfg.Arm64 = &ateletpb.RunscPlatformConfig{
			Sha256Hash: state.ActorTemplate.Spec.Runsc.ARM64.SHA256Hash,
			Url:        state.ActorTemplate.Spec.Runsc.ARM64.URL,
		}
	}
	if state.ActorTemplate.Spec.Runsc.Authentication.GCP != nil {
		authnCfg := &ateletpb.AuthenticationConfig{}
		authnCfg.Gcp = &ateletpb.GCPAuthenticationConfig{Use: true}
		runscCfg.Authentication = authnCfg
	}

	if state.Actor.LastSnapshot != "" {
		slog.InfoContext(ctx, "Actor has snapshot; Restoring from snapshot")

		req := &ateletpb.RestoreRequest{
			TargetAteomNamespace:   state.Actor.GetAteomPodNamespace(),
			TargetAteomName:        state.Actor.GetAteomPodName(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			ActorId:                state.Actor.GetActorId(),
			Runsc:                  runscCfg,
			Spec:                   workloadSpec,
			SnapshotUriPrefix:      state.Actor.GetLastSnapshot(),
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
			TargetAteomNamespace:   state.Actor.GetAteomPodNamespace(),
			TargetAteomName:        state.Actor.GetAteomPodName(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			ActorId:                state.Actor.GetActorId(),
			Runsc:                  runscCfg,
			Spec:                   workloadSpec,
			SnapshotUriPrefix:      snapshot,
		}
		_, err = client.Restore(ctx, req)
		if err != nil {
			return fmt.Errorf("while creating workload from golden snapshot: %w", err)
		}
		return nil
	} else {
		slog.InfoContext(ctx, "Actor has no snapshot; ActorTemplate has no golden snapshot; Booting from ActorTemplate spec")
		req := &ateletpb.RunRequest{
			TargetAteomNamespace:   state.Actor.GetAteomPodNamespace(),
			TargetAteomName:        state.Actor.GetAteomPodName(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			ActorId:                state.Actor.GetActorId(),
			Runsc:                  runscCfg,
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
