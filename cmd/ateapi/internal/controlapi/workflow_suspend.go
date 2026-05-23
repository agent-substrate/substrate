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
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/wait"
)

// SuspendInput holds the immutable parameters requested by the client.
type SuspendInput struct {
	ActorID string
}

// SuspendState holds the mutable state loaded and modified during execution.
type SuspendState struct {
	Actor         *ateapipb.Actor
	ActorTemplate *atev1alpha1.ActorTemplate
}

type LoadActorForSuspendStep struct {
	store               store.Interface
	actorTemplateLister listersv1alpha1.ActorTemplateLister
}

func (s *LoadActorForSuspendStep) Name() string { return "LoadActorForSuspend" }
func (s *LoadActorForSuspendStep) IsComplete(ctx context.Context, input *SuspendInput, state *SuspendState) (bool, error) {
	// Always run to get the freshest state
	return false, nil
}
func (s *LoadActorForSuspendStep) Execute(ctx context.Context, input *SuspendInput, state *SuspendState) error {
	actor, err := s.store.GetActor(ctx, input.ActorID)
	if err != nil {
		return err
	}
	state.Actor = actor

	actorTemplate, err := s.actorTemplateLister.ActorTemplates(actor.GetActorTemplateNamespace()).Get(actor.GetActorTemplateName())
	if err != nil {
		return fmt.Errorf("while getting ActorTemplate: %w", err)
	}
	state.ActorTemplate = actorTemplate

	return nil
}

func (s *LoadActorForSuspendStep) RetryBackoff() *wait.Backoff { return nil }

type MarkSuspendingStep struct {
	store store.Interface
}

func (s *MarkSuspendingStep) Name() string { return "MarkSuspending" }
func (s *MarkSuspendingStep) IsComplete(ctx context.Context, input *SuspendInput, state *SuspendState) (bool, error) {
	// Fast forward if we've already marked our intent or if we are further along.
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_SUSPENDING || state.Actor.GetStatus() == ateapipb.Actor_STATUS_SUSPENDED, nil
}
func (s *MarkSuspendingStep) Execute(ctx context.Context, input *SuspendInput, state *SuspendState) error {
	if state.Actor.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		return nil
	}

	state.Actor.Status = ateapipb.Actor_STATUS_SUSPENDING
	snapshotID := time.Now().Format(time.RFC3339) + "-" + rand.Text()
	state.Actor.InProgressSnapshot = strings.TrimSuffix(state.ActorTemplate.Spec.SnapshotsConfig.Location, "/") + "/" + input.ActorID + "/" + snapshotID
	return s.store.UpdateActor(ctx, state.Actor, state.Actor.GetVersion())
}

func (s *MarkSuspendingStep) RetryBackoff() *wait.Backoff { return nil }

type CallAteletSuspendStep struct {
	dialer *AteletDialer
}

func (s *CallAteletSuspendStep) Name() string { return "CallAteletSuspend" }
func (s *CallAteletSuspendStep) IsComplete(ctx context.Context, input *SuspendInput, state *SuspendState) (bool, error) {
	// If we are already SUSPENDED, we've already called Atelet
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_SUSPENDED, nil
}
func (s *CallAteletSuspendStep) Execute(ctx context.Context, input *SuspendInput, state *SuspendState) error {
	if state.Actor.GetAteomPodNamespace() == "" {
		return fmt.Errorf("actor is in SUSPENDING state but has no active worker")
	}

	ateletConn, err := s.dialer.DialForWorker(state.Actor.GetAteomPodNamespace(), state.Actor.GetAteomPodName())
	if err != nil {
		if errors.Is(err, ErrWorkerPodNotFound) {
			slog.Warn("Skipping suspend for dangling worker pod", "namespace", state.Actor.GetAteomPodNamespace(), "pod", state.Actor.GetAteomPodName())
			return nil
		}
		return fmt.Errorf("while getting atelet conn for worker pod: %w", err)
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

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

	req := &ateletpb.CheckpointRequest{
		TargetAteomNamespace:   state.Actor.GetAteomPodNamespace(),
		TargetAteomName:        state.Actor.GetAteomPodName(),
		ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
		ActorTemplateName:      state.Actor.GetActorTemplateName(),
		ActorId:                state.Actor.GetActorId(),
		Runsc:                  runscCfg,
		Spec: &ateletpb.WorkloadSpec{
			PauseImage: state.ActorTemplate.Spec.PauseImage,
		},
		SnapshotUriPrefix: state.Actor.GetInProgressSnapshot(),
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
		req.Spec.Containers = append(req.Spec.Containers, ateletCtr)
	}
	_, err = client.Checkpoint(ctx, req)
	if err != nil {
		return fmt.Errorf("while checkpointing workload: %w", err)
	}

	return nil
}

func (s *CallAteletSuspendStep) RetryBackoff() *wait.Backoff { return nil }

type FinalizeSuspendedStep struct {
	store store.Interface
}

func (s *FinalizeSuspendedStep) Name() string { return "FinalizeSuspended" }
func (s *FinalizeSuspendedStep) IsComplete(ctx context.Context, input *SuspendInput, state *SuspendState) (bool, error) {
	// The workflow is completely done ONLY if the status is SUSPENDED *and* we've successfully freed the worker.
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_SUSPENDED && state.Actor.GetAteomPodNamespace() == "", nil
}
func (s *FinalizeSuspendedStep) Execute(ctx context.Context, input *SuspendInput, state *SuspendState) error {
	latestActor, err := s.store.GetActor(ctx, input.ActorID)
	if err != nil {
		return err
	}

	// 1. Free the worker (if it hasn't been freed yet)
	if latestActor.GetAteomPodNamespace() != "" {
		workerNs := latestActor.GetAteomPodNamespace()
		workerPod := latestActor.GetAteomPodName()

		workerPool := state.ActorTemplate.Spec.WorkerPoolRef.Name

		worker, err := s.store.GetWorker(ctx, workerNs, workerPool, workerPod)
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				return fmt.Errorf("while getting worker for release: %w", err)
			}
			slog.Warn("Worker already gone during finalize suspend, skipping release", "worker", workerPod)
		} else {
			// Only free it if it still belongs to us
			if worker.GetActorId() == input.ActorID {
				worker.ActorNamespace = ""
				worker.ActorTemplate = ""
				worker.ActorId = ""

				err = s.store.UpdateWorker(ctx, worker, worker.Version)
				if err != nil {
					return err
				}
			}
		}

		// 2. Safely clear ActiveWorker now that the worker object in DB is freed
		latestActor, err = s.store.GetActor(ctx, input.ActorID)
		if err != nil {
			return err
		}
		latestActor.Status = ateapipb.Actor_STATUS_SUSPENDED
		if latestActor.InProgressSnapshot != "" {
			latestActor.LastSnapshot = latestActor.InProgressSnapshot
			latestActor.InProgressSnapshot = ""
		}
		latestActor.AteomPodNamespace = ""
		latestActor.AteomPodName = ""
		latestActor.AteomPodIp = ""
		err = s.store.UpdateActor(ctx, latestActor, latestActor.GetVersion())
		if err != nil {
			return err
		}
	}

	state.Actor = latestActor
	return nil
}

func (s *FinalizeSuspendedStep) RetryBackoff() *wait.Backoff { return nil }
