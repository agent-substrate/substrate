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
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/wait"
)

// PauseInput holds the immutable parameters requested by the client.
type PauseInput struct {
	ActorID string
}

// PauseState holds the mutable state loaded and modified during execution.
type PauseState struct {
	Actor         *ateapipb.Actor
	ActorTemplate *atev1alpha1.ActorTemplate
}

type LoadActorForPauseStep struct {
	store               store.Interface
	actorTemplateLister listersv1alpha1.ActorTemplateLister
}

func (s *LoadActorForPauseStep) Name() string { return "LoadActorForPause" }
func (s *LoadActorForPauseStep) IsComplete(ctx context.Context, input *PauseInput, state *PauseState) (bool, error) {
	// Always run to get the freshest state
	return false, nil
}
func (s *LoadActorForPauseStep) Execute(ctx context.Context, input *PauseInput, state *PauseState) error {
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

func (s *LoadActorForPauseStep) RetryBackoff() *wait.Backoff { return nil }

type MarkPausingStep struct {
	store store.Interface
}

func (s *MarkPausingStep) Name() string { return "MarkPausing" }
func (s *MarkPausingStep) IsComplete(ctx context.Context, input *PauseInput, state *PauseState) (bool, error) {
	// Fast forward if we've already marked our intent or if we are further along.
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_PAUSING || state.Actor.GetStatus() == ateapipb.Actor_STATUS_PAUSED, nil
}
func (s *MarkPausingStep) Execute(ctx context.Context, input *PauseInput, state *PauseState) error {
	if state.Actor.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		return nil
	}

	state.Actor.Status = ateapipb.Actor_STATUS_PAUSING
	state.Actor.InProgressSnapshot = fmt.Sprintf("%s-%s-%s", state.Actor.GetActorId(), time.Now().Format(time.RFC3339), rand.Text())
	return s.store.UpdateActor(ctx, state.Actor, state.Actor.GetVersion())
}

func (s *MarkPausingStep) RetryBackoff() *wait.Backoff { return nil }

type CallAteletPauseStep struct {
	dialer *AteletDialer
}

func (s *CallAteletPauseStep) Name() string { return "CallAteletPause" }
func (s *CallAteletPauseStep) IsComplete(ctx context.Context, input *PauseInput, state *PauseState) (bool, error) {
	// If we are already PAUSED, we've already called Atelet
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_PAUSED, nil
}
func (s *CallAteletPauseStep) Execute(ctx context.Context, input *PauseInput, state *PauseState) error {
	if state.Actor.GetAteomPodNamespace() == "" {
		return fmt.Errorf("actor is in PAUSING state but has no active worker")
	}

	ateletConn, err := s.dialer.DialForWorker(state.Actor.GetAteomPodNamespace(), state.Actor.GetAteomPodName())
	if err != nil {
		if errors.Is(err, ErrWorkerPodNotFound) {
			slog.Warn("Skipping pause for dangling worker pod", "namespace", state.Actor.GetAteomPodNamespace(), "pod", state.Actor.GetAteomPodName())
			return nil
		}
		return fmt.Errorf("while getting atelet conn for worker pod: %w", err)
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

	// Checkpoint does not carry the sandbox config: atelet uses the version the
	// actor is currently running (recorded on-node at Run/Restore) and pins it
	// into the snapshot manifest.
	req := &ateletpb.CheckpointRequest{
		TargetAteomUid:         state.Actor.GetAteomPodUid(),
		ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
		ActorTemplateName:      state.Actor.GetActorTemplateName(),
		ActorId:                state.Actor.GetActorId(),
		Spec: &ateletpb.WorkloadSpec{
			PauseImage: state.ActorTemplate.Spec.PauseImage,
		},
		Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
		Config: &ateletpb.CheckpointRequest_LocalConfig{
			LocalConfig: &ateletpb.LocalCheckpointConfiguration{
				SnapshotPrefix: state.Actor.InProgressSnapshot,
			},
		},
	}
	for _, ctr := range state.ActorTemplate.Spec.Containers {
		ateletCtr := &ateletpb.Container{
			Name:    ctr.Name,
			Image:   ctr.Image,
			Command: ctr.Command,
		}
		for _, env := range ctr.Env {
			var val string
			if env.Value != nil {
				val = *env.Value
			}
			ateletEnv := &ateletpb.EnvEntry{
				Name:  env.Name,
				Value: val,
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

func (s *CallAteletPauseStep) RetryBackoff() *wait.Backoff { return nil }

type FinalizePausedStep struct {
	store store.Interface
}

func (s *FinalizePausedStep) Name() string { return "FinalizePaused" }
func (s *FinalizePausedStep) IsComplete(ctx context.Context, input *PauseInput, state *PauseState) (bool, error) {
	// The workflow is completely done ONLY if the status is PAUSED *and* we've successfully freed the worker.
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_PAUSED && state.Actor.GetAteomPodNamespace() == "", nil
}
func (s *FinalizePausedStep) Execute(ctx context.Context, input *PauseInput, state *PauseState) error {
	latestActor, err := s.store.GetActor(ctx, input.ActorID)
	if err != nil {
		return err
	}

	// 1. Free the worker (if it hasn't been freed yet)
	if latestActor.GetAteomPodNamespace() != "" {
		workerNs := latestActor.GetAteomPodNamespace()
		workerPod := latestActor.GetAteomPodName()

		workerPool := latestActor.GetWorkerPoolName()

		worker, err := s.store.GetWorker(ctx, workerNs, workerPool, workerPod)
		nodeName := ""
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				return fmt.Errorf("while getting worker for release: %w", err)
			}
			slog.Warn("Worker already gone during finalize pause, skipping release", "worker", workerPod)
		} else {
			// TODO(dberkov) - what if worker does not belong to this actor?
			nodeName = worker.GetNodeName()
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
		latestActor.Status = ateapipb.Actor_STATUS_PAUSED
		// TODO(dberkov) - what if we still don't know the node name? Maybe move to CRASHED status?
		if nodeName == "" {
			slog.Warn("Node name not found during finalize pause", "actor", input.ActorID)
		}
		// TODO(dberkov) - what if InProgressSnapshot is empty? That shouldn't be possible.
		if latestActor.InProgressSnapshot != "" {
			latestActor.LatestSnapshotInfo = &ateapipb.SnapshotInfo{
				Type: ateapipb.SnapshotType_SNAPSHOT_TYPE_LOCAL,
				Data: &ateapipb.SnapshotInfo_Local{
					Local: &ateapipb.LocalSnapshotInfo{
						SnapshotPrefix:            latestActor.InProgressSnapshot,
						NodeVmsWithLocalSnapshots: []string{nodeName},
					},
				},
			}
			latestActor.InProgressSnapshot = ""
		}
		latestActor.AteomPodNamespace = ""
		latestActor.AteomPodName = ""
		latestActor.AteomPodIp = ""
		latestActor.WorkerPoolName = ""
		err = s.store.UpdateActor(ctx, latestActor, latestActor.GetVersion())
		if err != nil {
			return err
		}
	}

	state.Actor = latestActor
	return nil
}

func (s *FinalizePausedStep) RetryBackoff() *wait.Backoff { return nil }
