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
	ActorName string
	Atespace  string
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
	actor, err := s.store.GetActor(ctx, input.Atespace, input.ActorName)
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
	state.Actor.InProgressSnapshot = fmt.Sprintf("%s-%s-%s", state.Actor.GetMetadata().GetName(), time.Now().Format(time.RFC3339), rand.Text())
	updatedActor, err := s.store.UpdateActor(ctx, state.Actor, state.Actor.GetMetadata().GetVersion())
	if err != nil {
		return err
	}
	state.Actor = updatedActor
	return nil
}

func (s *MarkPausingStep) RetryBackoff() *wait.Backoff { return nil }

type CallAteletPauseStep struct {
	store  store.Interface
	dialer *AteletDialer
}

func (s *CallAteletPauseStep) Name() string { return "CallAteletPause" }
func (s *CallAteletPauseStep) IsComplete(ctx context.Context, input *PauseInput, state *PauseState) (bool, error) {
	// If we are already PAUSED, we've already called Atelet
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_PAUSED, nil
}
func (s *CallAteletPauseStep) Execute(ctx context.Context, input *PauseInput, state *PauseState) error {
	if state.Actor.GetAteomPodNamespace() == "" || state.Actor.GetAteomPodName() == "" {
		if err := crashActor(ctx, s.store, state.Actor.GetMetadata().GetAtespace(), state.Actor.GetMetadata().GetName()); err != nil {
			slog.Error("Failed to crash actor", slog.String("err", err.Error()))
		}
		return fmt.Errorf("actor is CRASHED because it was in PAUSING state but has no active worker")
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

	workloadSpec := workloadSpecFromActorTemplate(state.ActorTemplate)

	// Checkpoint does not carry the sandbox config: atelet uses the version the
	// actor is currently running (recorded on-node at Run/Restore) and pins it
	// into the snapshot manifest.
	req := &ateletpb.CheckpointRequest{
		TargetAteomUid:         state.Actor.GetAteomPodUid(),
		Atespace:               state.Actor.GetMetadata().GetAtespace(),
		ActorName:              state.Actor.GetMetadata().GetName(),
		ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
		ActorTemplateName:      state.Actor.GetActorTemplateName(),
		Spec:                   workloadSpec,
		Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
		Config: &ateletpb.CheckpointRequest_LocalConfig{
			LocalConfig: &ateletpb.LocalCheckpointConfiguration{
				SnapshotPrefix: state.Actor.InProgressSnapshot,
			},
		},
		Scope:    toAteletSnapshotScope(state.ActorTemplate.Spec.SnapshotsConfig.OnPause),
		ActorUid: state.Actor.GetMetadata().Uid,
	}

	_, err = client.Checkpoint(ctx, req)
	return maybeCrashActor(ctx, s.store, input.Atespace, input.ActorName, err, "while checkpointing workload")
}

func (s *CallAteletPauseStep) RetryBackoff() *wait.Backoff { return nil }

type FinalizePausedStep struct {
	store store.Interface
}

func (s *FinalizePausedStep) Name() string { return "FinalizePaused" }
func (s *FinalizePausedStep) IsComplete(ctx context.Context, input *PauseInput, state *PauseState) (bool, error) {
	// The workflow is done once the worker is freed and the actor reached PAUSED,
	// or CRASHED (node name was lost, so it can never be safely resumed).
	status := state.Actor.GetStatus()
	terminal := status == ateapipb.Actor_STATUS_PAUSED || status == ateapipb.Actor_STATUS_CRASHED
	return terminal && state.Actor.GetAteomPodNamespace() == "", nil
}
func (s *FinalizePausedStep) Execute(ctx context.Context, input *PauseInput, state *PauseState) error {
	latestActor, err := s.store.GetActor(ctx, input.Atespace, input.ActorName)
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

			if wass := worker.Assignment; wass != nil {
				if wass.Actor.Atespace == input.Atespace && wass.Actor.Name == input.ActorName {
					worker.Assignment = nil
					err = s.store.UpdateWorker(ctx, worker, worker.Version)
					if err != nil {
						return err
					}
				}
			}
		}

		// 2. Safely clear ActiveWorker now that the worker object in DB is freed
		latestActor, err = s.store.GetActor(ctx, input.Atespace, input.ActorName)
		if err != nil {
			return err
		}
		latestActor.Status = ateapipb.Actor_STATUS_PAUSED
		if nodeName == "" {
			// Without a node name we cannot record where the local snapshot lives,
			// so the actor can never be resumed (findFreeWorker would search for a
			// worker on an unknown node forever). Crash it instead of leaving it
			// stuck in PAUSED.
			slog.ErrorContext(ctx, "Node name not found during finalize pause, crashing actor", "actor", input.ActorName)
			latestActor.Status = ateapipb.Actor_STATUS_CRASHED
		}
		// TODO(dberkov) - what if InProgressSnapshot is empty? That shouldn't be possible.
		if latestActor.InProgressSnapshot != "" {
			localInfo := &ateapipb.LocalSnapshotInfo{
				SnapshotPrefix: latestActor.InProgressSnapshot,
			}
			if latestActor.Status != ateapipb.Actor_STATUS_CRASHED {
				localInfo.NodeVmsWithLocalSnapshots = []string{nodeName}
			}
			latestActor.LatestSnapshotInfo = &ateapipb.SnapshotInfo{
				Data: &ateapipb.SnapshotInfo_Local{Local: localInfo},
			}
			latestActor.InProgressSnapshot = ""
		}
		latestActor.AteomPodNamespace = ""
		latestActor.AteomPodName = ""
		latestActor.AteomPodIp = ""
		latestActor.WorkerPoolName = ""
		updatedActor, err := s.store.UpdateActor(ctx, latestActor, latestActor.GetMetadata().GetVersion())
		if err != nil {
			return err
		}
		latestActor = updatedActor
	}

	state.Actor = latestActor
	return nil
}

func (s *FinalizePausedStep) RetryBackoff() *wait.Backoff { return nil }
