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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecycleWorker reclaims a Worker whose ateom container was replaced under it.
// The record stays because re-registering would lose the reported capacity: an
// ateom reports once per process and CreateWorker leaves capacity unset, which
// is a Worker nothing can ever be placed on.
//
// Re-drivable like DeleteWorker: a failure leaves the Worker draining with the
// unreleased Actors still bound, and a retry fast-forwards.
func (w *WorkerWorkflow) RecycleWorker(ctx context.Context, name, ateomContainerID, terminatedReason string) (*ateapipb.Worker, error) {
	worker, err := w.loadWorkerForRecycle(ctx, name)
	if err != nil {
		return nil, err
	}

	switch stored := worker.GetStatus().GetAteomContainerId(); {
	case stored == ateomContainerID:
		// A retry, or two reporters racing.
		markSkipped(ctx, "worker already recycled for this ateom container")
		return worker, nil
	case stored == "":
		// Registered before the control plane recorded the container, so nothing
		// says anything was lost. Adopting rather than recycling is what stops
		// the first reconcile after an upgrade from crashing the whole fleet.
		markSkipped(ctx, "worker carries no ateom container id yet, adopting")
		return w.recordAteomContainer(ctx, worker, ateomContainerID, false)
	}

	// A Worker already draining is one whose pod is going away; restoring it to
	// ACTIVE afterwards would undo that, so remember who drained it.
	wasDraining := worker.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_DRAINING

	// Scheduling only places on ACTIVE Workers, so draining first stops a
	// concurrent resume binding an Actor the sweep would then crash.
	worker, err = w.ensureDraining(ctx, worker)
	if err != nil {
		return nil, err
	}

	cause := crashCause{
		reason:                    ateattr.ReasonWorkerSandboxGone,
		containerTerminatedReason: ateattr.NormalizeContainerTerminationReason(terminatedReason),
	}
	if err := w.ensureWorkerEmptied(ctx, worker, cause); err != nil {
		return nil, err
	}

	return w.finalizeRecycled(ctx, name, ateomContainerID, !wasDraining)
}

// loadWorkerForRecycle fetches the current worker record. An absent Worker is
// NOT_FOUND: the syncer registers only once a pod is Ready, so a restart seen
// before that has nothing to recycle.
func (w *WorkerWorkflow) loadWorkerForRecycle(ctx context.Context, name string) (_ *ateapipb.Worker, err error) {
	ctx, done := stepSpan(ctx, "LoadWorkerForRecycle")
	defer func() { err = done(err) }()

	worker, err := w.store.GetWorker(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
		}
		return nil, fmt.Errorf("while fetching worker: %w", err)
	}
	return worker, nil
}

// ensureWorkerEmptied releases every Actor the replaced sandbox held.
//
// No record removal to cascade the assignment rows away, so each is released
// explicitly — including for the Actors releaseBoundActor leaves alone, whose
// rows would otherwise keep the Worker occupied for a sandbox that is gone.
//
// Deleting rows while paging over them invalidates a forward page token, so
// this drains the first page repeatedly. A page yielding no release at all
// errors rather than spins.
func (w *WorkerWorkflow) ensureWorkerEmptied(ctx context.Context, worker *ateapipb.Worker, cause crashCause) (err error) {
	ctx, done := stepSpan(ctx, "EmptyWorker")
	defer func() { err = done(err) }()

	name := worker.GetMetadata().GetName()
	var released int
	for {
		page, err := w.store.ListWorkerAssignments(ctx, name, store.ListOptions{})
		if err != nil {
			return fmt.Errorf("while listing the assignments of worker %s: %w", name, err)
		}
		if len(page.Items) == 0 {
			break
		}
		var progressed bool
		for _, assignment := range page.Items {
			if err := w.releaseBoundActor(ctx, worker, assignment, cause); err != nil {
				return err
			}
			dropped, err := w.store.ReleaseActorFromWorker(ctx, name, assignment.GetActorUid())
			if err != nil {
				return fmt.Errorf("while releasing actor %s from worker %s: %w", assignment.GetActorUid(), name, err)
			}
			progressed = progressed || dropped != nil
			released++
		}
		if !progressed {
			return fmt.Errorf("worker %s still lists %d assignments that release does not remove", name, len(page.Items))
		}
	}
	if released == 0 {
		markSkipped(ctx, "worker has no actors assigned")
	}
	return nil
}

// finalizeRecycled records the container the Worker now runs and, if this
// workflow is what drained it, hands it back to the scheduler. The re-read is
// because the drain and every release moved the version.
func (w *WorkerWorkflow) finalizeRecycled(ctx context.Context, name, ateomContainerID string, restoreActive bool) (_ *ateapipb.Worker, err error) {
	ctx, done := stepSpan(ctx, "FinalizeRecycled")
	defer func() { err = done(err) }()

	worker, err := w.store.GetWorker(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
		}
		return nil, fmt.Errorf("while re-reading worker to finalize recycle: %w", err)
	}
	return w.recordAteomContainer(ctx, worker, ateomContainerID, restoreActive)
}

// recordAteomContainer writes the container id onto the Worker, optionally
// returning it to ACTIVE. The id lives in status, so this is the only thing
// that moves it: a client's UpdateWorker cannot.
func (w *WorkerWorkflow) recordAteomContainer(ctx context.Context, worker *ateapipb.Worker, ateomContainerID string, restoreActive bool) (*ateapipb.Worker, error) {
	name := worker.GetMetadata().GetName()
	updated, err := w.store.UpdateWorker(ctx, name, store.PreconditionFrom(worker),
		func(toUpdate *ateapipb.Worker) error {
			if toUpdate.Status == nil {
				toUpdate.Status = &ateapipb.WorkerStatus{}
			}
			toUpdate.Status.AteomContainerId = ateomContainerID
			if restoreActive {
				toUpdate.Status.State = ateapipb.WorkerState_WORKER_STATE_ACTIVE
			}
			return nil
		})
	switch {
	case err == nil:
		return updated, nil
	case errors.Is(err, store.ErrNotFound):
		return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
	case errors.Is(err, store.ErrUIDConflict), errors.Is(err, store.ErrVersionConflict):
		return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
	default:
		return nil, fmt.Errorf("while recording the ateom container of worker %s: %w", name, err)
	}
}
