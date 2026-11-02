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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
)

// Stale assignment cleanup. v1.0.0-rc.1 wrote an Actor and its assignment row
// separately, so a database it ran against can hold a row its Actor does not
// point back at, or an Actor that names a Worker holding no row for it.
// ate-api-server's clean-stale-assignments subcommand runs
// CleanStaleAssignments after an upgrade from rc.1; see docs/guides/upgrade.md.
// Remove this file, the subcommand, its section in docs/guides/upgrade.md,
// crashMessageAssignmentMissing and ReleaseActorFromWorker in the store and
// ServiceImpl once upgrades from rc.1 are unsupported.

// staleAssignmentStore enumerates the exact storage methods needed by
// CleanStaleAssignments and nothing more.
type staleAssignmentStore interface {
	workerWorkflowStore
	ListWorkers(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapi.Worker], error)
	ListAtespaces(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapi.Atespace], error)
	ListActors(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapi.Actor], error)
	GetWorkerAssignment(ctx context.Context, workerName, actorUID string) (*ateapi.ActorAssignment, error)
	ReleaseActorFromWorker(ctx context.Context, workerName string, actorUID string) (*ateapi.Worker, error)
}

// errActorLeft reports an Actor naming a Worker with no row for it whose
// sandbox may still run. Retrying the Actor's pending operation, or
// suspending a RUNNING Actor, clears it.
var errActorLeft = errors.New("actor left for its operation to be retried")

// CleanStaleAssignments releases every assignment whose Actor does not name
// the Worker holding it. It then crashes each Actor naming a Worker with no
// row for it whose resume cannot finish or whose sandbox is gone; see
// cleanActor. It returns how many records it fixed. With dryRun it only logs
// them.
//
// Actors that are busy, or left for an operator because their sandbox may
// still run, are skipped until the rest are done, then reported, so the run
// can be repeated.
func CleanStaleAssignments(ctx context.Context, st staleAssignmentStore, dryRun bool) (int, error) {
	c := &staleAssignmentCleaner{store: st, workflow: NewWorkerWorkflow(st, nil), dryRun: dryRun}
	// Rows go first: releasing a row an Actor does not name can leave that
	// Actor naming a Worker with no row, which the Actor pass then finds.
	if err := c.cleanRows(ctx); err != nil {
		return c.fixed, err
	}
	if err := c.cleanActors(ctx); err != nil {
		return c.fixed, err
	}
	var errs []error
	if c.busy > 0 {
		errs = append(errs, fmt.Errorf("%d records left for a retry: %w", c.busy, errActorBusy))
	}
	if c.left > 0 {
		errs = append(errs, fmt.Errorf("%d actors left: %w", c.left, errActorLeft))
	}
	return c.fixed, errors.Join(errs...)
}

type staleAssignmentCleaner struct {
	store    staleAssignmentStore
	workflow *WorkerWorkflow
	dryRun   bool

	fixed, busy, left int
}

// count tallies the outcome of one record.
func (c *staleAssignmentCleaner) count(fixed bool, err error) error {
	switch {
	case errors.Is(err, errActorBusy):
		c.busy++
	case errors.Is(err, errActorLeft):
		c.left++
	case err != nil:
		return err
	case fixed:
		c.fixed++
	}
	return nil
}

func (c *staleAssignmentCleaner) cleanRows(ctx context.Context) error {
	for workerToken := ""; ; {
		workers, err := c.store.ListWorkers(ctx, store.ListOptions{PageToken: workerToken})
		if err != nil {
			return fmt.Errorf("while listing workers: %w", err)
		}
		for _, worker := range workers.Items {
			name := worker.GetMetadata().GetName()
			// Releasing deletes rows mid-scan, which paging tolerates: the
			// cursor is the last actor UID listed, not an offset.
			for token := ""; ; {
				page, err := c.store.ListWorkerAssignments(ctx, name, store.ListOptions{PageToken: token})
				if err != nil {
					return fmt.Errorf("while listing the assignments of worker %s: %w", name, err)
				}
				for _, assignment := range page.Items {
					if err := c.count(c.cleanRow(ctx, name, assignment)); err != nil {
						return err
					}
				}
				if !page.HasNextPage() {
					break
				}
				token = page.NextPageToken
			}
		}
		if !workers.HasNextPage() {
			return nil
		}
		workerToken = workers.NextPageToken
	}
}

// cleanRow releases one assignment on workerName, under its Actor's lease, if
// the Actor does not name workerName. It reports whether the assignment was
// stale.
//
//   - Actor gone, or a different incarnation: the row is released alone. No
//     bind can name that UID again.
//   - Actor names no worker: the row is released in a write pinned to the
//     Actor as read.
//   - Actor names another worker: the row is released alone, and the Actor
//     pass handles the worker it names.
func (c *staleAssignmentCleaner) cleanRow(ctx context.Context, workerName string, assignment *ateapi.ActorAssignment) (bool, error) {
	actorUID := assignment.GetActorUid()
	actorRef := resources.ActorRefFromObjectRef(assignment.GetActor())
	// Most rows are consistent, so check before taking the lease.
	if _, reason, err := c.readForRow(ctx, actorRef, actorUID, workerName); err != nil || reason == "" {
		return false, err
	}

	ctx, lease, err := c.acquireLease(ctx, actorRef)
	if err != nil {
		return false, err
	}
	defer lease.Close()

	actor, reason, err := c.readForRow(ctx, actorRef, actorUID, workerName)
	if err != nil || reason == "" {
		return false, err
	}
	// Read after the Actor, so a row still here was not released by a write
	// the Actor read already shows.
	if hosted, err := workerHostsActor(ctx, c.store, workerName, actorUID); err != nil || !hosted {
		return false, err
	}
	slog.WarnContext(ctx, "Releasing a stale assignment",
		slog.Any("actor", actorRef),
		slog.String("actor_uid", actorUID),
		slog.String("worker", workerName),
		slog.String("reason", reason),
		slog.Bool("dry_run", c.dryRun))
	if c.dryRun {
		return true, nil
	}

	released := true
	if reason == reasonNamesNoWorker {
		_, err = c.store.UpdateActorAndRelease(ctx, actorRef, store.PreconditionFrom(actor), func(*ateapi.Actor) error {
			return nil
		})
	} else {
		// ReleaseActorFromWorker matches the row on workerName too, so a row
		// rebound elsewhere since the listing is left alone.
		var worker *ateapi.Worker
		worker, err = c.store.ReleaseActorFromWorker(ctx, workerName, actorUID)
		released = worker != nil
	}
	if errors.Is(err, store.ErrVersionConflict) {
		return false, errActorBusy
	}
	if err != nil {
		return false, fmt.Errorf("while releasing actor %s from worker %s: %w", actorUID, workerName, err)
	}
	return released, nil
}

// Why cleanRow releases a row.
const (
	reasonActorGone          = "actor is gone"
	reasonNamesNoWorker      = "actor names no worker"
	reasonNamesAnotherWorker = "actor names another worker"
)

// readForRow reads the Actor behind actorUID's row on workerName, nil if it is
// gone, and why the row is stale, or "" if it is not.
func (c *staleAssignmentCleaner) readForRow(ctx context.Context, actorRef resources.ActorRef, actorUID, workerName string) (*ateapi.Actor, string, error) {
	actor, err := c.store.GetActor(ctx, actorRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, reasonActorGone, nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("while getting actor %s: %w", actorRef, err)
	}
	switch named := actor.GetStatus().GetWorkerAssignment().GetWorker().GetName(); {
	case actor.GetMetadata().GetUid() != actorUID:
		return actor, reasonActorGone, nil
	case named == "":
		return actor, reasonNamesNoWorker, nil
	case named != workerName:
		return actor, reasonNamesAnotherWorker, nil
	}
	return actor, "", nil
}

func (c *staleAssignmentCleaner) cleanActors(ctx context.Context) error {
	for atespaceToken := ""; ; {
		atespaces, err := c.store.ListAtespaces(ctx, store.ListOptions{PageToken: atespaceToken})
		if err != nil {
			return fmt.Errorf("while listing atespaces: %w", err)
		}
		for _, atespace := range atespaces.Items {
			name := atespace.GetMetadata().GetName()
			for token := ""; ; {
				page, err := c.store.ListActors(ctx, name, store.ListOptions{PageToken: token})
				if err != nil {
					return fmt.Errorf("while listing the actors of atespace %s: %w", name, err)
				}
				for _, actor := range page.Items {
					if err := c.count(c.cleanActor(ctx, actor)); err != nil {
						return err
					}
				}
				if !page.HasNextPage() {
					break
				}
				token = page.NextPageToken
			}
		}
		if !atespaces.HasNextPage() {
			return nil
		}
		atespaceToken = atespaces.NextPageToken
	}
}

// cleanActor handles an Actor that names a Worker holding no row for it. It
// reports whether it fixed the Actor.
//
//   - RESUMING: crashed. Its resume aborts on the missing row.
//   - RUNNING, SUSPENDING, REVERTING or DELETING, and the Worker is gone or
//     registered a later epoch than the Actor ran under: crashed, as a worker
//     delete or restart does. Its sandbox went with the Worker or its process.
//     crashBoundActor only clears a DELETING Actor's assignment.
//   - Anything else: left, and logged. A RUNNING Actor's sandbox may still
//     run, and a suspend either checkpoints it or crashes it. Retrying a
//     pending operation clears the others.
func (c *staleAssignmentCleaner) cleanActor(ctx context.Context, listed *ateapi.Actor) (bool, error) {
	// Most Actors are consistent, so check before taking the lease.
	if stale, err := c.namesWorkerWithoutRow(ctx, listed); err != nil || !stale {
		return false, err
	}

	actorRef := resources.ActorRefFromActor(listed)
	ctx, lease, err := c.acquireLease(ctx, actorRef)
	if err != nil {
		return false, err
	}
	defer lease.Close()

	actor, err := c.store.GetActor(ctx, actorRef)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("while getting actor %s: %w", actorRef, err)
	}
	// The row is read after the Actor, so a row missing now was missing for
	// the Actor as read.
	if stale, err := c.namesWorkerWithoutRow(ctx, actor); err != nil || !stale {
		return false, err
	}

	assignment := actor.GetStatus().GetWorkerAssignment()
	workerName := assignment.GetWorker().GetName()
	worker, err := c.store.GetWorker(ctx, workerName)
	if errors.Is(err, store.ErrNotFound) {
		worker = nil
	} else if err != nil {
		return false, fmt.Errorf("while getting worker %s: %w", workerName, err)
	}
	state := actor.GetStatus().GetState()
	logAttrs := []any{
		slog.Any("actor", actorRef),
		slog.String("worker", workerName),
		slog.String("state", state.String()),
		slog.Bool("dry_run", c.dryRun),
	}

	var crashMsg string
	switch state {
	case ateapi.ActorState_ACTOR_STATE_RESUMING:
		// Its worker_epoch is only recorded once it is RUNNING, so it says
		// nothing about a restart here.
		crashMsg = crashMessageAssignmentMissing
		if worker == nil {
			crashMsg = crashMessageWorkerPodGone
		}
	case ateapi.ActorState_ACTOR_STATE_RUNNING, ateapi.ActorState_ACTOR_STATE_SUSPENDING,
		ateapi.ActorState_ACTOR_STATE_REVERTING, ateapi.ActorState_ACTOR_STATE_DELETING:
		switch {
		case worker == nil:
			crashMsg = crashMessageWorkerPodGone
		case assignment.GetWorkerEpoch() < worker.GetStatus().GetRegisteredEpoch():
			crashMsg = crashMessageWorkerRestarted
		}
	}
	if crashMsg == "" {
		slog.WarnContext(ctx, "Leaving an actor whose worker holds no assignment for it; suspend it if RUNNING, or retry its operation", logAttrs...)
		return false, errActorLeft
	}
	if worker == nil {
		// crashBoundActor reads only the name, for its log and errors.
		worker = &ateapi.Worker{Metadata: &ateapi.ResourceMetadata{Name: workerName}}
	}

	if c.dryRun {
		slog.WarnContext(ctx, "Would crash an actor whose worker holds no assignment for it", append(logAttrs, slog.String("reason", crashMsg))...)
		return true, nil
	}
	// crashBoundActor logs this before the write, and the Crashed actor event
	// once it commits.
	if err := c.workflow.crashBoundActor(ctx, worker, actorRef, actor, "Crashing an actor whose worker holds no assignment for it: "+crashMsg, crashMsg); err != nil {
		if apierror.Code(err) == codes.Aborted {
			return false, errActorBusy
		}
		return false, err
	}
	return true, nil
}

// namesWorkerWithoutRow reports whether actor names a Worker that holds no
// assignment for it.
func (c *staleAssignmentCleaner) namesWorkerWithoutRow(ctx context.Context, actor *ateapi.Actor) (bool, error) {
	workerName := actor.GetStatus().GetWorkerAssignment().GetWorker().GetName()
	if workerName == "" {
		return false, nil
	}
	hosted, err := workerHostsActor(ctx, c.store, workerName, actor.GetMetadata().GetUid())
	return !hosted, err
}

// acquireLease takes actorRef's lease, reporting a held one as errActorBusy.
func (c *staleAssignmentCleaner) acquireLease(ctx context.Context, actorRef resources.ActorRef) (context.Context, *store.Lease, error) {
	lease, err := c.store.AcquireLease(ctx, actorLeaseKey(actorRef))
	if errors.Is(err, store.ErrLeaseConflict) {
		return nil, nil, errActorBusy
	}
	if err != nil {
		return nil, nil, fmt.Errorf("while acquiring the lease of actor %s: %w", actorRef, err)
	}
	return lease.Context(), lease, nil
}
