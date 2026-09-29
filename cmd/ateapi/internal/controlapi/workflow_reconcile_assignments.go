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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// needsAssignmentReconcile reports whether worker's epoch has risen past the
// last one its earlier Actors were released for, or its ips have changed since
// the last ones written to all its Actors.
func needsAssignmentReconcile(worker *ateapipb.Worker) bool {
	return worker.GetEpoch() > worker.GetStatus().GetObservedEpoch() ||
		worker.GetStatus().GetIpsGeneration() > worker.GetStatus().GetObservedIpsGeneration()
}

// ReconcileAssignments brings a Worker's status.observed_epoch up to its epoch
// and status.observed_ips_generation up to its status.ips_generation.
//
// A raised epoch means its ateom restarted, taking with it the sandboxes of the
// Actors placed during an earlier epoch, so those Actors are crashed and their
// assignments released. A raised ips_generation means its ips changed, so the
// Actors still assigned to the Worker, and their assignment rows, have their
// worker_pod_ips rewritten. Only then are observed_epoch and
// observed_ips_generation recorded. A failure leaves them where they were, so
// the next pass redoes the work.
//
// Nothing to do, including a Worker that is gone, is success.
func (w *WorkerWorkflow) ReconcileAssignments(ctx context.Context, name string) (err error) {
	ctx, done := stepSpan(ctx, "ReconcileAssignments")
	defer func() { err = done(err) }()

	worker, err := w.store.GetWorker(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		markSkipped(ctx, "worker not found")
		return nil
	}
	if err != nil {
		return fmt.Errorf("while fetching worker %s: %w", name, err)
	}
	if !needsAssignmentReconcile(worker) {
		markSkipped(ctx, "epoch and ips already observed")
		return nil
	}
	epoch := worker.GetEpoch()
	ipsGeneration := worker.GetStatus().GetIpsGeneration()

	// A Worker whose observed_epoch is 0 was registered before epochs were
	// reported, so its current epoch is recorded without releasing anything:
	// nothing says which of its Actors predate it.
	releaseEarlier := worker.GetStatus().GetObservedEpoch() != 0 && epoch > worker.GetStatus().GetObservedEpoch()
	rewriteIPs := ipsGeneration > worker.GetStatus().GetObservedIpsGeneration()

	if releaseEarlier || rewriteIPs {
		if err := w.reconcileAssignments(ctx, worker, releaseEarlier, rewriteIPs); err != nil {
			return err
		}
	}
	return w.recordObserved(ctx, name, worker.GetMetadata().GetUid(), epoch, ipsGeneration)
}

// recordObservedAttempts bounds how often recordObserved rereads a Worker that
// other writes keep moving on.
const recordObservedAttempts = 5

// recordObserved raises observed_epoch to epoch and observed_ips_generation to
// ipsGeneration on the Worker incarnation uid names. A Worker that is gone, or
// replaced by a new incarnation, is left alone.
//
// A write that lands on the Worker after the sweep does not make the record
// wrong, so a version conflict is retried against a fresh read. Every
// assignment is stamped with the Worker's epoch, ips and ips_generation under
// its row lock, the same lock that raises epoch and ips_generation, so a row
// stamped before they were committed was committed before they were read and
// the sweep found it. Later binds are stamped with epoch or higher, and with
// either the ips of ipsGeneration or newer ones, which the sweep leaves alone
// and whose raised ips_generation leaves for the next pass.
func (w *WorkerWorkflow) recordObserved(ctx context.Context, name, uid string, epoch, ipsGeneration int64) error {
	for attempt := 1; ; attempt++ {
		worker, err := w.store.GetWorker(ctx, name)
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("while re-fetching worker %s: %w", name, err)
		}
		_, err = w.store.UpdateWorker(ctx, name, store.Precondition{UID: uid, Version: worker.GetMetadata().GetVersion()}, func(toUpdate *ateapipb.Worker) error {
			toUpdate.Status.ObservedEpoch = max(toUpdate.GetStatus().GetObservedEpoch(), epoch)
			toUpdate.Status.ObservedIpsGeneration = max(toUpdate.GetStatus().GetObservedIpsGeneration(), ipsGeneration)
			return nil
		})
		switch {
		case err == nil, errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrUIDConflict):
			return nil
		case errors.Is(err, store.ErrVersionConflict) && attempt < recordObservedAttempts:
			continue
		default:
			return fmt.Errorf("while recording observed epoch %d and ips generation %d on worker %s: %w", epoch, ipsGeneration, name, err)
		}
	}
}

// errActorBusy reports an assignment skipped because an operation holds its
// Actor's lease.
var errActorBusy = errors.New("actor has an operation in progress")

// reconcileAssignments passes over every assignment on worker. If
// releaseEarlier, those made during an epoch before worker's epoch are
// released. If rewriteIPs, the Actors of the rest are pointed at worker's ips.
// An assignment whose Actor is busy is skipped until the rest are done, then
// reported, so the pass is retried.
func (w *WorkerWorkflow) reconcileAssignments(ctx context.Context, worker *ateapipb.Worker, releaseEarlier, rewriteIPs bool) error {
	name := worker.GetMetadata().GetName()
	busy := 0
	// Releasing deletes rows mid-scan, which paging tolerates: the cursor is
	// the last actor UID listed, not an offset.
	for token := ""; ; {
		page, err := w.store.ListWorkerAssignments(ctx, name, store.ListOptions{PageToken: token})
		if err != nil {
			return fmt.Errorf("while listing the assignments of worker %s: %w", name, err)
		}
		for _, assignment := range page.Items {
			earlier := releaseEarlier && assignment.GetWorkerEpoch() < worker.GetEpoch()
			// The row's worker_ips_generation is not a filter: the row and its
			// Actor are written separately, so a failure between them can leave
			// them apart. Only the Actor's own copy, read under its lease, says
			// whether it needs the rewrite.
			if !earlier && !rewriteIPs {
				continue
			}
			err := w.reconcileAssignment(ctx, worker, assignment, earlier, rewriteIPs)
			if errors.Is(err, errActorBusy) {
				busy++
				continue
			}
			if err != nil {
				return err
			}
		}
		if !page.HasNextPage() {
			break
		}
		token = page.NextPageToken
	}
	if busy > 0 {
		return fmt.Errorf("%d assignments on worker %s left for a retry: %w", busy, name, errActorBusy)
	}
	return nil
}

// reconcileAssignment brings one assignment on the Worker in line with it,
// under its Actor's lease so no operation on the Actor is midway.
//
// If earlier, the assignment was made during an earlier epoch of the Worker and
// is cleared. The Actor, read under the lease, may have been bound here again
// in the current epoch since the list; its assignment then carries that epoch,
// as the rebound row does, and is not cleared. An Actor still running on a
// cleared assignment is crashed first and its row released after, so a failure
// in between leaves the row for a retry to find, and the Actor already CRASHED.
// Otherwise, if rewriteIPs, the worker_pod_ips of the Actor and of its row are
// rewritten to the Worker's ips.
//
// Either way, a row whose Actor does not point here is left over from an
// operation that failed partway, and is released.
func (w *WorkerWorkflow) reconcileAssignment(ctx context.Context, worker *ateapipb.Worker, assignment *ateapipb.ActorAssignment, earlier, rewriteIPs bool) error {
	name := worker.GetMetadata().GetName()
	release := func() error {
		if _, err := w.store.ReleaseActorFromWorker(ctx, name, assignment.GetActorUid()); err != nil {
			return fmt.Errorf("while releasing actor %s from worker %s: %w", assignment.GetActorUid(), name, err)
		}
		return nil
	}

	if assignment.GetActor() == nil {
		return release()
	}
	actorRef := resources.ActorRefFromObjectRef(assignment.GetActor())
	lease, err := w.store.AcquireLease(ctx, actorLeaseKey(actorRef))
	if errors.Is(err, store.ErrLeaseConflict) {
		return errActorBusy
	}
	if err != nil {
		return fmt.Errorf("while acquiring the lease of actor %s: %w", actorRef, err)
	}
	defer lease.Close()
	ctx = lease.Context()

	actor, err := w.store.GetActor(ctx, actorRef)
	if errors.Is(err, store.ErrNotFound) {
		return release()
	}
	if err != nil {
		return fmt.Errorf("while getting actor assigned to worker %s: %w", name, err)
	}
	if actor.GetMetadata().GetUid() != assignment.GetActorUid() {
		return release()
	}

	if actor.GetStatus().GetWorkerAssignment().GetWorker().GetName() != name {
		return release()
	}
	if actor.GetStatus().GetWorkerAssignment().GetWorkerEpoch() >= worker.GetEpoch() {
		earlier = false
	}
	if !earlier {
		if !rewriteIPs {
			return nil
		}
		return w.rewriteWorkerPodIPs(ctx, worker, actorRef, actor)
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		if err := w.crashBoundActor(ctx, worker, actorRef, actor, "Releasing actor from a worker whose ateom restarted", crashMessageAteomRestarted); err != nil {
			return err
		}
	}
	return release()
}

// rewriteWorkerPodIPs points actor, assigned to worker, at worker's ips, then
// its assignment row. The Actor goes first because it is what requests are
// routed by.
//
// Each write is skipped where it already holds worker's ips_generation or a
// later one, so a retry after a failure between them redoes only the row. An
// Actor bound since worker was read holds that bind's ips, stamped under the
// Worker's row lock with their generation; the lease held here keeps it from
// being bound again until the write is done, so the write never replaces
// newer ips with older ones.
func (w *WorkerWorkflow) rewriteWorkerPodIPs(ctx context.Context, worker *ateapipb.Worker, actorRef resources.ActorRef, actor *ateapipb.Actor) error {
	name := worker.GetMetadata().GetName()
	ips, ipsGeneration := worker.GetIps(), worker.GetStatus().GetIpsGeneration()
	if actor.GetStatus().GetWorkerAssignment().GetWorkerIpsGeneration() < ipsGeneration {
		_, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
			toUpdate.Status.WorkerAssignment.WorkerPodIps = ips
			toUpdate.Status.WorkerAssignment.WorkerIpsGeneration = ipsGeneration
			return nil
		})
		switch {
		case err == nil:
			slog.LogAttrs(ctx, slog.LevelInfo, "Rewrote the worker pod IPs of an actor",
				append(ateattr.ActorLogAttrs(resources.ActorAttributionFromActor(actor)),
					slog.String("worker", name),
					slog.Any("ips", ips))...)
		case errors.Is(err, store.ErrNotFound):
			// The actor was deleted out from under us; nothing points here anymore.
			return nil
		default:
			return fmt.Errorf("while rewriting the worker pod IPs of actor %s on worker %s: %w", actorRef, name, err)
		}
	}
	err := w.store.SetAssignmentWorkerPodIPs(ctx, name, actor.GetMetadata().GetUid(), ips, ipsGeneration)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("while rewriting the worker pod IPs of the assignment of actor %s on worker %s: %w", actorRef, name, err)
	}
	return nil
}
