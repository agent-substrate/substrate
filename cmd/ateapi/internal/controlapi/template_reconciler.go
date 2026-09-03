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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const (
	templateResyncInterval = 20 * time.Second
	templateListPageSize   = 100

	// templateWorkerCount is the number of goroutines draining the work
	// queue.
	templateWorkerCount = 5

	// goldenSnapshotWarmup is the default wall-clock delay between resuming
	// the golden actor and taking its snapshot, for templates without a
	// readiness probe on every container.
	goldenSnapshotWarmup = 20 * time.Second
)

const (
	reasonGoldenActorInvalid = "GoldenActorInvalid"
	reasonGoldenActorCrashed = "GoldenActorCrashed"
	reasonUnexpectedState    = "GoldenActorUnexpectedState"
)

// templateReconcilerStore enumerates the exact storage methods needed by
// ActorTemplateReconciler and nothing more.
type templateReconcilerStore interface {
	GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error)
	ListActorTemplates(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorTemplate], error)
	UpdateActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.Precondition, mutate func(dbTemplate *ateapipb.ActorTemplate) error) (*ateapipb.ActorTemplate, error)
	AcquireLease(ctx context.Context, key string) (*store.Lease, error)
}

// goldenActorControl is the in-process slice of the Control service the
// reconciler drives golden actors through. *RPCService satisfies it.
type goldenActorControl interface {
	CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error)
	CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error)
	GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error)
	ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error)
	SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error)
}

// ActorTemplateReconciler drives stored ActorTemplates through the golden
// actor state machine.
type ActorTemplateReconciler struct {
	persistence templateReconcilerStore
	control     goldenActorControl
	queue       workqueue.TypedRateLimitingInterface[resources.ActorTemplateRef]
}

func NewActorTemplateReconciler(persistence templateReconcilerStore, control goldenActorControl) *ActorTemplateReconciler {
	return &ActorTemplateReconciler{
		persistence: persistence,
		control:     control,
		// Create rate-limiting queue with exponential backoff
		queue: workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[resources.ActorTemplateRef]()),
	}
}

// Start launches the queue workers and the resync producer; there is no event
// source for stored templates, so the periodic list is the event source.
func (r *ActorTemplateReconciler) Start(ctx context.Context) {
	go func() {
		defer r.queue.ShutDown()
		for range templateWorkerCount {
			go wait.UntilWithContext(ctx, r.runWorker, time.Second)
		}
		wait.UntilWithContext(ctx, r.resync, templateResyncInterval)
	}()
}

// resync lists ActorTemplate and adds them to the work queue.
func (r *ActorTemplateReconciler) resync(ctx context.Context) {
	pageToken := ""
	for {
		// TODO: need sharding
		page, err := r.persistence.ListActorTemplates(ctx, "", store.ListOptions{PageSize: templateListPageSize, PageToken: pageToken})
		if err != nil {
			slog.ErrorContext(ctx, "Failed to list actor templates", slog.Any("err", err))
			return
		}
		for _, tmpl := range page.Items {
			ref := resources.ActorTemplateRefFromActorTemplate(tmpl)
			if goldenSnapshotDone(tmpl.GetStatus().GetGoldenSnapshotStatus()) {
				slog.DebugContext(ctx, "Skipping actor template with terminal golden snapshot status", slog.String("ActorTemplate", ref.String()))
			} else {
				r.queue.Add(ref)
				slog.InfoContext(ctx, "Added actor template to work queue", slog.String("ActorTemplate", ref.String()))
			}
		}
		if page.NextPageToken == "" {
			return
		}
		pageToken = page.NextPageToken
	}
}

func (r *ActorTemplateReconciler) runWorker(ctx context.Context) {
	for r.processNextWorkItem(ctx) {
	}
}

func (r *ActorTemplateReconciler) processNextWorkItem(ctx context.Context) bool {
	ref, quit := r.queue.Get()
	if quit {
		return false
	}
	defer r.queue.Done(ref)

	requeueAfter, err := r.reconcileOne(ctx, ref)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to reconcile actor template, requeueing",
			slog.Any("template", ref),
			slog.Any("err", err))
		r.queue.AddRateLimited(ref)
		return true
	}
	r.queue.Forget(ref)
	if requeueAfter > 0 {
		r.queue.AddAfter(ref, requeueAfter)
	}
	return true
}

// reconcileOne advances one template as far as it can go in this pass, holding
// the template's lease so concurrent replicas don't interleave transitions. The
// pass is level-triggered: each iteration re-derives the next action from the
// observed golden actor rather than stored progress, so every action must be
// reentrant. A positive requeueAfter asks the caller to revisit the template
// once its snapshot deadline (or a transitional actor state) passes.
func (r *ActorTemplateReconciler) reconcileOne(ctx context.Context, ref resources.ActorTemplateRef) (requeueAfter time.Duration, err error) {
	lease, err := r.persistence.AcquireLease(ctx, "lease:actortemplate:"+ref.Atespace+":"+ref.Name)
	if err != nil {
		if errors.Is(err, store.ErrLeaseConflict) {
			// Another replica owns this template for now.
			return 0, nil
		}
		return 0, fmt.Errorf("while acquiring lease: %w", err)
	}
	defer lease.Close()
	ctx = lease.Context()

	tmpl, err := r.persistence.GetActorTemplate(ctx, ref)
	if err != nil {
		// Template was deleted after we enqueued the work item.
		if errors.Is(err, store.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}

	goldenActorRef := &ateapipb.ObjectRef{
		// Golden actors live in the reserved ate-golden atespace, because
		// the suspend workflow relies on the ate-golden system atespace to
		// always take a full snapshot of the golden actor.
		// https://github.com/agent-substrate/substrate/blob/cb7c8385ef2bb489c3d5f7bfa71820fd33935d91/cmd/ateapi/internal/controlapi/workflow_suspend.go#L170-L173
		Atespace: resources.GoldenActorAtespace,
		// Use the template's UID as golden actor's name to prevent collision
		// when templates are recreated with the same name.
		Name: tmpl.GetMetadata().GetUid(),
	}

	// Each iteration observes the golden actor, takes the one action that
	// fact demands, and re-observes; the pass ends at a terminal condition,
	// a deadline wait, or an error the workqueue retries.
	for {
		goldenSnapshotStatus := tmpl.GetStatus().GetGoldenSnapshotStatus()
		if goldenSnapshotStatus.GetErrorMessage() != "" {
			// The snapshot has already failed.
			return 0, nil
		}
		if goldenSnapshotStatus.GetGoldenSnapshot() != nil {
			// The golden snapshot exists already.
			return 0, nil
		}

		actor, err := r.ensureActorExists(ctx, tmpl, goldenActorRef)
		if err != nil {
			if status.Code(err) == codes.InvalidArgument {
				// Invalid template spec; retrying can't help.
				return 0, r.fail(ctx, tmpl, reasonGoldenActorInvalid, err.Error())
			}
			return 0, err
		}

		switch state := actor.GetStatus().GetState(); state {
		case ateapipb.ActorState_ACTOR_STATE_CRASHED:
			return 0, r.fail(ctx, tmpl, reasonGoldenActorCrashed, "golden actor crashed before its snapshot was taken")

		case ateapipb.ActorState_ACTOR_STATE_RUNNING:
			takeAt := goldenSnapshotStatus.GetTakeGoldenSnapshotAt()
			if takeAt == nil {
				// Resumed, but the deadline write was lost (a replica died
				// after ResumeActor). The elapsed warmup is unknowable, so
				// restart the clock.
				slog.WarnContext(ctx, "Golden actor running without a snapshot deadline; restarting warmup", slog.String("ActorTemplate", ref.String()))
				deadline := time.Now().Add(goldenSnapshotWarmupFor(tmpl.GetContainers()))
				if tmpl, err = r.checkpoint(ctx, tmpl, func(snapshotStatus *ateapipb.GoldenSnapshotStatus) {
					snapshotStatus.TakeGoldenSnapshotAt = timestamppb.New(deadline)
				}); err != nil {
					return 0, err
				}
				continue
			}
			// Not time to take the golden snapshot yet; requeue.
			if rem := time.Until(takeAt.AsTime()); rem > 0 {
				return rem, nil
			}
			// Warmup done: suspend the golden actor and record its snapshot.
			snapshot, err := r.suspendActor(ctx, goldenActorRef)
			if err != nil {
				return 0, err
			}
			return 0, r.saveGoldenSnapshot(ctx, tmpl, snapshot)

		case ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
			// A previous pass died mid-suspend; retry suspend.
			snapshot, err := r.suspendActor(ctx, goldenActorRef)
			if err != nil {
				return 0, err
			}
			return 0, r.saveGoldenSnapshot(ctx, tmpl, snapshot)

		case ateapipb.ActorState_ACTOR_STATE_RESUMING,
			ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
			// The golden actor was never resumed, or a previous resume didn't
			// finish; ResumeActor is reentrant from both.
			if snapshot := actor.GetStatus().GetLatestSnapshot(); snapshot != nil {
				// Golden actors never start from a source snapshot, so an
				// existing snapshot means an earlier suspend completed
				// without being recorded.
				return 0, r.saveGoldenSnapshot(ctx, tmpl, snapshot)
			}
			if _, err := r.control.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: goldenActorRef}); err != nil {
				// A crash during resume is observed as CRASHED on the retry.
				return 0, fmt.Errorf("while resuming golden actor: %w", err)
			}
			deadline := time.Now().Add(goldenSnapshotWarmupFor(tmpl.GetContainers()))
			if tmpl, err = r.checkpoint(ctx, tmpl, func(snapshotStatus *ateapipb.GoldenSnapshotStatus) {
				snapshotStatus.TakeGoldenSnapshotAt = timestamppb.New(deadline)
			}); err != nil {
				return 0, err
			}
		case ateapipb.ActorState_ACTOR_STATE_DELETING, ateapipb.ActorState_ACTOR_STATE_PAUSED, ateapipb.ActorState_ACTOR_STATE_PAUSING:
			// Nothing in the golden flow deletes or pauses the actor before
			// the snapshot is taken; someone else interfered.
			return 0, r.fail(ctx, tmpl, reasonUnexpectedState, fmt.Sprintf("golden actor in unexpected state %v", state))

		default:
			return templateResyncInterval, nil
		}
	}
}

// suspendActor suspends the golden actor and returns the resulting snapshot.
// Reentrant: SuspendActor completes an in-flight suspend and is a no-op on an
// already-suspended actor, returning the existing snapshot either way.
func (r *ActorTemplateReconciler) suspendActor(ctx context.Context, goldenRef *ateapipb.ObjectRef) (*ateapipb.ObjectRef, error) {
	resp, err := r.control.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: goldenRef})
	if err != nil {
		// A crash during suspend is observed as CRASHED on the retry.
		return nil, fmt.Errorf("while suspending golden actor: %w", err)
	}
	snapshot := resp.GetActor().GetStatus().GetLatestSnapshot()
	if snapshot == nil {
		return nil, fmt.Errorf("suspending golden actor returned no ActorSnapshot")
	}
	return snapshot, nil
}

// saveGoldenSnapshot records the golden snapshot, the terminal success state
// that marks the template ready for use, ending the reconcile pass.
func (r *ActorTemplateReconciler) saveGoldenSnapshot(ctx context.Context, observed *ateapipb.ActorTemplate, snapshot *ateapipb.ObjectRef) error {
	_, err := r.checkpoint(ctx, observed, func(snapshotStatus *ateapipb.GoldenSnapshotStatus) {
		snapshotStatus.GoldenSnapshot = snapshot
	})
	return err
}

// checkpoint commits a golden snapshot status mutation unless a concurrent
// writer already drove the template to a terminal state. The write is guarded
// by the observed template's uid and version, so a stale observation surfaces
// as a conflict for the workqueue to retry.
func (r *ActorTemplateReconciler) checkpoint(ctx context.Context, observed *ateapipb.ActorTemplate, mutate func(*ateapipb.GoldenSnapshotStatus)) (*ateapipb.ActorTemplate, error) {
	ref := resources.ActorTemplateRefFromActorTemplate(observed)
	updated, err := r.persistence.UpdateActorTemplate(ctx, ref, store.PreconditionFrom(observed), func(dbTemplate *ateapipb.ActorTemplate) error {
		if goldenSnapshotDone(dbTemplate.GetStatus().GetGoldenSnapshotStatus()) {
			return fmt.Errorf("actor template reached a terminal golden snapshot state concurrently")
		}
		if dbTemplate.Status == nil {
			dbTemplate.Status = &ateapipb.ActorTemplateStatus{}
		}
		if dbTemplate.Status.GoldenSnapshotStatus == nil {
			dbTemplate.Status.GoldenSnapshotStatus = &ateapipb.GoldenSnapshotStatus{}
		}
		mutate(dbTemplate.Status.GoldenSnapshotStatus)
		return nil
	})

	return updated, err
}

// fail commits the terminal error message, prefixed with a machine-readable
// reason.
func (r *ActorTemplateReconciler) fail(ctx context.Context, observed *ateapipb.ActorTemplate, reason, msg string) error {
	_, err := r.checkpoint(ctx, observed, func(snapshotStatus *ateapipb.GoldenSnapshotStatus) {
		snapshotStatus.ErrorMessage = reason + ": " + msg
	})
	return err
}

// goldenSnapshotDone reports whether the golden snapshot build reached a
// terminal state: the snapshot was recorded, or the build failed.
func goldenSnapshotDone(snapshotStatus *ateapipb.GoldenSnapshotStatus) bool {
	return snapshotStatus.GetGoldenSnapshot() != nil || snapshotStatus.GetErrorMessage() != ""
}

// goldenSnapshotWarmupFor returns 0 when every container has a readyz probe
// (ResumeActor already blocked until the workload reported 200), and the
// default warmup otherwise. Mirrors the CRD controller's function of the
// same name; keep both in sync.
func goldenSnapshotWarmupFor(containers []*ateapipb.Container) time.Duration {
	if len(containers) == 0 {
		return goldenSnapshotWarmup
	}
	for _, container := range containers {
		if container.GetReadyz() == nil {
			return goldenSnapshotWarmup
		}
	}
	return 0
}

// ensureActorExists returns the golden actor, creating it first if it does
// not exist yet.
func (r *ActorTemplateReconciler) ensureActorExists(ctx context.Context, tmpl *ateapipb.ActorTemplate, goldenActorRef *ateapipb.ObjectRef) (*ateapipb.Actor, error) {
	actor, err := r.control.GetActor(ctx, &ateapipb.GetActorRequest{Actor: goldenActorRef})
	if err == nil {
		return actor, nil
	}
	if status.Code(err) != codes.NotFound {
		return nil, fmt.Errorf("while getting golden actor: %w", err)
	}
	// Golden actor has not yet been created. Its reserved atespace is
	// system-owned, so ensure it exists rather than assuming bootstrap did.
	if _, err := r.control.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: goldenActorRef.GetAtespace()}},
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		return nil, fmt.Errorf("while ensuring atespace %q: %w", goldenActorRef.GetAtespace(), err)
	}
	actor, err = r.control.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: goldenActorRef.GetAtespace(),
				Name:     goldenActorRef.GetName(),
			},
			ActorTemplate: resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef(),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("while creating golden actor: %w", err)
	}
	return actor, nil
}
