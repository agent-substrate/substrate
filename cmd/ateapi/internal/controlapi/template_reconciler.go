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
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const (
	templateResyncInterval = 5 * time.Second
	templateListPageSize   = 100

	// templateWorkerCount is the number of goroutines draining the work
	// queue.
	templateWorkerCount = 2

	// goldenSnapshotWarmup is the default wall-clock delay between resuming
	// the golden actor and taking its snapshot, for templates without a
	// readiness probe on every container. Mirrors the ActorTemplate CRD
	// controller (cmd/atecontroller/internal/controllers); keep in sync.
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
	UpdateActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, mutate func(dbTemplate *ateapipb.ActorTemplate) error) (*ateapipb.ActorTemplate, error)
	AcquireLock(ctx context.Context, key string) (*store.Lock, error)
}

// goldenActorControl is the in-process slice of the Control service the
// reconciler drives golden actors through. *Service satisfies it.
type goldenActorControl interface {
	CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error)
	GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error)
	ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error)
	SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error)
}

// ActorTemplateReconciler drives stored ActorTemplates through the golden
// actor state machine.
type ActorTemplateReconciler struct {
	persistence    templateReconcilerStore
	control        goldenActorControl
	sandboxConfigs listersv1alpha1.SandboxConfigLister
	queue          workqueue.TypedRateLimitingInterface[resources.ActorTemplateRef]
}

func NewActorTemplateReconciler(persistence templateReconcilerStore, control goldenActorControl, sandboxConfigs listersv1alpha1.SandboxConfigLister) *ActorTemplateReconciler {
	return &ActorTemplateReconciler{
		persistence:    persistence,
		control:        control,
		sandboxConfigs: sandboxConfigs,
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
			conds := tmpl.GetStatus().GetConditions()
			if conditionIsTrue(conds, conditionReady) || conditionIsTrue(conds, conditionFailed) {
				slog.DebugContext(ctx, "Skipping actor template", slog.String("ActorTemplate", ref.String()), slog.String("conditions", conditionsSummary(conds)))
			} else {
				r.queue.Add(ref)
				slog.InfoContext(ctx, "Added actor template to work queue", slog.String("ActorTemplate", ref.String()), slog.String("conditions", conditionsSummary(conds)))
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
// the template's lock so concurrent replicas don't interleave transitions. The
// pass is level-triggered: each iteration re-derives the next action from the
// observed golden actor rather than stored progress, so every action must be
// reentrant. A positive requeueAfter asks the caller to revisit the template
// once its snapshot deadline (or a transitional actor state) passes.
func (r *ActorTemplateReconciler) reconcileOne(ctx context.Context, ref resources.ActorTemplateRef) (requeueAfter time.Duration, err error) {
	lock, err := r.persistence.AcquireLock(ctx, "lock:actortemplate:"+ref.Atespace+":"+ref.Name)
	if err != nil {
		if errors.Is(err, store.ErrLockConflict) {
			// Another replica owns this template for now.
			return 0, nil
		}
		return 0, fmt.Errorf("while acquiring lock: %w", err)
	}
	defer lock.Close()
	ctx = lock.Context()

	tmpl, err := r.persistence.GetActorTemplate(ctx, ref)
	if err != nil {
		// Template was deleted after we enqueued the work item.
		if errors.Is(err, store.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}

	goldenRef := &ateapipb.ObjectRef{
		Atespace: tmpl.GetMetadata().GetAtespace(),
		// Use the template's UID as golden actor's name to prevent collision
		// when templates are recreated with the same name.
		Name: tmpl.GetMetadata().GetUid(),
	}

	// Each iteration observes the golden actor, takes the one action that
	// fact demands, and re-observes; the pass ends at a terminal condition,
	// a deadline wait, or an error the workqueue retries.
	for {
		conds := tmpl.GetStatus().GetConditions()
		if conditionIsTrue(conds, conditionReady) || conditionIsTrue(conds, conditionFailed) {
			return 0, nil
		}

		// TODO: Freeze sandbox assets before creating the golden actor.
		actor, err := r.control.GetActor(ctx, &ateapipb.GetActorRequest{Actor: goldenRef})
		if status.Code(err) == codes.NotFound {
			// Golden actor has not yet been created.
			if _, err := r.control.CreateActor(ctx, &ateapipb.CreateActorRequest{
				Actor: &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{
						Atespace: goldenRef.GetAtespace(),
						Name:     goldenRef.GetName(),
					},
					ActorTemplate: ref.ToObjectRef(),
				},
			}); err != nil && status.Code(err) != codes.AlreadyExists {
				if status.Code(err) == codes.InvalidArgument {
					return 0, r.fail(ctx, ref, reasonGoldenActorInvalid, fmt.Sprintf("creating golden actor: %v", err))
				}
				// If the error is retriable, return an error here so the workqueue will retry.
				return 0, fmt.Errorf("while creating golden actor: %w", err)
			}
			// Re-observe the freshly created actor at the begining of the loop.
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("while getting golden actor: %w", err)
		}

		switch state := actor.GetStatus().GetState(); state {
		case ateapipb.ActorState_ACTOR_STATE_CRASHED:
			return 0, r.fail(ctx, ref, reasonGoldenActorCrashed, "golden actor crashed before its snapshot was taken")

		case ateapipb.ActorState_ACTOR_STATE_RUNNING:
			takeAt := tmpl.GetStatus().GetTakeGoldenSnapshotAt()
			if takeAt == nil {
				// Resumed, but the deadline write was lost (a replica died
				// after ResumeActor). The elapsed warmup is unknowable, so
				// restart the clock.
				slog.WarnContext(ctx, "Golden actor running without a snapshot deadline; restarting warmup", slog.String("ActorTemplate", ref.String()))
				deadline := time.Now().Add(goldenSnapshotWarmupFor(tmpl.GetContainers()))
				if tmpl, err = r.checkpoint(ctx, ref, func(templateStatus *ateapipb.ActorTemplateStatus) {
					templateStatus.TakeGoldenSnapshotAt = timestamppb.New(deadline)
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
			snapshot, err := r.suspendActor(ctx, goldenRef)
			if err != nil {
				return 0, err
			}
			if tmpl, err = r.saveGoldenSnapshot(ctx, ref, snapshot); err != nil {
				return 0, err
			}

		case ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
			// A previous pass died mid-suspend; retry suspend.
			snapshot, err := r.suspendActor(ctx, goldenRef)
			if err != nil {
				return 0, err
			}
			if tmpl, err = r.saveGoldenSnapshot(ctx, ref, snapshot); err != nil {
				return 0, err
			}

		case ateapipb.ActorState_ACTOR_STATE_RESUMING,
			ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
			// The golden actor was never resumed, or a previous resume didn't
			// finish; ResumeActor is reentrant from both.
			if snapshot := actor.GetStatus().GetLatestSnapshot(); snapshot != nil {
				// Golden actors never start from a source snapshot, so an
				// existing snapshot means an earlier suspend completed
				// without being recorded.
				if tmpl, err = r.saveGoldenSnapshot(ctx, ref, snapshot); err != nil {
					return 0, err
				}
				continue
			}
			if _, err := r.control.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: goldenRef}); err != nil {
				// A crash during resume is observed as CRASHED on the retry.
				return 0, fmt.Errorf("while resuming golden actor: %w", err)
			}
			deadline := time.Now().Add(goldenSnapshotWarmupFor(tmpl.GetContainers()))
			if tmpl, err = r.checkpoint(ctx, ref, func(templateStatus *ateapipb.ActorTemplateStatus) {
				templateStatus.TakeGoldenSnapshotAt = timestamppb.New(deadline)
			}); err != nil {
				return 0, err
			}
		case ateapipb.ActorState_ACTOR_STATE_DELETING, ateapipb.ActorState_ACTOR_STATE_PAUSED, ateapipb.ActorState_ACTOR_STATE_PAUSING:
			// Nothing in the golden flow deletes or pauses the actor before
			// the snapshot is taken; someone else interfered.
			return 0, r.fail(ctx, ref, reasonUnexpectedState, fmt.Sprintf("golden actor in unexpected state %v", state))

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

// saveGoldenSnapshot records the golden snapshot and marks the template
// Ready.
func (r *ActorTemplateReconciler) saveGoldenSnapshot(ctx context.Context, ref resources.ActorTemplateRef, snapshot *ateapipb.ObjectRef) (*ateapipb.ActorTemplate, error) {
	return r.checkpoint(ctx, ref, func(templateStatus *ateapipb.ActorTemplateStatus) {
		templateStatus.GoldenSnapshot = snapshot
		setCondition(&templateStatus.Conditions, conditionReady, ateapipb.ConditionStatus_CONDITION_STATUS_TRUE, conditionReady, "golden snapshot taken; template ready for use")
	})
}

// checkpoint commits a status mutation unless a concurrent writer already
// drove the template to a terminal condition.
func (r *ActorTemplateReconciler) checkpoint(ctx context.Context, ref resources.ActorTemplateRef, mutate func(*ateapipb.ActorTemplateStatus)) (*ateapipb.ActorTemplate, error) {
	updated, err := r.persistence.UpdateActorTemplate(ctx, ref, func(dbTemplate *ateapipb.ActorTemplate) error {
		conds := dbTemplate.GetStatus().GetConditions()
		if conditionIsTrue(conds, conditionReady) || conditionIsTrue(conds, conditionFailed) {
			return fmt.Errorf("actor template reached a terminal condition concurrently")
		}
		if dbTemplate.Status == nil {
			dbTemplate.Status = &ateapipb.ActorTemplateStatus{}
		}
		mutate(dbTemplate.Status)
		return nil
	})

	return updated, err
}

// fail commits the terminal Failed condition with a machine-readable reason
// and a human-readable message.
func (r *ActorTemplateReconciler) fail(ctx context.Context, ref resources.ActorTemplateRef, reason, msg string) error {
	_, err := r.checkpoint(ctx, ref, func(templateStatus *ateapipb.ActorTemplateStatus) {
		setCondition(&templateStatus.Conditions, conditionFailed, ateapipb.ConditionStatus_CONDITION_STATUS_TRUE, reason, msg)
	})
	return err
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
