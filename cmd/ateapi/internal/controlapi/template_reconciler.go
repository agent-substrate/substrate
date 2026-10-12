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
	"unicode/utf8"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/snapshotlayout"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const (
	templateListPageSize = 100

	// templateWorkerCount is the number of goroutines draining the work
	// queue.
	templateWorkerCount = 5

	// goldenSnapshotWarmup is the default wall-clock delay between resuming
	// the golden actor and taking its snapshot, for templates without a
	// wakeup probe on every container.
	goldenSnapshotWarmup = 20 * time.Second
)

const (
	reasonGoldenTagConflict  = "GoldenTagConflict"
	reasonGoldenActorInvalid = "GoldenActorInvalid"
	reasonGoldenActorCrashed = "GoldenActorCrashed"
	reasonUnexpectedState    = "GoldenActorUnexpectedState"
)

// maxGoldenErrorMessageLen is the maxLength of
// GoldenSnapshotStatus.error_message.
const maxGoldenErrorMessageLen = 4096

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
	GetTag(ctx context.Context, req *ateapipb.GetTagRequest) (*ateapipb.Tag, error)
	CreateTag(ctx context.Context, req *ateapipb.CreateTagRequest) (*ateapipb.Tag, error)
	DeleteTag(ctx context.Context, req *ateapipb.DeleteTagRequest) (*ateapipb.Tag, error)
	DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.Actor, error)
	CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error)
	CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error)
	GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error)
	ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error)
	SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error)
}

// ActorTemplateReconciler drives stored ActorTemplates through the golden
// actor state machine, and through it again once the workers write a newer
// snapshot layout than a template's golden was written in.
type ActorTemplateReconciler struct {
	persistence    templateReconcilerStore
	control        goldenActorControl
	queue          workqueue.TypedRateLimitingInterface[resources.ActorTemplateRef]
	resyncInterval time.Duration
}

func NewActorTemplateReconciler(persistence templateReconcilerStore, control goldenActorControl, resyncInterval time.Duration) *ActorTemplateReconciler {
	return &ActorTemplateReconciler{
		persistence:    persistence,
		control:        control,
		resyncInterval: resyncInterval,
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
		wait.UntilWithContext(ctx, r.resync, r.resyncInterval)
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
			if tmpl.GetSnapshotConfig().GetGoldenSnapshotConfig().GetMode() == ateapipb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED {
				continue
			}
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

	if tmpl.GetSnapshotConfig().GetGoldenSnapshotConfig().GetMode() == ateapipb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED {
		return 0, nil
	}

	goldenActorRef := goldenActorRefFor(tmpl)

	// Each iteration observes the golden actor, takes the one action that
	// fact demands, and re-observes; the pass ends at a terminal condition,
	// a deadline wait, or an error the workqueue retries.
	for {
		goldenSnapshotStatus := tmpl.GetStatus().GetGoldenSnapshotStatus()
		if goldenSnapshotStatus.GetErrorMessage() != "" {
			// The snapshot has already failed.
			return 0, nil
		}
		if goldenSnapshotStatus.GetGoldenTag() != nil && !staleGoldenSnapshotLayout(goldenSnapshotStatus) {
			// The golden snapshot exists already, in the layout the workers write.
			return 0, nil
		}
		// The tag this pass records: a first golden's, or, for a golden in an
		// older layout, the one taken again beside it, the recorded tag staying
		// the template's golden until the new one is complete.
		tagRef := goldenTagRef(tmpl, goldenSnapshotStatus)

		// A completed tag survives a crash during actor deletion or checkpointing.
		tag, err := r.control.GetTag(ctx, &ateapipb.GetTagRequest{Tag: tagRef})
		if err != nil && apierror.Code(err) != codes.NotFound {
			return 0, fmt.Errorf("while getting golden tag: %w", err)
		}
		if err == nil {
			if tag.GetStatus().GetActorTemplateUid() != tmpl.GetMetadata().GetUid() || resources.ActorRefFromObjectRef(tag.GetSourceActor()) != resources.ActorRefFromObjectRef(goldenActorRef) {
				return 0, r.fail(ctx, tmpl, reasonGoldenTagConflict, "golden tag belongs to another actor or template")
			}
			if tagDurableSnapshotURI(tag, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED) != "" {
				return 0, r.saveGoldenTag(ctx, tmpl, goldenActorRef, tagRef)
			}
			// CreateTag cannot resume an incomplete copy. Delete it before retrying.
			if _, err := r.control.DeleteTag(ctx, &ateapipb.DeleteTagRequest{Tag: tagRef}); err != nil && apierror.Code(err) != codes.NotFound {
				return 0, fmt.Errorf("while deleting incomplete golden tag: %w", err)
			}
		}

		actor, err := r.ensureActorExists(ctx, tmpl, goldenActorRef)
		if err != nil {
			if apierror.Code(err) == codes.InvalidArgument {
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
			err := r.suspendActor(ctx, goldenActorRef)
			if err != nil {
				return 0, err
			}
			return 0, r.tagGoldenActor(ctx, tmpl, goldenActorRef, tagRef)

		case ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
			// A previous pass died mid-suspend; retry suspend.
			err := r.suspendActor(ctx, goldenActorRef)
			if err != nil {
				return 0, err
			}
			return 0, r.tagGoldenActor(ctx, tmpl, goldenActorRef, tagRef)

		case ateapipb.ActorState_ACTOR_STATE_RESUMING,
			ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
			// The golden actor was never resumed, or a previous resume didn't
			// finish; ResumeActor is reentrant from both.
			if findLatestDurableSnapshot(actor.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED) != nil {
				// Golden actors never start from a source snapshot, so an
				// existing snapshot means an earlier suspend completed
				// without being recorded.
				return 0, r.tagGoldenActor(ctx, tmpl, goldenActorRef, tagRef)
			}
			if goldenSnapshotStatus.GetTakeGoldenSnapshotAt() != nil {
				// The deadline is an earlier warmup's: the one the golden being
				// taken again was first taken on. The resume below starts a
				// new warmup, whose deadline is recorded once the resume
				// returns; cleared first, a lost deadline write restarts that
				// warmup instead of ending it on the stale deadline.
				if tmpl, err = r.checkpoint(ctx, tmpl, func(snapshotStatus *ateapipb.GoldenSnapshotStatus) {
					snapshotStatus.TakeGoldenSnapshotAt = nil
				}); err != nil {
					return 0, err
				}
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
		case ateapipb.ActorState_ACTOR_STATE_DELETING, ateapipb.ActorState_ACTOR_STATE_PAUSED, ateapipb.ActorState_ACTOR_STATE_PAUSING, ateapipb.ActorState_ACTOR_STATE_REVERTING:
			// Nothing in the golden flow deletes, pauses, or reverts the actor before
			// the snapshot is taken; someone else interfered.
			return 0, r.fail(ctx, tmpl, reasonUnexpectedState, fmt.Sprintf("golden actor in unexpected state %v", state))

		default:
			return r.resyncInterval, nil
		}
	}
}

// suspendActor waits for the golden actor to produce an external snapshot.
// SuspendActor completes an in-flight suspend and is a no-op if already suspended.
func (r *ActorTemplateReconciler) suspendActor(ctx context.Context, goldenRef *ateapipb.ObjectRef) error {
	resp, err := r.control.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: goldenRef})
	if err != nil {
		// A crash during suspend is observed as CRASHED on the retry.
		return fmt.Errorf("while suspending golden actor: %w", err)
	}
	if findLatestDurableSnapshot(resp.GetActor().GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED) == nil {
		return fmt.Errorf("suspending golden actor produced no external snapshot")
	}
	return nil
}

// tagGoldenActor copies the snapshot into the tag before releasing the actor's
// copy.
func (r *ActorTemplateReconciler) tagGoldenActor(ctx context.Context, tmpl *ateapipb.ActorTemplate, goldenActorRef, tagRef *ateapipb.ObjectRef) error {
	_, err := r.control.CreateTag(ctx, &ateapipb.CreateTagRequest{Tag: &ateapipb.Tag{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: tagRef.GetAtespace(), Name: tagRef.GetName()},
		SourceActor: goldenActorRef,
		Scope:       ateapipb.TagScope_TAG_SCOPE_PUBLISHED,
	}})
	if err != nil {
		return fmt.Errorf("while creating golden tag: %w", err)
	}
	return r.saveGoldenTag(ctx, tmpl, goldenActorRef, tagRef)
}

// saveGoldenTag finishes cleanup before recording terminal success, so retries
// can rediscover the tag even if deletion or the status write fails: the
// golden actor is released and, for a golden taken again in a newer layout,
// the tag it replaces is deleted with its snapshot, having stayed the
// template's golden until now. The record names the tag and the layout the
// workers write, which its snapshot was taken in.
func (r *ActorTemplateReconciler) saveGoldenTag(ctx context.Context, tmpl *ateapipb.ActorTemplate, goldenActorRef, tagRef *ateapipb.ObjectRef) error {
	if _, err := r.control.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: goldenActorRef}); err != nil && apierror.Code(err) != codes.NotFound {
		return fmt.Errorf("while deleting golden actor: %w", err)
	}
	snapshotStatus := tmpl.GetStatus().GetGoldenSnapshotStatus()
	if replaced := snapshotStatus.GetGoldenTag(); replaced.GetName() != "" && resources.TagRefFromObjectRef(replaced) != resources.TagRefFromObjectRef(tagRef) {
		if _, err := r.control.DeleteTag(ctx, &ateapipb.DeleteTagRequest{Tag: replaced}); err != nil && apierror.Code(err) != codes.NotFound {
			return fmt.Errorf("while deleting the golden tag taken in an older snapshot layout: %w", err)
		}
		slog.InfoContext(ctx, "Replaced the golden tag taken in an older snapshot layout",
			slog.String("ActorTemplate", resources.ActorTemplateRefFromActorTemplate(tmpl).String()),
			slog.String("replacedTag", resources.TagRefFromObjectRef(replaced).String()),
			slog.Int("replacedLayout", int(snapshotStatus.GetSnapshotLayout())),
			slog.String("goldenTag", resources.TagRefFromObjectRef(tagRef).String()),
			slog.Int("layout", int(snapshotlayout.Current)))
	}
	_, err := r.checkpoint(ctx, tmpl, func(snapshotStatus *ateapipb.GoldenSnapshotStatus) {
		snapshotStatus.GoldenTag = tagRef
		snapshotStatus.SnapshotLayout = snapshotlayout.Current
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
// reason and truncated to fit error_message's bound.
func (r *ActorTemplateReconciler) fail(ctx context.Context, observed *ateapipb.ActorTemplate, reason, msg string) error {
	_, err := r.checkpoint(ctx, observed, func(snapshotStatus *ateapipb.GoldenSnapshotStatus) {
		snapshotStatus.ErrorMessage = truncateUTF8(reason+": "+msg, maxGoldenErrorMessageLen)
	})
	return err
}

// truncateUTF8 cuts s to at most n bytes without splitting a UTF-8 sequence,
// since proto string fields must hold valid UTF-8.
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// goldenSnapshotDone reports whether the golden snapshot build reached a
// terminal state: the snapshot was recorded, in the layout the workers write,
// or the build failed. A golden in an older layout, or recorded without its
// layout, is not done: it is still to be taken again in the current one.
func goldenSnapshotDone(snapshotStatus *ateapipb.GoldenSnapshotStatus) bool {
	if snapshotStatus.GetErrorMessage() != "" {
		return true
	}
	return snapshotStatus.GetGoldenTag() != nil && !staleGoldenSnapshotLayout(snapshotStatus)
}

// goldenSnapshotWarmupFor returns 0 when every container has a wakeup probe
// (ResumeActor already blocked until the workload reported 200), and the
// default warmup otherwise.
func goldenSnapshotWarmupFor(containers []*ateapipb.Container) time.Duration {
	if len(containers) == 0 {
		return goldenSnapshotWarmup
	}
	for _, container := range containers {
		if container.GetWakeupProbe() == nil {
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
	if apierror.Code(err) != codes.NotFound {
		return nil, fmt.Errorf("while getting golden actor: %w", err)
	}
	// Golden actor has not yet been created. Its reserved atespace is
	// system-owned, so ensure it exists rather than assuming bootstrap did.
	if _, err := r.control.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: goldenActorRef.GetAtespace()}},
	}); err != nil && apierror.Code(err) != codes.AlreadyExists {
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
