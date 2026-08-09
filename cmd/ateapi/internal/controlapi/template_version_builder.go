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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// templateVersionBuildInterval is the poll period for versions with a
	// build in progress. Coarse is fine: a build takes tens of seconds.
	templateVersionBuildInterval = 5 * time.Second

	// templateVersionBuildTimeout bounds a build measured from the version's
	// create_time; transient errors retry until it elapses, then the version
	// goes FAILED with the last error.
	templateVersionBuildTimeout = 20 * time.Minute

	// templateVersionWarmup is the delay between resuming the golden actor
	// and snapshotting it, for versions without readyz on every container
	// (mirrors the CRD reconciler's goldenSnapshotWarmup).
	templateVersionWarmup = 20 * time.Second

	templateVersionListPageSize = 1000
)

// actorTemplateVersionLockKey serializes the build loop across ateapi
// replicas, per version. Distinct from actorTemplateLockKey, which guards
// template-level invariants.
func actorTemplateVersionLockKey(name string) string {
	return "lock:actor-template-version:" + name
}

// TemplateVersionBuilder drives ActorTemplateVersions from STATE_INITIAL to
// STATE_READY by building their golden snapshot: boot a golden actor in the
// reserved ate-golden atespace, wait for warmup, suspend it, and record the
// resulting snapshot. The in-process port of the CRD ActorTemplateReconciler.
//
// All progress is persisted in the version's status (CAS on
// metadata.version), so any replica can pick up a build at any step; a
// per-version distributed lock keeps replicas from stepping on each other.
type TemplateVersionBuilder struct {
	service *Service
	store   store.Interface

	interval     time.Duration
	buildTimeout time.Duration
	now          func() time.Time
}

func NewTemplateVersionBuilder(service *Service, persistence store.Interface) *TemplateVersionBuilder {
	return &TemplateVersionBuilder{
		service:      service,
		store:        persistence,
		interval:     templateVersionBuildInterval,
		buildTimeout: templateVersionBuildTimeout,
		now:          time.Now,
	}
}

// Start runs the build loop until ctx is canceled. An interrupted transition
// is retried here or by a peer replica from the persisted status.
func (b *TemplateVersionBuilder) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(b.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.runOnce(ctx)
			}
		}
	}()
}

// runOnce advances every non-terminal version by at most one state
// transition. Errors are contained per version: one broken build never
// stalls the others.
func (b *TemplateVersionBuilder) runOnce(ctx context.Context) {
	pageToken := ""
	for {
		versions, nextToken, err := b.store.ListActorTemplateVersions(ctx, "", templateVersionListPageSize, pageToken)
		if err != nil {
			slog.ErrorContext(ctx, "Template version build: list failed", slog.Any("error", err))
			return
		}
		for _, atv := range versions {
			if templateVersionStateTerminal(atv.GetStatus().GetState()) {
				continue
			}
			b.buildOne(ctx, atv.GetMetadata().GetName())
		}
		if nextToken == "" {
			return
		}
		pageToken = nextToken
	}
}

// buildOne performs one state transition for the named version under its
// distributed lock, classifying errors into retry (next tick), FAILED on
// build timeout, or FAILED immediately for permanent ones.
func (b *TemplateVersionBuilder) buildOne(ctx context.Context, name string) {
	lock, err := b.store.AcquireLock(ctx, actorTemplateVersionLockKey(name))
	if err != nil {
		if !errors.Is(err, store.ErrLockConflict) {
			slog.ErrorContext(ctx, "Template version build: lock failed", slog.String("version", name), slog.Any("error", err))
		}
		return
	}
	defer lock.Close()
	ctx = lock.Context()

	// Re-read under the lock: the listed copy may be stale.
	atv, err := b.store.GetActorTemplateVersion(ctx, name)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			slog.ErrorContext(ctx, "Template version build: get failed", slog.String("version", name), slog.Any("error", err))
		}
		return
	}
	if templateVersionStateTerminal(atv.GetStatus().GetState()) {
		return
	}

	transitionErr := b.advance(ctx, atv)
	if transitionErr == nil {
		return
	}
	if !templateVersionBuildErrPermanent(transitionErr) &&
		b.now().Sub(atv.GetMetadata().GetCreateTime().AsTime()) < b.buildTimeout {
		slog.WarnContext(ctx, "Template version build: transition failed, will retry",
			slog.String("version", name), slog.String("state", atv.GetStatus().GetState().String()), slog.Any("error", transitionErr))
		return
	}

	slog.ErrorContext(ctx, "Template version build failed",
		slog.String("version", name), slog.String("state", atv.GetStatus().GetState().String()), slog.Any("error", transitionErr))
	failed := proto.Clone(atv.GetStatus()).(*ateapipb.ActorTemplateVersionStatus)
	failed.State = ateapipb.ActorTemplateVersionStatus_STATE_FAILED
	failed.Message = transitionErr.Error()
	if _, err := b.store.UpdateActorTemplateVersionStatus(ctx, name, failed, atv.GetMetadata().GetVersion()); err != nil {
		slog.ErrorContext(ctx, "Template version build: persisting FAILED state failed", slog.String("version", name), slog.Any("error", err))
	}
}

// advance performs the single transition the version's persisted state calls
// for. Every service call tolerates "already done" so a transition
// interrupted after acting but before persisting replays cleanly.
func (b *TemplateVersionBuilder) advance(ctx context.Context, atv *ateapipb.ActorTemplateVersion) error {
	name := atv.GetMetadata().GetName()
	next := proto.Clone(atv.GetStatus()).(*ateapipb.ActorTemplateVersionStatus)

	switch atv.GetStatus().GetState() {
	case ateapipb.ActorTemplateVersionStatus_STATE_UNSPECIFIED, ateapipb.ActorTemplateVersionStatus_STATE_INITIAL:
		// Golden actors live in the reserved ate-golden system atespace and
		// are named by the version's uid.
		goldenActor := &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: atv.GetMetadata().GetUid()}
		if _, err := b.service.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
			Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: resources.GoldenActorAtespace}},
		}); err != nil && status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("while ensuring atespace %q: %w", resources.GoldenActorAtespace, err)
		}
		if _, err := b.service.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:             &ateapipb.ResourceMetadata{Atespace: goldenActor.GetAtespace(), Name: goldenActor.GetName()},
			ActorTemplate:        atv.GetActorTemplate().GetName(),
			ActorTemplateVersion: name,
		}}); err != nil && status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("while creating golden actor: %w", err)
		}
		next.State = ateapipb.ActorTemplateVersionStatus_STATE_RESUME_GOLDEN_ACTOR
		next.GoldenActor = goldenActor

	case ateapipb.ActorTemplateVersionStatus_STATE_RESUME_GOLDEN_ACTOR:
		// Resuming a version with no golden snapshot boots the workload from
		// its spec; with readyz on every container the call only returns
		// once the workload reports ready, so no extra warmup is needed.
		if _, err := b.service.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: atv.GetStatus().GetGoldenActor()}); err != nil {
			return fmt.Errorf("while resuming golden actor: %w", err)
		}
		next.State = ateapipb.ActorTemplateVersionStatus_STATE_WAIT_GOLDEN_ACTOR
		next.TakeGoldenSnapshotAt = timestamppb.New(b.now().Add(templateVersionWarmupFor(atv.GetSpec())))

	case ateapipb.ActorTemplateVersionStatus_STATE_WAIT_GOLDEN_ACTOR:
		if b.now().Before(atv.GetStatus().GetTakeGoldenSnapshotAt().AsTime()) {
			return nil
		}
		// Suspending an already-SUSPENDED actor fast-forwards, so an
		// interrupted transition still reads the snapshot back here.
		resp, err := b.service.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: atv.GetStatus().GetGoldenActor()})
		if err != nil {
			return fmt.Errorf("while suspending golden actor: %w", err)
		}
		snapshot := resp.GetActor().GetLatestSnapshot()
		if snapshot == nil {
			return fmt.Errorf("suspending golden actor returned no ActorSnapshot")
		}
		next.State = ateapipb.ActorTemplateVersionStatus_STATE_READY
		next.GoldenSnapshot = snapshot
		next.Message = ""
		if _, err := b.store.UpdateActorTemplateVersionStatus(ctx, name, next, atv.GetMetadata().GetVersion()); err != nil {
			return fmt.Errorf("while persisting status: %w", err)
		}
		// READY is durable; only now delete the golden actor (its snapshot
		// record survives — an improvement over the CRD reconciler, which
		// leaks it). Best-effort: a leak here is harmless.
		if _, err := b.service.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: atv.GetStatus().GetGoldenActor()}); err != nil {
			slog.WarnContext(ctx, "Template version build: deleting golden actor failed",
				slog.String("version", name), slog.Any("error", err))
		}
		slog.InfoContext(ctx, "Template version build: READY",
			slog.String("version", name), slog.String("golden_snapshot", snapshot.GetName()))
		return nil

	default:
		return fmt.Errorf("unrecognized state %q", atv.GetStatus().GetState())
	}

	if _, err := b.store.UpdateActorTemplateVersionStatus(ctx, name, next, atv.GetMetadata().GetVersion()); err != nil {
		return fmt.Errorf("while persisting status: %w", err)
	}
	return nil
}

func templateVersionStateTerminal(state ateapipb.ActorTemplateVersionStatus_State) bool {
	return state == ateapipb.ActorTemplateVersionStatus_STATE_READY ||
		state == ateapipb.ActorTemplateVersionStatus_STATE_FAILED
}

// templateVersionBuildErrPermanent reports whether a transition error can
// never heal by retrying: a malformed spec or lost data. Everything else
// (no free worker, a worker mid-drain, a CAS conflict) retries until the
// build timeout.
func templateVersionBuildErrPermanent(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.DataLoss:
		return true
	}
	// The golden actor vanished mid-build; recreating it belongs to a new
	// version, not endless retries against a missing actor.
	return status.Code(err) == codes.NotFound
}

// templateVersionWarmupFor mirrors the CRD reconciler's
// goldenSnapshotWarmupFor: zero when every container has readyz (resume
// already blocked on readiness), the default warmup otherwise.
func templateVersionWarmupFor(spec *ateapipb.ActorTemplateVersionSpec) time.Duration {
	containers := spec.GetContainers()
	if len(containers) == 0 {
		return templateVersionWarmup
	}
	for _, ctr := range containers {
		if ctr.GetReadyz() == nil {
			return templateVersionWarmup
		}
	}
	return 0
}
