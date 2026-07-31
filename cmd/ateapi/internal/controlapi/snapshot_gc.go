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
	"sort"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	defaultSnapshotMinimumCount      = int32(3)
	defaultSnapshotMinimumAgeSeconds = int32(86400)
	snapshotGCPageSize               = int32(1000)
)

type snapshotKey struct {
	atespace string
	name     string
}

func snapshotKeyFromRef(ref *ateapipb.ObjectRef) snapshotKey {
	return snapshotKey{atespace: ref.GetAtespace(), name: ref.GetName()}
}

func snapshotKeyFromSnapshot(snapshot *ateapipb.ActorSnapshot) snapshotKey {
	return snapshotKeyFromRef(&ateapipb.ObjectRef{
		Atespace: snapshot.GetMetadata().GetAtespace(),
		Name:     snapshot.GetMetadata().GetName(),
	})
}

type snapshotGCState struct {
	actors    []*ateapipb.Actor
	snapshots []*ateapipb.ActorSnapshot
	tags      []*ateapipb.ActorSnapshotTag
	templates []*atev1alpha1.ActorTemplate
}

// SnapshotGC removes committed ActorSnapshots that are outside their source
// Actor's retention policy and have no Actor or tag references.
type SnapshotGC struct {
	persistence         store.Interface
	actorTemplateLister listersv1alpha1.ActorTemplateLister
	dialer              *AteletDialer
	now                 func() time.Time
	concurrency         int
	deleteSnapshot      func(context.Context, string) error
	operations          metric.Int64Counter
}

func NewSnapshotGC(persistence store.Interface, actorTemplateLister listersv1alpha1.ActorTemplateLister, dialer *AteletDialer, concurrency int) *SnapshotGC {
	if concurrency < 1 {
		concurrency = 1
	}
	gc := &SnapshotGC{
		persistence:         persistence,
		actorTemplateLister: actorTemplateLister,
		dialer:              dialer,
		now:                 time.Now,
		concurrency:         concurrency,
	}
	operations, err := otel.Meter("ateapi").Int64Counter(
		"ate.actor_snapshot.gc.operations",
		metric.WithUnit("{operation}"),
		metric.WithDescription("ActorSnapshot garbage collection operations by outcome."),
	)
	if err != nil {
		slog.Warn("Failed to create ActorSnapshot GC metric", "err", err)
	} else {
		gc.operations = operations
	}
	gc.deleteSnapshot = gc.deleteExternalSnapshot
	return gc
}

func (g *SnapshotGC) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	for {
		if err := g.Collect(ctx); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "ActorSnapshot garbage collection failed", "err", err)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (g *SnapshotGC) Collect(ctx context.Context) error {
	state, err := g.loadState(ctx)
	if err != nil {
		return err
	}
	eligible := eligibleSnapshots(state, g.now())
	if err := g.reconcileDeletionMarks(ctx, state.snapshots, eligible); err != nil {
		return err
	}

	// Reload every root after marking. New tag/clone operations either finish
	// before the mark and appear here, or observe the mark and fail safely.
	state, err = g.loadState(ctx)
	if err != nil {
		return err
	}
	eligible = eligibleSnapshots(state, g.now())

	var group errgroup.Group
	group.SetLimit(g.concurrency)
	for _, snapshot := range state.snapshots {
		snapshot := snapshot
		marked, err := g.persistence.ActorSnapshotDeleting(ctx, snapshot.GetMetadata().GetAtespace(), snapshot.GetMetadata().GetName())
		if err != nil {
			return err
		}
		if !marked {
			continue
		}
		key := snapshotKeyFromSnapshot(snapshot)
		if _, ok := eligible[key]; !ok {
			if err := g.setDeleting(ctx, key, false); err != nil {
				return err
			}
			continue
		}
		group.Go(func() error { return g.delete(ctx, key) })
	}
	return group.Wait()
}

func (g *SnapshotGC) reconcileDeletionMarks(ctx context.Context, snapshots []*ateapipb.ActorSnapshot, eligible map[snapshotKey]struct{}) error {
	for _, snapshot := range snapshots {
		key := snapshotKeyFromSnapshot(snapshot)
		marked, err := g.persistence.ActorSnapshotDeleting(ctx, key.atespace, key.name)
		if err != nil {
			return err
		}
		_, shouldDelete := eligible[key]
		if marked != shouldDelete {
			if err := g.setDeleting(ctx, key, shouldDelete); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *SnapshotGC) setDeleting(ctx context.Context, key snapshotKey, deleting bool) error {
	lock, err := g.persistence.AcquireLock(ctx, "lock:actor-snapshot:"+key.atespace+":"+key.name)
	if errors.Is(err, store.ErrLockConflict) {
		return nil
	}
	if err != nil {
		return err
	}
	defer lock.Close()
	return g.persistence.SetActorSnapshotDeleting(lock.Context(), key.atespace, key.name, deleting)
}

func (g *SnapshotGC) delete(ctx context.Context, key snapshotKey) (retErr error) {
	_, location, err := g.persistence.GetActorSnapshot(ctx, key.atespace, key.name)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	g.recordOperation(ctx, "attempted")
	defer func() {
		if retErr != nil {
			g.recordOperation(ctx, "failed")
		}
	}()
	if err := g.deleteSnapshot(ctx, location); err != nil {
		return fmt.Errorf("delete ActorSnapshot %s/%s data: %w", key.atespace, key.name, err)
	}

	lock, err := g.persistence.AcquireLock(ctx, "lock:actor-snapshot:"+key.atespace+":"+key.name)
	if err != nil {
		return fmt.Errorf("lock ActorSnapshot %s/%s after deleting data: %w", key.atespace, key.name, err)
	}
	defer lock.Close()
	if err := g.persistence.DeleteActorSnapshot(lock.Context(), key.atespace, key.name); err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	g.recordOperation(ctx, "deleted")
	slog.InfoContext(ctx, "Garbage collected ActorSnapshot", "atespace", key.atespace, "name", key.name)
	return nil
}

func (g *SnapshotGC) recordOperation(ctx context.Context, outcome string) {
	if g.operations != nil {
		g.operations.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	}
}

func (g *SnapshotGC) deleteExternalSnapshot(ctx context.Context, location string) error {
	conn, err := g.dialer.DialAny()
	if err != nil {
		return err
	}
	_, err = ateletpb.NewAteomHerderClient(conn).DeleteExternalSnapshot(ctx, &ateletpb.DeleteExternalSnapshotRequest{SnapshotUriPrefix: location})
	return err
}

func (g *SnapshotGC) loadState(ctx context.Context) (snapshotGCState, error) {
	actors, err := g.listActors(ctx)
	if err != nil {
		return snapshotGCState{}, fmt.Errorf("list Actors for snapshot GC: %w", err)
	}
	snapshots, err := g.listSnapshots(ctx)
	if err != nil {
		return snapshotGCState{}, fmt.Errorf("list ActorSnapshots for snapshot GC: %w", err)
	}
	tags, err := g.listTags(ctx)
	if err != nil {
		return snapshotGCState{}, fmt.Errorf("list ActorSnapshotTags for snapshot GC: %w", err)
	}
	templates, err := g.actorTemplateLister.List(labels.Everything())
	if err != nil {
		return snapshotGCState{}, fmt.Errorf("list ActorTemplates for snapshot GC: %w", err)
	}
	return snapshotGCState{actors: actors, snapshots: snapshots, tags: tags, templates: templates}, nil
}

func (g *SnapshotGC) listActors(ctx context.Context) (result []*ateapipb.Actor, err error) {
	for token := ""; ; {
		page, next, err := g.persistence.ListActors(ctx, "", snapshotGCPageSize, token)
		if err != nil {
			return nil, err
		}
		result = append(result, page...)
		if next == "" {
			return result, nil
		}
		token = next
	}
}

func (g *SnapshotGC) listSnapshots(ctx context.Context) (result []*ateapipb.ActorSnapshot, err error) {
	for token := ""; ; {
		page, next, err := g.persistence.ListActorSnapshots(ctx, "", snapshotGCPageSize, token)
		if err != nil {
			return nil, err
		}
		result = append(result, page...)
		if next == "" {
			return result, nil
		}
		token = next
	}
}

func (g *SnapshotGC) listTags(ctx context.Context) (result []*ateapipb.ActorSnapshotTag, err error) {
	for token := ""; ; {
		page, next, err := g.persistence.ListActorSnapshotTags(ctx, "", snapshotGCPageSize, token)
		if err != nil {
			return nil, err
		}
		result = append(result, page...)
		if next == "" {
			return result, nil
		}
		token = next
	}
}

func eligibleSnapshots(state snapshotGCState, now time.Time) map[snapshotKey]struct{} {
	protected := make(map[snapshotKey]struct{})
	for _, actor := range state.actors {
		if actor.GetLatestSnapshot() != nil {
			protected[snapshotKeyFromRef(actor.GetLatestSnapshot())] = struct{}{}
		}
	}
	for _, tag := range state.tags {
		protected[snapshotKeyFromRef(tag.GetSnapshot())] = struct{}{}
	}
	for _, template := range state.templates {
		if template.Status.GoldenSnapshot != "" {
			protected[snapshotKey{atespace: resources.GoldenActorAtespace, name: template.Status.GoldenSnapshot}] = struct{}{}
		}
	}

	liveActors := make(map[string]*ateapipb.Actor, len(state.actors))
	for _, actor := range state.actors {
		liveActors[actor.GetMetadata().GetUid()] = actor
	}
	bySource := make(map[string][]*ateapipb.ActorSnapshot)
	for _, snapshot := range state.snapshots {
		bySource[snapshot.GetSourceActorUid()] = append(bySource[snapshot.GetSourceActorUid()], snapshot)
	}
	for uid, snapshots := range bySource {
		actor, live := liveActors[uid]
		if !live {
			continue
		}
		if actor.GetInProgressSnapshot() != "" {
			for _, snapshot := range snapshots {
				if snapshot.GetSourceActorVersion() == actor.GetInProgressSnapshotSourceActorVersion() {
					protected[snapshotKeyFromSnapshot(snapshot)] = struct{}{}
				}
			}
		}
		policy, ok := retentionPolicy(actor, state.templates)
		if !ok {
			for _, snapshot := range snapshots {
				protected[snapshotKeyFromSnapshot(snapshot)] = struct{}{}
			}
			continue
		}
		sort.Slice(snapshots, func(i, j int) bool {
			iTime := snapshots[i].GetMetadata().GetCreateTime().AsTime()
			jTime := snapshots[j].GetMetadata().GetCreateTime().AsTime()
			if iTime.Equal(jTime) {
				return snapshots[i].GetMetadata().GetName() > snapshots[j].GetMetadata().GetName()
			}
			return iTime.After(jTime)
		})
		for i, snapshot := range snapshots {
			created := snapshot.GetMetadata().GetCreateTime()
			if i < int(policy.minimumCount) || created == nil || now.Sub(created.AsTime()) < policy.minimumAge {
				protected[snapshotKeyFromSnapshot(snapshot)] = struct{}{}
			}
		}
	}

	eligible := make(map[snapshotKey]struct{})
	for _, snapshot := range state.snapshots {
		key := snapshotKeyFromSnapshot(snapshot)
		if _, ok := protected[key]; !ok {
			eligible[key] = struct{}{}
		}
	}
	return eligible
}

type effectiveRetentionPolicy struct {
	minimumCount int32
	minimumAge   time.Duration
}

func retentionPolicy(actor *ateapipb.Actor, templates []*atev1alpha1.ActorTemplate) (effectiveRetentionPolicy, bool) {
	for _, template := range templates {
		if template.Namespace != actor.GetActorTemplateNamespace() || template.Name != actor.GetActorTemplateName() {
			continue
		}
		count := defaultSnapshotMinimumCount
		ageSeconds := defaultSnapshotMinimumAgeSeconds
		if policy := template.Spec.SnapshotsConfig.RetentionPolicy; policy != nil {
			if policy.MinimumCount != nil {
				count = *policy.MinimumCount
			}
			if policy.MinimumAgeSeconds != nil {
				ageSeconds = *policy.MinimumAgeSeconds
			}
		}
		return effectiveRetentionPolicy{minimumCount: count, minimumAge: time.Duration(ageSeconds) * time.Second}, true
	}
	return effectiveRetentionPolicy{}, false
}
