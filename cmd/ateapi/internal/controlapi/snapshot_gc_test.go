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
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

func int32Ptr(v int32) *int32 { return &v }

func TestEligibleSnapshots(t *testing.T) {
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	template := &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "templates", Name: "app"},
		Spec: atev1alpha1.ActorTemplateSpec{SnapshotsConfig: atev1alpha1.SnapshotsConfig{
			RetentionPolicy: &atev1alpha1.SnapshotRetentionPolicy{
				MinimumCount:      int32Ptr(3),
				MinimumAgeSeconds: int32Ptr(86400),
			},
		}},
	}
	actor := &ateapipb.Actor{
		Metadata:                             &ateapipb.ResourceMetadata{Atespace: "team", Name: "actor", Uid: "actor-uid"},
		ActorTemplateNamespace:               "templates",
		ActorTemplateName:                    "app",
		LatestSnapshot:                       &ateapipb.ObjectRef{Atespace: "team", Name: "s5"},
		InProgressSnapshot:                   "gs://bucket/snapshots/in-progress",
		InProgressSnapshotSourceActorVersion: 42,
	}
	snapshot := func(name, uid string, age time.Duration) *ateapipb.ActorSnapshot {
		return &ateapipb.ActorSnapshot{
			Metadata:       &ateapipb.ResourceMetadata{Atespace: "team", Name: name, CreateTime: timestamppb.New(now.Add(-age))},
			SourceActorUid: uid,
		}
	}
	state := snapshotGCState{
		actors: []*ateapipb.Actor{actor, {
			Metadata:       &ateapipb.ResourceMetadata{Atespace: "other", Name: "clone", Uid: "clone-uid"},
			LatestSnapshot: &ateapipb.ObjectRef{Atespace: "team", Name: "deleted-source-root"},
		}},
		snapshots: []*ateapipb.ActorSnapshot{
			snapshot("s1", "actor-uid", 120*time.Hour),
			snapshot("s2", "actor-uid", 96*time.Hour),
			snapshot("s3", "actor-uid", 72*time.Hour),
			snapshot("s4", "actor-uid", 48*time.Hour),
			snapshot("s5", "actor-uid", time.Hour),
			{Metadata: &ateapipb.ResourceMetadata{Atespace: "team", Name: "in-progress", CreateTime: timestamppb.New(now.Add(-120 * time.Hour))}, SourceActorUid: "actor-uid", SourceActorVersion: 42},
			snapshot("deleted-source", "gone", time.Hour),
			snapshot("deleted-source-root", "gone", 120*time.Hour),
			snapshot("tagged", "gone", 120*time.Hour),
			{Metadata: &ateapipb.ResourceMetadata{Atespace: resources.GoldenActorAtespace, Name: "golden", CreateTime: timestamppb.New(now.Add(-120 * time.Hour))}},
		},
		tags: []*ateapipb.ActorSnapshotTag{{Snapshot: &ateapipb.ObjectRef{Atespace: "team", Name: "tagged"}}},
		templates: []*atev1alpha1.ActorTemplate{
			template,
			{Status: atev1alpha1.ActorTemplateStatus{GoldenSnapshot: "golden"}},
		},
	}

	eligible := eligibleSnapshots(state, now)
	want := map[snapshotKey]struct{}{
		{atespace: "team", name: "s1"}:             {},
		{atespace: "team", name: "s2"}:             {},
		{atespace: "team", name: "deleted-source"}: {},
	}
	if len(eligible) != len(want) {
		t.Fatalf("eligible = %v, want %v", eligible, want)
	}
	for key := range want {
		if _, ok := eligible[key]; !ok {
			t.Errorf("eligible missing %v", key)
		}
	}
}

func TestSnapshotGCDeletesDataBeforeMetadataAndRetries(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	gc := NewSnapshotGC(persistence, listersv1alpha1.NewActorTemplateLister(indexer), nil, 1)

	created, err := persistence.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{
		Metadata:       &ateapipb.ResourceMetadata{Atespace: "team", Name: "old"},
		SourceActorUid: "deleted-actor",
	}, "gs://bucket/snapshots/old")
	if err != nil {
		t.Fatalf("CreateActorSnapshot: %v", err)
	}

	deleteErr := errors.New("storage unavailable")
	var deletedLocation string
	gc.deleteSnapshot = func(_ context.Context, location string) error {
		deletedLocation = location
		return deleteErr
	}
	if err := gc.Collect(ctx); !errors.Is(err, deleteErr) {
		t.Fatalf("Collect error = %v, want storage error", err)
	}
	if _, _, err := persistence.GetActorSnapshot(ctx, "team", "old"); err != nil {
		t.Fatalf("metadata removed after failed data deletion: %v", err)
	}
	if marked, err := persistence.ActorSnapshotDeleting(ctx, "team", "old"); err != nil || !marked {
		t.Fatalf("deletion mark = (%v, %v), want true", marked, err)
	}

	gc.deleteSnapshot = func(_ context.Context, location string) error {
		deletedLocation = location
		return nil
	}
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("Collect retry: %v", err)
	}
	if deletedLocation != "gs://bucket/snapshots/old" {
		t.Errorf("deleted location = %q", deletedLocation)
	}
	if _, _, err := persistence.GetActorSnapshot(ctx, "team", "old"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("metadata after successful retry = %v, want ErrNotFound", err)
	}
	if created.GetMetadata().GetName() != "old" {
		t.Fatalf("created snapshot changed unexpectedly: %v", created)
	}
}

func TestSnapshotDeletionMarkBlocksNewReferences(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	if _, err := persistence.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team", Name: "old"},
	}, "gs://bucket/snapshots/old"); err != nil {
		t.Fatalf("CreateActorSnapshot: %v", err)
	}
	if err := persistence.SetActorSnapshotDeleting(ctx, "team", "old", true); err != nil {
		t.Fatalf("SetActorSnapshotDeleting: %v", err)
	}

	service := &Service{persistence: persistence}
	_, err := service.TagActorSnapshot(ctx, &ateapipb.TagActorSnapshotRequest{
		Snapshot: &ateapipb.ActorSnapshotRef{Reference: &ateapipb.ActorSnapshotRef_Snapshot{Snapshot: &ateapipb.ObjectRef{Atespace: "team", Name: "old"}}},
		Tag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team", Name: "too-late"},
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("TagActorSnapshot error = %v, want FailedPrecondition", err)
	}
}
