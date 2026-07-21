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

package ateredis

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func setupTest(t *testing.T) (*miniredis.Miniredis, *Persistence, context.Context) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	// Miniredis runs as a single node, but ClusterClient can work with it
	// if we don't use cluster-specific commands that miniredis doesn't support.
	// Miniredis supports most standard commands.
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{mr.Addr()},
	})
	return mr, &Persistence{rdb: rdb}, context.Background()
}

// testAtespace is the atespace used by tests that create a single actor. Actors
// are atespace-scoped, so a real atespace must always be part of their identity.
const testAtespace = "test-atespace"

// Atomic cmp options to skip individual server-owned ResourceMetadata fields in
// proto diffs. Compose the ones a given assertion needs — e.g. ignore uid and
// timestamps but keep version when the test asserts a specific version.
var (
	ignoreUID        = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "uid")
	ignoreVersion    = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "version")
	ignoreTimestamps = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "create_time", "update_time")
)

func TestGetActor_NotFound(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	_, err := s.GetActor(ctx, testAtespace, "non-existent")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestCreateActor_Success(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "test-template",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
	}

	created, err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// CreateActor returns the stored resource with server-assigned metadata.
	if created.GetMetadata().GetUid() == "" {
		t.Errorf("CreateActor returned empty uid; want server-assigned uid")
	}
	if created.GetMetadata().GetVersion() != 1 {
		t.Errorf("CreateActor returned version %d, want 1", created.GetMetadata().GetVersion())
	}
	if created.GetMetadata().GetCreateTime() == nil || created.GetMetadata().GetUpdateTime() == nil {
		t.Errorf("CreateActor returned unset create/update time")
	}

	// The input must not be mutated.
	if actor.GetMetadata().GetUid() != "" || actor.GetMetadata().GetVersion() != 0 {
		t.Errorf("CreateActor must not mutate its input, got metadata %v", actor.GetMetadata())
	}

	// The returned resource is exactly what GetActor reads back.
	got, err := s.GetActor(ctx, actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName())
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("CreateActor return does not match stored state (-created +got):\n%s", diff)
	}

	// Structurally: the input fields plus server-assigned metadata.
	expected := proto.Clone(actor).(*ateapipb.Actor)
	expected.Metadata.Version = 1
	if diff := cmp.Diff(expected, created, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("CreateActor returned unexpected actor (-want +got):\n%s", diff)
	}
}

func TestCreateActor_AlreadyExists(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "test-template",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
	}

	_, err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	_, err = s.CreateActor(ctx, actor)
	if err == nil {
		t.Errorf("expected error creating existing actor, got nil")
	}
}

func TestUpdateActor_Success(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "test-template",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
	}

	created, err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	toUpdate := proto.Clone(created).(*ateapipb.Actor)
	toUpdate.Status = ateapipb.Actor_STATUS_RUNNING
	updated, err := s.UpdateActor(ctx, toUpdate, created.GetMetadata().GetVersion())
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	// UpdateActor returns the stored resource: the mutation applied and version
	// advanced, with uid and create_time preserved from creation.
	if updated.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		t.Errorf("UpdateActor returned status %v, want RUNNING", updated.GetStatus())
	}
	if updated.GetMetadata().GetVersion() != 2 {
		t.Errorf("UpdateActor returned version %d, want 2", updated.GetMetadata().GetVersion())
	}
	if updated.GetMetadata().GetUid() != created.GetMetadata().GetUid() {
		t.Errorf("uid changed on update: got %q, want %q", updated.GetMetadata().GetUid(), created.GetMetadata().GetUid())
	}
	if !updated.GetMetadata().GetCreateTime().AsTime().Equal(created.GetMetadata().GetCreateTime().AsTime()) {
		t.Errorf("create_time changed on update: got %v, want %v", updated.GetMetadata().GetCreateTime().AsTime(), created.GetMetadata().GetCreateTime().AsTime())
	}

	// The input must not be mutated.
	if toUpdate.GetMetadata().GetVersion() != created.GetMetadata().GetVersion() {
		t.Errorf("UpdateActor must not mutate its input; version changed to %d", toUpdate.GetMetadata().GetVersion())
	}

	// The returned resource is exactly what GetActor reads back.
	got, err := s.GetActor(ctx, actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName())
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if diff := cmp.Diff(updated, got, protocmp.Transform()); diff != "" {
		t.Errorf("UpdateActor return does not match stored state (-updated +got):\n%s", diff)
	}
}

func TestUpdateActor_Conflict(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "test-template",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
	}

	_, err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Fetch instance 1
	actor1, err := s.GetActor(ctx, actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName())
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}

	// Fetch instance 2 (stale after actor1 updates)
	actor2, err := s.GetActor(ctx, actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName())
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}

	// Update instance 1
	actor1.Status = ateapipb.Actor_STATUS_RUNNING
	_, err = s.UpdateActor(ctx, actor1, actor1.GetMetadata().GetVersion())
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	// Try to update instance 2 (which has stale version)
	actor2.Status = ateapipb.Actor_STATUS_SUSPENDED
	_, err = s.UpdateActor(ctx, actor2, actor2.GetMetadata().GetVersion())
	if !errors.Is(err, store.ErrPersistenceRetry) {
		t.Errorf("expected ErrPersistenceRetry, got %v", err)
	}
}

func TestGetWorker_NotFound(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	_, err := s.GetWorker(ctx, "default", "pool-1", "non-existent")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestCreateWorker_Success(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	watch, err := s.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}

	worker := &ateapipb.Worker{
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
	}

	err = s.CreateWorker(ctx, worker)
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	got, err := s.GetWorker(ctx, "default", "pool-1", "pod-1")
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}

	if got.Version != 1 {
		t.Errorf("expected version 1, got %d", got.Version)
	}

	worker.Version = 1
	if diff := cmp.Diff(worker, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetWorker returned unexpected worker (-want +got):\n%s", diff)
	}

	event := receiveEvent(t, watch.Events)
	if event.Type != store.WorkerEventCreated {
		t.Errorf("expected WorkerEventCreated, got %v", event.Type)
	}
	if diff := cmp.Diff(worker, event.Worker, protocmp.Transform()); diff != "" {
		t.Errorf("created event worker mismatch (-want +got):\n%s", diff)
	}
}

func TestUpdateWorker_Success(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	worker := &ateapipb.Worker{
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
	}

	if err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	// Subscribe after create so the create event doesn't pollute the channel.
	watch, err := s.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}

	worker.Assignment = &ateapipb.Assignment{
		ActorTemplate: &ateapipb.KubeNamespacedObjectRef{
			Namespace: "default",
			Name:      "test-template",
		},
		Actor: &ateapipb.ObjectRef{
			Name: "session-1",
		},
	}

	if err := s.UpdateWorker(ctx, worker, 1); err != nil {
		t.Fatalf("UpdateWorker failed: %v", err)
	}

	got, err := s.GetWorker(ctx, "default", "pool-1", "pod-1")
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}

	if got.Version != 2 {
		t.Errorf("expected version 2, got %d", got.Version)
	}

	worker.Version = 2
	if diff := cmp.Diff(worker, got, protocmp.Transform()); diff != "" {
		t.Errorf("UpdateWorker yielded unexpected state in DB (-want +got):\n%s", diff)
	}

	event := receiveEvent(t, watch.Events)
	if event.Type != store.WorkerEventUpdated {
		t.Errorf("expected WorkerEventUpdated, got %v", event.Type)
	}
	if diff := cmp.Diff(worker, event.Worker, protocmp.Transform()); diff != "" {
		t.Errorf("updated event worker mismatch (-want +got):\n%s", diff)
	}
}

func TestDeleteWorker(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	worker := &ateapipb.Worker{
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
	}

	if err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	// Subscribe after create so the create event doesn't pollute the channel.
	watch, err := s.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}

	if err := s.DeleteWorker(ctx, "default", "pool-1", "pod-1"); err != nil {
		t.Fatalf("DeleteWorker failed: %v", err)
	}

	_, err = s.GetWorker(ctx, "default", "pool-1", "pod-1")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}

	event := receiveEvent(t, watch.Events)
	if event.Type != store.WorkerEventDeleted {
		t.Errorf("expected WorkerEventDeleted, got %v", event.Type)
	}
	want := &ateapipb.Worker{WorkerNamespace: "default", WorkerPod: "pod-1"}
	if diff := cmp.Diff(want, event.Worker, protocmp.Transform()); diff != "" {
		t.Errorf("deleted event worker mismatch (-want +got):\n%s", diff)
	}
}

func TestDeleteActor(t *testing.T) {
	tests := []struct {
		name    string
		status  ateapipb.Actor_Status
		wantErr error
	}{
		{name: "suspended", status: ateapipb.Actor_STATUS_SUSPENDED},
		{name: "crashed", status: ateapipb.Actor_STATUS_CRASHED},
		{name: "running", status: ateapipb.Actor_STATUS_RUNNING, wantErr: store.ErrFailedPrecondition},
		{name: "paused", status: ateapipb.Actor_STATUS_PAUSED, wantErr: store.ErrFailedPrecondition},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr, s, ctx := setupTest(t)
			defer mr.Close()

			actor := &ateapipb.Actor{
				Metadata:               &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
				ActorTemplateNamespace: "default",
				ActorTemplateName:      "test-template",
				Status:                 tt.status,
			}

			if _, err := s.CreateActor(ctx, actor); err != nil {
				t.Fatalf("CreateActor failed: %v", err)
			}

			deleted, err := s.DeleteActor(ctx, testAtespace, "session-1")
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("DeleteActor: expected %v, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("DeleteActor failed: %v", err)
			}
			// DeleteActor returns the deleted resource.
			if got := deleted.GetMetadata().GetName(); got != "session-1" {
				t.Errorf("deleted actor name = %q, want session-1", got)
			}

			if _, err := s.GetActor(ctx, testAtespace, "session-1"); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("expected ErrNotFound after delete, got %v", err)
			}
		})
	}
}

func TestDeleteActor_NotFound(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	_, err := s.DeleteActor(ctx, testAtespace, "non-existent")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound deleting non-existent actor, got %v", err)
	}
}

func TestListWorkers(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	worker1 := &ateapipb.Worker{
		WorkerNamespace: "ns1",
		WorkerPool:      "pool1",
		WorkerPod:       "pod1",
	}
	worker2 := &ateapipb.Worker{
		WorkerNamespace: "ns1",
		WorkerPool:      "pool1",
		WorkerPod:       "pod2",
	}
	if err := s.CreateWorker(ctx, worker1); err != nil {
		t.Fatalf("failed to create worker1: %v", err)
	}
	if err := s.CreateWorker(ctx, worker2); err != nil {
		t.Fatalf("failed to create worker2: %v", err)
	}

	workers, _, err := s.ListWorkers(ctx, 1000, "")
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}

	if len(workers) != 2 {
		t.Errorf("expected 2 workers, got %d", len(workers))
	}

	found1 := false
	found2 := false
	for _, w := range workers {
		if w.GetWorkerPod() == "pod1" {
			found1 = true
		}
		if w.GetWorkerPod() == "pod2" {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Errorf("did not find all workers: found1=%t, found2=%t", found1, found2)
	}
}

func TestListActors(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	actor1 := &ateapipb.Actor{

		Metadata:               &ateapipb.ResourceMetadata{Name: "id1", Atespace: testAtespace},
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		LatestSnapshotInfo: &ateapipb.SnapshotInfo{
			Data: &ateapipb.SnapshotInfo_External{
				External: &ateapipb.ExternalSnapshotInfo{
					SnapshotUriPrefix: "gs://b1/f1",
				},
			},
		},
	}
	actor2 := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: "id2", Atespace: testAtespace},
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		LatestSnapshotInfo: &ateapipb.SnapshotInfo{
			Data: &ateapipb.SnapshotInfo_External{
				External: &ateapipb.ExternalSnapshotInfo{
					SnapshotUriPrefix: "gs://b1/f2",
				},
			},
		},
	}

	if _, err := s.CreateActor(ctx, actor1); err != nil {
		t.Fatalf("failed to create actor1: %v", err)
	}
	if _, err := s.CreateActor(ctx, actor2); err != nil {
		t.Fatalf("failed to create actor2: %v", err)
	}

	actors, _, err := s.ListActors(ctx, "", 1000, "")
	if err != nil {
		t.Fatalf("ListActors failed: %v", err)
	}

	if len(actors) != 2 {
		t.Errorf("expected 2 actors, got %d", len(actors))
	}

	found1 := false
	found2 := false
	for _, a := range actors {
		if a.GetMetadata().GetName() == "id1" {
			found1 = true
		}
		if a.GetMetadata().GetName() == "id2" {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Errorf("did not find all actors: found1=%t, found2=%t", found1, found2)
	}
}

func TestUpdateWorker_Conflict(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	worker := &ateapipb.Worker{
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
	}

	err := s.CreateWorker(ctx, worker)
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	// Fetch instance 1
	worker1, err := s.GetWorker(ctx, "default", "pool-1", "pod-1")
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}

	// Fetch instance 2
	worker2, err := s.GetWorker(ctx, "default", "pool-1", "pod-1")
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}

	// Update instance 1
	worker1.Assignment = &ateapipb.Assignment{Actor: &ateapipb.ObjectRef{Name: "session-1"}}
	err = s.UpdateWorker(ctx, worker1, worker1.Version)
	if err != nil {
		t.Fatalf("UpdateWorker failed: %v", err)
	}

	// Try to update instance 2
	worker2.Assignment = &ateapipb.Assignment{Actor: &ateapipb.ObjectRef{Name: "session-2"}}
	err = s.UpdateWorker(ctx, worker2, worker2.Version)
	if !errors.Is(err, store.ErrPersistenceRetry) {
		t.Errorf("expected ErrPersistenceRetry, got %v", err)
	}
}

func TestCreateWorker_AlreadyExists(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	worker := &ateapipb.Worker{
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
	}

	err := s.CreateWorker(ctx, worker)
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	err = s.CreateWorker(ctx, worker)
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestListWorkers_Empty(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	workers, _, err := s.ListWorkers(ctx, 1000, "")
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}

	if len(workers) != 0 {
		t.Errorf("expected 0 workers, got %d", len(workers))
	}
}

func TestListWorkers_Pagination(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	for i := 0; i < 5; i++ {
		worker := &ateapipb.Worker{
			WorkerNamespace: "ns1",
			WorkerPool:      "pool1",
			WorkerPod:       fmt.Sprintf("pod%d", i),
		}
		if err := s.CreateWorker(ctx, worker); err != nil {
			t.Fatalf("failed to create worker %d: %v", i, err)
		}
	}

	var allWorkers []*ateapipb.Worker
	pageToken := ""

	for {
		workers, nextToken, err := s.ListWorkers(ctx, 2, pageToken)
		if err != nil {
			t.Fatalf("ListWorkers failed: %v", err)
		}

		allWorkers = append(allWorkers, workers...)
		pageToken = nextToken
		if pageToken == "" {
			break
		}
	}

	if len(allWorkers) != 5 {
		t.Fatalf("expected 5 workers total, got %d", len(allWorkers))
	}

	seen := make(map[string]bool)
	for _, w := range allWorkers {
		if seen[w.GetWorkerPod()] {
			t.Errorf("duplicate worker found in paginated results: %s", w.GetWorkerPod())
		}
		seen[w.GetWorkerPod()] = true
	}
}

func TestListAtespaces_Pagination(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	for i := 0; i < 5; i++ {
		if _, err := s.CreateAtespace(ctx, newTestAtespace(fmt.Sprintf("team-%d", i))); err != nil {
			t.Fatalf("failed to create atespace %d: %v", i, err)
		}
	}

	var allAtespaces []*ateapipb.Atespace
	pageToken := ""

	for {
		atespaces, nextToken, err := s.ListAtespaces(ctx, 2, pageToken)
		if err != nil {
			t.Fatalf("ListAtespaces failed: %v", err)
		}

		allAtespaces = append(allAtespaces, atespaces...)
		pageToken = nextToken
		if pageToken == "" {
			break
		}
	}

	if len(allAtespaces) != 5 {
		t.Fatalf("expected 5 atespaces total, got %d", len(allAtespaces))
	}

	seen := make(map[string]bool)
	for _, a := range allAtespaces {
		if seen[a.GetMetadata().GetName()] {
			t.Errorf("duplicate atespace found in paginated results: %s", a.GetMetadata().GetName())
		}
		seen[a.GetMetadata().GetName()] = true
	}
}

func TestListActors_Empty(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	actors, _, err := s.ListActors(ctx, "", 1000, "")
	if err != nil {
		t.Fatalf("ListActors failed: %v", err)
	}

	if len(actors) != 0 {
		t.Errorf("expected 0 actors, got %d", len(actors))
	}
}

func TestListActors_Pagination(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	for i := 0; i < 5; i++ {
		actor := &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Name: fmt.Sprintf("name%d", i), Atespace: testAtespace},
			ActorTemplateNamespace: "ns1",
			ActorTemplateName:      "tmpl1",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		}
		if _, err := s.CreateActor(ctx, actor); err != nil {
			t.Fatalf("failed to create actor %d: %v", i, err)
		}
	}

	var allActors []*ateapipb.Actor
	pageToken := ""

	for {
		actors, nextToken, err := s.ListActors(ctx, "", 2, pageToken)
		if err != nil {
			t.Fatalf("ListActors failed: %v", err)
		}

		allActors = append(allActors, actors...)
		pageToken = nextToken
		if pageToken == "" {
			break
		}
	}

	if len(allActors) != 5 {
		t.Fatalf("expected 5 actors total, got %d", len(allActors))
	}

	seen := make(map[string]bool)
	for _, a := range allActors {
		if seen[a.GetMetadata().GetName()] {
			t.Errorf("duplicate actor found in paginated results: %s", a.GetMetadata().GetName())
		}
		seen[a.GetMetadata().GetName()] = true
	}
}

func TestAcquireLock_Success(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	key := "test-lock"
	value := "token-1"
	wrongValue := "token-2"
	newValue := "token-3"
	ttl := 10 * time.Second

	// 1. Acquire lock
	acquired, err := s.AcquireLock(ctx, key, value, ttl)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	if !acquired {
		t.Errorf("expected lock to be acquired")
	}

	// 2. Try to release with WRONG value
	err = s.ReleaseLock(ctx, key, wrongValue)
	if err != nil {
		t.Fatalf("ReleaseLock failed: %v", err)
	}

	// Verify it is STILL THERE by trying to acquire it again
	acquired, err = s.AcquireLock(ctx, key, newValue, ttl)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	if acquired {
		t.Errorf("expected lock to still be held by token-1, but token-3 successfully acquired it!")
	}

	// 3. Try to release with CORRECT value
	err = s.ReleaseLock(ctx, key, value)
	if err != nil {
		t.Fatalf("ReleaseLock failed: %v", err)
	}

	// Verify it is GONE by trying to acquire it again!
	acquired, err = s.AcquireLock(ctx, key, newValue, ttl)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	if !acquired {
		t.Errorf("expected lock to be free, but it could not be acquired!")
	}
}

func TestAcquireLock_Conflict(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	key := "test-lock"
	value1 := "token-1"
	value2 := "token-2"
	ttl := 10 * time.Second

	acquired, err := s.AcquireLock(ctx, key, value1, ttl)
	if err != nil {
		t.Fatalf("first AcquireLock failed: %v", err)
	}
	if !acquired {
		t.Fatalf("expected first lock to be acquired")
	}

	acquired, err = s.AcquireLock(ctx, key, value2, ttl)
	if err != nil {
		t.Fatalf("second AcquireLock failed: %v", err)
	}
	if acquired {
		t.Errorf("expected second lock to fail (conflict)")
	}
}

func TestReleaseLock_Success(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	key := "test-lock"
	value := "token-1"
	ttl := 10 * time.Second

	s.AcquireLock(ctx, key, value, ttl)

	err := s.ReleaseLock(ctx, key, value)
	if err != nil {
		t.Fatalf("ReleaseLock failed: %v", err)
	}

	// Verify it's gone
	if mr.Exists(key) {
		t.Errorf("expected lock to be deleted")
	}
}

func TestReleaseLock_Unsafe(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	key := "test-lock"
	value1 := "token-1"
	value2 := "token-2"
	value3 := "token-3"
	ttl := 10 * time.Second

	s.AcquireLock(ctx, key, value1, ttl)

	// Try to release with WRONG token
	err := s.ReleaseLock(ctx, key, value2)
	if err != nil {
		t.Fatalf("ReleaseLock failed: %v", err)
	}

	// Verify it is STILL THERE by trying to acquire it again!
	acquired, err := s.AcquireLock(ctx, key, value3, ttl)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	if acquired {
		t.Errorf("expected lock to still be held by token-1, but token-3 successfully acquired it!")
	}
}

func TestAcquireLock_TTLExpiration(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	key := "test-lock"
	value1 := "token-1"
	value2 := "token-2"
	ttl := 5 * time.Second

	// 1. Acquire lock
	acquired, err := s.AcquireLock(ctx, key, value1, ttl)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	if !acquired {
		t.Fatalf("expected lock to be acquired")
	}

	// 2. Fast-forward time past TTL
	mr.FastForward(6 * time.Second)

	// 3. Try to acquire again with different token
	acquired, err = s.AcquireLock(ctx, key, value2, ttl)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	if !acquired {
		t.Errorf("expected lock to be acquired by token-2 after TTL expiration")
	}
}

func receiveEvent(t *testing.T, ch <-chan store.WorkerEvent) store.WorkerEvent {
	t.Helper()
	select {
	case event, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker event")
		return store.WorkerEvent{} // unreachable
	}
}

func TestAcquireLock_NonReentry(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	key := "test-lock"
	value := "token-1"
	ttl := 10 * time.Second

	// 1. Acquire lock first time
	acquired, err := s.AcquireLock(ctx, key, value, ttl)
	if err != nil {
		t.Fatalf("first AcquireLock failed: %v", err)
	}
	if !acquired {
		t.Fatalf("expected first lock to be acquired")
	}

	// 2. Try to acquire lock again with SAME token
	acquired, err = s.AcquireLock(ctx, key, value, ttl)
	if err != nil {
		t.Fatalf("second AcquireLock failed: %v", err)
	}
	if acquired {
		t.Errorf("expected second lock acquisition to fail (non-reentrant)")
	}
}

func TestListActors_ScopedByAtespace(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	mkActor := func(atespace, name string) *ateapipb.Actor {
		return &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Name: name, Atespace: atespace},
			ActorTemplateNamespace: "ns1",
			ActorTemplateName:      "tmpl1",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		}
	}
	for _, a := range []*ateapipb.Actor{
		mkActor("team-a", "a1"),
		mkActor("team-a", "a2"),
		mkActor("team-b", "b1"),
	} {
		if _, err := s.CreateActor(ctx, a); err != nil {
			t.Fatalf("CreateActor(%s/%s) failed: %v", a.GetMetadata().GetAtespace(), a.GetMetadata().GetName(), err)
		}
	}

	// List is scoped to one atespace.
	teamA, _, err := s.ListActors(ctx, "team-a", 1000, "")
	if err != nil {
		t.Fatalf("ListActors(team-a) failed: %v", err)
	}
	if got := actorNameSet(teamA); !got["a1"] || !got["a2"] || got["b1"] || len(got) != 2 {
		t.Errorf("ListActors(team-a) = %v, want exactly {a1, a2}", got)
	}

	teamB, _, err := s.ListActors(ctx, "team-b", 1000, "")
	if err != nil {
		t.Fatalf("ListActors(team-b) failed: %v", err)
	}
	if got := actorNameSet(teamB); !got["b1"] || got["a1"] || len(got) != 1 {
		t.Errorf("ListActors(team-b) = %v, want exactly {b1}", got)
	}

	// An empty atespace lists across all atespaces (the admin/dev `-A` view).
	all, _, err := s.ListActors(ctx, "", 1000, "")
	if err != nil {
		t.Fatalf("ListActors(all) failed: %v", err)
	}
	if got := actorNameSet(all); !got["a1"] || !got["a2"] || !got["b1"] || len(got) != 3 {
		t.Errorf("ListActors(all) = %v, want exactly {a1, a2, b1}", got)
	}

	// Get is scoped too: right atespace hits, wrong/empty atespace misses.
	if _, err := s.GetActor(ctx, "team-a", "a1"); err != nil {
		t.Errorf("GetActor(team-a, a1) failed: %v", err)
	}
	if _, err := s.GetActor(ctx, "team-b", "a1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetActor(team-b, a1) = %v, want ErrNotFound", err)
	}
	if _, err := s.GetActor(ctx, "", "a1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetActor(empty, a1) = %v, want ErrNotFound", err)
	}
}

func actorNameSet(actors []*ateapipb.Actor) map[string]bool {
	set := make(map[string]bool, len(actors))
	for _, a := range actors {
		set[a.GetMetadata().GetName()] = true
	}
	return set
}

func newTestAtespace(name string) *ateapipb.Atespace {
	return &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}}
}

func TestCreateAtespace_Success(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	want := newTestAtespace("team-a")
	created, err := s.CreateAtespace(ctx, want)
	if err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}

	// CreateAtespace returns the stored resource with server-assigned metadata.
	if created.GetMetadata().GetUid() == "" {
		t.Errorf("CreateAtespace returned empty uid; want server-assigned uid")
	}
	if created.GetMetadata().GetVersion() != 1 {
		t.Errorf("CreateAtespace returned version %d, want 1", created.GetMetadata().GetVersion())
	}

	// The returned resource is exactly what GetAtespace reads back.
	got, err := s.GetAtespace(ctx, "team-a")
	if err != nil {
		t.Fatalf("GetAtespace failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("CreateAtespace return does not match stored state (-created +got):\n%s", diff)
	}

	// want is the pre-create input; the server stamps uid, version, and timestamps.
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreUID, ignoreTimestamps, ignoreVersion); diff != "" {
		t.Errorf("CreateAtespace returned unexpected atespace (-want +got):\n%s", diff)
	}
}

func TestCreateAtespace_AlreadyExists(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("first CreateAtespace failed: %v", err)
	}
	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestGetAtespace_NotFound(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if _, err := s.GetAtespace(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestAtespaceExists(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if ok, err := s.AtespaceExists(ctx, "team-a"); err != nil || ok {
		t.Fatalf("AtespaceExists before create = (%v, %v), want (false, nil)", ok, err)
	}
	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if ok, err := s.AtespaceExists(ctx, "team-a"); err != nil || !ok {
		t.Fatalf("AtespaceExists after create = (%v, %v), want (true, nil)", ok, err)
	}
}

func TestListAtespaces(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	names := []string{"team-a", "team-b", "team-c"}
	for _, n := range names {
		if _, err := s.CreateAtespace(ctx, newTestAtespace(n)); err != nil {
			t.Fatalf("CreateAtespace(%s) failed: %v", n, err)
		}
	}
	got, _, err := s.ListAtespaces(ctx, 1000, "")
	if err != nil {
		t.Fatalf("ListAtespaces failed: %v", err)
	}
	if len(got) != len(names) {
		t.Fatalf("ListAtespaces returned %d atespaces, want %d", len(got), len(names))
	}
	gotNames := map[string]bool{}
	for _, a := range got {
		gotNames[a.GetMetadata().GetName()] = true
	}
	for _, n := range names {
		if !gotNames[n] {
			t.Errorf("ListAtespaces missing %q; got %v", n, gotNames)
		}
	}
}

func TestListAtespaces_Empty(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	got, _, err := s.ListAtespaces(ctx, 1000, "")
	if err != nil {
		t.Fatalf("ListAtespaces failed: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("ListAtespaces on empty store = %v, want empty", got)
	}
}

func TestDeleteAtespace_Empty(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	deleted, err := s.DeleteAtespace(ctx, "team-a")
	if err != nil {
		t.Fatalf("DeleteAtespace failed: %v", err)
	}
	// DeleteAtespace returns the deleted resource.
	if got := deleted.GetMetadata().GetName(); got != "team-a" {
		t.Errorf("deleted atespace name = %q, want team-a", got)
	}
	if _, err := s.GetAtespace(ctx, "team-a"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("after delete, GetAtespace = %v, want ErrNotFound", err)
	}
}

func TestDeleteAtespace_NotFound(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if _, err := s.DeleteAtespace(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestDeleteAtespace_NonEmpty_Rejected(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if _, err := s.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: "id1", Atespace: "team-a"}, Status: ateapipb.Actor_STATUS_SUSPENDED}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := s.DeleteAtespace(ctx, "team-a"); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("DeleteAtespace on non-empty = %v, want ErrFailedPrecondition", err)
	}
	// The atespace must survive a rejected delete.
	if _, err := s.GetAtespace(ctx, "team-a"); err != nil {
		t.Errorf("atespace should still exist after rejected delete, got %v", err)
	}
}

func TestDeleteAtespace_EmptyAfterActorsRemoved(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if _, err := s.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: "id1", Atespace: "team-a"}, Status: ateapipb.Actor_STATUS_SUSPENDED}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := s.DeleteAtespace(ctx, "team-a"); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("expected rejection while non-empty, got %v", err)
	}
	if _, err := s.DeleteActor(ctx, "team-a", "id1"); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	if _, err := s.DeleteAtespace(ctx, "team-a"); err != nil {
		t.Errorf("DeleteAtespace after actor removed = %v, want nil (re-scan should find it empty)", err)
	}
}

func TestDeleteAtespace_EmptyWhileOtherAtespaceNonEmpty(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace(team-a) failed: %v", err)
	}
	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-b")); err != nil {
		t.Fatalf("CreateAtespace(team-b) failed: %v", err)
	}
	// Actor lives ONLY in team-b.
	if _, err := s.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: "id1", Atespace: "team-b"}, Status: ateapipb.Actor_STATUS_SUSPENDED}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// team-a is empty → delete must succeed.
	if _, err := s.DeleteAtespace(ctx, "team-a"); err != nil {
		t.Errorf("DeleteAtespace(team-a, empty) = %v, want nil (must not be blocked by team-b's actor)", err)
	}
	if _, err := s.GetAtespace(ctx, "team-a"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("after delete, GetAtespace(team-a) = %v, want ErrNotFound", err)
	}
	// team-b is still non-empty → still rejected.
	if _, err := s.DeleteAtespace(ctx, "team-b"); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("DeleteAtespace(team-b, non-empty) = %v, want ErrFailedPrecondition", err)
	}
}

// concurrentMasterClient fakes a cluster with several masters. Like the real
// ClusterClient.ForEachMaster, it invokes the callback concurrently, one
// goroutine per master.
type concurrentMasterClient struct {
	redisClient
	masters []*redis.Client
}

func (c *concurrentMasterClient) ForEachMaster(ctx context.Context, fn func(ctx context.Context, client *redis.Client) error) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	for _, master := range c.masters {
		wg.Add(1)
		go func(master *redis.Client) {
			defer wg.Done()
			if err := fn(ctx, master); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(master)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// TestGetSortedMasters_ConcurrentCallbacks guards against dropping a shard
// when ForEachMaster's concurrent callbacks append to the shared slice: a
// dropped master makes ListActors silently skip every actor on that shard.
// Run with -race; the pre-fix unsynchronized append fails here.
func TestGetSortedMasters_ConcurrentCallbacks(t *testing.T) {
	const numMasters = 8
	fake := &concurrentMasterClient{}
	want := make([]string, 0, numMasters)
	for i := range numMasters {
		addr := fmt.Sprintf("shard-%d:6379", i)
		// Never connected to: getSortedMasters only reads Options().Addr.
		fake.masters = append(fake.masters, redis.NewClient(&redis.Options{Addr: addr}))
		want = append(want, addr)
	}
	sort.Strings(want)
	s := &Persistence{rdb: fake}

	for range 100 {
		masters, err := s.getSortedMasters(context.Background())
		if err != nil {
			t.Fatalf("getSortedMasters failed: %v", err)
		}
		got := make([]string, 0, len(masters))
		for _, m := range masters {
			got = append(got, m.Options().Addr)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("getSortedMasters returned wrong masters (-want +got):\n%s", diff)
		}
	}
}

// TestListActors_MultiMaster_Pagination verifies that pagination across multiple
// Redis master shards collects items from every shard without skipping or duplicating
// shards when page boundaries align with shard boundaries.
func TestListActors_MultiMaster_Pagination(t *testing.T) {
	ctx := context.Background()
	const numShards = 3

	type shard struct {
		client        *redis.Client
		clusterClient *redis.ClusterClient
	}
	var shards []shard
	for i := 0; i < numShards; i++ {
		mr, err := miniredis.Run()
		if err != nil {
			t.Fatalf("failed to start miniredis %d: %v", i, err)
		}
		defer mr.Close()

		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		clusterClient := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{mr.Addr()}})
		defer client.Close()
		defer clusterClient.Close()

		shards = append(shards, shard{
			client:        client,
			clusterClient: clusterClient,
		})
	}

	sort.Slice(shards, func(i, j int) bool {
		return shards[i].client.Options().Addr < shards[j].client.Options().Addr
	})

	var clients []*redis.Client
	for shardIdx, sh := range shards {
		clients = append(clients, sh.client)
		tempS := &Persistence{rdb: sh.clusterClient}
		for itemIdx := 0; itemIdx < 3; itemIdx++ {
			actor := &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{
					Name:     fmt.Sprintf("actor-shard%d-item%d", shardIdx, itemIdx),
					Atespace: testAtespace,
				},
				ActorTemplateNamespace: "default",
				ActorTemplateName:      "test-template",
				Status:                 ateapipb.Actor_STATUS_SUSPENDED,
			}
			if _, err := tempS.CreateActor(ctx, actor); err != nil {
				t.Fatalf("failed to seed actor: %v", err)
			}
		}
	}

	fake := &concurrentMasterClient{
		redisClient: shards[0].clusterClient,
		masters:     clients,
	}
	s := &Persistence{rdb: fake}

	var allActors []*ateapipb.Actor
	pageToken := ""
	for {
		actors, nextToken, err := s.ListActors(ctx, testAtespace, 2, pageToken)
		if err != nil {
			t.Fatalf("ListActors failed: %v", err)
		}
		allActors = append(allActors, actors...)
		if nextToken == "" {
			break
		}
		pageToken = nextToken
	}

	if len(allActors) != 9 {
		t.Fatalf("expected 9 total actors across %d shards, got %d", numShards, len(allActors))
	}

	seen := make(map[string]bool)
	for _, a := range allActors {
		if seen[a.GetMetadata().GetName()] {
			t.Errorf("duplicate actor found in paginated results: %s", a.GetMetadata().GetName())
		}
		seen[a.GetMetadata().GetName()] = true
	}
}

// newMultiMasterStore returns a Persistence whose master iteration spans
// numShards independent miniredis instances, plus a per-shard Persistence for
// seeding data onto a specific shard.
func newMultiMasterStore(t *testing.T, numShards int) (*Persistence, []*Persistence) {
	t.Helper()
	type shard struct {
		client        *redis.Client
		clusterClient *redis.ClusterClient
	}
	var shards []shard
	for i := 0; i < numShards; i++ {
		mr, err := miniredis.Run()
		if err != nil {
			t.Fatalf("failed to start miniredis %d: %v", i, err)
		}
		t.Cleanup(mr.Close)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		clusterClient := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{mr.Addr()}})
		t.Cleanup(func() { client.Close() })
		t.Cleanup(func() { clusterClient.Close() })
		shards = append(shards, shard{client: client, clusterClient: clusterClient})
	}
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].client.Options().Addr < shards[j].client.Options().Addr
	})
	var clients []*redis.Client
	var perShard []*Persistence
	for _, sh := range shards {
		clients = append(clients, sh.client)
		perShard = append(perShard, &Persistence{rdb: sh.clusterClient})
	}
	fake := &concurrentMasterClient{redisClient: shards[0].clusterClient, masters: clients}
	return &Persistence{rdb: fake}, perShard
}

// TestListWorkers_MultiMaster_Pagination mirrors
// TestListActors_MultiMaster_Pagination for ListWorkers, sweeping page sizes
// so page boundaries both do and do not align with shard boundaries (the
// aligned case is the #425 shard-skip regression).
func TestListWorkers_MultiMaster_Pagination(t *testing.T) {
	ctx := context.Background()
	const numShards = 3
	for _, pageSize := range []int32{1, 2, 3, 4} {
		t.Run(fmt.Sprintf("pageSize=%d", pageSize), func(t *testing.T) {
			s, perShard := newMultiMasterStore(t, numShards)
			for shardIdx, ps := range perShard {
				for itemIdx := 0; itemIdx < 3; itemIdx++ {
					worker := &ateapipb.Worker{
						WorkerNamespace: "ns",
						WorkerPool:      "pool",
						WorkerPod:       fmt.Sprintf("pod-shard%d-item%d", shardIdx, itemIdx),
					}
					if err := ps.CreateWorker(ctx, worker); err != nil {
						t.Fatalf("failed to seed worker: %v", err)
					}
				}
			}

			seen := make(map[string]bool)
			pageToken := ""
			for {
				workers, next, err := s.ListWorkers(ctx, pageSize, pageToken)
				if err != nil {
					t.Fatalf("ListWorkers: %v", err)
				}
				for _, w := range workers {
					if seen[w.GetWorkerPod()] {
						t.Errorf("duplicate worker in paginated results: %s", w.GetWorkerPod())
					}
					seen[w.GetWorkerPod()] = true
				}
				if next == "" {
					break
				}
				pageToken = next
			}
			if len(seen) != numShards*3 {
				t.Fatalf("expected %d workers across %d shards, got %d", numShards*3, numShards, len(seen))
			}
		})
	}
}

// TestListAtespaces_MultiMaster_Pagination mirrors
// TestListWorkers_MultiMaster_Pagination for ListAtespaces.
func TestListAtespaces_MultiMaster_Pagination(t *testing.T) {
	ctx := context.Background()
	const numShards = 3
	for _, pageSize := range []int32{1, 2, 3, 4} {
		t.Run(fmt.Sprintf("pageSize=%d", pageSize), func(t *testing.T) {
			s, perShard := newMultiMasterStore(t, numShards)
			for shardIdx, ps := range perShard {
				for itemIdx := 0; itemIdx < 3; itemIdx++ {
					atespace := &ateapipb.Atespace{
						Metadata: &ateapipb.ResourceMetadata{
							Name: fmt.Sprintf("space-shard%d-item%d", shardIdx, itemIdx),
						},
					}
					if _, err := ps.CreateAtespace(ctx, atespace); err != nil {
						t.Fatalf("failed to seed atespace: %v", err)
					}
				}
			}

			seen := make(map[string]bool)
			pageToken := ""
			for {
				atespaces, next, err := s.ListAtespaces(ctx, pageSize, pageToken)
				if err != nil {
					t.Fatalf("ListAtespaces: %v", err)
				}
				for _, a := range atespaces {
					if seen[a.GetMetadata().GetName()] {
						t.Errorf("duplicate atespace in paginated results: %s", a.GetMetadata().GetName())
					}
					seen[a.GetMetadata().GetName()] = true
				}
				if next == "" {
					break
				}
				pageToken = next
			}
			if len(seen) != numShards*3 {
				t.Fatalf("expected %d atespaces across %d shards, got %d", numShards*3, numShards, len(seen))
			}
		})
	}
}
