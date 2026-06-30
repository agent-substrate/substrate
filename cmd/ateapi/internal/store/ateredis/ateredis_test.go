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

func TestGetActor_NotFound(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	_, err := s.GetActor(ctx, "", "non-existent")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestCreateActor_Success(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	actor := &ateapipb.Actor{
		ActorId:                "session-1",
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "test-template",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
	}

	err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	got, err := s.GetActor(ctx, actor.GetAtespace(), actor.ActorId)
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}

	expected := proto.Clone(actor).(*ateapipb.Actor)
	expected.Version = 1

	if diff := cmp.Diff(expected, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetActor returned unexpected actor (-want +got):\n%s", diff)
	}
}

func TestCreateActor_AlreadyExists(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	actor := &ateapipb.Actor{
		ActorId:                "session-1",
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "test-template",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
	}

	err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	err = s.CreateActor(ctx, actor)
	if err == nil {
		t.Errorf("expected error creating existing actor, got nil")
	}
}

func TestUpdateActor_Success(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	actor := &ateapipb.Actor{
		ActorId:                "session-1",
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "test-template",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
	}

	err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	actor.Status = ateapipb.Actor_STATUS_RUNNING
	actor.Version = 1

	err = s.UpdateActor(ctx, actor, 1)
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	if actor.Version != 2 {
		t.Errorf("expected actor.Version to be updated to 2, got %d", actor.Version)
	}

	updated, err := s.GetActor(ctx, actor.GetAtespace(), actor.ActorId)
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}

	expected := proto.Clone(actor).(*ateapipb.Actor)

	if diff := cmp.Diff(expected, updated, protocmp.Transform()); diff != "" {
		t.Errorf("UpdateActor yielded unexpected state in DB (-want +got):\n%s", diff)
	}
}

func TestUpdateActor_Conflict(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	actor := &ateapipb.Actor{
		ActorId:                "session-1",
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "test-template",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
	}

	err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Fetch instance 1
	actor1, err := s.GetActor(ctx, actor.GetAtespace(), actor.ActorId)
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}

	// Fetch instance 2 (stale after actor1 updates)
	actor2, err := s.GetActor(ctx, actor.GetAtespace(), actor.ActorId)
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}

	// Update instance 1
	actor1.Status = ateapipb.Actor_STATUS_RUNNING
	err = s.UpdateActor(ctx, actor1, actor1.GetVersion())
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	// Try to update instance 2 (which has stale version)
	actor2.Status = ateapipb.Actor_STATUS_SUSPENDED
	err = s.UpdateActor(ctx, actor2, actor2.GetVersion())
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

	worker.ActorNamespace = "default"
	worker.ActorTemplate = "test-template"
	worker.ActorId = "session-1"

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
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	actor := &ateapipb.Actor{
		ActorId:                "session-1",
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "test-template",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
	}

	err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	err = s.DeleteActor(ctx, "", "session-1")
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}

	_, err = s.GetActor(ctx, "", "session-1")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestDeleteActor_NotSuspended(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	actor := &ateapipb.Actor{
		ActorId:                "session-1",
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "test-template",
		Status:                 ateapipb.Actor_STATUS_RUNNING,
	}

	err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	err = s.DeleteActor(ctx, "", "session-1")
	if !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("expected ErrFailedPrecondition deleting running actor, got %v", err)
	}
}

func TestDeleteActor_NotFound(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	err := s.DeleteActor(ctx, "", "non-existent")
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

	workers, err := s.ListWorkers(ctx)
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

		ActorId:                "id1",
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		LatestSnapshotInfo: &ateapipb.SnapshotInfo{
			Type: ateapipb.SnapshotType_SNAPSHOT_TYPE_EXTERNAL,
			Data: &ateapipb.SnapshotInfo_External{
				External: &ateapipb.ExternalSnapshotInfo{
					SnapshotUriPrefix: "gs://b1/f1",
				},
			},
		},
	}
	actor2 := &ateapipb.Actor{
		ActorId:                "id2",
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		LatestSnapshotInfo: &ateapipb.SnapshotInfo{
			Type: ateapipb.SnapshotType_SNAPSHOT_TYPE_EXTERNAL,
			Data: &ateapipb.SnapshotInfo_External{
				External: &ateapipb.ExternalSnapshotInfo{
					SnapshotUriPrefix: "gs://b1/f2",
				},
			},
		},
	}

	if err := s.CreateActor(ctx, actor1); err != nil {
		t.Fatalf("failed to create actor1: %v", err)
	}
	if err := s.CreateActor(ctx, actor2); err != nil {
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
		if a.GetActorId() == "id1" {
			found1 = true
		}
		if a.GetActorId() == "id2" {
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
	worker1.ActorId = "session-1"
	err = s.UpdateWorker(ctx, worker1, worker1.Version)
	if err != nil {
		t.Fatalf("UpdateWorker failed: %v", err)
	}

	// Try to update instance 2
	worker2.ActorId = "session-2"
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

	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}

	if len(workers) != 0 {
		t.Errorf("expected 0 workers, got %d", len(workers))
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
			ActorId:                fmt.Sprintf("id%d", i),
			ActorTemplateNamespace: "ns1",
			ActorTemplateName:      "tmpl1",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		}
		if err := s.CreateActor(ctx, actor); err != nil {
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
		if seen[a.ActorId] {
			t.Errorf("duplicate actor found in paginated results: %s", a.ActorId)
		}
		seen[a.ActorId] = true
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

	mkActor := func(id, atespace string) *ateapipb.Actor {
		return &ateapipb.Actor{
			ActorId:                id,
			Atespace:               atespace,
			ActorTemplateNamespace: "ns1",
			ActorTemplateName:      "tmpl1",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		}
	}
	for _, a := range []*ateapipb.Actor{
		mkActor("a1", "team-a"),
		mkActor("a2", "team-a"),
		mkActor("b1", "team-b"),
	} {
		if err := s.CreateActor(ctx, a); err != nil {
			t.Fatalf("CreateActor(%s/%s) failed: %v", a.GetAtespace(), a.GetActorId(), err)
		}
	}

	// List is scoped to one atespace.
	teamA, _, err := s.ListActors(ctx, "team-a", 1000, "")
	if err != nil {
		t.Fatalf("ListActors(team-a) failed: %v", err)
	}
	if got := actorIDSet(teamA); !got["a1"] || !got["a2"] || got["b1"] || len(got) != 2 {
		t.Errorf("ListActors(team-a) = %v, want exactly {a1, a2}", got)
	}

	teamB, _, err := s.ListActors(ctx, "team-b", 1000, "")
	if err != nil {
		t.Fatalf("ListActors(team-b) failed: %v", err)
	}
	if got := actorIDSet(teamB); !got["b1"] || got["a1"] || len(got) != 1 {
		t.Errorf("ListActors(team-b) = %v, want exactly {b1}", got)
	}

	// An empty atespace lists across all atespaces (the admin/dev `-A` view).
	all, _, err := s.ListActors(ctx, "", 1000, "")
	if err != nil {
		t.Fatalf("ListActors(all) failed: %v", err)
	}
	if got := actorIDSet(all); !got["a1"] || !got["a2"] || !got["b1"] || len(got) != 3 {
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

func actorIDSet(actors []*ateapipb.Actor) map[string]bool {
	set := make(map[string]bool, len(actors))
	for _, a := range actors {
		set[a.GetActorId()] = true
	}
	return set
}

func newTestAtespace(name string) *ateapipb.Atespace {
	return &ateapipb.Atespace{Name: name}
}

func TestCreateAtespace_Success(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	want := newTestAtespace("team-a")
	if err := s.CreateAtespace(ctx, want); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	got, err := s.GetAtespace(ctx, "team-a")
	if err != nil {
		t.Fatalf("GetAtespace failed: %v", err)
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("round-trip mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateAtespace_AlreadyExists(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("first CreateAtespace failed: %v", err)
	}
	if err := s.CreateAtespace(ctx, newTestAtespace("team-a")); !errors.Is(err, store.ErrAlreadyExists) {
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
	if err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
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
		if err := s.CreateAtespace(ctx, newTestAtespace(n)); err != nil {
			t.Fatalf("CreateAtespace(%s) failed: %v", n, err)
		}
	}
	got, err := s.ListAtespaces(ctx)
	if err != nil {
		t.Fatalf("ListAtespaces failed: %v", err)
	}
	if len(got) != len(names) {
		t.Fatalf("ListAtespaces returned %d atespaces, want %d", len(got), len(names))
	}
	gotNames := map[string]bool{}
	for _, a := range got {
		gotNames[a.GetName()] = true
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

	got, err := s.ListAtespaces(ctx)
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

	if err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if err := s.DeleteAtespace(ctx, "team-a"); err != nil {
		t.Fatalf("DeleteAtespace failed: %v", err)
	}
	if _, err := s.GetAtespace(ctx, "team-a"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("after delete, GetAtespace = %v, want ErrNotFound", err)
	}
}

func TestDeleteAtespace_NotFound(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if err := s.DeleteAtespace(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestDeleteAtespace_NonEmpty_Rejected(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if err := s.CreateActor(ctx, &ateapipb.Actor{ActorId: "id1", Atespace: "team-a", Status: ateapipb.Actor_STATUS_SUSPENDED}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if err := s.DeleteAtespace(ctx, "team-a"); !errors.Is(err, store.ErrFailedPrecondition) {
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

	if err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if err := s.CreateActor(ctx, &ateapipb.Actor{ActorId: "id1", Atespace: "team-a", Status: ateapipb.Actor_STATUS_SUSPENDED}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if err := s.DeleteAtespace(ctx, "team-a"); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("expected rejection while non-empty, got %v", err)
	}
	if err := s.DeleteActor(ctx, "team-a", "id1"); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	if err := s.DeleteAtespace(ctx, "team-a"); err != nil {
		t.Errorf("DeleteAtespace after actor removed = %v, want nil (re-scan should find it empty)", err)
	}
}

func TestDeleteAtespace_EmptyWhileOtherAtespaceNonEmpty(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace(team-a) failed: %v", err)
	}
	if err := s.CreateAtespace(ctx, newTestAtespace("team-b")); err != nil {
		t.Fatalf("CreateAtespace(team-b) failed: %v", err)
	}
	// Actor lives ONLY in team-b.
	if err := s.CreateActor(ctx, &ateapipb.Actor{ActorId: "id1", Atespace: "team-b", Status: ateapipb.Actor_STATUS_SUSPENDED}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// team-a is empty → delete must succeed.
	if err := s.DeleteAtespace(ctx, "team-a"); err != nil {
		t.Errorf("DeleteAtespace(team-a, empty) = %v, want nil (must not be blocked by team-b's actor)", err)
	}
	if _, err := s.GetAtespace(ctx, "team-a"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("after delete, GetAtespace(team-a) = %v, want ErrNotFound", err)
	}
	// team-b is still non-empty → still rejected.
	if err := s.DeleteAtespace(ctx, "team-b"); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("DeleteAtespace(team-b, non-empty) = %v, want ErrFailedPrecondition", err)
	}
}
