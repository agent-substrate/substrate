//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package ateaerospike

import (
	"context"
	"errors"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/servers/ateapi/store"
	"github.com/agent-substrate/substrate/proto/ateapipb"
)

func setupTest(t *testing.T) (*as.Client, *Persistence, context.Context) {
	t.Helper()

	// Connect to local Aerospike server (default port 3000)
	// If not running, we gracefully skip the test.
	client, err := as.NewClient("localhost", 3000)
	if err != nil {
		t.Skipf("Skipping Aerospike integration test (Aerospike server not running at localhost:3000): %v", err)
	}

	namespace := "test" // Default Aerospike test namespace name
	p := NewPersistence(client, namespace)

	ctx := context.Background()
	// Clear previous test data if any
	_ = p.DebugClearAll(ctx)

	return client, p, ctx
}

func TestGetActor_NotFound(t *testing.T) {
	client, s, ctx := setupTest(t)
	defer client.Close()

	_, err := s.GetActor(ctx, "non-existent")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestCreateActor_Success(t *testing.T) {
	client, s, ctx := setupTest(t)
	defer client.Close()

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

	got, err := s.GetActor(ctx, actor.ActorId)
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}

	expected := proto.Clone(actor).(*ateapipb.Actor)
	expected.Version = 1 // First write generation in Aerospike is 1

	if diff := cmp.Diff(expected, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetActor returned unexpected actor (-want +got):\n%s", diff)
	}
}

func TestCreateActor_AlreadyExists(t *testing.T) {
	client, s, ctx := setupTest(t)
	defer client.Close()

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
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists creating existing actor, got %v", err)
	}
}

func TestUpdateActor_Success(t *testing.T) {
	client, s, ctx := setupTest(t)
	defer client.Close()

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
	err = s.UpdateActor(ctx, actor, 1) // expectedVersion = 1
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	if actor.Version != 2 {
		t.Errorf("expected actor.Version to be updated to 2, got %d", actor.Version)
	}

	updated, err := s.GetActor(ctx, actor.ActorId)
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}

	expected := proto.Clone(actor).(*ateapipb.Actor)

	if diff := cmp.Diff(expected, updated, protocmp.Transform()); diff != "" {
		t.Errorf("UpdateActor yielded unexpected state (-want +got):\n%s", diff)
	}
}

func TestUpdateActor_Conflict(t *testing.T) {
	client, s, ctx := setupTest(t)
	defer client.Close()

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
	actor1, err := s.GetActor(ctx, actor.ActorId)
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}

	// Fetch instance 2 (which will become stale)
	actor2, err := s.GetActor(ctx, actor.ActorId)
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}

	// Update instance 1 (advances generation/version to 2)
	actor1.Status = ateapipb.Actor_STATUS_RUNNING
	err = s.UpdateActor(ctx, actor1, actor1.Version)
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	// Try to update instance 2 using stale version 1
	actor2.Status = ateapipb.Actor_STATUS_SUSPENDED
	err = s.UpdateActor(ctx, actor2, actor2.Version)
	if !errors.Is(err, store.ErrPersistenceRetry) {
		t.Errorf("expected ErrPersistenceRetry, got %v", err)
	}
}

func TestGetWorker_NotFound(t *testing.T) {
	client, s, ctx := setupTest(t)
	defer client.Close()

	_, err := s.GetWorker(ctx, "default", "pool-1", "non-existent")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestCreateWorker_Success(t *testing.T) {
	client, s, ctx := setupTest(t)
	defer client.Close()

	worker := &ateapipb.Worker{
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
	}

	err := s.CreateWorker(ctx, worker)
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
}

func TestUpdateWorker_Success(t *testing.T) {
	client, s, ctx := setupTest(t)
	defer client.Close()

	worker := &ateapipb.Worker{
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
	}

	err := s.CreateWorker(ctx, worker)
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	worker.ActorNamespace = "default"
	worker.ActorTemplate = "test-template"
	worker.ActorId = "session-1"

	err = s.UpdateWorker(ctx, worker, 1)
	if err != nil {
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
		t.Errorf("UpdateWorker yielded unexpected state (-want +got):\n%s", diff)
	}
}

func TestDeleteWorker(t *testing.T) {
	client, s, ctx := setupTest(t)
	defer client.Close()

	worker := &ateapipb.Worker{
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
	}

	err := s.CreateWorker(ctx, worker)
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	err = s.DeleteWorker(ctx, "default", "pool-1", "pod-1")
	if err != nil {
		t.Fatalf("DeleteWorker failed: %v", err)
	}

	_, err = s.GetWorker(ctx, "default", "pool-1", "pod-1")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestDeleteActor(t *testing.T) {
	client, s, ctx := setupTest(t)
	defer client.Close()

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

	err = s.DeleteActor(ctx, "session-1")
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}

	_, err = s.GetActor(ctx, "session-1")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestDeleteActor_NotSuspended(t *testing.T) {
	client, s, ctx := setupTest(t)
	defer client.Close()

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

	err = s.DeleteActor(ctx, "session-1")
	if !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("expected ErrFailedPrecondition deleting running actor, got %v", err)
	}
}

func TestListWorkers(t *testing.T) {
	client, s, ctx := setupTest(t)
	defer client.Close()

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
	client, s, ctx := setupTest(t)
	defer client.Close()

	actor1 := &ateapipb.Actor{
		ActorId:                "id1",
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		LastSnapshot:           "gs://b1/f1",
	}
	actor2 := &ateapipb.Actor{
		ActorId:                "id2",
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		LastSnapshot:           "gs://b1/f2",
	}

	if err := s.CreateActor(ctx, actor1); err != nil {
		t.Fatalf("failed to create actor1: %v", err)
	}
	if err := s.CreateActor(ctx, actor2); err != nil {
		t.Fatalf("failed to create actor2: %v", err)
	}

	actors, err := s.ListActors(ctx)
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

func TestAcquireLock_Success(t *testing.T) {
	client, s, ctx := setupTest(t)
	defer client.Close()

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
