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

// Package storecontract provides backend-neutral assertions for store.Interface
// implementations.
package storecontract

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// testAtespace is the atespace used by tests that create a single actor.
const testAtespace = "test-atespace"

// Atomic cmp options to skip individual server-owned ResourceMetadata fields
// in proto diffs.
var (
	ignoreUID        = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "uid")
	ignoreVersion    = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "version")
	ignoreTimestamps = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "create_time", "update_time")
)

func newTestAtespace(name string) *ateapipb.Atespace {
	return &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}}
}

// mustCreateAtespace creates the atespace an actor test is about to populate.
// Backends that enforce the actor->atespace foreign key (atepg) reject
// CreateActor for a nonexistent atespace, so every actor test needs a real
// parent atespace even though ateredis doesn't check.
func mustCreateAtespace(t *testing.T, s store.Interface, name string) {
	t.Helper()
	if _, err := s.CreateAtespace(context.Background(), newTestAtespace(name)); err != nil {
		t.Fatalf("CreateAtespace(%q) failed: %v", name, err)
	}
}

func actorNameSet(actors []*ateapipb.Actor) map[string]bool {
	set := make(map[string]bool, len(actors))
	for _, a := range actors {
		set[a.GetMetadata().GetName()] = true
	}
	return set
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

// RunContractTests runs the backend-neutral store.Interface assertions
// against a fresh store.Interface built by setup for each subtest. setup is
// responsible for its own cleanup (e.g. via t.Cleanup).
//
// Backend-specific behavior (e.g. ateredis's multi-shard pagination, atepg's
// foreign-key races and transactional notifications) is NOT covered here; see
// each backend's own test file for that.
func RunContractTests(t *testing.T, setup func(t *testing.T) store.Interface) {
	t.Helper()

	t.Run("GetActor_NotFound", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		_, err := s.GetActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: "non-existent"})
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("CreateActor_Success", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)

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

		if created.GetMetadata().GetUid() == "" {
			t.Errorf("CreateActor returned empty uid; want server-assigned uid")
		}
		if created.GetMetadata().GetVersion() != 1 {
			t.Errorf("CreateActor returned version %d, want 1", created.GetMetadata().GetVersion())
		}
		if created.GetMetadata().GetCreateTime() == nil || created.GetMetadata().GetUpdateTime() == nil {
			t.Errorf("CreateActor returned unset create/update time")
		}

		if actor.GetMetadata().GetUid() != "" || actor.GetMetadata().GetVersion() != 0 {
			t.Errorf("CreateActor must not mutate its input, got metadata %v", actor.GetMetadata())
		}

		got, err := s.GetActor(ctx, resources.ActorRefFromActor(actor))
		if err != nil {
			t.Fatalf("GetActor failed: %v", err)
		}
		if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
			t.Errorf("CreateActor return does not match stored state (-created +got):\n%s", diff)
		}

		expected := proto.Clone(actor).(*ateapipb.Actor)
		expected.Metadata.Version = 1
		if diff := cmp.Diff(expected, created, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
			t.Errorf("CreateActor returned unexpected actor (-want +got):\n%s", diff)
		}
	})

	t.Run("CreateActor_AlreadyExists", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)

		actor := &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
			ActorTemplateNamespace: "default",
			ActorTemplateName:      "test-template",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		}

		if _, err := s.CreateActor(ctx, actor); err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
		if _, err := s.CreateActor(ctx, actor); !errors.Is(err, store.ErrAlreadyExists) {
			t.Errorf("expected ErrAlreadyExists, got %v", err)
		}
	})

	t.Run("UpdateActor_Success", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)

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
		// Server-owned metadata must be derived from the stored resource, not
		// accepted from an update payload.
		toUpdate.Metadata.Uid = "client-supplied-uid"
		toUpdate.Metadata.CreateTime = timestamppb.New(time.Unix(1, 0))
		updated, err := s.UpdateActor(ctx, toUpdate, created.GetMetadata().GetVersion())
		if err != nil {
			t.Fatalf("UpdateActor failed: %v", err)
		}

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

		if toUpdate.GetMetadata().GetVersion() != created.GetMetadata().GetVersion() {
			t.Errorf("UpdateActor must not mutate its input; version changed to %d", toUpdate.GetMetadata().GetVersion())
		}

		got, err := s.GetActor(ctx, resources.ActorRefFromActor(actor))
		if err != nil {
			t.Fatalf("GetActor failed: %v", err)
		}
		if diff := cmp.Diff(updated, got, protocmp.Transform()); diff != "" {
			t.Errorf("UpdateActor return does not match stored state (-updated +got):\n%s", diff)
		}
	})

	t.Run("UpdateActor_Conflict", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)

		actor := &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
			ActorTemplateNamespace: "default",
			ActorTemplateName:      "test-template",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		}

		if _, err := s.CreateActor(ctx, actor); err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}

		actor1, err := s.GetActor(ctx, resources.ActorRefFromActor(actor))
		if err != nil {
			t.Fatalf("GetActor failed: %v", err)
		}
		actor2, err := s.GetActor(ctx, resources.ActorRefFromActor(actor))
		if err != nil {
			t.Fatalf("GetActor failed: %v", err)
		}

		actor1.Status = ateapipb.Actor_STATUS_RUNNING
		if _, err := s.UpdateActor(ctx, actor1, actor1.GetMetadata().GetVersion()); err != nil {
			t.Fatalf("UpdateActor failed: %v", err)
		}

		actor2.Status = ateapipb.Actor_STATUS_SUSPENDED
		_, err = s.UpdateActor(ctx, actor2, actor2.GetMetadata().GetVersion())
		if !errors.Is(err, store.ErrVersionConflict) {
			t.Errorf("expected ErrVersionConflict, got %v", err)
		}
	})

	t.Run("UpdateActor_ImmutableFields", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)

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
		toUpdate.ActorTemplateName = "other-template"
		if _, err := s.UpdateActor(ctx, toUpdate, created.GetMetadata().GetVersion()); err == nil {
			t.Errorf("expected error updating actor_template_name, got nil")
		} else if errors.Is(err, store.ErrVersionConflict) || errors.Is(err, store.ErrNotFound) {
			t.Errorf("expected a plain immutable-field error, got sentinel %v", err)
		}
	})

	t.Run("GetWorker_NotFound", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		_, err := s.GetWorker(ctx, "default", "pool-1", "non-existent")
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("CreateWorker_Success", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		watch, err := s.WatchWorkers(ctx)
		if err != nil {
			t.Fatalf("WatchWorkers failed: %v", err)
		}
		defer watch.Close()

		worker := &ateapipb.Worker{
			WorkerNamespace: "default",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
		}

		if err := s.CreateWorker(ctx, worker); err != nil {
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
	})

	t.Run("CreateWorker_AlreadyExists", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		worker := &ateapipb.Worker{WorkerNamespace: "default", WorkerPool: "pool-1", WorkerPod: "pod-1"}
		if err := s.CreateWorker(ctx, worker); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		if err := s.CreateWorker(ctx, worker); !errors.Is(err, store.ErrAlreadyExists) {
			t.Errorf("expected ErrAlreadyExists, got %v", err)
		}
	})

	t.Run("UpdateWorker_Success", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		worker := &ateapipb.Worker{WorkerNamespace: "default", WorkerPool: "pool-1", WorkerPod: "pod-1"}
		if err := s.CreateWorker(ctx, worker); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		// Subscribe after create so the create event doesn't pollute the channel.
		watch, err := s.WatchWorkers(ctx)
		if err != nil {
			t.Fatalf("WatchWorkers failed: %v", err)
		}
		defer watch.Close()

		worker.Assignment = &ateapipb.Assignment{
			ActorTemplate: &ateapipb.KubeNamespacedObjectRef{Namespace: "default", Name: "test-template"},
			Actor:         &ateapipb.ObjectRef{Name: "session-1"},
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
	})

	t.Run("UpdateWorker_Conflict", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		worker := &ateapipb.Worker{WorkerNamespace: "default", WorkerPool: "pool-1", WorkerPod: "pod-1"}
		if err := s.CreateWorker(ctx, worker); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		worker1, err := s.GetWorker(ctx, "default", "pool-1", "pod-1")
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		worker2, err := s.GetWorker(ctx, "default", "pool-1", "pod-1")
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}

		worker1.Assignment = &ateapipb.Assignment{Actor: &ateapipb.ObjectRef{Name: "session-1"}}
		if err := s.UpdateWorker(ctx, worker1, worker1.Version); err != nil {
			t.Fatalf("UpdateWorker failed: %v", err)
		}

		worker2.Assignment = &ateapipb.Assignment{Actor: &ateapipb.ObjectRef{Name: "session-2"}}
		err = s.UpdateWorker(ctx, worker2, worker2.Version)
		if !errors.Is(err, store.ErrVersionConflict) {
			t.Errorf("expected ErrVersionConflict, got %v", err)
		}
	})

	t.Run("UpdateWorker_IPChanged", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		worker := &ateapipb.Worker{WorkerNamespace: "default", WorkerPool: "pool-1", WorkerPod: "pod-1", Ip: "10.0.0.1"}
		if err := s.CreateWorker(ctx, worker); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		worker.Version = 1
		worker.Ip = "10.0.0.2"
		if err := s.UpdateWorker(ctx, worker, 1); err != nil {
			t.Fatalf("UpdateWorker failed: %v", err)
		}
		got, err := s.GetWorker(ctx, "default", "pool-1", "pod-1")
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if got.GetIp() != "10.0.0.2" {
			t.Errorf("worker ip = %q, want 10.0.0.2", got.GetIp())
		}
		if got.GetVersion() != 2 {
			t.Errorf("worker version = %d, want 2", got.GetVersion())
		}
	})

	t.Run("DeleteWorker", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		worker := &ateapipb.Worker{WorkerNamespace: "default", WorkerPool: "pool-1", WorkerPod: "pod-1"}
		if err := s.CreateWorker(ctx, worker); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		watch, err := s.WatchWorkers(ctx)
		if err != nil {
			t.Fatalf("WatchWorkers failed: %v", err)
		}
		defer watch.Close()

		if err := s.DeleteWorker(ctx, "default", "pool-1", "pod-1"); err != nil {
			t.Fatalf("DeleteWorker failed: %v", err)
		}
		if _, err := s.GetWorker(ctx, "default", "pool-1", "pod-1"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("expected ErrNotFound after delete, got %v", err)
		}

		event := receiveEvent(t, watch.Events)
		if event.Type != store.WorkerEventDeleted {
			t.Errorf("expected WorkerEventDeleted, got %v", event.Type)
		}
	})

	t.Run("DeleteWorker_Idempotent", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if err := s.DeleteWorker(ctx, "default", "pool-1", "non-existent"); err != nil {
			t.Errorf("DeleteWorker of a missing worker should be a no-op, got %v", err)
		}
	})

	t.Run("WatchWorkers_ClosedOnClose", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		watch, err := s.WatchWorkers(ctx)
		if err != nil {
			t.Fatalf("WatchWorkers failed: %v", err)
		}
		watch.Close()

		select {
		case _, ok := <-watch.Events:
			if ok {
				t.Errorf("expected Events to be closed after Close, got an event")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for Events to close after Close")
		}
	})

	t.Run("DeleteActor", func(t *testing.T) {
		tests := []struct {
			name    string
			status  ateapipb.Actor_Status
			wantErr error
		}{
			{name: "suspended", status: ateapipb.Actor_STATUS_SUSPENDED, wantErr: store.ErrFailedPrecondition},
			{name: "crashed", status: ateapipb.Actor_STATUS_CRASHED, wantErr: store.ErrFailedPrecondition},
			{name: "deleting", status: ateapipb.Actor_STATUS_DELETING},
			{name: "running", status: ateapipb.Actor_STATUS_RUNNING, wantErr: store.ErrFailedPrecondition},
			{name: "paused", status: ateapipb.Actor_STATUS_PAUSED, wantErr: store.ErrFailedPrecondition},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				s := setup(t)
				ctx := context.Background()
				mustCreateAtespace(t, s, testAtespace)

				actor := &ateapipb.Actor{
					Metadata:               &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
					ActorTemplateNamespace: "default",
					ActorTemplateName:      "test-template",
					Status:                 tt.status,
				}
				if _, err := s.CreateActor(ctx, actor); err != nil {
					t.Fatalf("CreateActor failed: %v", err)
				}

				deleted, err := s.DeleteActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: "session-1"})
				if tt.wantErr != nil {
					if !errors.Is(err, tt.wantErr) {
						t.Errorf("DeleteActor: expected %v, got %v", tt.wantErr, err)
					}
					got, getErr := s.GetActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: "session-1"})
					if getErr != nil {
						t.Fatalf("GetActor after rejected delete failed: %v", getErr)
					}
					if got.GetStatus() != tt.status {
						t.Errorf("actor status after rejected delete = %v, want %v", got.GetStatus(), tt.status)
					}
					return
				}
				if err != nil {
					t.Fatalf("DeleteActor failed: %v", err)
				}
				if got := deleted.GetMetadata().GetName(); got != "session-1" {
					t.Errorf("deleted actor name = %q, want session-1", got)
				}
				if _, err := s.GetActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: "session-1"}); !errors.Is(err, store.ErrNotFound) {
					t.Errorf("expected ErrNotFound after delete, got %v", err)
				}
			})
		}
	})

	t.Run("DeleteActor_NotFound", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.DeleteActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: "non-existent"}); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("expected ErrNotFound deleting non-existent actor, got %v", err)
		}
	})

	t.Run("ListWorkers", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		worker1 := &ateapipb.Worker{WorkerNamespace: "ns1", WorkerPool: "pool1", WorkerPod: "pod1"}
		worker2 := &ateapipb.Worker{WorkerNamespace: "ns1", WorkerPool: "pool1", WorkerPod: "pod2"}
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

		found1, found2 := false, false
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
	})

	t.Run("ListWorkers_Empty", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		workers, _, err := s.ListWorkers(ctx, 1000, "")
		if err != nil {
			t.Fatalf("ListWorkers failed: %v", err)
		}
		if len(workers) != 0 {
			t.Errorf("expected 0 workers, got %d", len(workers))
		}
	})

	t.Run("ListWorkers_Pagination", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		for i := 0; i < 5; i++ {
			worker := &ateapipb.Worker{WorkerNamespace: "ns1", WorkerPool: "pool1", WorkerPod: fmt.Sprintf("pod%d", i)}
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
	})

	t.Run("ListAtespaces_Pagination", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

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
	})

	t.Run("ListActors_Empty", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		actors, _, err := s.ListActors(ctx, "", 1000, "")
		if err != nil {
			t.Fatalf("ListActors failed: %v", err)
		}
		if len(actors) != 0 {
			t.Errorf("expected 0 actors, got %d", len(actors))
		}
	})

	t.Run("ListActors", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)

		actor1 := &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Name: "id1", Atespace: testAtespace},
			ActorTemplateNamespace: "ns1",
			ActorTemplateName:      "tmpl1",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
			LatestSnapshot:         &ateapipb.ObjectRef{Atespace: testAtespace, Name: "snapshot-1"},
		}
		actor2 := &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Name: "id2", Atespace: testAtespace},
			ActorTemplateNamespace: "ns1",
			ActorTemplateName:      "tmpl1",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
			LatestSnapshot:         &ateapipb.ObjectRef{Atespace: testAtespace, Name: "snapshot-2"},
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
		if got := actorNameSet(actors); !got["id1"] || !got["id2"] {
			t.Errorf("did not find all actors: %v", got)
		}
	})

	t.Run("ListActors_Pagination", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)

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
	})

	t.Run("ListActors_ScopedByAtespace", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, "team-a")
		mustCreateAtespace(t, s, "team-b")

		mkActor := func(atespace, name string) *ateapipb.Actor {
			return &ateapipb.Actor{
				Metadata:               &ateapipb.ResourceMetadata{Name: name, Atespace: atespace},
				ActorTemplateNamespace: "ns1",
				ActorTemplateName:      "tmpl1",
				Status:                 ateapipb.Actor_STATUS_SUSPENDED,
			}
		}
		for _, a := range []*ateapipb.Actor{mkActor("team-a", "a1"), mkActor("team-a", "a2"), mkActor("team-b", "b1")} {
			if _, err := s.CreateActor(ctx, a); err != nil {
				t.Fatalf("CreateActor(%s/%s) failed: %v", a.GetMetadata().GetAtespace(), a.GetMetadata().GetName(), err)
			}
		}

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

		all, _, err := s.ListActors(ctx, "", 1000, "")
		if err != nil {
			t.Fatalf("ListActors(all) failed: %v", err)
		}
		if got := actorNameSet(all); !got["a1"] || !got["a2"] || !got["b1"] || len(got) != 3 {
			t.Errorf("ListActors(all) = %v, want exactly {a1, a2, b1}", got)
		}

		if _, err := s.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "a1"}); err != nil {
			t.Errorf("GetActor(team-a, a1) failed: %v", err)
		}
		if _, err := s.GetActor(ctx, resources.ActorRef{Atespace: "team-b", Name: "a1"}); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("GetActor(team-b, a1) = %v, want ErrNotFound", err)
		}
		if _, err := s.GetActor(ctx, resources.ActorRef{Name: "a1"}); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("GetActor(empty, a1) = %v, want ErrNotFound", err)
		}
	})

	t.Run("AcquireLock_Success", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		lock, err := s.AcquireLock(ctx, "test-lock")
		if err != nil {
			t.Fatalf("AcquireLock failed: %v", err)
		}
		if lock == nil {
			t.Fatal("AcquireLock returned a nil lock")
		}
		if err := lock.Context().Err(); err != nil {
			t.Errorf("new lock context is already done: %v", err)
		}
		lock.Close()
	})

	t.Run("AcquireLock_Conflict", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		lock, err := s.AcquireLock(ctx, "test-lock")
		if err != nil {
			t.Fatalf("first AcquireLock failed: %v", err)
		}
		defer lock.Close()

		if _, err := s.AcquireLock(ctx, "test-lock"); !errors.Is(err, store.ErrLockConflict) {
			t.Errorf("second AcquireLock error = %v, want ErrLockConflict", err)
		}
	})

	t.Run("AcquireLock_NonReentry", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		lock, err := s.AcquireLock(ctx, "test-lock")
		if err != nil {
			t.Fatalf("first AcquireLock failed: %v", err)
		}
		defer lock.Close()

		if _, err := s.AcquireLock(ctx, "test-lock"); !errors.Is(err, store.ErrLockConflict) {
			t.Errorf("reentrant AcquireLock error = %v, want ErrLockConflict", err)
		}
	})

	t.Run("Lock_Close_Releases", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		lock, err := s.AcquireLock(ctx, "test-lock")
		if err != nil {
			t.Fatalf("AcquireLock failed: %v", err)
		}
		lock.Close()

		newLock, err := s.AcquireLock(ctx, "test-lock")
		if err != nil {
			t.Fatalf("AcquireLock after Close failed: %v", err)
		}
		newLock.Close()
	})

	t.Run("Lock_Close_Idempotent", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		lock, err := s.AcquireLock(ctx, "test-lock")
		if err != nil {
			t.Fatalf("AcquireLock failed: %v", err)
		}
		lock.Close()
		lock.Close()
	})

	t.Run("DebugClearAll", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
			t.Fatalf("CreateAtespace failed: %v", err)
		}
		if _, err := s.CreateActor(ctx, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Name: "id1", Atespace: "team-a"},
			Status:   ateapipb.Actor_STATUS_SUSPENDED,
		}); err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
		if err := s.CreateWorker(ctx, &ateapipb.Worker{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "pod"}); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		lock, err := s.AcquireLock(ctx, "lock-1")
		if err != nil {
			t.Fatalf("AcquireLock failed: %v", err)
		}
		defer lock.Close()

		if err := s.DebugClearAll(ctx); err != nil {
			t.Fatalf("DebugClearAll failed: %v", err)
		}

		if _, err := s.GetAtespace(ctx, "team-a"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("atespace survived DebugClearAll: %v", err)
		}
		if actors, _, err := s.ListActors(ctx, "", 1000, ""); err != nil || len(actors) != 0 {
			t.Errorf("actors survived DebugClearAll: actors=%v err=%v", actors, err)
		}
		if workers, _, err := s.ListWorkers(ctx, 1000, ""); err != nil || len(workers) != 0 {
			t.Errorf("workers survived DebugClearAll: workers=%v err=%v", workers, err)
		}
		reacquired, err := s.AcquireLock(ctx, "lock-1")
		if err != nil {
			t.Errorf("lock survived DebugClearAll: %v", err)
		} else {
			reacquired.Close()
		}
	})

	t.Run("CreateAtespace_Success", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		want := newTestAtespace("team-a")
		created, err := s.CreateAtespace(ctx, want)
		if err != nil {
			t.Fatalf("CreateAtespace failed: %v", err)
		}
		if created.GetMetadata().GetUid() == "" {
			t.Errorf("CreateAtespace returned empty uid; want server-assigned uid")
		}
		if created.GetMetadata().GetVersion() != 1 {
			t.Errorf("CreateAtespace returned version %d, want 1", created.GetMetadata().GetVersion())
		}

		got, err := s.GetAtespace(ctx, "team-a")
		if err != nil {
			t.Fatalf("GetAtespace failed: %v", err)
		}
		if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
			t.Errorf("CreateAtespace return does not match stored state (-created +got):\n%s", diff)
		}
		if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreUID, ignoreTimestamps, ignoreVersion); diff != "" {
			t.Errorf("CreateAtespace returned unexpected atespace (-want +got):\n%s", diff)
		}
	})

	t.Run("CreateAtespace_AlreadyExists", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
			t.Fatalf("first CreateAtespace failed: %v", err)
		}
		if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); !errors.Is(err, store.ErrAlreadyExists) {
			t.Errorf("expected ErrAlreadyExists, got %v", err)
		}
	})

	t.Run("GetAtespace_NotFound", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.GetAtespace(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("AtespaceExists", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if ok, err := s.AtespaceExists(ctx, "team-a"); err != nil || ok {
			t.Fatalf("AtespaceExists before create = (%v, %v), want (false, nil)", ok, err)
		}
		if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
			t.Fatalf("CreateAtespace failed: %v", err)
		}
		if ok, err := s.AtespaceExists(ctx, "team-a"); err != nil || !ok {
			t.Fatalf("AtespaceExists after create = (%v, %v), want (true, nil)", ok, err)
		}
	})

	t.Run("ListAtespaces", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

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
	})

	t.Run("ListAtespaces_Empty", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		got, _, err := s.ListAtespaces(ctx, 1000, "")
		if err != nil {
			t.Fatalf("ListAtespaces failed: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("ListAtespaces on empty store = %v, want empty", got)
		}
	})

	t.Run("DeleteAtespace_Empty", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
			t.Fatalf("CreateAtespace failed: %v", err)
		}
		deleted, err := s.DeleteAtespace(ctx, "team-a")
		if err != nil {
			t.Fatalf("DeleteAtespace failed: %v", err)
		}
		if got := deleted.GetMetadata().GetName(); got != "team-a" {
			t.Errorf("deleted atespace name = %q, want team-a", got)
		}
		if _, err := s.GetAtespace(ctx, "team-a"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("after delete, GetAtespace = %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteAtespace_NotFound", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.DeleteAtespace(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("DeleteAtespace_NonEmpty_Rejected", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
			t.Fatalf("CreateAtespace failed: %v", err)
		}
		if _, err := s.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: "id1", Atespace: "team-a"}, Status: ateapipb.Actor_STATUS_DELETING}); err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
		if _, err := s.DeleteAtespace(ctx, "team-a"); !errors.Is(err, store.ErrFailedPrecondition) {
			t.Errorf("DeleteAtespace on non-empty = %v, want ErrFailedPrecondition", err)
		}
		if _, err := s.GetAtespace(ctx, "team-a"); err != nil {
			t.Errorf("atespace should still exist after rejected delete, got %v", err)
		}
	})

	t.Run("DeleteAtespace_EmptyAfterActorsRemoved", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
			t.Fatalf("CreateAtespace failed: %v", err)
		}
		if _, err := s.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: "id1", Atespace: "team-a"}, Status: ateapipb.Actor_STATUS_DELETING}); err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
		if _, err := s.DeleteAtespace(ctx, "team-a"); !errors.Is(err, store.ErrFailedPrecondition) {
			t.Fatalf("expected rejection while non-empty, got %v", err)
		}
		if _, err := s.DeleteActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}); err != nil {
			t.Fatalf("DeleteActor failed: %v", err)
		}
		if _, err := s.DeleteAtespace(ctx, "team-a"); err != nil {
			t.Errorf("DeleteAtespace after actor removed = %v, want nil", err)
		}
	})

	t.Run("DeleteAtespace_EmptyWhileOtherAtespaceNonEmpty", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
			t.Fatalf("CreateAtespace(team-a) failed: %v", err)
		}
		if _, err := s.CreateAtespace(ctx, newTestAtespace("team-b")); err != nil {
			t.Fatalf("CreateAtespace(team-b) failed: %v", err)
		}
		if _, err := s.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: "id1", Atespace: "team-b"}, Status: ateapipb.Actor_STATUS_SUSPENDED}); err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}

		if _, err := s.DeleteAtespace(ctx, "team-a"); err != nil {
			t.Errorf("DeleteAtespace(team-a, empty) = %v, want nil (must not be blocked by team-b's actor)", err)
		}
		if _, err := s.GetAtespace(ctx, "team-a"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("after delete, GetAtespace(team-a) = %v, want ErrNotFound", err)
		}
		if _, err := s.DeleteAtespace(ctx, "team-b"); !errors.Is(err, store.ErrFailedPrecondition) {
			t.Errorf("DeleteAtespace(team-b, non-empty) = %v, want ErrFailedPrecondition", err)
		}
	})

	t.Run("ActorSnapshotAndTag_Lifecycle", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, "team-a")

		input := &ateapipb.ActorSnapshot{
			Metadata:           &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "snapshot-1"},
			SourceActor:        &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-1"},
			SourceActorUid:     "actor-uid",
			SourceActorVersion: 7,
			ContentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
		}
		created, err := s.CreateActorSnapshot(ctx, input, "gs://private/snapshot-1")
		if err != nil {
			t.Fatalf("CreateActorSnapshot failed: %v", err)
		}
		if created.GetMetadata().GetVersion() != 1 || created.GetMetadata().GetUid() == "" {
			t.Errorf("created snapshot metadata = %v, want server-owned uid and version 1", created.GetMetadata())
		}
		if input.GetMetadata().GetUid() != "" || input.GetMetadata().GetVersion() != 0 {
			t.Errorf("CreateActorSnapshot mutated its input metadata: %v", input.GetMetadata())
		}
		if _, err := s.CreateActorSnapshot(ctx, input, "gs://private/duplicate"); !errors.Is(err, store.ErrAlreadyExists) {
			t.Errorf("duplicate CreateActorSnapshot = %v, want ErrAlreadyExists", err)
		}

		got, location, err := s.GetActorSnapshot(ctx, "team-a", "snapshot-1")
		if err != nil {
			t.Fatalf("GetActorSnapshot failed: %v", err)
		}
		if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
			t.Errorf("GetActorSnapshot mismatch (-created +got):\n%s", diff)
		}
		if location != "gs://private/snapshot-1" {
			t.Errorf("snapshot location = %q, want private location", location)
		}
		if _, _, err := s.GetActorSnapshot(ctx, "team-a", "missing"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("missing GetActorSnapshot = %v, want ErrNotFound", err)
		}

		tagInput := &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "production"},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		}
		tag, err := s.TagActorSnapshot(ctx, "team-a", "snapshot-1", tagInput)
		if err != nil {
			t.Fatalf("TagActorSnapshot failed: %v", err)
		}
		if tag.GetSnapshot().GetAtespace() != "team-a" || tag.GetSnapshot().GetName() != "snapshot-1" {
			t.Errorf("tag snapshot = %v, want team-a/snapshot-1", tag.GetSnapshot())
		}
		if tagInput.GetSnapshot() != nil || tagInput.GetMetadata().GetVersion() != 0 {
			t.Errorf("TagActorSnapshot mutated its input: %v", tagInput)
		}
		idempotent, err := s.TagActorSnapshot(ctx, "team-a", "snapshot-1", tagInput)
		if err != nil || !proto.Equal(idempotent, tag) {
			t.Errorf("idempotent TagActorSnapshot = (%v, %v), want existing tag", idempotent, err)
		}
		conflicting := proto.Clone(tagInput).(*ateapipb.ActorSnapshotTag)
		conflicting.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
		if _, err := s.TagActorSnapshot(ctx, "team-a", "snapshot-1", conflicting); !errors.Is(err, store.ErrAlreadyExists) {
			t.Errorf("conflicting TagActorSnapshot = %v, want ErrAlreadyExists", err)
		}
		if _, err := s.TagActorSnapshot(ctx, "team-a", "missing", &ateapipb.ActorSnapshotTag{Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "missing"}}); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("tagging missing snapshot = %v, want ErrNotFound", err)
		}

		resolved, resolvedLocation, resolvedTag, err := s.GetActorSnapshotByTag(ctx, "team-a", "production")
		if err != nil {
			t.Fatalf("GetActorSnapshotByTag failed: %v", err)
		}
		if !proto.Equal(resolved, created) || !proto.Equal(resolvedTag, tag) || resolvedLocation != location {
			t.Errorf("resolved tag = (%v, %q, %v), want created snapshot, location, and tag", resolved, resolvedLocation, resolvedTag)
		}

		updated, err := s.UpdateActorSnapshotTag(ctx, "team-a", "production", ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED, tag.GetMetadata().GetVersion())
		if err != nil {
			t.Fatalf("UpdateActorSnapshotTag failed: %v", err)
		}
		if updated.GetScope() != ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED || updated.GetMetadata().GetVersion() != tag.GetMetadata().GetVersion()+1 {
			t.Errorf("updated tag = %v, want published scope and advanced version", updated)
		}
		if _, err := s.UpdateActorSnapshotTag(ctx, "team-a", "production", tag.GetScope(), tag.GetMetadata().GetVersion()); !errors.Is(err, store.ErrVersionConflict) {
			t.Errorf("stale UpdateActorSnapshotTag = %v, want ErrVersionConflict", err)
		}
		if _, err := s.DeleteAtespace(ctx, "team-a"); !errors.Is(err, store.ErrFailedPrecondition) {
			t.Errorf("DeleteAtespace with tag = %v, want ErrFailedPrecondition", err)
		}

		deleted, err := s.DeleteActorSnapshotTag(ctx, "team-a", "production")
		if err != nil || !proto.Equal(deleted, updated) {
			t.Errorf("DeleteActorSnapshotTag = (%v, %v), want updated tag", deleted, err)
		}
		if _, _, _, err := s.GetActorSnapshotByTag(ctx, "team-a", "production"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("deleted GetActorSnapshotByTag = %v, want ErrNotFound", err)
		}
		if _, err := s.DeleteAtespace(ctx, "team-a"); err != nil {
			t.Errorf("DeleteAtespace after tag deletion = %v, want nil", err)
		}
	})

	t.Run("ListActorSnapshots_PaginationAndScope", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		for _, atespace := range []string{"team-a", "team-b"} {
			for i := 0; i < 3; i++ {
				name := fmt.Sprintf("snapshot-%d", i)
				if _, err := s.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name}}, "gs://private/"+atespace+"/"+name); err != nil {
					t.Fatalf("CreateActorSnapshot(%s/%s) failed: %v", atespace, name, err)
				}
			}
		}

		var scoped []*ateapipb.ActorSnapshot
		for token := ""; ; {
			page, next, err := s.ListActorSnapshots(ctx, "team-a", 2, token)
			if err != nil {
				t.Fatalf("scoped ListActorSnapshots failed: %v", err)
			}
			scoped = append(scoped, page...)
			if next == "" {
				break
			}
			token = next
		}
		if len(scoped) != 3 {
			t.Errorf("scoped ListActorSnapshots returned %d snapshots, want 3", len(scoped))
		}

		var global []*ateapipb.ActorSnapshot
		for token := ""; ; {
			page, next, err := s.ListActorSnapshots(ctx, "", 2, token)
			if err != nil {
				t.Fatalf("global ListActorSnapshots failed: %v", err)
			}
			global = append(global, page...)
			if next == "" {
				break
			}
			token = next
		}
		if len(global) != 6 {
			t.Errorf("global ListActorSnapshots returned %d snapshots, want 6", len(global))
		}
	})
}
