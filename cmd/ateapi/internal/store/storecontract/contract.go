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
	"strings"
	"sync"
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

// Worker resource names. They are opaque to the store, which only ever uses
// them as the row key.
const (
	testWorkerName      = "3f9a5c2e-1d47-4b8a-9f21-6c0e5a7d3b18"
	otherTestWorkerName = "7c1b8d4a-92e6-4f30-a5c7-2b8f61d0e934"

	// testWorkerPodUID is deliberately not equal to either worker name, so no
	// contract test can come to depend on them matching.
	testWorkerPodUID = "d41b6e97-5a02-4c8d-b3f7-90e2c1a86d54"
)

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

// The fixture populates the volume/env union fields so the round-trip covers
// the plain-field union shape (issue #962), including presence of the
// optional value and type fields (empty string != unset).
func newTestActorTemplate(atespace, name string) *ateapipb.ActorTemplate {
	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		Containers: []*ateapipb.Container{{
			Name:         "main",
			Image:        "example.com/app@sha256:0000000000000000000000000000000000000000000000000000000000000000",
			Env:          []*ateapipb.EnvVar{{Name: "MODE", Value: "VAL"}},
			VolumeMounts: []*ateapipb.VolumeMount{{Name: "data", MountPath: "/data"}},
		}},
		Volumes: []*ateapipb.Volume{{
			Name:       "data",
			DurableDir: &ateapipb.DurableDirVolumeSource{},
			Type:       "DurableDir",
		}},
	}
}

// newTestWorker builds a global-scoped Worker. name is the resource name the
// store keys the row by; it is unrelated to worker_pod_uid.
func newTestWorker(name, pod string) *ateapipb.Worker {
	return &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: name},
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       pod,
		WorkerPodUid:    testWorkerPodUID,
		Capacity:        &ateapipb.WorkerCapacity{CpuMilli: 2000, MemoryBytes: 4 << 30},
		Status:          &ateapipb.WorkerStatus{},
	}
}

// mustCreateAtespace creates the atespace an actor test is about to populate.
// The PostgreSQL store enforces the actor->atespace foreign key.
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
// PostgreSQL-specific behavior such as foreign-key races and transactional
// notifications is not covered here; see atepg's own test file for that.
func RunContractTests(t *testing.T, setup func(t *testing.T) store.Interface) {
	runActorContractTests(t, setup)
	runWorkerContractTests(t, setup)
	runAtespaceContractTests(t, setup)
	runActorTemplateContractTests(t, setup)
	runActorSnapshotContractTests(t, setup)
	runLeaseContractTests(t, setup)
	runListOptionsContractTests(t, setup)
	runDebugContractTests(t, setup)
}

func runListOptionsContractTests(t *testing.T, setup func(t *testing.T) store.Interface) {
	t.Helper()

	t.Run("ListOptions_InvalidPageSize", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		calls := []struct {
			name string
			call func(store.ListOptions) error
		}{
			{"atespaces", func(opts store.ListOptions) error { _, err := s.ListAtespaces(ctx, opts); return err }},
			{"actors", func(opts store.ListOptions) error { _, err := s.ListActors(ctx, "", opts); return err }},
			{"actor templates", func(opts store.ListOptions) error { _, err := s.ListActorTemplates(ctx, "", opts); return err }},
			{"actor snapshots", func(opts store.ListOptions) error { _, err := s.ListActorSnapshots(ctx, "", opts); return err }},
			{"workers", func(opts store.ListOptions) error { _, err := s.ListWorkers(ctx, opts); return err }},
		}
		for _, call := range calls {
			t.Run(call.name, func(t *testing.T) {
				if err := call.call(store.ListOptions{PageSize: -1}); !errors.Is(err, store.ErrInvalidPageSize) {
					t.Errorf("negative PageSize error = %v, want ErrInvalidPageSize", err)
				}
				if err := call.call(store.ListOptions{}); err != nil {
					t.Errorf("zero PageSize error = %v, want nil", err)
				}
			})
		}
	})

	t.Run("ListOptions_InvalidPageToken", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		calls := []struct {
			name string
			call func(store.ListOptions) error
		}{
			{"atespaces", func(opts store.ListOptions) error { _, err := s.ListAtespaces(ctx, opts); return err }},
			{"actors", func(opts store.ListOptions) error { _, err := s.ListActors(ctx, "", opts); return err }},
			{"actor templates", func(opts store.ListOptions) error { _, err := s.ListActorTemplates(ctx, "", opts); return err }},
			{"actor snapshots", func(opts store.ListOptions) error { _, err := s.ListActorSnapshots(ctx, "", opts); return err }},
			{"workers", func(opts store.ListOptions) error { _, err := s.ListWorkers(ctx, opts); return err }},
		}
		for _, call := range calls {
			t.Run(call.name, func(t *testing.T) {
				if err := call.call(store.ListOptions{PageSize: 1, PageToken: "%%%"}); !errors.Is(err, store.ErrInvalidPageToken) {
					t.Errorf("malformed PageToken error = %v, want ErrInvalidPageToken", err)
				}
			})
		}
	})
}

func runActorContractTests(t *testing.T, setup func(t *testing.T) store.Interface) {
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
			Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
			Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
			Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		}

		created, err := s.CreateActor(ctx, actor)
		if err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}

		actorRef := resources.ActorRefFromActor(created)
		updated, err := s.UpdateActor(ctx, actorRef, store.PreconditionFrom(created), func(dbActor *ateapipb.Actor) error {
			dbActor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
			// Server-owned metadata must be derived from the stored resource, not
			// accepted from the mutation.
			dbActor.Metadata.Uid = "client-supplied-uid"
			dbActor.Metadata.CreateTime = timestamppb.New(time.Unix(1, 0))
			dbActor.Metadata.Version = 99
			return nil
		})
		if err != nil {
			t.Fatalf("UpdateActor failed: %v", err)
		}

		if updated.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
			t.Errorf("UpdateActor returned state %v, want RUNNING", updated.GetStatus().GetState())
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

		if created.GetMetadata().GetVersion() != 1 {
			t.Errorf("UpdateActor mutated the previously returned actor; version changed to %d", created.GetMetadata().GetVersion())
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
			Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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

		actorRef := resources.ActorRefFromActor(actor1)
		if _, err := s.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor1), func(dbActor *ateapipb.Actor) error {
			dbActor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
			return nil
		}); err != nil {
			t.Fatalf("UpdateActor failed: %v", err)
		}

		_, err = s.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor2), func(dbActor *ateapipb.Actor) error {
			dbActor.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
			return nil
		})
		if !errors.Is(err, store.ErrVersionConflict) {
			t.Errorf("expected ErrVersionConflict, got %v", err)
		}
	})

	t.Run("UpdateActor_UIDConflict", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)

		newActor := func() *ateapipb.Actor {
			return &ateapipb.Actor{
				Metadata:               &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
				ActorTemplateNamespace: "default",
				ActorTemplateName:      "test-template",
				Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
			}
		}

		original, err := s.CreateActor(ctx, newActor())
		if err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
		actorRef := resources.ActorRefFromActor(original)

		if _, err := s.UpdateActor(ctx, actorRef, store.PreconditionFrom(original), func(dbActor *ateapipb.Actor) error {
			dbActor.Status.State = ateapipb.ActorState_ACTOR_STATE_DELETING
			return nil
		}); err != nil {
			t.Fatalf("marking actor deleting failed: %v", err)
		}
		if _, err := s.DeleteActor(ctx, actorRef); err != nil {
			t.Fatalf("DeleteActor failed: %v", err)
		}
		recreated, err := s.CreateActor(ctx, newActor())
		if err != nil {
			t.Fatalf("recreate CreateActor failed: %v", err)
		}
		if recreated.GetMetadata().GetUid() == original.GetMetadata().GetUid() {
			t.Fatalf("recreated actor reused uid %s, want a fresh one", recreated.GetMetadata().GetUid())
		}

		// original and recreated have different UIDs, so the mutation must never run
		_, err = s.UpdateActor(ctx, actorRef, store.PreconditionFrom(original), func(dbActor *ateapipb.Actor) error {
			t.Error("mutate ran past its precondition once the guarded incarnation was gone")
			dbActor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
			return nil
		})
		if !errors.Is(err, store.ErrUIDConflict) {
			t.Errorf("UpdateActor error = %v, want one matching store.ErrUIDConflict", err)
		}

		got, err := s.GetActor(ctx, actorRef)
		if err != nil {
			t.Fatalf("GetActor failed: %v", err)
		}
		if diff := cmp.Diff(recreated, got, protocmp.Transform()); diff != "" {
			t.Errorf("rejected update changed the stored actor (-recreated +got):\n%s", diff)
		}
	})

	t.Run("UpdateActor_MutateError", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)

		created, err := s.CreateActor(ctx, &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
			ActorTemplateNamespace: "default",
			ActorTemplateName:      "test-template",
			Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		})
		if err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}

		mutateErr := errors.New("mutation rejected")
		if _, err := s.UpdateActor(ctx, resources.ActorRefFromActor(created), store.PreconditionFrom(created), func(dbActor *ateapipb.Actor) error {
			dbActor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
			return fmt.Errorf("checking mutation: %w", mutateErr)
		}); !errors.Is(err, mutateErr) {
			t.Fatalf("UpdateActor error = %v, want one wrapping %v", err, mutateErr)
		}

		got, err := s.GetActor(ctx, resources.ActorRefFromActor(created))
		if err != nil {
			t.Fatalf("GetActor after rejected mutation failed: %v", err)
		}
		if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
			t.Errorf("rejected mutation changed stored actor (-created +got):\n%s", diff)
		}
	})

	t.Run("UpdateActor_MissingPrecondition", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)

		created, err := s.CreateActor(ctx, &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
			ActorTemplateNamespace: "default",
			ActorTemplateName:      "test-template",
			Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		})
		if err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
		actorRef := resources.ActorRefFromActor(created)

		tests := []struct {
			name         string
			actorRef     resources.ActorRef
			precondition store.Precondition
		}{
			{
				name:         "no precondition",
				actorRef:     actorRef,
				precondition: store.Precondition{},
			},
			{
				name:         "guarding on only a uid",
				actorRef:     actorRef,
				precondition: store.Precondition{UID: created.GetMetadata().GetUid()},
			},
			{
				name:         "guarding on only a version",
				actorRef:     actorRef,
				precondition: store.Precondition{Version: created.GetMetadata().GetVersion()},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := s.UpdateActor(ctx, tt.actorRef, tt.precondition, func(dbActor *ateapipb.Actor) error {
					t.Fatal("mutate ran for a blind write")
					return nil
				})
				if !errors.Is(err, store.ErrPreconditionRequired) {
					t.Errorf("UpdateActor error = %v, want one matching store.ErrPreconditionRequired", err)
				}
			})
		}
	})

	t.Run("DeleteActor", func(t *testing.T) {
		tests := []struct {
			name    string
			state   ateapipb.ActorState
			wantErr error
		}{
			{name: "suspended", state: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, wantErr: store.ErrFailedPrecondition},
			{name: "crashed", state: ateapipb.ActorState_ACTOR_STATE_CRASHED, wantErr: store.ErrFailedPrecondition},
			{name: "deleting", state: ateapipb.ActorState_ACTOR_STATE_DELETING},
			{name: "running", state: ateapipb.ActorState_ACTOR_STATE_RUNNING, wantErr: store.ErrFailedPrecondition},
			{name: "paused", state: ateapipb.ActorState_ACTOR_STATE_PAUSED, wantErr: store.ErrFailedPrecondition},
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
					Status:                 &ateapipb.ActorStatus{State: tt.state},
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
					if got.GetStatus().GetState() != tt.state {
						t.Errorf("actor state after rejected delete = %v, want %v", got.GetStatus().GetState(), tt.state)
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

	t.Run("ListActors_Empty", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		actorsResp, err := s.ListActors(ctx, "", store.ListOptions{PageSize: 1000})
		if err != nil {
			t.Fatalf("ListActors failed: %v", err)
		}
		if len(actorsResp.Items) != 0 {
			t.Errorf("expected 0 actors, got %d", len(actorsResp.Items))
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
			Status: &ateapipb.ActorStatus{
				State:          ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
				LatestSnapshot: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "snapshot-1"},
			},
		}
		actor2 := &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Name: "id2", Atespace: testAtespace},
			ActorTemplateNamespace: "ns1",
			ActorTemplateName:      "tmpl1",
			Status: &ateapipb.ActorStatus{
				State:          ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
				LatestSnapshot: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "snapshot-2"},
			},
		}
		if _, err := s.CreateActor(ctx, actor1); err != nil {
			t.Fatalf("failed to create actor1: %v", err)
		}
		if _, err := s.CreateActor(ctx, actor2); err != nil {
			t.Fatalf("failed to create actor2: %v", err)
		}

		actorsResp, err := s.ListActors(ctx, "", store.ListOptions{PageSize: 1000})
		if err != nil {
			t.Fatalf("ListActors failed: %v", err)
		}
		actors := actorsResp.Items
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
				Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
			}
			if _, err := s.CreateActor(ctx, actor); err != nil {
				t.Fatalf("failed to create actor %d: %v", i, err)
			}
		}

		var allActors []*ateapipb.Actor
		pageToken := ""
		for {
			page, err := s.ListActors(ctx, "", store.ListOptions{PageSize: 2, PageToken: pageToken})
			if err != nil {
				t.Fatalf("ListActors failed: %v", err)
			}
			allActors = append(allActors, page.Items...)
			pageToken = page.NextPageToken
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
				Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
			}
		}
		for _, a := range []*ateapipb.Actor{mkActor("team-a", "a1"), mkActor("team-a", "a2"), mkActor("team-b", "b1")} {
			if _, err := s.CreateActor(ctx, a); err != nil {
				t.Fatalf("CreateActor(%s/%s) failed: %v", a.GetMetadata().GetAtespace(), a.GetMetadata().GetName(), err)
			}
		}

		teamAResp, err := s.ListActors(ctx, "team-a", store.ListOptions{PageSize: 1000})
		if err != nil {
			t.Fatalf("ListActors(team-a) failed: %v", err)
		}
		if got := actorNameSet(teamAResp.Items); !got["a1"] || !got["a2"] || got["b1"] || len(got) != 2 {
			t.Errorf("ListActors(team-a) = %v, want exactly {a1, a2}", got)
		}

		teamBResp, err := s.ListActors(ctx, "team-b", store.ListOptions{PageSize: 1000})
		if err != nil {
			t.Fatalf("ListActors(team-b) failed: %v", err)
		}
		if got := actorNameSet(teamBResp.Items); !got["b1"] || got["a1"] || len(got) != 1 {
			t.Errorf("ListActors(team-b) = %v, want exactly {b1}", got)
		}

		allResp, err := s.ListActors(ctx, "", store.ListOptions{PageSize: 1000})
		if err != nil {
			t.Fatalf("ListActors(all) failed: %v", err)
		}
		if got := actorNameSet(allResp.Items); !got["a1"] || !got["a2"] || !got["b1"] || len(got) != 3 {
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
}

func runActorTemplateContractTests(t *testing.T, setup func(t *testing.T) store.Interface) {
	t.Helper()

	t.Run("ActorTemplateAndVersion_Lifecycle", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, "team-a")

		input := newTestActorTemplate("team-a", "tmpl-a")
		created, err := s.CreateActorTemplate(ctx, input)
		if err != nil {
			t.Fatalf("CreateActorTemplate failed: %v", err)
		}
		if created.GetMetadata().GetUid() == "" || created.GetMetadata().GetVersion() != 1 {
			t.Errorf("created template metadata = %v, want assigned uid and version 1", created.GetMetadata())
		}
		if input.GetMetadata().GetUid() != "" || input.GetMetadata().GetVersion() != 0 {
			t.Errorf("CreateActorTemplate mutated its input: %v", input.GetMetadata())
		}
		templateRef := resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-a"}
		gotTemplate, err := s.GetActorTemplate(ctx, templateRef)
		if err != nil {
			t.Fatalf("GetActorTemplate failed: %v", err)
		}
		if diff := cmp.Diff(created, gotTemplate, protocmp.Transform()); diff != "" {
			t.Errorf("stored template mismatch (-created +got):\n%s", diff)
		}

		if _, err := s.DeleteActorTemplate(ctx, templateRef); err != nil {
			t.Fatalf("DeleteActorTemplate failed: %v", err)
		}
	})

	t.Run("UpdateActorTemplate_MissingPrecondition", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, "team-a")

		created, err := s.CreateActorTemplate(ctx, newTestActorTemplate("team-a", "tmpl-a"))
		if err != nil {
			t.Fatalf("CreateActorTemplate failed: %v", err)
		}
		templateRef := resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-a"}

		tests := []struct {
			name         string
			precondition store.Precondition
		}{
			{
				name:         "no precondition",
				precondition: store.Precondition{},
			},
			{
				name:         "guarding on only a uid",
				precondition: store.Precondition{UID: created.GetMetadata().GetUid()},
			},
			{
				name:         "guarding on only a version",
				precondition: store.Precondition{Version: created.GetMetadata().GetVersion()},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := s.UpdateActorTemplate(ctx, templateRef, tt.precondition, func(dbTemplate *ateapipb.ActorTemplate) error {
					t.Fatal("mutate ran for a blind write")
					return nil
				})
				if !errors.Is(err, store.ErrPreconditionRequired) {
					t.Errorf("UpdateActorTemplate error = %v, want one matching store.ErrPreconditionRequired", err)
				}
			})
		}
	})

	t.Run("ActorTemplateResources_BlockAtespaceDeletion", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, "team-a")
		if _, err := s.CreateActorTemplate(ctx, newTestActorTemplate("team-a", "tmpl-a")); err != nil {
			t.Fatalf("CreateActorTemplate failed: %v", err)
		}
		if _, err := s.DeleteAtespace(ctx, "team-a"); !errors.Is(err, store.ErrFailedPrecondition) {
			t.Errorf("DeleteAtespace with template = %v, want ErrFailedPrecondition", err)
		}
	})
}

func runActorSnapshotContractTests(t *testing.T, setup func(t *testing.T) store.Interface) {
	t.Helper()

	t.Run("ActorSnapshotAndTag_Lifecycle", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, "team-a")

		input := &ateapipb.ActorSnapshot{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "snapshot-1"},
			Status: &ateapipb.ActorSnapshotStatus{
				SourceActor:        &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-1"},
				SourceActorUid:     "actor-uid",
				SourceActorVersion: 7,
				ContentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
				SnapshotUri:        "gs://private/snapshot-1",
			},
		}
		created, err := s.CreateActorSnapshot(ctx, input)
		if err != nil {
			t.Fatalf("CreateActorSnapshot failed: %v", err)
		}
		if created.GetMetadata().GetVersion() != 1 || created.GetMetadata().GetUid() == "" {
			t.Errorf("created snapshot metadata = %v, want server-owned uid and version 1", created.GetMetadata())
		}
		if input.GetMetadata().GetUid() != "" || input.GetMetadata().GetVersion() != 0 {
			t.Errorf("CreateActorSnapshot mutated its input metadata: %v", input.GetMetadata())
		}
		if _, err := s.CreateActorSnapshot(ctx, input); !errors.Is(err, store.ErrAlreadyExists) {
			t.Errorf("duplicate CreateActorSnapshot = %v, want ErrAlreadyExists", err)
		}

		got, err := s.GetActorSnapshot(ctx, resources.ActorSnapshotRef{Atespace: "team-a", Name: "snapshot-1"})
		if err != nil {
			t.Fatalf("GetActorSnapshot failed: %v", err)
		}
		if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
			t.Errorf("GetActorSnapshot mismatch (-created +got):\n%s", diff)
		}
		if got.GetStatus().GetSnapshotUri() != "gs://private/snapshot-1" {
			t.Errorf("snapshot_uri = %q, want gs://private/snapshot-1", got.GetStatus().GetSnapshotUri())
		}
		if _, err := s.GetActorSnapshot(ctx, resources.ActorSnapshotRef{Atespace: "team-a", Name: "missing"}); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("missing GetActorSnapshot = %v, want ErrNotFound", err)
		}

		tagInput := &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "production"},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		}
		tag, err := s.CreateActorSnapshotTag(ctx, resources.ActorSnapshotRef{Atespace: "team-a", Name: "snapshot-1"}, tagInput)
		if err != nil {
			t.Fatalf("CreateActorSnapshotTag failed: %v", err)
		}
		if tag.GetSnapshot().GetAtespace() != "team-a" || tag.GetSnapshot().GetName() != "snapshot-1" {
			t.Errorf("tag snapshot = %v, want team-a/snapshot-1", tag.GetSnapshot())
		}
		if tagInput.GetSnapshot() != nil || tagInput.GetMetadata().GetVersion() != 0 {
			t.Errorf("CreateActorSnapshotTag mutated its input: %v", tagInput)
		}
		idempotent, err := s.CreateActorSnapshotTag(ctx, resources.ActorSnapshotRef{Atespace: "team-a", Name: "snapshot-1"}, tagInput)
		if err != nil || !proto.Equal(idempotent, tag) {
			t.Errorf("idempotent CreateActorSnapshotTag = (%v, %v), want existing tag", idempotent, err)
		}
		conflicting := proto.Clone(tagInput).(*ateapipb.ActorSnapshotTag)
		conflicting.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
		if _, err := s.CreateActorSnapshotTag(ctx, resources.ActorSnapshotRef{Atespace: "team-a", Name: "snapshot-1"}, conflicting); !errors.Is(err, store.ErrAlreadyExists) {
			t.Errorf("conflicting CreateActorSnapshotTag = %v, want ErrAlreadyExists", err)
		}
		if _, err := s.CreateActorSnapshotTag(ctx, resources.ActorSnapshotRef{Atespace: "team-a", Name: "missing"}, &ateapipb.ActorSnapshotTag{Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "missing"}}); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("tagging missing snapshot = %v, want ErrNotFound", err)
		}

		resolvedTag, err := s.GetActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "production"})
		if err != nil {
			t.Fatalf("GetActorSnapshotTag failed: %v", err)
		}
		if !proto.Equal(resolvedTag, tag) {
			t.Errorf("resolved tag = %v, want created tag", resolvedTag)
		}
		resolved, err := s.GetActorSnapshot(ctx, resources.ActorSnapshotRefFromObjectRef(resolvedTag.GetSnapshot()))
		if err != nil || !proto.Equal(resolved, created) {
			t.Errorf("GetActorSnapshot(resolved tag target) = (%v, %v), want created snapshot", resolved, err)
		}

		updated, err := s.UpdateActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "production"}, store.PreconditionFrom(tag), func(toUpdate *ateapipb.ActorSnapshotTag) error {
			toUpdate.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
			return nil
		})
		if err != nil {
			t.Fatalf("UpdateActorSnapshotTag failed: %v", err)
		}
		if updated.GetScope() != ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED || updated.GetMetadata().GetVersion() != tag.GetMetadata().GetVersion()+1 {
			t.Errorf("updated tag = %v, want published scope and advanced version", updated)
		}
		if _, err := s.UpdateActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "production"}, store.PreconditionFrom(tag), func(toUpdate *ateapipb.ActorSnapshotTag) error {
			toUpdate.Scope = tag.GetScope()
			return nil
		}); !errors.Is(err, store.ErrVersionConflict) {
			t.Errorf("stale UpdateActorSnapshotTag = %v, want ErrVersionConflict", err)
		}
		if _, err := s.DeleteAtespace(ctx, "team-a"); !errors.Is(err, store.ErrFailedPrecondition) {
			t.Errorf("DeleteAtespace with tag = %v, want ErrFailedPrecondition", err)
		}

		deleted, err := s.DeleteActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "production"})
		if err != nil || !proto.Equal(deleted, updated) {
			t.Errorf("DeleteActorSnapshotTag = (%v, %v), want updated tag", deleted, err)
		}
		if _, err := s.GetActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "production"}); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("deleted GetActorSnapshotTag = %v, want ErrNotFound", err)
		}
		if _, err := s.DeleteAtespace(ctx, "team-a"); err != nil {
			t.Errorf("DeleteAtespace after tag deletion = %v, want nil", err)
		}
	})

	t.Run("UpdateActorSnapshotTag_MissingPrecondition", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, "team-a")

		if _, err := s.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "snapshot-1"},
			Status: &ateapipb.ActorSnapshotStatus{
				SourceActor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-1"},
				SnapshotUri: "gs://private/snapshot-1",
			},
		}); err != nil {
			t.Fatalf("CreateActorSnapshot failed: %v", err)
		}
		created, err := s.CreateActorSnapshotTag(ctx, resources.ActorSnapshotRef{Atespace: "team-a", Name: "snapshot-1"}, &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "production"},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		})
		if err != nil {
			t.Fatalf("CreateActorSnapshotTag failed: %v", err)
		}

		tests := []struct {
			name         string
			precondition store.Precondition
		}{
			{
				name:         "no precondition",
				precondition: store.Precondition{},
			},
			{
				name:         "guarding on only a uid",
				precondition: store.Precondition{UID: created.GetMetadata().GetUid()},
			},
			{
				name:         "guarding on only a version",
				precondition: store.Precondition{Version: created.GetMetadata().GetVersion()},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := s.UpdateActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "production"}, tt.precondition, func(toUpdate *ateapipb.ActorSnapshotTag) error {
					t.Fatal("mutate ran for a blind write")
					return nil
				})
				if !errors.Is(err, store.ErrPreconditionRequired) {
					t.Errorf("UpdateActorSnapshotTag error = %v, want one matching store.ErrPreconditionRequired", err)
				}
			})
		}
	})

	t.Run("ListActorSnapshots_PaginationAndScope", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		for _, atespace := range []string{"team-a", "team-b"} {
			for i := 0; i < 3; i++ {
				name := fmt.Sprintf("snapshot-%d", i)
				if _, err := s.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{
					Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
					Status:   &ateapipb.ActorSnapshotStatus{SnapshotUri: "gs://private/" + atespace + "/" + name},
				}); err != nil {
					t.Fatalf("CreateActorSnapshot(%s/%s) failed: %v", atespace, name, err)
				}
			}
		}

		var scoped []*ateapipb.ActorSnapshot
		for token := ""; ; {
			page, err := s.ListActorSnapshots(ctx, "team-a", store.ListOptions{PageSize: 2, PageToken: token})
			if err != nil {
				t.Fatalf("scoped ListActorSnapshots failed: %v", err)
			}
			scoped = append(scoped, page.Items...)
			if page.NextPageToken == "" {
				break
			}
			token = page.NextPageToken
		}
		if len(scoped) != 3 {
			t.Errorf("scoped ListActorSnapshots returned %d snapshots, want 3", len(scoped))
		}

		var global []*ateapipb.ActorSnapshot
		for token := ""; ; {
			page, err := s.ListActorSnapshots(ctx, "", store.ListOptions{PageSize: 2, PageToken: token})
			if err != nil {
				t.Fatalf("global ListActorSnapshots failed: %v", err)
			}
			global = append(global, page.Items...)
			if page.NextPageToken == "" {
				break
			}
			token = page.NextPageToken
		}
		if len(global) != 6 {
			t.Errorf("global ListActorSnapshots returned %d snapshots, want 6", len(global))
		}
	})
}

func runWorkerContractTests(t *testing.T, setup func(t *testing.T) store.Interface) {
	t.Helper()

	t.Run("GetWorker_NotFound", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		_, err := s.GetWorker(ctx, testWorkerName)
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

		worker := newTestWorker(testWorkerName, "pod-1")
		created, err := s.CreateWorker(ctx, worker)
		if err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		got, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if diff := cmp.Diff(got, created, protocmp.Transform()); diff != "" {
			t.Errorf("CreateWorker returned a different worker than it stored (-stored +returned):\n%s", diff)
		}
		if got.GetMetadata().GetUid() == "" {
			t.Errorf("CreateWorker stored an empty uid; want server-assigned uid")
		}
		if got.GetMetadata().GetVersion() != 1 {
			t.Errorf("expected version 1, got %d", got.GetMetadata().GetVersion())
		}
		if got.GetMetadata().GetAtespace() != "" {
			t.Errorf("expected empty atespace, got %q", got.GetMetadata().GetAtespace())
		}
		if got.GetMetadata().GetCreateTime() == nil || got.GetMetadata().GetUpdateTime() == nil {
			t.Errorf("CreateWorker stored unset create/update time")
		}

		want := proto.Clone(worker).(*ateapipb.Worker)
		want.Metadata.Version = 1
		if diff := cmp.Diff(want, got, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
			t.Errorf("GetWorker returned unexpected worker (-want +got):\n%s", diff)
		}

		event := receiveEvent(t, watch.Events)
		if event.Type != store.WorkerEventCreated {
			t.Errorf("expected WorkerEventCreated, got %v", event.Type)
		}
		if diff := cmp.Diff(got, event.Worker, protocmp.Transform()); diff != "" {
			t.Errorf("created event worker mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("CreateWorker_AlreadyExists", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		worker := newTestWorker(testWorkerName, "pod-1")
		if _, err := s.CreateWorker(ctx, worker); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		if _, err := s.CreateWorker(ctx, worker); !errors.Is(err, store.ErrAlreadyExists) {
			t.Errorf("expected ErrAlreadyExists, got %v", err)
		}
	})

	t.Run("UpdateWorker_Success", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		worker := newTestWorker(testWorkerName, "pod-1")
		created, err := s.CreateWorker(ctx, worker)
		if err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		// Subscribe after create so the create event doesn't pollute the channel.
		watch, err := s.WatchWorkers(ctx)
		if err != nil {
			t.Fatalf("WatchWorkers failed: %v", err)
		}
		defer watch.Close()

		assignment := &ateapipb.ActorAssignment{
			ActorTemplate: &ateapipb.KubeNamespacedObjectRef{Namespace: "default", Name: "test-template"},
			Actor:         &ateapipb.ObjectRef{Name: "session-1"},
		}
		updated, err := s.UpdateWorker(ctx, testWorkerName, store.PreconditionFrom(created), func(toUpdate *ateapipb.Worker) error {
			toUpdate.Status.Assignment = assignment
			return nil
		})
		if err != nil {
			t.Fatalf("UpdateWorker failed: %v", err)
		}

		got, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if diff := cmp.Diff(got, updated, protocmp.Transform()); diff != "" {
			t.Errorf("UpdateWorker returned a different worker than it stored (-stored +returned):\n%s", diff)
		}
		if got.GetMetadata().GetVersion() != 2 {
			t.Errorf("expected version 2, got %d", got.GetMetadata().GetVersion())
		}

		want := proto.Clone(worker).(*ateapipb.Worker)
		want.Status.Assignment = assignment
		want.Metadata.Version = 2
		if diff := cmp.Diff(want, got, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
			t.Errorf("UpdateWorker yielded unexpected state in DB (-want +got):\n%s", diff)
		}

		event := receiveEvent(t, watch.Events)
		if event.Type != store.WorkerEventUpdated {
			t.Errorf("expected WorkerEventUpdated, got %v", event.Type)
		}
		if diff := cmp.Diff(got, event.Worker, protocmp.Transform()); diff != "" {
			t.Errorf("updated event worker mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("UpdateWorker_NotFound", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		// A well-formed precondition, so it is the missing worker rather than the
		// guard that decides the error.
		pre := store.Precondition{UID: otherTestWorkerName, Version: 1}
		_, err := s.UpdateWorker(ctx, testWorkerName, pre, func(*ateapipb.Worker) error {
			t.Error("mutate ran for a worker that does not exist")
			return nil
		})
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("UpdateWorker_MissingPrecondition", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		created, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1"))
		if err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		for _, tt := range []struct {
			name         string
			precondition store.Precondition
		}{
			{"no precondition", store.Precondition{}},
			{"guarding on only a uid", store.Precondition{UID: created.GetMetadata().GetUid()}},
			{"guarding on only a version", store.Precondition{Version: created.GetMetadata().GetVersion()}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				_, err := s.UpdateWorker(ctx, testWorkerName, tt.precondition, func(*ateapipb.Worker) error {
					t.Error("mutate ran for a blind write")
					return nil
				})
				if !errors.Is(err, store.ErrPreconditionRequired) {
					t.Errorf("UpdateWorker error = %v, want one matching store.ErrPreconditionRequired", err)
				}
			})
		}
	})

	// A worker name is a pod UID, so a name is only reused when the same pod is
	// re-registered. The store still hands out a fresh uid, and a guard naming
	// the old one must not reach the new incarnation.
	t.Run("UpdateWorker_UIDConflict", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		original, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1"))
		if err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		if _, err := s.DeleteWorker(ctx, testWorkerName, store.DeletePreconditions{}); err != nil {
			t.Fatalf("DeleteWorker failed: %v", err)
		}
		recreated, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1"))
		if err != nil {
			t.Fatalf("recreate CreateWorker failed: %v", err)
		}
		if recreated.GetMetadata().GetUid() == original.GetMetadata().GetUid() {
			t.Fatalf("recreated worker reused uid %s, want a fresh one", recreated.GetMetadata().GetUid())
		}

		_, err = s.UpdateWorker(ctx, testWorkerName, store.PreconditionFrom(original), func(toUpdate *ateapipb.Worker) error {
			t.Error("mutate ran past its precondition once the guarded incarnation was gone")
			toUpdate.SandboxClass = "edited-anyway"
			return nil
		})
		if !errors.Is(err, store.ErrUIDConflict) {
			t.Errorf("UpdateWorker error = %v, want one matching store.ErrUIDConflict", err)
		}

		got, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if diff := cmp.Diff(recreated, got, protocmp.Transform()); diff != "" {
			t.Errorf("rejected update changed the stored worker (-recreated +got):\n%s", diff)
		}
	})

	t.Run("UpdateWorker_Conflict", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		// Both readers observe version 1; the first update moves the worker
		// past it, so the second one's precondition can no longer hold.
		observed, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if _, err := s.UpdateWorker(ctx, testWorkerName, store.PreconditionFrom(observed), func(toUpdate *ateapipb.Worker) error {
			toUpdate.Status.Assignment = &ateapipb.ActorAssignment{Actor: &ateapipb.ObjectRef{Name: "session-1"}}
			return nil
		}); err != nil {
			t.Fatalf("UpdateWorker failed: %v", err)
		}

		_, err = s.UpdateWorker(ctx, testWorkerName, store.PreconditionFrom(observed), func(toUpdate *ateapipb.Worker) error {
			toUpdate.Status.Assignment = &ateapipb.ActorAssignment{Actor: &ateapipb.ObjectRef{Name: "session-2"}}
			return nil
		})
		if !errors.Is(err, store.ErrVersionConflict) {
			t.Errorf("expected ErrVersionConflict, got %v", err)
		}
	})

	// A mutation that reports an error leaves the worker exactly as it was, at
	// the version it was already at. Callers depend on this to report "already
	// in the desired state" without a write: DrainWorker on a worker already
	// DRAINING, and the in-process release of a worker that is not assigned.
	t.Run("UpdateWorker_MutateError", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		created, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1"))
		if err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		sentinel := errors.New("nothing to do")
		_, err = s.UpdateWorker(ctx, testWorkerName, store.PreconditionFrom(created), func(toUpdate *ateapipb.Worker) error {
			toUpdate.SandboxClass = "edited-anyway"
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Errorf("expected the mutate's error verbatim, got %v", err)
		}

		got, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if got.GetSandboxClass() != "" {
			t.Errorf("aborted mutation was written: sandbox_class is %q", got.GetSandboxClass())
		}
		if got.GetMetadata().GetVersion() != 1 {
			t.Errorf("aborted mutation bumped the version to %d, want 1", got.GetMetadata().GetVersion())
		}
	})

	// Every backend must reject a mutation that touches an immutable field, and
	// must name the field it rejected on. This is the case that holds any new
	// backend to that.
	t.Run("UpdateWorker_ImmutableFields", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		// Every case below is rejected, so nothing writes and this stays the
		// current incarnation for all of them.
		created, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1"))
		if err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		for _, tc := range []struct {
			name   string
			field  string
			mutate func(*ateapipb.Worker)
		}{
			{"worker_namespace", "worker_namespace", func(w *ateapipb.Worker) { w.WorkerNamespace = "other-ns" }},
			{"worker_pool", "worker_pool", func(w *ateapipb.Worker) { w.WorkerPool = "other-pool" }},
			{"worker_pod", "worker_pod", func(w *ateapipb.Worker) { w.WorkerPod = "other-pod" }},
			{"worker_pod_uid", "worker_pod_uid", func(w *ateapipb.Worker) { w.WorkerPodUid = otherTestWorkerName }},
			{"node_name", "node_name", func(w *ateapipb.Worker) { w.NodeName = "other-node" }},
			{"ip", "ip", func(w *ateapipb.Worker) { w.Ip = "10.0.0.9" }},
			{"capacity_changed", "capacity", func(w *ateapipb.Worker) { w.Capacity.CpuMilli = 4000 }},
			// An update replaces the worker, so a caller that leaves capacity
			// out is asking to clear it. That is a change like any other.
			{"capacity_cleared", "capacity", func(w *ateapipb.Worker) { w.Capacity = nil }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := s.UpdateWorker(ctx, testWorkerName, store.PreconditionFrom(created), func(toUpdate *ateapipb.Worker) error {
					tc.mutate(toUpdate)
					return nil
				})
				if !errors.Is(err, store.ErrImmutableField) {
					t.Fatalf("changing %s returned %v, want ErrImmutableField", tc.field, err)
				}
				if !strings.Contains(err.Error(), tc.field) {
					t.Errorf("error %v does not name the offending field %s", err, tc.field)
				}
				got, err := s.GetWorker(ctx, testWorkerName)
				if err != nil {
					t.Fatalf("GetWorker failed: %v", err)
				}
				if got.GetMetadata().GetVersion() != 1 {
					t.Errorf("rejected mutation bumped the version to %d, want 1", got.GetMetadata().GetVersion())
				}
			})
		}
	})

	// Claimants that all observed the same free worker must not all win. Two
	// things keep that true and this exercises both: the precondition rejects
	// every claimant whose read the winner has since invalidated, and the
	// occupancy test inside mutate runs against the state the write lands on
	// rather than the state the claimant read.
	t.Run("UpdateWorker_ConcurrentAssign", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		created, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1"))
		if err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		const claimants = 8
		errTaken := errors.New("already assigned")
		var wg sync.WaitGroup
		won := make([]bool, claimants)
		for i := range claimants {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.UpdateWorker(ctx, testWorkerName, store.PreconditionFrom(created), func(toUpdate *ateapipb.Worker) error {
					if toUpdate.GetStatus().GetAssignment() != nil {
						return errTaken
					}
					toUpdate.Status.Assignment = &ateapipb.ActorAssignment{
						Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: fmt.Sprintf("actor-%d", i)},
						ActorUid: fmt.Sprintf("uid-%d", i),
					}
					return nil
				})
				switch {
				case err == nil:
					won[i] = true
				case errors.Is(err, errTaken), errors.Is(err, store.ErrVersionConflict):
				default:
					t.Errorf("claimant %d: unexpected error %v", i, err)
				}
			}()
		}
		wg.Wait()

		winners := 0
		for _, w := range won {
			if w {
				winners++
			}
		}
		if winners != 1 {
			t.Fatalf("%d of %d claimants won the assignment, want exactly 1", winners, claimants)
		}

		got, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if uid := got.GetStatus().GetAssignment().GetActorUid(); !strings.HasPrefix(uid, "uid-") {
			t.Errorf("stored assignment names %q, want one of the claimants", uid)
		}
		// One winning write on top of the create, and no partial ones.
		if got.GetMetadata().GetVersion() != 2 {
			t.Errorf("worker is at version %d, want 2 (create plus the single winning assign)", got.GetMetadata().GetVersion())
		}
	})

	t.Run("DeleteWorker", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		created, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1"))
		if err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		watch, err := s.WatchWorkers(ctx)
		if err != nil {
			t.Fatalf("WatchWorkers failed: %v", err)
		}
		defer watch.Close()

		deleted, err := s.DeleteWorker(ctx, testWorkerName, store.DeletePreconditions{})
		if err != nil {
			t.Fatalf("DeleteWorker failed: %v", err)
		}
		if diff := cmp.Diff(created, deleted, protocmp.Transform()); diff != "" {
			t.Errorf("DeleteWorker returned something other than what it removed (-want +got):\n%s", diff)
		}
		if _, err := s.GetWorker(ctx, testWorkerName); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("expected ErrNotFound after delete, got %v", err)
		}

		event := receiveEvent(t, watch.Events)
		if event.Type != store.WorkerEventDeleted {
			t.Errorf("expected WorkerEventDeleted, got %v", event.Type)
		}
		if name := event.Worker.GetMetadata().GetName(); name != testWorkerName {
			t.Errorf("deleted event named %q, want %q", name, testWorkerName)
		}
	})

	// Absence is reported, not swallowed. Deletes of Workers used to succeed
	// silently, unlike every other Delete on the interface; callers that want
	// re-drivable cleanup treat ErrNotFound as success themselves.
	t.Run("DeleteWorker_NotFound", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.DeleteWorker(ctx, testWorkerName, store.DeletePreconditions{}); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("expected ErrNotFound deleting a missing worker, got %v", err)
		}
	})

	t.Run("DeleteWorker_Preconditions", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		created, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1"))
		if err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		uid, version := created.GetMetadata().GetUid(), created.GetMetadata().GetVersion()

		if _, err := s.DeleteWorker(ctx, testWorkerName, store.DeletePreconditions{Version: version + 1}); !errors.Is(err, store.ErrVersionConflict) {
			t.Errorf("expected ErrVersionConflict for a stale version, got %v", err)
		}
		if _, err := s.DeleteWorker(ctx, testWorkerName, store.DeletePreconditions{UID: otherTestWorkerName}); !errors.Is(err, store.ErrUIDConflict) {
			t.Errorf("expected ErrUIDConflict for a foreign uid, got %v", err)
		}
		if _, err := s.GetWorker(ctx, testWorkerName); err != nil {
			t.Fatalf("a rejected delete removed the worker anyway: %v", err)
		}

		if _, err := s.DeleteWorker(ctx, testWorkerName, store.DeletePreconditions{UID: uid, Version: version}); err != nil {
			t.Errorf("DeleteWorker with matching preconditions failed: %v", err)
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

	t.Run("ListWorkers", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod1")); err != nil {
			t.Fatalf("failed to create worker1: %v", err)
		}
		if _, err := s.CreateWorker(ctx, newTestWorker(otherTestWorkerName, "pod2")); err != nil {
			t.Fatalf("failed to create worker2: %v", err)
		}

		workersResp, err := s.ListWorkers(ctx, store.ListOptions{PageSize: 1000})
		if err != nil {
			t.Fatalf("ListWorkers failed: %v", err)
		}
		workers := workersResp.Items
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

		workersResp, err := s.ListWorkers(ctx, store.ListOptions{PageSize: 1000})
		if err != nil {
			t.Fatalf("ListWorkers failed: %v", err)
		}
		if len(workersResp.Items) != 0 {
			t.Errorf("expected 0 workers, got %d", len(workersResp.Items))
		}
	})

	t.Run("ListWorkers_Pagination", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		for i := 0; i < 5; i++ {
			worker := newTestWorker(fmt.Sprintf("bb2e6a1c-0000-4000-8000-00000000000%d", i), fmt.Sprintf("pod%d", i))
			if _, err := s.CreateWorker(ctx, worker); err != nil {
				t.Fatalf("failed to create worker %d: %v", i, err)
			}
		}

		var allWorkers []*ateapipb.Worker
		pageToken := ""
		for {
			page, err := s.ListWorkers(ctx, store.ListOptions{PageSize: 2, PageToken: pageToken})
			if err != nil {
				t.Fatalf("ListWorkers failed: %v", err)
			}
			allWorkers = append(allWorkers, page.Items...)
			pageToken = page.NextPageToken
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
}

func runAtespaceContractTests(t *testing.T, setup func(t *testing.T) store.Interface) {
	t.Helper()

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
			page, err := s.ListAtespaces(ctx, store.ListOptions{PageSize: 2, PageToken: pageToken})
			if err != nil {
				t.Fatalf("ListAtespaces failed: %v", err)
			}
			allAtespaces = append(allAtespaces, page.Items...)
			pageToken = page.NextPageToken
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

	t.Run("ListAtespaces", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		names := []string{"team-a", "team-b", "team-c"}
		for _, n := range names {
			if _, err := s.CreateAtespace(ctx, newTestAtespace(n)); err != nil {
				t.Fatalf("CreateAtespace(%s) failed: %v", n, err)
			}
		}
		gotResp, err := s.ListAtespaces(ctx, store.ListOptions{PageSize: 1000})
		if err != nil {
			t.Fatalf("ListAtespaces failed: %v", err)
		}
		got := gotResp.Items
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

		got, err := s.ListAtespaces(ctx, store.ListOptions{PageSize: 1000})
		if err != nil {
			t.Fatalf("ListAtespaces failed: %v", err)
		}
		if len(got.Items) != 0 {
			t.Errorf("ListAtespaces on empty store = %v, want empty", got.Items)
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
		if _, err := s.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: "id1", Atespace: "team-a"}, Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING}}); err != nil {
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
		if _, err := s.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: "id1", Atespace: "team-a"}, Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING}}); err != nil {
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
		if _, err := s.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: "id1", Atespace: "team-b"}, Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED}}); err != nil {
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
}

func runLeaseContractTests(t *testing.T, setup func(t *testing.T) store.Interface) {
	t.Helper()

	t.Run("AcquireLease_Success", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		lease, err := s.AcquireLease(ctx, "test-lease")
		if err != nil {
			t.Fatalf("AcquireLease failed: %v", err)
		}
		if lease == nil {
			t.Fatal("AcquireLease returned a nil lease")
		}
		if err := lease.Context().Err(); err != nil {
			t.Errorf("new lease context is already done: %v", err)
		}
		lease.Close()
	})

	t.Run("AcquireLease_Conflict", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		lease, err := s.AcquireLease(ctx, "test-lease")
		if err != nil {
			t.Fatalf("first AcquireLease failed: %v", err)
		}
		defer lease.Close()

		if _, err := s.AcquireLease(ctx, "test-lease"); !errors.Is(err, store.ErrLeaseConflict) {
			t.Errorf("second AcquireLease error = %v, want ErrLeaseConflict", err)
		}
	})

	t.Run("AcquireLease_NonReentry", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		lease, err := s.AcquireLease(ctx, "test-lease")
		if err != nil {
			t.Fatalf("first AcquireLease failed: %v", err)
		}
		defer lease.Close()

		if _, err := s.AcquireLease(ctx, "test-lease"); !errors.Is(err, store.ErrLeaseConflict) {
			t.Errorf("reentrant AcquireLease error = %v, want ErrLeaseConflict", err)
		}
	})

	t.Run("Lease_Close_Releases", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		lease, err := s.AcquireLease(ctx, "test-lease")
		if err != nil {
			t.Fatalf("AcquireLease failed: %v", err)
		}
		lease.Close()

		newLease, err := s.AcquireLease(ctx, "test-lease")
		if err != nil {
			t.Fatalf("AcquireLease after Close failed: %v", err)
		}
		newLease.Close()
	})

	t.Run("Lease_Close_Idempotent", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		lease, err := s.AcquireLease(ctx, "test-lease")
		if err != nil {
			t.Fatalf("AcquireLease failed: %v", err)
		}
		lease.Close()
		lease.Close()
	})
}

// runDebugContractTests covers store-wide debug operations, which span every
// resource kind rather than belonging to any single one.
func runDebugContractTests(t *testing.T, setup func(t *testing.T) store.Interface) {
	t.Helper()

	t.Run("DebugClearAll", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
			t.Fatalf("CreateAtespace failed: %v", err)
		}
		if _, err := s.CreateActor(ctx, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Name: "id1", Atespace: "team-a"},
			Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		}); err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
		if _, err := s.CreateWorker(ctx, &ateapipb.Worker{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "pod"}); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		lease, err := s.AcquireLease(ctx, "lease-1")
		if err != nil {
			t.Fatalf("AcquireLease failed: %v", err)
		}
		defer lease.Close()

		if err := s.DebugClearAll(ctx); err != nil {
			t.Fatalf("DebugClearAll failed: %v", err)
		}

		if _, err := s.GetAtespace(ctx, "team-a"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("atespace survived DebugClearAll: %v", err)
		}
		if actors, err := s.ListActors(ctx, "", store.ListOptions{PageSize: 1000}); err != nil || len(actors.Items) != 0 {
			t.Errorf("actors survived DebugClearAll: actors=%v err=%v", actors.Items, err)
		}
		if workers, err := s.ListWorkers(ctx, store.ListOptions{PageSize: 1000}); err != nil || len(workers.Items) != 0 {
			t.Errorf("workers survived DebugClearAll: workers=%v err=%v", workers.Items, err)
		}
		reacquired, err := s.AcquireLease(ctx, "lease-1")
		if err != nil {
			t.Errorf("lease survived DebugClearAll: %v", err)
		} else {
			reacquired.Close()
		}
	})
}
