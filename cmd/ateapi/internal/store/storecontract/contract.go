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
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/emptypb"
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
		}, {
			Name: "system-info",
			SystemInfo: &ateapipb.SystemInfoVolumeSource{
				DataSources: []*ateapipb.SystemInfoDataSource{
					{ActorMetadata: &ateapipb.ActorMetadataDataSource{
						Items: []*ateapipb.ActorMetadataItem{{
							Field: ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME,
							Path:  "actor-id",
						}},
					}},
					{TrustBundle: &ateapipb.TrustBundleDataSource{
						Name: "egress-mitm.ate.dev",
						Path: "trust-bundle.pem",
					}},
				},
			},
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
		Status: &ateapipb.WorkerStatus{
			Capacity: &ateapipb.WorkerResources{Resources: resources.CPUMemory(2000, 4<<30)},
		},
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

// withUnknownField adds a field number no message in this binary declares to m and returns m.
func withUnknownField[M proto.Message](m M) M {
	b := protowire.AppendTag(nil, 9999, protowire.VarintType)
	m.ProtoReflect().SetUnknown(protowire.AppendVarint(b, 42))
	return m
}

// requireNoUnknownFields fails the test if m or any message nested in it carries unknown fields.
func requireNoUnknownFields(t *testing.T, op string, m proto.Message) {
	t.Helper()
	err := protorange.Range(m.ProtoReflect(), func(p protopath.Values) error {
		if msg, ok := p.Index(-1).Value.Interface().(protoreflect.Message); ok && len(msg.GetUnknown()) > 0 {
			return fmt.Errorf("%s returned unknown fields at %v", op, p.Path)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

// assertPruned checks that got is want with its unknown fields dropped, ignoring the server-owned version and timestamps.
func assertPruned(t *testing.T, op string, want, got proto.Message) {
	t.Helper()
	requireNoUnknownFields(t, op, got)
	if diff := cmp.Diff(want, got, protocmp.Transform(), protocmp.IgnoreUnknown(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("%s mismatch (-want +got):\n%s", op, diff)
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
	runEgressPolicyContractTests(t, setup)
	runWorkerContractTests(t, setup)
	runWorkerAssignmentContractTests(t, setup)
	runAtespaceContractTests(t, setup)
	runActorTemplateContractTests(t, setup)
	runActorSnapshotContractTests(t, setup)
	runLeaseContractTests(t, setup)
	runListOptionsContractTests(t, setup)
	runUnknownFieldContractTests(t, setup)
}

func runEgressPolicyContractTests(t *testing.T, setup func(t *testing.T) store.Interface) {
	t.Helper()

	t.Run("EgressPolicy_Lifecycle", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)
		actor, err := s.CreateActor(ctx, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "session-1"},
			Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
		})
		if err != nil {
			t.Fatal(err)
		}
		actorRef := resources.ActorRefFromActor(actor)
		policy := &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{
			Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}},
		}}}

		created, err := s.CreateEgressPolicy(ctx, actorRef, policy)
		if err != nil {
			t.Fatalf("CreateEgressPolicy failed: %v", err)
		}
		if md := created.GetMetadata(); md.GetName() != "default" || md.GetAtespace() != testAtespace || md.GetUid() == "" || md.GetVersion() != 1 || md.GetCreateTime() == nil || md.GetUpdateTime() == nil {
			t.Fatalf("created metadata = %v", md)
		}
		if policy.GetMetadata() != nil {
			t.Fatalf("input metadata = %v; want nil", policy.GetMetadata())
		}
		if _, err := s.CreateEgressPolicy(ctx, actorRef, policy); !errors.Is(err, store.ErrAlreadyExists) {
			t.Fatalf("duplicate create error = %v, want ErrAlreadyExists", err)
		}
		got, err := s.GetEgressPolicy(ctx, actorRef)
		if err != nil || !proto.Equal(got, created) {
			t.Fatalf("GetEgressPolicy = %v, %v; want %v", got, err, created)
		}
		if _, err := s.UpdateEgressPolicy(ctx, actorRef, store.Precondition{}, func(*ateapipb.EgressPolicy) error { return nil }); !errors.Is(err, store.ErrPreconditionRequired) {
			t.Fatalf("unguarded update error = %v, want ErrPreconditionRequired", err)
		}
		if _, err := s.UpdateEgressPolicy(ctx, actorRef, store.Precondition{UID: created.GetMetadata().GetUid(), Version: 99}, func(*ateapipb.EgressPolicy) error { return nil }); !errors.Is(err, store.ErrVersionConflict) {
			t.Fatalf("stale update error = %v, want ErrVersionConflict", err)
		}
		if _, err := s.UpdateEgressPolicy(ctx, actorRef, store.Precondition{UID: "replacement-uid", Version: 1}, func(*ateapipb.EgressPolicy) error { return nil }); !errors.Is(err, store.ErrUIDConflict) {
			t.Fatalf("wrong UID update error = %v, want ErrUIDConflict", err)
		}
		updated, err := s.UpdateEgressPolicy(ctx, actorRef, store.PreconditionFrom(created), func(policy *ateapipb.EgressPolicy) error {
			policy.Metadata.Atespace = "other"
			policy.Metadata.Name = "other"
			policy.Rules = []*ateapipb.EgressRule{{All: &emptypb.Empty{}}}
			return nil
		})
		if err != nil || updated.GetMetadata().GetAtespace() != testAtespace || updated.GetMetadata().GetName() != "default" || updated.GetMetadata().GetVersion() != 2 || updated.GetMetadata().GetUid() != created.GetMetadata().GetUid() {
			t.Fatalf("UpdateEgressPolicy = %v, %v; want version 2", updated, err)
		}
		deleted, err := s.DeleteEgressPolicy(ctx, actorRef)
		if err != nil || !proto.Equal(deleted, updated) {
			t.Fatalf("DeleteEgressPolicy = %v, %v; want %v", deleted, err, updated)
		}
		if _, err := s.GetEgressPolicy(ctx, actorRef); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("GetEgressPolicy after delete error = %v, want ErrNotFound", err)
		}
	})

	t.Run("EgressPolicy_ActorLifecycle", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)
		actorRef := resources.ActorRef{Atespace: testAtespace, Name: "session-1"}
		policy := &ateapipb.EgressPolicy{}
		if _, err := s.CreateEgressPolicy(ctx, actorRef, policy); !errors.Is(err, store.ErrFailedPrecondition) {
			t.Fatalf("policy without Actor error = %v, want ErrFailedPrecondition", err)
		}
		actor, err := s.CreateActor(ctx, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: actorRef.Name},
			Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.CreateEgressPolicy(ctx, actorRef, policy); err != nil {
			t.Fatal(err)
		}
		if _, err := s.DeleteActor(ctx, actorRef); err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetEgressPolicy(ctx, actorRef); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("policy after Actor deletion error = %v, want ErrNotFound", err)
		}
		replacement, err := s.CreateActor(ctx, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: actorRef.Name},
			Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
		})
		if err != nil {
			t.Fatal(err)
		}
		if replacement.GetMetadata().GetUid() == actor.GetMetadata().GetUid() {
			t.Fatal("replacement Actor reused UID")
		}
		if _, err := s.GetEgressPolicy(ctx, actorRef); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("replacement Actor inherited policy: %v", err)
		}
	})
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
			Metadata:      &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "test-template"},
			Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
			Metadata:      &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "test-template"},
			Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
			Metadata:      &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "test-template"},
			Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
			Metadata:      &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "test-template"},
			Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
				Metadata:      &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "test-template"},
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
			Metadata:      &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "test-template"},
			Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
			Metadata:      &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "test-template"},
			Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
					Metadata:      &ateapipb.ResourceMetadata{Name: "session-1", Atespace: testAtespace},
					ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "test-template"},
					Status:        &ateapipb.ActorStatus{State: tt.state},
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
			Metadata:      &ateapipb.ResourceMetadata{Name: "id1", Atespace: testAtespace},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
			Status: &ateapipb.ActorStatus{
				State:          ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
				LatestSnapshot: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "snapshot-1"},
			},
		}
		actor2 := &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Name: "id2", Atespace: testAtespace},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
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
				Metadata:      &ateapipb.ResourceMetadata{Name: fmt.Sprintf("name%d", i), Atespace: testAtespace},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
				Metadata:      &ateapipb.ResourceMetadata{Name: name, Atespace: atespace},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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

		updated, err := s.UpdateWorker(ctx, testWorkerName, store.PreconditionFrom(created), func(toUpdate *ateapipb.Worker) error {
			toUpdate.Status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
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
		want.Status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
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
			toUpdate.Status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
			return nil
		}); err != nil {
			t.Fatalf("UpdateWorker failed: %v", err)
		}

		_, err = s.UpdateWorker(ctx, testWorkerName, store.PreconditionFrom(observed), func(toUpdate *ateapipb.Worker) error {
			toUpdate.Status.State = ateapipb.WorkerState_WORKER_STATE_ACTIVE
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
				// Claimants contend over the allocation total, which is what a
				// claim actually moves on the Worker record now that the
				// assignments themselves are separate records. CpuMilli carries
				// the claimant's number so the winner is identifiable.
				_, err := s.UpdateWorker(ctx, testWorkerName, store.PreconditionFrom(created), func(toUpdate *ateapipb.Worker) error {
					if toUpdate.GetStatus().GetAllocated().GetActors() > 0 {
						return errTaken
					}
					toUpdate.Status.Allocated = &ateapipb.WorkerResources{Actors: 1, Resources: resources.CPUMemory(int64(i)+1, 0)}
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
		if n := got.GetStatus().GetAllocated().GetActors(); n < 1 || n > int32(claimants) {
			t.Errorf("stored claim names %d, want one of the claimants", n)
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

// runWorkerAssignmentContractTests covers the records that say which Actors a
// Worker hosts, and the allocation total that has to move with them.
func runWorkerAssignmentContractTests(t *testing.T, setup func(t *testing.T) store.Interface) {
	t.Helper()

	// newTestAssignment describes one Actor's placement, sized so that a total
	// over several of them is unambiguous about which were counted.
	newTestAssignment := func(actorUID string, cpuMilli, memoryBytes int64) *ateapipb.ActorAssignment {
		return &ateapipb.ActorAssignment{
			ActorTemplateRef: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "test-template"},
			Actor:            &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-" + actorUID},
			ActorUid:         actorUID,
			Resources:        resources.CPUMemory(cpuMilli, memoryBytes),
		}
	}

	// bind binds one assignment with no admission check, for the tests that care
	// about the outcome rather than about admission.
	bind := func(t *testing.T, s store.Interface, workerName string, assignment *ateapipb.ActorAssignment) {
		t.Helper()
		ctx := context.Background()
		if err := s.BindActorToWorker(ctx, workerName, assignment, nil); err != nil {
			t.Fatalf("BindActorToWorker(%s) failed: %v", assignment.GetActorUid(), err)
		}
	}

	t.Run("BindActorToWorker_AddsAssignmentAndAllocation", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		watch, err := s.WatchWorkers(ctx)
		if err != nil {
			t.Fatalf("WatchWorkers failed: %v", err)
		}
		defer watch.Close()

		assignment := newTestAssignment("uid-1", 500, 1<<20)
		bind(t, s, testWorkerName, assignment)

		got, err := s.GetWorkerAssignment(ctx, testWorkerName, "uid-1")
		if err != nil {
			t.Fatalf("GetWorkerAssignment failed: %v", err)
		}
		if diff := cmp.Diff(assignment, got, protocmp.Transform()); diff != "" {
			t.Errorf("stored assignment mismatch (-want +got):\n%s", diff)
		}

		worker, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		want := &ateapipb.WorkerResources{Actors: 1, Resources: resources.CPUMemory(500, 1<<20)}
		if diff := cmp.Diff(want, worker.GetStatus().GetAllocated(), protocmp.Transform()); diff != "" {
			t.Errorf("allocated mismatch (-want +got):\n%s", diff)
		}
		if worker.GetMetadata().GetVersion() != 2 {
			t.Errorf("worker version = %d, want 2: a bind advances the worker", worker.GetMetadata().GetVersion())
		}

		event := receiveEvent(t, watch.Events)
		if event.Type != store.WorkerEventUpdated {
			t.Errorf("expected WorkerEventUpdated, got %v", event.Type)
		}
		if diff := cmp.Diff(want, event.Worker.GetStatus().GetAllocated(), protocmp.Transform()); diff != "" {
			t.Errorf("event allocated mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("BindActorToWorker_ReplacesSameActor", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		// A retried claim binds the same Actor twice. It must be counted once,
		// at its latest size, or the Worker leaks capacity it never gave out.
		bind(t, s, testWorkerName, newTestAssignment("uid-1", 500, 1<<20))
		bind(t, s, testWorkerName, newTestAssignment("uid-1", 250, 1<<21))

		worker, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		want := &ateapipb.WorkerResources{Actors: 1, Resources: resources.CPUMemory(250, 1<<21)}
		if diff := cmp.Diff(want, worker.GetStatus().GetAllocated(), protocmp.Transform()); diff != "" {
			t.Errorf("allocated mismatch after rebinding the same actor (-want +got):\n%s", diff)
		}
		assignmentsPage, err := s.ListWorkerAssignments(ctx, testWorkerName, store.ListOptions{})
		if err != nil {
			t.Fatalf("ListWorkerAssignments failed: %v", err)
		}
		assignments := assignmentsPage.Items
		if len(assignments) != 1 {
			t.Errorf("worker holds %d assignments, want 1", len(assignments))
		}
	})

	t.Run("BindActorToWorker_RebindKeepsAssignmentIdentity", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		bind(t, s, testWorkerName, newTestAssignment("uid-1", 500, 1<<20))
		first, err := s.GetWorkerAssignment(ctx, testWorkerName, "uid-1")
		if err != nil {
			t.Fatalf("GetWorkerAssignment failed: %v", err)
		}

		// A retried claim rebinds the same Actor, which updates the assignment
		// already recorded. A reader watching uid or create_time must not see a
		// different subresource.
		bind(t, s, testWorkerName, newTestAssignment("uid-1", 250, 1<<21))
		again, err := s.GetWorkerAssignment(ctx, testWorkerName, "uid-1")
		if err != nil {
			t.Fatalf("GetWorkerAssignment after rebind failed: %v", err)
		}

		if got, want := again.GetMetadata().GetUid(), first.GetMetadata().GetUid(); got != want {
			t.Errorf("rebind changed the assignment uid to %q, want %q", got, want)
		}
		if diff := cmp.Diff(first.GetMetadata().GetCreateTime(), again.GetMetadata().GetCreateTime(), protocmp.Transform()); diff != "" {
			t.Errorf("rebind moved create_time (-want +got):\n%s", diff)
		}
		if got, want := again.GetMetadata().GetVersion(), first.GetMetadata().GetVersion()+1; got != want {
			t.Errorf("rebind left version at %d, want %d: an update advances it", got, want)
		}
	})

	t.Run("BindActorToWorker_RefusedAdmissionLeavesNothingBehind", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		full := errors.New("worker is full")
		err := s.BindActorToWorker(ctx, testWorkerName, newTestAssignment("uid-1", 500, 0),
			func(*ateapipb.Worker) error { return full })
		if !errors.Is(err, full) {
			t.Errorf("BindActorToWorker with a refusing admit = %v, want %v", err, full)
		}
		// The bind speculates the row in before it asks, so a refusal has to
		// take it back out.
		if _, err := s.GetWorkerAssignment(ctx, testWorkerName, "uid-1"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("a refused bind left an assignment behind: %v", err)
		}
		worker, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if got := worker.GetStatus().GetAllocated().GetActors(); got != 0 {
			t.Errorf("a refused bind left %d actors allocated, want 0", got)
		}
	})

	// What the caller needs from admission: it runs against the Worker as the
	// bind will write it, so concurrent binds cannot both find room for the last
	// place. Nothing outside the store can offer that -- a check made before the
	// call is stale by the time it commits.
	// An ActorTemplate is mutable, so a retried claim can come back bigger than
	// the one already booked. Admission has to run on the replacement, judged
	// against the Worker with the old reservation taken off.
	t.Run("BindActorToWorker_AdmitsAReplacementThatGrew", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		bind(t, s, testWorkerName, newTestAssignment("uid-1", 500, 0))

		var admitted []*ateapipb.WorkerResources
		full := errors.New("worker is full")
		err := s.BindActorToWorker(ctx, testWorkerName, newTestAssignment("uid-1", 5000, 0),
			func(fresh *ateapipb.Worker) error {
				admitted = append(admitted, fresh.GetStatus().GetAllocated())
				return full
			})
		if !errors.Is(err, full) {
			t.Fatalf("rebinding a grown assignment = %v, want the admit refusal %v", err, full)
		}
		if len(admitted) != 1 {
			t.Fatalf("admit ran %d times on a replacement, want once", len(admitted))
		}
		// Judged without the old reservation: otherwise the Actor is counted
		// twice and a replacement that merely stayed the same size is refused.
		if got := admitted[0].GetActors(); got != 0 {
			t.Errorf("admit saw %d actors allocated, want 0: the previous reservation should be off", got)
		}

		// A refusal leaves the original booking exactly as it was.
		worker, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		want := &ateapipb.WorkerResources{Actors: 1, Resources: resources.CPUMemory(500, 0)}
		if diff := cmp.Diff(want, worker.GetStatus().GetAllocated(), protocmp.Transform()); diff != "" {
			t.Errorf("a refused replacement changed the allocation (-want +got):\n%s", diff)
		}
	})

	t.Run("BindActorToWorker_AdmitSeesConcurrentBinds", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		const claims = 16
		full := errors.New("worker is full")
		roomForOne := func(w *ateapipb.Worker) error {
			if w.GetStatus().GetAllocated().GetActors() >= 1 {
				return full
			}
			return nil
		}

		var wg sync.WaitGroup
		won := make([]bool, claims)
		for i := range claims {
			wg.Go(func() {
				err := s.BindActorToWorker(ctx, testWorkerName,
					newTestAssignment(fmt.Sprintf("uid-%d", i), 0, 0), roomForOne)
				won[i] = err == nil
			})
		}
		wg.Wait()

		var winners int
		for _, w := range won {
			if w {
				winners++
			}
		}
		if winners != 1 {
			t.Errorf("%d of %d concurrent binds were admitted, want exactly 1", winners, claims)
		}
		worker, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if got := worker.GetStatus().GetAllocated().GetActors(); got != 1 {
			t.Errorf("worker allocation counts %d actors, want 1", got)
		}
	})

	// An Actor belongs to one Worker. A second Worker claiming it must be
	// refused rather than quietly taking the row, or the first goes on counting
	// an Actor it does not host and never gets that capacity back.
	t.Run("BindActorToWorker_ActorAlreadyOnAnotherWorker", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		for _, name := range []string{testWorkerName, otherTestWorkerName} {
			if _, err := s.CreateWorker(ctx, newTestWorker(name, "pod-"+name)); err != nil {
				t.Fatalf("CreateWorker(%s) failed: %v", name, err)
			}
		}
		bind(t, s, testWorkerName, newTestAssignment("uid-1", 500, 1<<20))

		other, err := s.GetWorker(ctx, otherTestWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if err := s.BindActorToWorker(ctx, otherTestWorkerName, newTestAssignment("uid-1", 500, 1<<20), nil); err == nil {
			t.Fatal("BindActorToWorker onto a second worker succeeded, want an error")
		}

		// The refusal has to leave both Workers exactly as they were: the first
		// still hosting the Actor and counting it, the second counting nothing.
		holder, err := s.FindWorkerHostingActor(ctx, "uid-1")
		if err != nil {
			t.Fatalf("FindWorkerHostingActor failed: %v", err)
		}
		if holder != testWorkerName {
			t.Errorf("actor moved to worker %q, want it left on %q", holder, testWorkerName)
		}
		first, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		want := &ateapipb.WorkerResources{Actors: 1, Resources: resources.CPUMemory(500, 1<<20)}
		if diff := cmp.Diff(want, first.GetStatus().GetAllocated(), protocmp.Transform()); diff != "" {
			t.Errorf("first worker's allocation changed (-want +got):\n%s", diff)
		}
		refused, err := s.GetWorker(ctx, otherTestWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if got := refused.GetStatus().GetAllocated().GetActors(); got != 0 {
			t.Errorf("refused worker counts %d actors, want 0", got)
		}
		if got, wantVersion := refused.GetMetadata().GetVersion(), other.GetMetadata().GetVersion(); got != wantVersion {
			t.Errorf("refused worker moved to version %d, want %d unchanged", got, wantVersion)
		}
	})

	// The same rule under contention, which is where it is actually decided.
	// Two replicas resuming one Actor pick different Workers and claim at the
	// same moment; a backend that decides on a read taken before the competing
	// write exists lets both through, and the loser is left counting an Actor
	// whose row went elsewhere.
	t.Run("BindActorToWorker_ConcurrentClaimsOfOneActor", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		names := []string{testWorkerName, otherTestWorkerName}
		versions := make([]int64, len(names))
		for i, name := range names {
			created, err := s.CreateWorker(ctx, newTestWorker(name, "pod-"+name))
			if err != nil {
				t.Fatalf("CreateWorker(%s) failed: %v", name, err)
			}
			versions[i] = created.GetMetadata().GetVersion()
		}

		var start sync.WaitGroup
		start.Add(1)
		var wg sync.WaitGroup
		won := make([]bool, len(names))
		for i, name := range names {
			wg.Add(1)
			go func() {
				defer wg.Done()
				start.Wait()
				won[i] = s.BindActorToWorker(ctx, name, newTestAssignment("uid-1", 500, 1<<20), nil) == nil
			}()
		}
		start.Done()
		wg.Wait()

		if winners := len(slices.DeleteFunc(slices.Clone(won), func(w bool) bool { return !w })); winners != 1 {
			t.Errorf("%d of %d claims won the actor, want exactly 1", winners, len(names))
		}
		// Whoever won, no Worker may count an Actor it does not hold.
		for _, name := range names {
			worker, err := s.GetWorker(ctx, name)
			if err != nil {
				t.Fatalf("GetWorker(%s) failed: %v", name, err)
			}
			assignmentsPage, err := s.ListWorkerAssignments(ctx, name, store.ListOptions{})
			if err != nil {
				t.Fatalf("ListWorkerAssignments(%s) failed: %v", name, err)
			}
			assignments := assignmentsPage.Items
			want, err := resources.SumAllocated(assignments)
			if err != nil {
				t.Fatalf("%s: summing assignments: %v", name, err)
			}
			// A Worker that never took anything has no allocation at all, while
			// a sum over no assignments is a zeroed message; the two say the
			// same thing, so compare what they mean rather than the messages.
			got := worker.GetStatus().GetAllocated()
			if got.GetActors() != want.GetActors() || !proto.Equal(got.GetResources(), want.GetResources()) {
				t.Errorf("%s: allocation %v disagrees with the %d assignments it holds (%v)",
					name, got, len(assignments), want)
			}
		}
	})

	t.Run("BindActorToWorker_WorkerNotFound", func(t *testing.T) {
		s := setup(t)
		err := s.BindActorToWorker(context.Background(), "no-such-worker", newTestAssignment("uid-1", 0, 0), nil)
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("BindActorToWorker on a missing worker = %v, want ErrNotFound", err)
		}
	})

	t.Run("ReleaseActorFromWorker", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		bind(t, s, testWorkerName, newTestAssignment("uid-1", 500, 1<<20))
		bind(t, s, testWorkerName, newTestAssignment("uid-2", 250, 1<<21))

		released, err := s.ReleaseActorFromWorker(ctx, testWorkerName, "uid-1")
		if err != nil {
			t.Fatalf("ReleaseActorFromWorker failed: %v", err)
		}
		if released == nil {
			t.Fatal("ReleaseActorFromWorker reported nothing to release")
		}
		// The returned Worker is what the caller feeds the cache, so it has to
		// be the post-release state, not the copy that went in.
		wantReleased := &ateapipb.WorkerResources{Actors: 1, Resources: resources.CPUMemory(250, 1<<21)}
		if diff := cmp.Diff(wantReleased, released.GetStatus().GetAllocated(), protocmp.Transform()); diff != "" {
			t.Errorf("returned worker's allocated mismatch (-want +got):\n%s", diff)
		}

		// Only the released Actor goes; the Worker's others are untouched.
		if _, err := s.GetWorkerAssignment(ctx, testWorkerName, "uid-1"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("released assignment is still readable: %v", err)
		}
		if _, err := s.GetWorkerAssignment(ctx, testWorkerName, "uid-2"); err != nil {
			t.Errorf("releasing one actor disturbed another: %v", err)
		}
		worker, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		want := &ateapipb.WorkerResources{Actors: 1, Resources: resources.CPUMemory(250, 1<<21)}
		if diff := cmp.Diff(want, worker.GetStatus().GetAllocated(), protocmp.Transform()); diff != "" {
			t.Errorf("allocated mismatch after release (-want +got):\n%s", diff)
		}
	})

	t.Run("ReleaseActorFromWorker_AlreadyReleased", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		worker, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		// Release runs on paths that retry, so a second pass has to converge
		// rather than fail -- and must not advance the Worker either.
		released, err := s.ReleaseActorFromWorker(ctx, testWorkerName, "uid-1")
		if err != nil {
			t.Fatalf("ReleaseActorFromWorker failed: %v", err)
		}
		if released != nil {
			t.Error("ReleaseActorFromWorker reported releasing an assignment that was never there")
		}
		after, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if after.GetMetadata().GetVersion() != worker.GetMetadata().GetVersion() {
			t.Errorf("worker version moved from %d to %d on a release that freed nothing",
				worker.GetMetadata().GetVersion(), after.GetMetadata().GetVersion())
		}
	})

	t.Run("ListWorkerAssignments_ScopedToOneWorker", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		for _, pod := range []string{"pod-1", "pod-2"} {
			if _, err := s.CreateWorker(ctx, newTestWorker("worker-"+pod, pod)); err != nil {
				t.Fatalf("CreateWorker failed: %v", err)
			}
		}
		bind(t, s, "worker-pod-1", newTestAssignment("uid-1", 0, 0))
		bind(t, s, "worker-pod-1", newTestAssignment("uid-2", 0, 0))
		bind(t, s, "worker-pod-2", newTestAssignment("uid-3", 0, 0))

		assignmentsPage, err := s.ListWorkerAssignments(ctx, "worker-pod-1", store.ListOptions{})
		if err != nil {
			t.Fatalf("ListWorkerAssignments failed: %v", err)
		}
		assignments := assignmentsPage.Items
		var got []string
		for _, assignment := range assignments {
			got = append(got, assignment.GetActorUid())
		}
		if diff := cmp.Diff([]string{"uid-1", "uid-2"}, got); diff != "" {
			t.Errorf("assignments of worker-pod-1 (-want +got):\n%s", diff)
		}

		// An Actor is hosted by one Worker, and only that Worker's list has it.
		if _, err := s.GetWorkerAssignment(ctx, "worker-pod-1", "uid-3"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("GetWorkerAssignment found another worker's actor: %v", err)
		}
	})

	// A Worker holds far more Actors than a page, so a caller that stops at the
	// first page sees an arbitrary subset. Walking the token must yield each
	// assignment exactly once.
	t.Run("ListWorkerAssignments_Paginates", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		const total = 5
		for i := range total {
			bind(t, s, testWorkerName, newTestAssignment(fmt.Sprintf("uid-%d", i), 1, 1))
		}

		seen := map[string]int{}
		pages := 0
		for token := ""; ; pages++ {
			page, err := s.ListWorkerAssignments(ctx, testWorkerName, store.ListOptions{PageSize: 2, PageToken: token})
			if err != nil {
				t.Fatalf("ListWorkerAssignments page %d failed: %v", pages, err)
			}
			if got := len(page.Items); got > 2 {
				t.Fatalf("page %d returned %d assignments, want at most the requested 2", pages, got)
			}
			for _, a := range page.Items {
				seen[a.GetActorUid()]++
			}
			if !page.HasNextPage() {
				break
			}
			token = page.NextPageToken
		}
		if len(seen) != total {
			t.Errorf("walking the pages saw %d distinct assignments, want %d", len(seen), total)
		}
		for uid, n := range seen {
			if n != 1 {
				t.Errorf("assignment %s appeared %d times across pages, want once", uid, n)
			}
		}
	})

	t.Run("ListWorkerAssignments_Empty", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		assignmentsPage, err := s.ListWorkerAssignments(ctx, testWorkerName, store.ListOptions{})
		if err != nil {
			t.Fatalf("ListWorkerAssignments failed: %v", err)
		}
		assignments := assignmentsPage.Items
		if len(assignments) != 0 {
			t.Errorf("an idle worker reported %d assignments, want 0", len(assignments))
		}
	})

	t.Run("FindWorkerHostingActor", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		if _, err := s.FindWorkerHostingActor(ctx, "uid-1"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("FindWorkerHostingActor before any bind = %v, want ErrNotFound", err)
		}

		bind(t, s, testWorkerName, newTestAssignment("uid-1", 0, 0))
		got, err := s.FindWorkerHostingActor(ctx, "uid-1")
		if err != nil {
			t.Fatalf("FindWorkerHostingActor failed: %v", err)
		}
		if got != testWorkerName {
			t.Errorf("FindWorkerHostingActor = %q, want %q", got, testWorkerName)
		}

		// Released, so nothing hosts it -- the recovery this exists for must
		// not resurrect a placement that is over.
		if _, err := s.ReleaseActorFromWorker(ctx, testWorkerName, "uid-1"); err != nil {
			t.Fatalf("ReleaseActorFromWorker failed: %v", err)
		}
		if _, err := s.FindWorkerHostingActor(ctx, "uid-1"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("FindWorkerHostingActor after release = %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteWorker_DropsItsAssignments", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		bind(t, s, testWorkerName, newTestAssignment("uid-1", 0, 0))
		if _, err := s.DeleteWorker(ctx, testWorkerName, store.DeletePreconditions{}); err != nil {
			t.Fatalf("DeleteWorker failed: %v", err)
		}

		// A Worker's assignments cannot outlive it: the pod is gone, so nothing
		// is hosted, and a leftover row would keep an Actor looking placed.
		assignmentsPage, err := s.ListWorkerAssignments(ctx, testWorkerName, store.ListOptions{})
		if err != nil {
			t.Fatalf("ListWorkerAssignments failed: %v", err)
		}
		assignments := assignmentsPage.Items
		if len(assignments) != 0 {
			t.Errorf("deleted worker still holds %d assignments", len(assignments))
		}
		if _, err := s.FindWorkerHostingActor(ctx, "uid-1"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("FindWorkerHostingActor still names a deleted worker: %v", err)
		}
	})

	// TestAllocatedTracksAssignmentsUnderChurn is the test a stored total needs,
	// because the risk of storing one is not that the arithmetic is wrong once
	// -- it is that some sequence of binds and releases leaves it disagreeing
	// with what it summarizes, quietly, forever after.
	//
	// So: churn a worker through hundreds of binds, rebinds and releases, and
	// after every one require the total to equal the assignments.
	t.Run("Allocated_TracksAssignmentsUnderChurn", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()

		if _, err := s.CreateWorker(ctx, newTestWorker(testWorkerName, "pod-1")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		// Deterministic, so a failure reproduces.
		rng := rand.New(rand.NewPCG(1, 2))
		for step := range 300 {
			actorUID := fmt.Sprintf("uid-%d", rng.IntN(12))

			if rng.IntN(3) < 2 {
				// Bind, sometimes over an actor already there and at a different
				// size, which is the case that has to subtract before it adds.
				assignment := newTestAssignment(actorUID, int64(rng.IntN(4)+1)*500, int64(rng.IntN(4)+1)<<24)
				if err := s.BindActorToWorker(ctx, testWorkerName, assignment, nil); err != nil {
					t.Fatalf("step %d: BindActorToWorker(%s) failed: %v", step, actorUID, err)
				}
			} else if _, err := s.ReleaseActorFromWorker(ctx, testWorkerName, actorUID); err != nil {
				t.Fatalf("step %d: ReleaseActorFromWorker(%s) failed: %v", step, actorUID, err)
			}

			worker, err := s.GetWorker(ctx, testWorkerName)
			if err != nil {
				t.Fatalf("GetWorker failed: %v", err)
			}
			assignmentsPage, err := s.ListWorkerAssignments(ctx, testWorkerName, store.ListOptions{})
			if err != nil {
				t.Fatalf("ListWorkerAssignments failed: %v", err)
			}
			assignments := assignmentsPage.Items
			want, err := resources.SumAllocated(assignments)
			if err != nil {
				t.Fatalf("summing assignments: %v", err)
			}
			if diff := cmp.Diff(want, worker.GetStatus().GetAllocated(), protocmp.Transform()); diff != "" {
				t.Fatalf("after step %d (actor %s) the total no longer matches the assignments (-want +got):\n%s", step, actorUID, diff)
			}
		}

		// And it must come back to exactly zero, not merely to something small:
		// a total drifting by a little per cycle is what this guards against.
		assignmentsPage, err := s.ListWorkerAssignments(ctx, testWorkerName, store.ListOptions{})
		if err != nil {
			t.Fatalf("ListWorkerAssignments failed: %v", err)
		}
		assignments := assignmentsPage.Items
		for _, assignment := range assignments {
			if _, err := s.ReleaseActorFromWorker(ctx, testWorkerName, assignment.GetActorUid()); err != nil {
				t.Fatalf("ReleaseActorFromWorker failed: %v", err)
			}
		}
		worker, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if got := worker.GetStatus().GetAllocated(); got.GetActors() != 0 || got.GetResources() != nil {
			t.Errorf("after releasing everything the total is %v, want all zero", got)
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

// runUnknownFieldContractTests asserts that every read path, including the
// object handed to a read-modify-write mutation, drops fields this binary has
// no descriptor for at every nesting level.
func runUnknownFieldContractTests(t *testing.T, setup func(t *testing.T) store.Interface) {
	t.Helper()

	t.Run("UnknownFields_Actor", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)
		created, err := s.CreateActor(ctx, withUnknownField(&ateapipb.Actor{
			Metadata:      withUnknownField(&ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "actor-1"}),
			ActorTemplate: withUnknownField(&ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-1"}),
			Status:        withUnknownField(&ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING}),
		}))
		if err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
		ref := resources.ActorRefFromActor(created)

		got, err := s.GetActor(ctx, ref)
		if err != nil {
			t.Fatalf("GetActor failed: %v", err)
		}
		assertPruned(t, "GetActor", created, got)

		list, err := s.ListActors(ctx, testAtespace, store.ListOptions{})
		if err != nil || len(list.Items) != 1 {
			t.Fatalf("ListActors = %d items, %v; want 1", len(list.Items), err)
		}
		assertPruned(t, "ListActors", created, list.Items[0])

		updated, err := s.UpdateActor(ctx, ref, store.PreconditionFrom(got), func(toUpdate *ateapipb.Actor) error {
			requireNoUnknownFields(t, "UpdateActor mutation input", toUpdate)
			return nil
		})
		if err != nil {
			t.Fatalf("UpdateActor failed: %v", err)
		}
		assertPruned(t, "UpdateActor", created, updated)

		deleted, err := s.DeleteActor(ctx, ref)
		if err != nil {
			t.Fatalf("DeleteActor failed: %v", err)
		}
		assertPruned(t, "DeleteActor", created, deleted)
	})

	t.Run("UnknownFields_EgressPolicy", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)
		actor, err := s.CreateActor(ctx, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "actor-1"},
			Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
		})
		if err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
		ref := resources.ActorRefFromActor(actor)
		created, err := s.CreateEgressPolicy(ctx, ref, withUnknownField(&ateapipb.EgressPolicy{
			Rules: []*ateapipb.EgressRule{withUnknownField(&ateapipb.EgressRule{
				Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}},
			})},
		}))
		if err != nil {
			t.Fatalf("CreateEgressPolicy failed: %v", err)
		}

		got, err := s.GetEgressPolicy(ctx, ref)
		if err != nil {
			t.Fatalf("GetEgressPolicy failed: %v", err)
		}
		assertPruned(t, "GetEgressPolicy", created, got)

		updated, err := s.UpdateEgressPolicy(ctx, ref, store.PreconditionFrom(got), func(toUpdate *ateapipb.EgressPolicy) error {
			requireNoUnknownFields(t, "UpdateEgressPolicy mutation input", toUpdate)
			return nil
		})
		if err != nil {
			t.Fatalf("UpdateEgressPolicy failed: %v", err)
		}
		assertPruned(t, "UpdateEgressPolicy", created, updated)

		deleted, err := s.DeleteEgressPolicy(ctx, ref)
		if err != nil {
			t.Fatalf("DeleteEgressPolicy failed: %v", err)
		}
		assertPruned(t, "DeleteEgressPolicy", created, deleted)
	})

	t.Run("UnknownFields_ActorSnapshotAndTag", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)
		snapshotRef := resources.ActorSnapshotRef{Atespace: testAtespace, Name: "snapshot-1"}
		created, err := s.CreateActorSnapshot(ctx, withUnknownField(&ateapipb.ActorSnapshot{
			Metadata: withUnknownField(&ateapipb.ResourceMetadata{Atespace: testAtespace, Name: snapshotRef.Name}),
			Status: withUnknownField(&ateapipb.ActorSnapshotStatus{
				SourceActor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-1"},
				SnapshotUri: "gs://private/snapshot-1",
			}),
		}))
		if err != nil {
			t.Fatalf("CreateActorSnapshot failed: %v", err)
		}

		got, err := s.GetActorSnapshot(ctx, snapshotRef)
		if err != nil {
			t.Fatalf("GetActorSnapshot failed: %v", err)
		}
		assertPruned(t, "GetActorSnapshot", created, got)

		list, err := s.ListActorSnapshots(ctx, testAtespace, store.ListOptions{})
		if err != nil || len(list.Items) != 1 {
			t.Fatalf("ListActorSnapshots = %d items, %v; want 1", len(list.Items), err)
		}
		assertPruned(t, "ListActorSnapshots", created, list.Items[0])

		tagRef := resources.ActorSnapshotTagRef{Atespace: testAtespace, Name: "production"}
		createdTag, err := s.CreateActorSnapshotTag(ctx, snapshotRef, withUnknownField(&ateapipb.ActorSnapshotTag{
			Metadata: withUnknownField(&ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tagRef.Name}),
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		}))
		if err != nil {
			t.Fatalf("CreateActorSnapshotTag failed: %v", err)
		}

		gotTag, err := s.GetActorSnapshotTag(ctx, tagRef)
		if err != nil {
			t.Fatalf("GetActorSnapshotTag failed: %v", err)
		}
		assertPruned(t, "GetActorSnapshotTag", createdTag, gotTag)

		updatedTag, err := s.UpdateActorSnapshotTag(ctx, tagRef, store.PreconditionFrom(gotTag), func(toUpdate *ateapipb.ActorSnapshotTag) error {
			requireNoUnknownFields(t, "UpdateActorSnapshotTag mutation input", toUpdate)
			return nil
		})
		if err != nil {
			t.Fatalf("UpdateActorSnapshotTag failed: %v", err)
		}
		assertPruned(t, "UpdateActorSnapshotTag", createdTag, updatedTag)

		deletedTag, err := s.DeleteActorSnapshotTag(ctx, tagRef)
		if err != nil {
			t.Fatalf("DeleteActorSnapshotTag failed: %v", err)
		}
		assertPruned(t, "DeleteActorSnapshotTag", createdTag, deletedTag)
	})

	t.Run("UnknownFields_Atespace", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		created, err := s.CreateAtespace(ctx, withUnknownField(&ateapipb.Atespace{
			Metadata: withUnknownField(&ateapipb.ResourceMetadata{Name: testAtespace}),
		}))
		if err != nil {
			t.Fatalf("CreateAtespace failed: %v", err)
		}

		got, err := s.GetAtespace(ctx, testAtespace)
		if err != nil {
			t.Fatalf("GetAtespace failed: %v", err)
		}
		assertPruned(t, "GetAtespace", created, got)

		list, err := s.ListAtespaces(ctx, store.ListOptions{})
		if err != nil || len(list.Items) != 1 {
			t.Fatalf("ListAtespaces = %d items, %v; want 1", len(list.Items), err)
		}
		assertPruned(t, "ListAtespaces", created, list.Items[0])

		deleted, err := s.DeleteAtespace(ctx, testAtespace)
		if err != nil {
			t.Fatalf("DeleteAtespace failed: %v", err)
		}
		assertPruned(t, "DeleteAtespace", created, deleted)
	})

	t.Run("UnknownFields_ActorTemplate", func(t *testing.T) {
		s := setup(t)
		ctx := context.Background()
		mustCreateAtespace(t, s, testAtespace)
		template := withUnknownField(newTestActorTemplate(testAtespace, "tmpl-1"))
		withUnknownField(template.Metadata)
		withUnknownField(template.Containers[0])
		created, err := s.CreateActorTemplate(ctx, template)
		if err != nil {
			t.Fatalf("CreateActorTemplate failed: %v", err)
		}
		ref := resources.ActorTemplateRef{Atespace: testAtespace, Name: "tmpl-1"}

		got, err := s.GetActorTemplate(ctx, ref)
		if err != nil {
			t.Fatalf("GetActorTemplate failed: %v", err)
		}
		assertPruned(t, "GetActorTemplate", created, got)

		list, err := s.ListActorTemplates(ctx, testAtespace, store.ListOptions{})
		if err != nil || len(list.Items) != 1 {
			t.Fatalf("ListActorTemplates = %d items, %v; want 1", len(list.Items), err)
		}
		assertPruned(t, "ListActorTemplates", created, list.Items[0])

		updated, err := s.UpdateActorTemplate(ctx, ref, store.PreconditionFrom(got), func(toUpdate *ateapipb.ActorTemplate) error {
			requireNoUnknownFields(t, "UpdateActorTemplate mutation input", toUpdate)
			return nil
		})
		if err != nil {
			t.Fatalf("UpdateActorTemplate failed: %v", err)
		}
		assertPruned(t, "UpdateActorTemplate", created, updated)

		deleted, err := s.DeleteActorTemplate(ctx, ref)
		if err != nil {
			t.Fatalf("DeleteActorTemplate failed: %v", err)
		}
		assertPruned(t, "DeleteActorTemplate", created, deleted)
	})

	t.Run("UnknownFields_Worker", func(t *testing.T) {
		s := setup(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		watch, err := s.WatchWorkers(ctx)
		if err != nil {
			t.Fatalf("WatchWorkers failed: %v", err)
		}
		defer watch.Close()

		worker := withUnknownField(newTestWorker(testWorkerName, "pod-1"))
		withUnknownField(worker.Metadata)
		withUnknownField(worker.Status.Capacity)
		created, err := s.CreateWorker(ctx, worker)
		if err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		event := receiveEvent(t, watch.Events)
		if event.Type != store.WorkerEventCreated {
			t.Fatalf("event type = %v, want WorkerEventCreated", event.Type)
		}
		assertPruned(t, "WatchWorkers created event", created, event.Worker)

		got, err := s.GetWorker(ctx, testWorkerName)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		assertPruned(t, "GetWorker", created, got)

		list, err := s.ListWorkers(ctx, store.ListOptions{})
		if err != nil || len(list.Items) != 1 {
			t.Fatalf("ListWorkers = %d items, %v; want 1", len(list.Items), err)
		}
		assertPruned(t, "ListWorkers", created, list.Items[0])

		updated, err := s.UpdateWorker(ctx, testWorkerName, store.PreconditionFrom(got), func(toUpdate *ateapipb.Worker) error {
			requireNoUnknownFields(t, "UpdateWorker mutation input", toUpdate)
			return nil
		})
		if err != nil {
			t.Fatalf("UpdateWorker failed: %v", err)
		}
		assertPruned(t, "UpdateWorker", created, updated)

		deleted, err := s.DeleteWorker(ctx, testWorkerName, store.DeletePreconditions{})
		if err != nil {
			t.Fatalf("DeleteWorker failed: %v", err)
		}
		assertPruned(t, "DeleteWorker", created, deleted)
	})
}
