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
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func setupTest(t *testing.T) (*miniredis.Miniredis, *Persistence, context.Context) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	// Miniredis runs as a single node, but ClusterClient can work with it
	// if we don't use cluster-specific commands that miniredis doesn't support.
	// Miniredis supports most standard commands.
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{mr.Addr()},
	})
	t.Cleanup(func() { rdb.Close() })
	return mr, NewPersistence(rdb), t.Context()
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
	_, s, ctx := setupTest(t)

	_, err := s.GetActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: "non-existent"})
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestCreateActor_Success(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.CreateAtespace(ctx, newTestAtespace(testAtespace)); err != nil {
		t.Fatalf("CreateAtespace() failed: %v", err)
	}

	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: testAtespace},
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "test-template",
		Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
	got, err := s.GetActor(ctx, resources.ActorRefFromActor(actor))
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
	_, s, ctx := setupTest(t)

	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: testAtespace},
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "test-template",
		Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}

	if _, err := s.CreateAtespace(ctx, newTestAtespace(actor.Metadata.Atespace)); err != nil {
		t.Fatalf("CreateAtespace() failed: %v", err)
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

// newTestActor returns an unsaved actor for the UpdateActor tests.
func newTestActor(name string) *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: name, Atespace: testAtespace},
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "test-template",
		Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}
}

func TestUpdateActor_Success(t *testing.T) {
	_, s, ctx := setupTest(t)
	actor := newTestActor("actor-1")
	if _, err := s.CreateAtespace(ctx, newTestAtespace(actor.Metadata.Atespace)); err != nil {
		t.Fatalf("CreateAtespace() failed: %v", err)
	}
	created, err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	actorRef := resources.ActorRefFromActor(actor)
	updated, err := s.UpdateActor(ctx, actorRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	// UpdateActor returns the stored resource: the mutation applied and version
	// advanced, with uid and create_time preserved from creation.
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

	// The returned resource is exactly what GetActor reads back.
	got, err := s.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if diff := cmp.Diff(updated, got, protocmp.Transform()); diff != "" {
		t.Errorf("UpdateActor return does not match stored state (-updated +got):\n%s", diff)
	}
}

func TestUpdateActor_MutateErrorAreNotRetried(t *testing.T) {
	_, s, ctx := setupTest(t)
	actor := newTestActor("actor-1")
	if _, err := s.CreateAtespace(ctx, newTestAtespace(actor.Metadata.Atespace)); err != nil {
		t.Fatalf("CreateAtespace() failed: %v", err)
	}
	created, err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	var mutationError = errors.New("mutation error")

	actorRef := resources.ActorRefFromActor(actor)
	callsToMutateFn := 0
	_, err = s.UpdateActor(ctx, actorRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.Actor) error {
		callsToMutateFn++
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		return fmt.Errorf("actor %s: %w", actorRef, mutationError)
	})
	// The error must arrive intact
	if !errors.Is(err, mutationError) {
		t.Errorf("UpdateActor error = %v, want one wrapping mutationError", err)
	}
	// Mutation errors are non-retriable
	if callsToMutateFn != 1 {
		t.Errorf("mutate ran %d times, want exactly 1 (a rejected precondition must not be retried)", callsToMutateFn)
	}

	got, err := s.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("aborted mutation was persisted (-created +got):\n%s", diff)
	}
}

func TestUpdateActor_DiscardsServerOwnedFieldsEdits(t *testing.T) {
	_, s, ctx := setupTest(t)

	actor := newTestActor("actor-1")
	if _, err := s.CreateAtespace(ctx, newTestAtespace(actor.Metadata.Atespace)); err != nil {
		t.Fatalf("CreateAtespace() failed: %v", err)
	}
	created, err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	actorRef := resources.ActorRefFromActor(actor)
	updated, err := s.UpdateActor(ctx, actorRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.Actor) error {
		// Metadata is server-owned: a closure must not be able to change it.
		toUpdate.Metadata.Uid = "forged-uid"
		toUpdate.Metadata.Version = 99
		toUpdate.Metadata.CreateTime = nil
		toUpdate.Metadata.UpdateTime = nil
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	if got := updated.GetMetadata().GetUid(); got != created.GetMetadata().GetUid() {
		t.Errorf("uid = %q, want the server-assigned %q", got, created.GetMetadata().GetUid())
	}
	if got := updated.GetMetadata().GetVersion(); got != created.GetMetadata().GetVersion()+1 {
		t.Errorf("version = %d, want %d (one past the stored version, not the forged value)", got, created.GetMetadata().GetVersion()+1)
	}
	if got := updated.GetMetadata().GetCreateTime(); got == nil || !got.AsTime().Equal(created.GetMetadata().GetCreateTime().AsTime()) {
		t.Errorf("create_time = %v, want the creation value %v", got, created.GetMetadata().GetCreateTime())
	}
	if updated.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("state = %v, want RUNNING: discarding metadata edits must not discard the mutation", updated.GetStatus().GetState())
	}
}

// TestUpdateActor_RejectsImmutableFieldChange covers the fields a mutation may
// not touch. Unlike the server-owned metadata, which is silently restored,
// these fail the call: a caller that renamed an actor or repointed its template
// asked for something the store cannot do, and must hear about it.
func TestUpdateActor_RejectsImmutableFieldChange(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(toUpdate *ateapipb.Actor)
		wantField string
	}{
		{
			name:      "atespace",
			mutate:    func(toUpdate *ateapipb.Actor) { toUpdate.Metadata.Atespace = "other-atespace" },
			wantField: "metadata.atespace",
		},
		{
			name:      "name",
			mutate:    func(toUpdate *ateapipb.Actor) { toUpdate.Metadata.Name = "other-name" },
			wantField: "metadata.name",
		},
		{
			name:      "actor template namespace",
			mutate:    func(toUpdate *ateapipb.Actor) { toUpdate.ActorTemplateNamespace = "other-ns" },
			wantField: "actor_template_namespace",
		},
		{
			name:      "actor template name",
			mutate:    func(toUpdate *ateapipb.Actor) { toUpdate.ActorTemplateName = "other-template" },
			wantField: "actor_template_name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, s, ctx := setupTest(t)
			actor := newTestActor("actor-1")
			if _, err := s.CreateAtespace(ctx, newTestAtespace(actor.Metadata.Atespace)); err != nil {
				t.Fatalf("CreateAtespace() failed: %v", err)
			}
			created, err := s.CreateActor(ctx, actor)
			if err != nil {
				t.Fatalf("CreateActor failed: %v", err)
			}

			actorRef := resources.ActorRefFromActor(actor)
			_, err = s.UpdateActor(ctx, actorRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.Actor) error {
				// Paired with a legitimate edit, so the rejection cannot be
				// mistaken for a no-op mutation.
				toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
				tt.mutate(toUpdate)
				return nil
			})
			// The message must name the offending field: the closure is buggy,
			// and whoever has to fix it only has this error to go on.
			if want := tt.wantField + " is immutable"; err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("UpdateActor changing %s = %v, want an error containing %q", tt.name, err, want)
			}

			got, err := s.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("GetActor failed: %v", err)
			}
			if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
				t.Errorf("rejected mutation was persisted anyway (-created +got):\n%s", diff)
			}
		})
	}
}

// watchInterceptor runs before each WATCH'd transaction body, so a test can
// write the watched key from another connection and make EXEC fail the way a
// real concurrent writer would.
type watchInterceptor struct {
	redisClient
	before func()
}

func (w *watchInterceptor) Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error {
	return w.redisClient.Watch(ctx, func(tx *redis.Tx) error {
		w.before()
		return fn(tx)
	}, keys...)
}

func TestUpdateActor_RetriesOnConcurrentWrite(t *testing.T) {
	mr, s, ctx := setupTest(t)
	actor := newTestActor("actor-1")
	if _, err := s.CreateAtespace(ctx, newTestAtespace(actor.Metadata.Atespace)); err != nil {
		t.Fatalf("CreateAtespace() failed: %v", err)
	}
	created, err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	actorRef := resources.ActorRefFromActor(actor)

	// A separate client, so its write lands outside the transaction's connection.
	otherClient := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{mr.Addr()}})
	t.Cleanup(func() { otherClient.Close() })

	attempts := 0
	interceptor := &watchInterceptor{redisClient: s.rdb, before: func() {
		// Only the first attempt races. We do this to make sure the second retry
		// will succeed.
		if attempts > 0 {
			return
		}
		concurrent, err := s.GetActor(ctx, actorRef)
		if err != nil {
			t.Errorf("GetActor for concurrent write failed: %v", err)
			return
		}
		concurrent.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
		val, err := protojson.Marshal(concurrent)
		if err != nil {
			t.Errorf("protojson.Marshal failed: %v", err)
			return
		}
		if err := otherClient.Set(ctx, actorDBKey(actorRef), val, 0).Err(); err != nil {
			t.Errorf("concurrent Set failed: %v", err)
		}
	}}
	racing := &Persistence{rdb: interceptor, lockTTL: defaultLockTTL}

	updated, err := racing.UpdateActor(ctx, actorRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.Actor) error {
		attempts++
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}
	if attempts < 2 {
		t.Errorf("mutate ran %d times, want at least 2: the firts write is racey and must be rejected", attempts)
	}
	if updated.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("state = %v, want RUNNING", updated.GetStatus().GetState())
	}
	// 1. The concurrent tx wrote "tier: paid" worker selector. This change should survive instead of
	// being reverted by a mutation computed against the older state.
	if got := updated.GetWorkerSelector().GetMatchLabels()["tier"]; got != "paid" {
		t.Errorf("worker_selector[tier] = %q, want %q: the retry clobbered the concurrent write", got, "paid")
	}
}

func TestUpdateActor_NotFound(t *testing.T) {
	_, s, ctx := setupTest(t)
	// Well-formed precondition, so the call gets as far as the read it is
	// meant to fail on.
	guard := store.Precondition{UID: "9a2b1c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d", Version: 1}
	_, err := s.UpdateActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: "non-existent"}, guard, func(toUpdate *ateapipb.Actor) error {
		t.Error("mutate must not run for a missing actor")
		return nil
	})
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected store.ErrNotFound, got %v", err)
	}
}

func TestUpdateActor_RejectsStaleUID(t *testing.T) {
	_, s, ctx := setupTest(t)

	a := newTestActor("actor-1")
	if _, err := s.CreateAtespace(ctx, newTestAtespace(a.Metadata.Atespace)); err != nil {
		t.Fatalf("CreateAtespace() failed: %v", err)
	}
	original, err := s.CreateActor(ctx, a)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	actorRef := resources.ActorRefFromActor(original)
	if _, err := s.UpdateActor(ctx, actorRef, store.PreconditionFrom(original), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_DELETING
		return nil
	}); err != nil {
		t.Fatalf("marking actor deleting failed: %v", err)
	}
	if _, err := s.DeleteActor(ctx, actorRef); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	recreated, err := s.CreateActor(ctx, newTestActor("actor-1"))
	if err != nil {
		t.Fatalf("recreate CreateActor failed: %v", err)
	}
	if recreated.GetMetadata().GetUid() == original.GetMetadata().GetUid() {
		t.Fatalf("recreated actor reused uid %s, want a fresh one", recreated.GetMetadata().GetUid())
	}

	// original guards on version 1, and the recreated actor is also at version 1, so
	// only the uid can tell the two incarnations apart.
	_, err = s.UpdateActor(ctx, actorRef, store.PreconditionFrom(original), func(toUpdate *ateapipb.Actor) error {
		t.Error("mutate ran past its precondition once the guarded incarnation was gone")
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		return nil
	})
	if !errors.Is(err, store.ErrUIDConflict) {
		t.Errorf("UpdateActor error = %v, want one matching store.ErrUIDConflict", err)
	}

	// The guarded version still matches, so this is the incarnation failure alone.
	if errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("UpdateActor error = %v, want no store.ErrVersionConflict match: the guarded version is the stored one", err)
	}

	stored, err := s.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := stored.GetMetadata().GetVersion(); got != recreated.GetMetadata().GetVersion() {
		t.Errorf("version = %d, want %d: the rejected update still wrote", got, recreated.GetMetadata().GetVersion())
	}
}

func TestUpdateActor_RejectsStaleVersion(t *testing.T) {
	_, s, ctx := setupTest(t)

	a := newTestActor("actor-1")
	if _, err := s.CreateAtespace(ctx, newTestAtespace(a.Metadata.Atespace)); err != nil {
		t.Fatalf("CreateAtespace() failed: %v", err)
	}
	created, err := s.CreateActor(ctx, a)
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	actorRef := resources.ActorRefFromActor(created)

	if _, err := s.UpdateActor(ctx, actorRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		return nil
	}); err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	// The write above moved the version, so created is now a stale observation.
	_, err = s.UpdateActor(ctx, actorRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.Actor) error {
		t.Error("mutate ran past its precondition once the guarded version had moved")
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
		return nil
	})
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("UpdateActor error = %v, want one matching store.ErrVersionConflict", err)
	}
	// The uid still matches, so this is not the incarnation failure: callers key
	// their retry decision off the difference.
	if errors.Is(err, store.ErrUIDConflict) {
		t.Errorf("UpdateActor error = %v, want no store.ErrUIDConflict match: the incarnation is unchanged", err)
	}

}

func TestGetWorker_NotFound(t *testing.T) {
	_, s, ctx := setupTest(t)

	_, err := s.GetWorker(ctx, "non-existent")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestCreateWorker_Success(t *testing.T) {
	_, s, ctx := setupTest(t)

	watch, err := s.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}

	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "worker-1"},
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
		Status:          &ateapipb.WorkerStatus{},
	}
	if err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	got, err := s.GetWorker(ctx, "worker-1")
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}
	if v := got.GetMetadata().GetVersion(); v != 1 {
		t.Errorf("expected version 1, got %d", v)
	}
	if diff := cmp.Diff(worker, got, protocmp.Transform(), ignoreUID, ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("GetWorker returned unexpected worker (-want +got):\n%s", diff)
	}

	event := receiveEvent(t, watch.Events)
	if event.Type != store.WorkerEventCreated {
		t.Errorf("expected WorkerEventCreated, got %v", event.Type)
	}
	if diff := cmp.Diff(worker, event.Worker, protocmp.Transform(), ignoreUID, ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("created event worker mismatch (-want +got):\n%s", diff)
	}
}

func TestUpdateWorker_Success(t *testing.T) {
	_, s, ctx := setupTest(t)

	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "worker-1"},
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
		Status:          &ateapipb.WorkerStatus{},
	}
	if err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	// Subscribe after create so the create event doesn't pollute the channel.
	watch, err := s.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}

	worker.Status.Assignment = &ateapipb.ActorAssignment{
		ActorTemplate: &ateapipb.KubeNamespacedObjectRef{Namespace: "default", Name: "test-template"},
		Actor:         &ateapipb.ObjectRef{Name: "actor-1"},
		ActorUid:      "actor-1-uid",
	}
	if err := s.UpdateWorker(ctx, worker, 1); err != nil {
		t.Fatalf("UpdateWorker failed: %v", err)
	}

	got, err := s.GetWorker(ctx, "worker-1")
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}
	if v := got.GetMetadata().GetVersion(); v != 2 {
		t.Errorf("expected version 2, got %d", v)
	}
	if diff := cmp.Diff(worker, got, protocmp.Transform(), ignoreUID, ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("UpdateWorker yielded unexpected state in DB (-want +got):\n%s", diff)
	}

	event := receiveEvent(t, watch.Events)
	if event.Type != store.WorkerEventUpdated {
		t.Errorf("expected WorkerEventUpdated, got %v", event.Type)
	}
	if diff := cmp.Diff(worker, event.Worker, protocmp.Transform(), ignoreUID, ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("updated event worker mismatch (-want +got):\n%s", diff)
	}
}

func TestUpdateWorker_NotFound(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "non-existent"},
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
		Status:          &ateapipb.WorkerStatus{},
	}
	err := s.UpdateWorker(ctx, worker, 1)
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected store.ErrNotFound, got %v", err)
	}
}

func TestUpdateWorker_Conflict(t *testing.T) {
	_, s, ctx := setupTest(t)

	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "worker-1"},
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
		Status:          &ateapipb.WorkerStatus{},
	}
	if err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	// Fetch instance 1
	worker1, err := s.GetWorker(ctx, "worker-1")
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}
	// Fetch instance 2
	worker2, err := s.GetWorker(ctx, "worker-1")
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}

	// Update instance 1
	worker1.Status.Assignment = &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-1"},
		ActorUid: "actor-1-uid",
	}
	if err := s.UpdateWorker(ctx, worker1, worker1.GetMetadata().GetVersion()); err != nil {
		t.Fatalf("UpdateWorker failed: %v", err)
	}

	// Try to update instance 2
	worker2.Status.Assignment = &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-2"},
		ActorUid: "actor-2-uid",
	}
	err = s.UpdateWorker(ctx, worker2, worker2.GetMetadata().GetVersion())
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("expected ErrVersionConflict, got %v", err)
	}
}

func TestCreateWorker_AlreadyExists(t *testing.T) {
	_, s, ctx := setupTest(t)

	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "worker-1"},
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
		Status:          &ateapipb.WorkerStatus{},
	}
	if err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	err := s.CreateWorker(ctx, worker)
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestDeleteWorker(t *testing.T) {
	_, s, ctx := setupTest(t)

	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "worker-1"},
		WorkerNamespace: "default",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
		Status:          &ateapipb.WorkerStatus{},
	}
	if err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	// Subscribe after create so the create event doesn't pollute the channel.
	watch, err := s.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}

	if err := s.DeleteWorker(ctx, "worker-1"); err != nil {
		t.Fatalf("DeleteWorker failed: %v", err)
	}
	if _, err := s.GetWorker(ctx, "worker-1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}

	event := receiveEvent(t, watch.Events)
	if event.Type != store.WorkerEventDeleted {
		t.Errorf("expected WorkerEventDeleted, got %v", event.Type)
	}
	// The delete event carries only the name: the row is gone, so there is
	// nothing else to report.
	want := &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: "worker-1"}}
	if diff := cmp.Diff(want, event.Worker, protocmp.Transform()); diff != "" {
		t.Errorf("deleted event worker mismatch (-want +got):\n%s", diff)
	}
}

func TestListWorkers(t *testing.T) {
	_, s, ctx := setupTest(t)

	worker1 := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "worker-1"},
		WorkerNamespace: "ns1",
		WorkerPool:      "pool1",
		WorkerPod:       "pod1",
	}
	worker2 := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "worker-2"},
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

	resp, err := s.ListWorkers(ctx, store.ListOptions{PageSize: 1000})
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Errorf("expected 2 workers, got %d", len(resp.Items))
	}

	found1 := false
	found2 := false
	for _, w := range resp.Items {
		if w.GetMetadata().GetName() == "worker-1" {
			found1 = true
		}
		if w.GetMetadata().GetName() == "worker-2" {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Errorf("did not find all workers: found1=%t, found2=%t", found1, found2)
	}
}

func TestListWorkers_Empty(t *testing.T) {
	_, s, ctx := setupTest(t)

	resp, err := s.ListWorkers(ctx, store.ListOptions{PageSize: 1000})
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	if len(resp.Items) != 0 {
		t.Errorf("expected 0 workers, got %d", len(resp.Items))
	}
}

func TestListWorkers_Pagination(t *testing.T) {
	_, s, ctx := setupTest(t)

	for i := 0; i < 5; i++ {
		worker := &ateapipb.Worker{
			Metadata:        &ateapipb.ResourceMetadata{Name: fmt.Sprintf("worker-%d", i)},
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
		name := w.GetMetadata().GetName()
		if seen[name] {
			t.Errorf("duplicate worker found in paginated results: %s", name)
		}
		seen[name] = true
	}
}

func TestDeleteActor(t *testing.T) {
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
			_, s, ctx := setupTest(t)

			actor := &ateapipb.Actor{
				Metadata:               &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: testAtespace},
				ActorTemplateNamespace: "default",
				ActorTemplateName:      "test-template",
				Status:                 &ateapipb.ActorStatus{State: tt.state},
			}

			if _, err := s.CreateAtespace(ctx, newTestAtespace(testAtespace)); err != nil {
				t.Fatalf("CreateAtespace(%s) failed: %v", testAtespace, err)
			}
			if _, err := s.CreateActor(ctx, actor); err != nil {
				t.Fatalf("CreateActor failed: %v", err)
			}

			deleted, err := s.DeleteActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: "actor-1"})
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
			if got := deleted.GetMetadata().GetName(); got != "actor-1" {
				t.Errorf("deleted actor name = %q, want actor-1", got)
			}

			if _, err := s.GetActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: "actor-1"}); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("expected ErrNotFound after delete, got %v", err)
			}
		})
	}
}

func TestDeleteActor_NotFound(t *testing.T) {
	_, s, ctx := setupTest(t)

	_, err := s.DeleteActor(ctx, resources.ActorRef{Atespace: testAtespace, Name: "non-existent"})
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound deleting non-existent actor, got %v", err)
	}
}

func TestListActors(t *testing.T) {
	_, s, ctx := setupTest(t)

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
	if _, err := s.CreateAtespace(ctx, newTestAtespace(testAtespace)); err != nil {
		t.Fatalf("CreateAtespace() failed: %v", err)
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

func TestActorSnapshotLifecycle(t *testing.T) {
	_, s, ctx := setupTest(t)
	snapshot := &ateapipb.ActorSnapshot{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "snapshot-1"},
		Status: &ateapipb.ActorSnapshotStatus{
			SourceActor:        &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-1"},
			SourceActorUid:     "actor-uid",
			SourceActorVersion: 7,
			ContentScope:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			SnapshotUri:        "gs://bucket/root/snapshots/" + testAtespace + "/snapshot-1",
		},
	}
	created, err := s.CreateActorSnapshot(ctx, snapshot)
	if err != nil {
		t.Fatalf("CreateActorSnapshot: %v", err)
	}
	got, err := s.GetActorSnapshot(ctx, testAtespace, "snapshot-1")
	if err != nil {
		t.Fatalf("GetActorSnapshot: %v", err)
	}
	// The store round-trips the whole resource, snapshot_uri included: it is
	// an ordinary field now, not a value the store keeps beside the record.
	if !proto.Equal(created, got) {
		t.Fatalf("GetActorSnapshot = %v, want %v", got, created)
	}
	tag := &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "before-upgrade"},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	}
	tagged, err := s.CreateActorSnapshotTag(ctx, testAtespace, "snapshot-1", tag)
	if err != nil || tagged.GetSnapshot().GetName() != "snapshot-1" {
		t.Fatalf("CreateActorSnapshotTag = (%v, %v), want stable tag", tagged, err)
	}
	resolvedTag, err := s.GetActorSnapshotTag(ctx, testAtespace, "before-upgrade")
	if err != nil || !proto.Equal(tagged, resolvedTag) {
		t.Fatalf("GetActorSnapshotTag = (%v, %v), want tagged tag", resolvedTag, err)
	}
	byTag, err := s.GetActorSnapshot(ctx, resolvedTag.GetSnapshot().GetAtespace(), resolvedTag.GetSnapshot().GetName())
	if err != nil || !proto.Equal(created, byTag) {
		t.Fatalf("GetActorSnapshot(resolved tag target) = (%v, %v), want tagged snapshot", byTag, err)
	}
	if _, err := s.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "other", Name: "snapshot-2"},
		Status:   &ateapipb.ActorSnapshotStatus{SnapshotUri: "gs://bucket/root/snapshots/other/snapshot-2"},
	}); err != nil {
		t.Fatalf("CreateActorSnapshot second snapshot: %v", err)
	}
	otherTag := &ateapipb.ActorSnapshotTag{Metadata: &ateapipb.ResourceMetadata{Atespace: "other", Name: "before-upgrade"}, Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE}
	if _, err := s.CreateActorSnapshotTag(ctx, "other", "snapshot-2", otherTag); err != nil {
		t.Fatalf("same tag name in another Atespace: %v", err)
	}
	if _, err := s.CreateActorSnapshotTag(ctx, "other", "snapshot-2", tag); !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("duplicate Atespace tag error = %v, want ErrAlreadyExists", err)
	}
	differentScope := proto.Clone(tag).(*ateapipb.ActorSnapshotTag)
	differentScope.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
	if _, err := s.CreateActorSnapshotTag(ctx, testAtespace, "snapshot-1", differentScope); !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("re-tag with different scope error = %v, want ErrAlreadyExists", err)
	}
	tagged, err = s.UpdateActorSnapshotTag(ctx, testAtespace, "before-upgrade", store.PreconditionFrom(tagged), func(toUpdate *ateapipb.ActorSnapshotTag) error {
		toUpdate.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
		return nil
	})
	if err != nil || tagged.GetScope() != ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED {
		t.Fatalf("UpdateActorSnapshotTag = (%v, %v), want published", tagged, err)
	}
	if resolvedTag, err = s.GetActorSnapshotTag(ctx, testAtespace, "before-upgrade"); err != nil || resolvedTag.GetScope() != ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED {
		t.Fatalf("tag after publication = (%v, %v), want published scope", resolvedTag, err)
	}
	if byTag, err = s.GetActorSnapshot(ctx, resolvedTag.GetSnapshot().GetAtespace(), resolvedTag.GetSnapshot().GetName()); err != nil || byTag.GetMetadata().GetUid() != created.GetMetadata().GetUid() {
		t.Fatalf("snapshot after publication = (%v, %v), want same address", byTag, err)
	}
	listed, err := s.ListActorSnapshots(ctx, testAtespace, store.ListOptions{PageSize: 10})
	if err != nil || len(listed.Items) != 1 {
		t.Fatalf("ListActorSnapshots = (%v, %v), want one", listed.Items, err)
	}

	deleted, err := s.DeleteActorSnapshotTag(ctx, testAtespace, "before-upgrade")
	if err != nil || deleted.GetMetadata().GetName() != "before-upgrade" {
		t.Fatalf("DeleteActorSnapshotTag = (%v, %v)", deleted, err)
	}
	if _, err := s.GetActorSnapshotTag(ctx, testAtespace, "before-upgrade"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("deleted tag lookup = %v, want ErrNotFound", err)
	}
	if got, err := s.GetActorSnapshot(ctx, testAtespace, "snapshot-1"); err != nil || got.GetMetadata().GetUid() != created.GetMetadata().GetUid() {
		t.Fatalf("snapshot after tag deletion = (%v, %v), want retained metadata", got, err)
	}
}

// seedTaggedSnapshot stores a snapshot and an Atespace-scoped tag pointing at
// it, and returns the stored tag.
func seedTaggedSnapshot(t *testing.T, s *Persistence, ctx context.Context, snapshotName, tagName string) *ateapipb.ActorSnapshotTag {
	t.Helper()
	if _, err := s.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: snapshotName},
		Status:   &ateapipb.ActorSnapshotStatus{SnapshotUri: "gs://bucket/root/snapshots/" + testAtespace + "/" + snapshotName},
	}); err != nil {
		t.Fatalf("CreateActorSnapshot(%s) failed: %v", snapshotName, err)
	}
	tagged, err := s.CreateActorSnapshotTag(ctx, testAtespace, snapshotName, &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tagName},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	})
	if err != nil {
		t.Fatalf("CreateActorSnapshotTag(%s) failed: %v", tagName, err)
	}
	return tagged
}

func TestUpdateActorSnapshotTag_MutateErrorAreNotRetried(t *testing.T) {
	_, s, ctx := setupTest(t)
	tagged := seedTaggedSnapshot(t, s, ctx, "snapshot-1", "tag-1")

	var mutationError = errors.New("mutation error")

	callsToMutateFn := 0
	_, err := s.UpdateActorSnapshotTag(ctx, testAtespace, "tag-1", store.PreconditionFrom(tagged), func(toUpdate *ateapipb.ActorSnapshotTag) error {
		callsToMutateFn++
		toUpdate.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
		return fmt.Errorf("tag %s/%s: %w", testAtespace, "tag-1", mutationError)
	})
	// The error must arrive intact
	if !errors.Is(err, mutationError) {
		t.Errorf("UpdateActorSnapshotTag error = %v, want one wrapping mutationError", err)
	}
	// Mutation errors are non-retriable
	if callsToMutateFn != 1 {
		t.Errorf("mutate ran %d times, want exactly 1 (a rejected precondition must not be retried)", callsToMutateFn)
	}

	got, err := s.GetActorSnapshotTag(ctx, testAtespace, "tag-1")
	if err != nil {
		t.Fatalf("GetActorSnapshotTag failed: %v", err)
	}
	if diff := cmp.Diff(tagged, got, protocmp.Transform()); diff != "" {
		t.Errorf("aborted mutation was persisted (-tagged +got):\n%s", diff)
	}
}

func TestUpdateActorSnapshotTag_DiscardsServerOwnedFieldsEdits(t *testing.T) {
	_, s, ctx := setupTest(t)
	tagged := seedTaggedSnapshot(t, s, ctx, "snapshot-1", "tag-1")

	updated, err := s.UpdateActorSnapshotTag(ctx, testAtespace, "tag-1", store.PreconditionFrom(tagged), func(toUpdate *ateapipb.ActorSnapshotTag) error {
		// Metadata is server-owned: a closure must not be able to change it.
		toUpdate.Metadata.Uid = "forged-uid"
		toUpdate.Metadata.Version = 99
		toUpdate.Metadata.CreateTime = nil
		toUpdate.Metadata.UpdateTime = nil
		toUpdate.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateActorSnapshotTag failed: %v", err)
	}

	if got := updated.GetMetadata().GetUid(); got != tagged.GetMetadata().GetUid() {
		t.Errorf("uid = %q, want the server-assigned %q", got, tagged.GetMetadata().GetUid())
	}
	if got := updated.GetMetadata().GetVersion(); got != tagged.GetMetadata().GetVersion()+1 {
		t.Errorf("version = %d, want %d (one past the stored version, not the forged value)", got, tagged.GetMetadata().GetVersion()+1)
	}
	if got := updated.GetMetadata().GetCreateTime(); got == nil || !got.AsTime().Equal(tagged.GetMetadata().GetCreateTime().AsTime()) {
		t.Errorf("create_time = %v, want the creation value %v", got, tagged.GetMetadata().GetCreateTime())
	}
	if got, want := updated.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED; got != want {
		t.Errorf("scope = %v, want %v: discarding metadata edits must not discard the mutation", got, want)
	}
}

// TestUpdateActorSnapshotTag_RejectsImmutableFieldChange covers the fields a
// mutation may not touch. Unlike the server-owned metadata, which is silently
// restored, these fail the call: a caller that renamed a tag or repointed it at
// another snapshot asked for something the store cannot do, and must hear about
// it.
func TestUpdateActorSnapshotTag_RejectsImmutableFieldChange(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(toUpdate *ateapipb.ActorSnapshotTag)
		wantField string
	}{
		{
			name:      "atespace",
			mutate:    func(toUpdate *ateapipb.ActorSnapshotTag) { toUpdate.Metadata.Atespace = "other-atespace" },
			wantField: "metadata.atespace",
		},
		{
			name:      "name",
			mutate:    func(toUpdate *ateapipb.ActorSnapshotTag) { toUpdate.Metadata.Name = "other-name" },
			wantField: "metadata.name",
		},
		{
			name:      "snapshot atespace",
			mutate:    func(toUpdate *ateapipb.ActorSnapshotTag) { toUpdate.Snapshot.Atespace = "other-atespace" },
			wantField: "snapshot.atespace",
		},
		{
			name:      "snapshot name",
			mutate:    func(toUpdate *ateapipb.ActorSnapshotTag) { toUpdate.Snapshot.Name = "other-snapshot" },
			wantField: "snapshot.name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, s, ctx := setupTest(t)
			tagged := seedTaggedSnapshot(t, s, ctx, "snapshot-1", "tag-1")

			_, err := s.UpdateActorSnapshotTag(ctx, testAtespace, "tag-1", store.PreconditionFrom(tagged), func(toUpdate *ateapipb.ActorSnapshotTag) error {
				// Paired with a legitimate edit, so the rejection cannot be
				// mistaken for a no-op mutation.
				toUpdate.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
				tt.mutate(toUpdate)
				return nil
			})
			// The message must name the offending field: the closure is buggy,
			// and whoever has to fix it only has this error to go on.
			if want := tt.wantField + " is immutable"; err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("UpdateActorSnapshotTag changing %s = %v, want an error containing %q", tt.name, err, want)
			}

			got, err := s.GetActorSnapshotTag(ctx, testAtespace, "tag-1")
			if err != nil {
				t.Fatalf("GetActorSnapshotTag failed: %v", err)
			}
			if diff := cmp.Diff(tagged, got, protocmp.Transform()); diff != "" {
				t.Errorf("rejected mutation was persisted anyway (-tagged +got):\n%s", diff)
			}
		})
	}
}

func TestUpdateActorSnapshotTag_RetriesOnConcurrentWrite(t *testing.T) {
	mr, s, ctx := setupTest(t)
	tagged := seedTaggedSnapshot(t, s, ctx, "snapshot-1", "tag-1")
	tagKey := actorSnapshotTagDBKey(testAtespace, "tag-1")

	// A separate client, so its write lands outside the transaction's connection.
	otherClient := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{mr.Addr()}})
	t.Cleanup(func() { otherClient.Close() })

	attempts := 0
	interceptor := &watchInterceptor{redisClient: s.rdb, before: func() {
		// Only the first attempt races. We do this to make sure the second retry
		// will succeed.
		if attempts > 0 {
			return
		}
		concurrent, err := s.GetActorSnapshotTag(ctx, testAtespace, "tag-1")
		if err != nil {
			t.Errorf("GetActorSnapshotTag for concurrent write failed: %v", err)
			return
		}
		// Repointing the tag is not something a mutation may do, but a writer
		// holding the key can: the retry must carry it forward, not revert it.
		concurrent.Snapshot = &ateapipb.ObjectRef{Atespace: testAtespace, Name: "snapshot-2"}
		val, err := protojson.Marshal(concurrent)
		if err != nil {
			t.Errorf("protojson.Marshal failed: %v", err)
			return
		}
		if err := otherClient.Set(ctx, tagKey, val, 0).Err(); err != nil {
			t.Errorf("concurrent Set failed: %v", err)
		}
	}}
	racing := &Persistence{rdb: interceptor, lockTTL: defaultLockTTL}

	updated, err := racing.UpdateActorSnapshotTag(ctx, testAtespace, "tag-1", store.PreconditionFrom(tagged), func(toUpdate *ateapipb.ActorSnapshotTag) error {
		attempts++
		toUpdate.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateActorSnapshotTag failed: %v", err)
	}
	if attempts < 2 {
		t.Errorf("mutate ran %d times, want at least 2: the first write is racey and must be rejected", attempts)
	}
	if got, want := updated.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED; got != want {
		t.Errorf("scope = %v, want %v", got, want)
	}
	// The concurrent tx repointed the tag at snapshot-2. That change should
	// survive instead of being reverted by a mutation computed against the
	// older state.
	if got := updated.GetSnapshot().GetName(); got != "snapshot-2" {
		t.Errorf("snapshot.name = %q, want %q: the retry clobbered the concurrent write", got, "snapshot-2")
	}
}

func TestUpdateActorSnapshotTag_NotFound(t *testing.T) {
	_, s, ctx := setupTest(t)
	// A well-formed precondition, so the call gets as far as the read it is
	// meant to fail on.
	guard := store.Precondition{UID: "9a2b1c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d", Version: 1}
	_, err := s.UpdateActorSnapshotTag(ctx, testAtespace, "does-not-exist", guard, func(toUpdate *ateapipb.ActorSnapshotTag) error {
		t.Error("mutate must not run for a missing tag")
		return nil
	})
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected store.ErrNotFound, got %v", err)
	}
}

func TestUpdateActorSnapshotTag_RejectsStaleUID(t *testing.T) {
	_, s, ctx := setupTest(t)

	original := seedTaggedSnapshot(t, s, ctx, "snapshot-1", "tag-1")
	if _, err := s.DeleteActorSnapshotTag(ctx, testAtespace, "tag-1"); err != nil {
		t.Fatalf("DeleteActorSnapshotTag failed: %v", err)
	}
	recreated, err := s.CreateActorSnapshotTag(ctx, testAtespace, "snapshot-1", &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag-1"},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	})
	if err != nil {
		t.Fatalf("re-tag CreateActorSnapshotTag failed: %v", err)
	}
	if recreated.GetMetadata().GetUid() == original.GetMetadata().GetUid() {
		t.Fatalf("recreated tag reused uid %s, want a fresh one", recreated.GetMetadata().GetUid())
	}
	// The version reset to 1 along with the uid, so a version guard alone would
	// have waved this write through. Only the uid distinguishes the lifecycles.
	if got, want := recreated.GetMetadata().GetVersion(), original.GetMetadata().GetVersion(); got != want {
		t.Fatalf("recreated version = %d, want %d: the version cannot tell the lifecycles apart", got, want)
	}

	// original and recreated have different UIDs, so the mutation must never run
	_, err = s.UpdateActorSnapshotTag(ctx, testAtespace, "tag-1", store.PreconditionFrom(original), func(toUpdate *ateapipb.ActorSnapshotTag) error {
		t.Error("mutate ran past its precondition once the guarded incarnation was gone")
		toUpdate.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
		return nil
	})
	if !errors.Is(err, store.ErrUIDConflict) {
		t.Errorf("UpdateActorSnapshotTag error = %v, want one matching store.ErrUIDConflict", err)
	}

	// The guarded version still matches, so this is the incarnation failure alone.
	if errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("UpdateActorSnapshotTag error = %v, want no store.ErrVersionConflict match: the guarded version is the stored one", err)
	}

	stored, err := s.GetActorSnapshotTag(ctx, testAtespace, "tag-1")
	if err != nil {
		t.Fatalf("GetActorSnapshotTag failed: %v", err)
	}
	if diff := cmp.Diff(recreated, stored, protocmp.Transform()); diff != "" {
		t.Errorf("the rejected update still wrote (-recreated +stored):\n%s", diff)
	}
}

func TestUpdateActorSnapshotTag_RejectsStaleVersion(t *testing.T) {
	_, s, ctx := setupTest(t)

	tagged := seedTaggedSnapshot(t, s, ctx, "snapshot-1", "tag-1")

	if _, err := s.UpdateActorSnapshotTag(ctx, testAtespace, "tag-1", store.PreconditionFrom(tagged), func(toUpdate *ateapipb.ActorSnapshotTag) error {
		toUpdate.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
		return nil
	}); err != nil {
		t.Fatalf("UpdateActorSnapshotTag failed: %v", err)
	}

	// The write above moved the version, so tagged is now a stale observation.
	_, err := s.UpdateActorSnapshotTag(ctx, testAtespace, "tag-1", store.PreconditionFrom(tagged), func(toUpdate *ateapipb.ActorSnapshotTag) error {
		t.Error("mutate ran past its precondition once the guarded version had moved")
		toUpdate.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE
		return nil
	})
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("UpdateActorSnapshotTag error = %v, want one matching store.ErrVersionConflict", err)
	}
	// The uid still matches, so this is not the incarnation failure: callers key
	// their retry decision off the difference.
	if errors.Is(err, store.ErrUIDConflict) {
		t.Errorf("UpdateActorSnapshotTag error = %v, want no store.ErrUIDConflict match: the incarnation is unchanged", err)
	}
}

func TestListAtespaces_Pagination(t *testing.T) {
	_, s, ctx := setupTest(t)

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
}

func TestListActors_Empty(t *testing.T) {
	_, s, ctx := setupTest(t)

	actors, err := s.ListActors(ctx, "", store.ListOptions{PageSize: 1000})
	if err != nil {
		t.Fatalf("ListActors failed: %v", err)
	}

	if len(actors.Items) != 0 {
		t.Errorf("expected 0 actors, got %d", len(actors.Items))
	}
}

func TestListActors_Pagination(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.CreateAtespace(ctx, newTestAtespace(testAtespace)); err != nil {
		t.Fatalf("CreateAtespace() failed: %v", err)
	}

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
}

func TestAcquireLock_Success(t *testing.T) {
	mr, s, ctx := setupTest(t)

	key := "test-lock"

	lock, err := s.AcquireLock(ctx, key)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	defer lock.Close()

	if !mr.Exists(key) {
		t.Errorf("expected lock key to exist after AcquireLock")
	}
}

func TestAcquireLock_Conflict(t *testing.T) {
	_, s, ctx := setupTest(t)

	key := "test-lock"

	lock, err := s.AcquireLock(ctx, key)
	if err != nil {
		t.Fatalf("first AcquireLock failed: %v", err)
	}
	defer lock.Close()

	_, err = s.AcquireLock(ctx, key)
	if !errors.Is(err, store.ErrLockConflict) {
		t.Errorf("second AcquireLock error = %v, want ErrLockConflict", err)
	}
}

func TestLock_Close_ReleasesLockImmediately(t *testing.T) {
	mr, s, ctx := setupTest(t)

	key := "test-lock"

	lock, err := s.AcquireLock(ctx, key)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	lock.Close()

	// Close should release the key immediately rather than making the next
	// caller wait out the rest of the TTL.
	if mr.Exists(key) {
		t.Errorf("expected lock to be deleted after Close")
	}

	next, err := s.AcquireLock(ctx, key)
	if err != nil {
		t.Fatalf("AcquireLock after Close failed: %v", err)
	}
	next.Close()
}

func TestLock_Close_CancelsContext(t *testing.T) {
	_, s, ctx := setupTest(t)

	lock, err := s.AcquireLock(ctx, "test-lock")
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	lock.Close()

	select {
	case <-lock.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("expected lock.Context() to be cancelled after Close")
	}
}

func TestLock_Close_ReleasesEvenAfterParentContextCancelled(t *testing.T) {
	mr, s, _ := setupTest(t)

	key := "test-lock"
	parentCtx, parentCancel := context.WithCancel(context.Background())

	lock, err := s.AcquireLock(parentCtx, key)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	// Simulate the caller's own context dying independently of Close, e.g. an
	// upstream RPC deadline. The renewal loop should stop as a result.
	parentCancel()

	select {
	case <-lock.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("expected lock.Context() to be cancelled once the parent context is cancelled")
	}

	// A real caller's `defer lock.Close()` still runs after this. Close must
	// still release the key even though the context it was acquired with is
	// already dead, since it releases via context.Background() internally.
	lock.Close()

	if mr.Exists(key) {
		t.Errorf("expected Close to release the lock even though the parent context was already cancelled")
	}
}

func TestAcquireLock_ExpiresAndIsReacquirableAfterHolderCrashes(t *testing.T) {
	mr, s, _ := setupTest(t)

	key := "test-lock"
	ttl := 300 * time.Millisecond
	s.lockTTL = ttl

	parentCtx, parentCancel := context.WithCancel(context.Background())
	lock, err := s.AcquireLock(parentCtx, key)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	// Simulate a hard crash: the holder disappears without ever calling
	// Close (e.g. the process is killed), so the key is never explicitly
	// released and is left to expire on its own TTL. Canceling the parent
	// context stops the renewal loop the same way process death would,
	// without releasing the key the way Close does.
	parentCancel()
	select {
	case <-lock.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("expected lock.Context() to be cancelled once the parent context is cancelled")
	}

	if !mr.Exists(key) {
		t.Fatal("expected the key to still exist right after the crash; only Close deletes it")
	}
	if _, err := s.AcquireLock(context.Background(), key); !errors.Is(err, store.ErrLockConflict) {
		t.Errorf("AcquireLock before TTL expiry: err = %v, want ErrLockConflict", err)
	}

	// Simulate real time passing with no renewer left alive, until the key's
	// actual Redis TTL elapses. miniredis's TTLs are purely virtual --
	// stored durations decremented only by FastForward, never by wall-clock
	// time -- so a real time.Sleep here would not expire the key at all.
	mr.FastForward(ttl + time.Second)

	if mr.Exists(key) {
		t.Fatal("expected the key to have expired in Redis once its TTL elapsed")
	}

	newOwner, err := s.AcquireLock(context.Background(), key)
	if err != nil {
		t.Fatalf("AcquireLock after crash + TTL expiry failed: %v", err)
	}
	defer newOwner.Close()
}

func TestLock_Close_DoesNotStealALockReacquiredAfterLeaseLoss(t *testing.T) {
	mr, s, ctx := setupTest(t)

	key := "test-lock"
	ttl := 300 * time.Millisecond
	s.lockTTL = ttl

	lock, err := s.AcquireLock(ctx, key)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	// Lose the lease out from under the renewal loop.
	mr.Del(key)
	select {
	case <-lock.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("expected lock.Context() to be cancelled once the lease is lost")
	}

	// A different holder acquires the same key once it's free.
	newOwner, err := s.AcquireLock(ctx, key)
	if err != nil {
		t.Fatalf("AcquireLock by new owner failed: %v", err)
	}
	defer newOwner.Close()

	// The original Lock no longer owns the key; Close must be a safe no-op
	// rather than deleting the new owner's lock out from under it.
	lock.Close()

	if !mr.Exists(key) {
		t.Errorf("expected the new owner's lock to survive the old Lock's Close, but the key was deleted")
	}
}

func TestLock_Close_Idempotent(t *testing.T) {
	_, s, ctx := setupTest(t)

	lock, err := s.AcquireLock(ctx, "test-lock")
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	lock.Close()
	lock.Close() // must not panic or double-release.
}

func TestRenewDeadlineFractionLeavesRetryHeadroom(t *testing.T) {
	const minRetries = 2

	intervalFraction := 1.0 / renewIntervalDivisor
	retryPeriodFraction := 1.0 / renewRetryPeriodDivisor
	floor := intervalFraction + minRetries*retryPeriodFraction

	if renewDeadlineFraction <= floor {
		t.Fatalf("renewDeadlineFraction (%v) must exceed intervalFraction + %d*retryPeriodFraction (%v) to leave room for %d retries; "+
			"at or below intervalFraction (%v) alone, the very first renewal attempt would already find the deadline elapsed",
			renewDeadlineFraction, minRetries, floor, minRetries, intervalFraction)
	}
}

func TestAcquireLock_RenewsUntilClosed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &mockRedisClient{SetNXFunc: acquires, EvalShaFunc: renews}
		s := &Persistence{rdb: mock, lockTTL: defaultLockTTL}

		lock, err := s.AcquireLock(t.Context(), "test-lock")
		if err != nil {
			t.Fatalf("AcquireLock failed: %v", err)
		}
		defer lock.Close()

		time.Sleep(3 * defaultLockTTL)
		synctest.Wait()

		if err := lock.Context().Err(); err != nil {
			t.Errorf("lock.Context().Err() = %v, want nil (lease still held)", err)
		}
	})
}

func TestLock_ContextCancelled_OnLeaseLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &mockRedisClient{SetNXFunc: acquires, EvalShaFunc: leaseLost}
		s := &Persistence{rdb: mock, lockTTL: defaultLockTTL}

		lock, err := s.AcquireLock(t.Context(), "test-lock")
		if err != nil {
			t.Fatalf("AcquireLock failed: %v", err)
		}
		defer lock.Close()

		time.Sleep(defaultLockTTL)
		synctest.Wait()

		if err := lock.Context().Err(); err == nil {
			t.Error("expected lock.Context() to be cancelled once renewal detects the lease is lost")
		}
	})
}

func TestAcquireLock_RenewalRecoversFromTransientError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Clears with margin to spare before the renew deadline, after a
		// couple of retryPeriod-spaced attempts.
		renewDeadline := time.Duration(float64(defaultLockTTL) * renewDeadlineFraction)
		retryPeriod := defaultLockTTL / renewRetryPeriodDivisor
		errorClearsAt := time.Now().Add(renewDeadline - 2*retryPeriod)

		mock := &mockRedisClient{SetNXFunc: acquires, EvalShaFunc: failsUntil(errorClearsAt, errors.New("connection refused"))}
		s := &Persistence{rdb: mock, lockTTL: defaultLockTTL}

		lock, err := s.AcquireLock(t.Context(), "test-lock")
		if err != nil {
			t.Fatalf("AcquireLock failed: %v", err)
		}
		defer lock.Close()

		time.Sleep(2 * defaultLockTTL)
		synctest.Wait()

		if err := lock.Context().Err(); err != nil {
			t.Errorf("lock.Context().Err() = %v, want nil (renewal should have recovered from the transient error)", err)
		}
	})
}

func TestAcquireLock_RenewalGivesUpOncePersistentErrorOutlastsTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &mockRedisClient{SetNXFunc: acquires, EvalShaFunc: failsWith(errors.New("connection refused"))}
		s := &Persistence{rdb: mock, lockTTL: defaultLockTTL}

		lock, err := s.AcquireLock(t.Context(), "test-lock")
		if err != nil {
			t.Fatalf("AcquireLock failed: %v", err)
		}
		defer lock.Close()

		time.Sleep(defaultLockTTL) // past the renew deadline (renewDeadlineFraction * defaultLockTTL)
		synctest.Wait()

		if err := lock.Context().Err(); err == nil {
			t.Error("expected lock.Context() to be cancelled once the persistent error outlasts the renew deadline")
		}
	})
}

func TestAcquireLock_RenewalGivesUpWhenRedisHangsUntilDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &mockRedisClient{SetNXFunc: acquires, EvalShaFunc: hangs}
		s := &Persistence{rdb: mock, lockTTL: defaultLockTTL}

		lock, err := s.AcquireLock(t.Context(), "test-lock")
		if err != nil {
			t.Fatalf("AcquireLock failed: %v", err)
		}

		time.Sleep(defaultLockTTL)
		synctest.Wait()

		if err := lock.Context().Err(); err == nil {
			t.Error("expected lock.Context() to be cancelled once every renewal attempt hangs past the renew deadline")
		}
	})
}

func TestAcquireLock_RenewalGivesUpAfterMixOfFastFailuresThenHang(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &mockRedisClient{SetNXFunc: acquires, EvalShaFunc: failsNTimesThenHangs(2, errors.New("connection refused"))}
		s := &Persistence{rdb: mock, lockTTL: defaultLockTTL}

		lock, err := s.AcquireLock(t.Context(), "test-lock")
		if err != nil {
			t.Fatalf("AcquireLock failed: %v", err)
		}

		time.Sleep(defaultLockTTL)
		synctest.Wait()

		if err := lock.Context().Err(); err == nil {
			t.Error("expected lock.Context() to be cancelled once the renew deadline elapses, whether attempts fail fast or hang")
		}
	})
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

func TestListActors_ScopedByAtespace(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace(team-a) failed: %v", err)
	}
	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-b")); err != nil {
		t.Fatalf("CreateAtespace(team-b) failed: %v", err)
	}

	mkActor := func(atespace, name string) *ateapipb.Actor {
		return &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Name: name, Atespace: atespace},
			ActorTemplateNamespace: "ns1",
			ActorTemplateName:      "tmpl1",
			Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
	teamA, err := s.ListActors(ctx, "team-a", store.ListOptions{PageSize: 1000})
	if err != nil {
		t.Fatalf("ListActors(team-a) failed: %v", err)
	}
	if got := actorNameSet(teamA.Items); !got["a1"] || !got["a2"] || got["b1"] || len(got) != 2 {
		t.Errorf("ListActors(team-a) = %v, want exactly {a1, a2}", got)
	}

	teamB, err := s.ListActors(ctx, "team-b", store.ListOptions{PageSize: 1000})
	if err != nil {
		t.Fatalf("ListActors(team-b) failed: %v", err)
	}
	if got := actorNameSet(teamB.Items); !got["b1"] || got["a1"] || len(got) != 1 {
		t.Errorf("ListActors(team-b) = %v, want exactly {b1}", got)
	}

	// An empty atespace lists across all atespaces (the admin/dev `-A` view).
	all, err := s.ListActors(ctx, "", store.ListOptions{PageSize: 1000})
	if err != nil {
		t.Fatalf("ListActors(all) failed: %v", err)
	}
	if got := actorNameSet(all.Items); !got["a1"] || !got["a2"] || !got["b1"] || len(got) != 3 {
		t.Errorf("ListActors(all) = %v, want exactly {a1, a2, b1}", got)
	}

	// Get is scoped too: right atespace hits, wrong/empty atespace misses.
	if _, err := s.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "a1"}); err != nil {
		t.Errorf("GetActor(team-a, a1) failed: %v", err)
	}
	if _, err := s.GetActor(ctx, resources.ActorRef{Atespace: "team-b", Name: "a1"}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetActor(team-b, a1) = %v, want ErrNotFound", err)
	}
	if _, err := s.GetActor(ctx, resources.ActorRef{Atespace: "", Name: "a1"}); !errors.Is(err, store.ErrNotFound) {
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
	_, s, ctx := setupTest(t)

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
	_, s, ctx := setupTest(t)

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("first CreateAtespace failed: %v", err)
	}
	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestGetAtespace_NotFound(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.GetAtespace(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestAtespaceExists(t *testing.T) {
	_, s, ctx := setupTest(t)

	if ok, err := s.atespaceExists(ctx, "team-a"); err != nil || ok {
		t.Fatalf("atespaceExists before create = (%v, %v), want (false, nil)", ok, err)
	}
	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if ok, err := s.atespaceExists(ctx, "team-a"); err != nil || !ok {
		t.Fatalf("atespaceExists after create = (%v, %v), want (true, nil)", ok, err)
	}
}

func TestListAtespaces(t *testing.T) {
	_, s, ctx := setupTest(t)

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
}

func TestListAtespaces_Empty(t *testing.T) {
	_, s, ctx := setupTest(t)

	got, err := s.ListAtespaces(ctx, store.ListOptions{PageSize: 1000})
	if err != nil {
		t.Fatalf("ListAtespaces failed: %v", err)
	}
	if len(got.Items) != 0 {
		t.Errorf("ListAtespaces on empty store = %v, want empty", got.Items)
	}
}

func TestDeleteAtespace_Empty(t *testing.T) {
	_, s, ctx := setupTest(t)

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

func TestDeleteAtespace_WithTags_Rejected(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace: %v", err)
	}
	if _, err := s.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "snapshot-1"},
		Status:   &ateapipb.ActorSnapshotStatus{SnapshotUri: "gs://bucket/root/snapshots/team-a/snapshot-1"},
	}); err != nil {
		t.Fatalf("CreateActorSnapshot: %v", err)
	}
	if _, err := s.CreateActorSnapshotTag(ctx, "team-a", "snapshot-1", &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "keep-me"},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
	}); err != nil {
		t.Fatalf("CreateActorSnapshotTag: %v", err)
	}
	if _, err := s.DeleteAtespace(ctx, "team-a"); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("DeleteAtespace = %v, want ErrFailedPrecondition", err)
	}
	if _, err := s.GetActorSnapshotTag(ctx, "team-a", "keep-me"); err != nil {
		t.Fatalf("GetActorSnapshotTag after rejected deletion: %v", err)
	}
	if _, err := s.GetAtespace(ctx, "team-a"); err != nil {
		t.Fatalf("GetAtespace after rejected deletion: %v", err)
	}
}

func TestDeleteAtespace_WithActorTemplates_Rejected(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace: %v", err)
	}
	if _, err := s.CreateActorTemplate(ctx, newTestActorTemplate("team-a", "tmpl-a")); err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	if _, err := s.DeleteAtespace(ctx, "team-a"); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("DeleteAtespace with templates = %v, want ErrFailedPrecondition", err)
	}

	if _, err := s.DeleteActorTemplate(ctx, resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-a"}); err != nil {
		t.Fatalf("DeleteActorTemplate: %v", err)
	}
	if _, err := s.DeleteAtespace(ctx, "team-a"); err != nil {
		t.Errorf("DeleteAtespace after template removed = %v, want nil", err)
	}
}

func TestDeleteAtespace_NotFound(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.DeleteAtespace(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestDeleteAtespace_NonEmpty_Rejected(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if _, err := s.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: "id1", Atespace: "team-a"}, Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED}}); err != nil {
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
	_, s, ctx := setupTest(t)

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
		t.Errorf("DeleteAtespace after actor removed = %v, want nil (re-scan should find it empty)", err)
	}
}

func TestDeleteAtespace_EmptyWhileOtherAtespaceNonEmpty(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace(team-a) failed: %v", err)
	}
	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-b")); err != nil {
		t.Fatalf("CreateAtespace(team-b) failed: %v", err)
	}

	// Actor lives ONLY in team-b.
	if _, err := s.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: "id1", Atespace: "team-b"}, Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED}}); err != nil {
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

		if _, err := tempS.CreateAtespace(ctx, newTestAtespace(testAtespace)); err != nil {
			t.Fatalf("failed to seed atespace: %v", err)
		}

		for itemIdx := 0; itemIdx < 3; itemIdx++ {
			actor := &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{
					Name:     fmt.Sprintf("actor-shard%d-item%d", shardIdx, itemIdx),
					Atespace: testAtespace,
				},
				ActorTemplateNamespace: "default",
				ActorTemplateName:      "test-template",
				Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
		page, err := s.ListActors(ctx, testAtespace, store.ListOptions{PageSize: 2, PageToken: pageToken})
		if err != nil {
			t.Fatalf("ListActors failed: %v", err)
		}
		allActors = append(allActors, page.Items...)
		if page.NextPageToken == "" {
			break
		}
		pageToken = page.NextPageToken
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
					pod := fmt.Sprintf("pod-shard%d-item%d", shardIdx, itemIdx)
					worker := &ateapipb.Worker{
						Metadata:        &ateapipb.ResourceMetadata{Name: "uid-" + pod},
						WorkerNamespace: "ns",
						WorkerPool:      "pool",
						WorkerPod:       pod,
					}
					if err := ps.CreateWorker(ctx, worker); err != nil {
						t.Fatalf("failed to seed worker: %v", err)
					}
				}
			}

			seen := make(map[string]bool)
			pageToken := ""
			for {
				page, err := s.ListWorkers(ctx, store.ListOptions{PageSize: pageSize, PageToken: pageToken})
				if err != nil {
					t.Fatalf("ListWorkers: %v", err)
				}
				for _, w := range page.Items {
					if seen[w.GetWorkerPod()] {
						t.Errorf("duplicate worker in paginated results: %s", w.GetWorkerPod())
					}
					seen[w.GetWorkerPod()] = true
				}
				if page.NextPageToken == "" {
					break
				}
				pageToken = page.NextPageToken
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
				page, err := s.ListAtespaces(ctx, store.ListOptions{PageSize: pageSize, PageToken: pageToken})
				if err != nil {
					t.Fatalf("ListAtespaces: %v", err)
				}
				for _, a := range page.Items {
					if seen[a.GetMetadata().GetName()] {
						t.Errorf("duplicate atespace in paginated results: %s", a.GetMetadata().GetName())
					}
					seen[a.GetMetadata().GetName()] = true
				}
				if page.NextPageToken == "" {
					break
				}
				pageToken = page.NextPageToken
			}
			if len(seen) != numShards*3 {
				t.Fatalf("expected %d atespaces across %d shards, got %d", numShards*3, numShards, len(seen))
			}
		})
	}
}

type setNXFunc func(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.BoolCmd

type evalFunc func(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd

type mockRedisClient struct {
	redisClient

	SetNXFunc   setNXFunc
	EvalShaFunc evalFunc
}

func (m *mockRedisClient) SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.BoolCmd {
	return m.SetNXFunc(ctx, key, value, ttl)
}

func (m *mockRedisClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return m.EvalShaFunc(ctx, sha1, keys, args...)
}

// intCmd and errCmd build the two possible shapes of a script-eval result:
// intCmd for the CAS script's 1 (applied) / 0 (not owned) return value, and
// errCmd for a failed call.
func intCmd(ctx context.Context, v int64) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(v)
	return cmd
}

func errCmd(ctx context.Context, err error) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetErr(err)
	return cmd
}

// acquires is a setNXFunc reporting the lock was acquired.
func acquires(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.BoolCmd {
	cmd := redis.NewBoolCmd(ctx)
	cmd.SetVal(true)
	return cmd
}

// renews is an evalFunc reporting a successful renewal.
func renews(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return intCmd(ctx, 1)
}

// leaseLost is an evalFunc reporting that the CAS check found we no longer
// own the key (someone else took over, or it was deleted) -- Mode 6: an
// authoritative "you don't hold this anymore," not a retryable failure.
func leaseLost(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return intCmd(ctx, 0)
}

// failsWith returns an evalFunc that always fails fast with err.
func failsWith(err error) evalFunc {
	return func(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
		return errCmd(ctx, err)
	}
}

// hangs is an evalFunc that blocks until ctx is done, simulating an
// unresponsive Redis.
func hangs(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	<-ctx.Done()
	return errCmd(ctx, ctx.Err())
}

// failsUntil returns an evalFunc that fails fast with err until t, then
// reports a successful renewal.
func failsUntil(t time.Time, err error) evalFunc {
	return func(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
		if time.Now().Before(t) {
			return errCmd(ctx, err)
		}
		return intCmd(ctx, 1)
	}
}

// failsNTimesThenHangs returns an evalFunc that fails fast with err for its
// first n calls, then hangs (as hangs does) on every call after that.
func failsNTimesThenHangs(n int, err error) evalFunc {
	var mu sync.Mutex
	left := n
	return func(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
		mu.Lock()
		fail := left > 0
		if fail {
			left--
		}
		mu.Unlock()

		if fail {
			return errCmd(ctx, err)
		}
		return hangs(ctx, sha1, keys, args...)
	}
}

func newTestActorTemplate(atespace, name string) *ateapipb.ActorTemplate {
	return &ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name}}
}

func TestActorTemplateLifecycle(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace() failed: %v", err)
	}

	want := newTestActorTemplate("team-a", "tmpl-a")
	created, err := s.CreateActorTemplate(ctx, want)
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	if created.GetMetadata().GetUid() == "" {
		t.Errorf("CreateActorTemplate returned empty uid; want server-assigned uid")
	}
	if created.GetMetadata().GetVersion() != 1 {
		t.Errorf("CreateActorTemplate returned version %d, want 1", created.GetMetadata().GetVersion())
	}
	if created.GetMetadata().GetCreateTime() == nil || created.GetMetadata().GetUpdateTime() == nil {
		t.Errorf("CreateActorTemplate returned unset create/update time")
	}
	// The input must not be mutated.
	if want.GetMetadata().GetUid() != "" || want.GetMetadata().GetVersion() != 0 {
		t.Errorf("CreateActorTemplate must not mutate its input, got metadata %v", want.GetMetadata())
	}

	got, err := s.GetActorTemplate(ctx, resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-a"})
	if err != nil {
		t.Fatalf("GetActorTemplate failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("CreateActorTemplate return does not match stored state (-created +got):\n%s", diff)
	}

	listResp, err := s.ListActorTemplates(ctx, "team-a", store.ListOptions{PageSize: 1000})
	if err != nil {
		t.Fatalf("ListActorTemplates failed: %v", err)
	}
	list := listResp.Items
	if len(list) != 1 || list[0].GetMetadata().GetName() != "tmpl-a" {
		t.Errorf("ListActorTemplates = %v, want [tmpl-a]", list)
	}

	deleted, err := s.DeleteActorTemplate(ctx, resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-a"})
	if err != nil {
		t.Fatalf("DeleteActorTemplate failed: %v", err)
	}
	if diff := cmp.Diff(created, deleted, protocmp.Transform()); diff != "" {
		t.Errorf("DeleteActorTemplate returned unexpected resource (-created +deleted):\n%s", diff)
	}
	if _, err := s.GetActorTemplate(ctx, resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-a"}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("after delete, GetActorTemplate = %v, want ErrNotFound", err)
	}
}

func TestCreateActorTemplate_AlreadyExists(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace() failed: %v", err)
	}

	if _, err := s.CreateActorTemplate(ctx, newTestActorTemplate("team-a", "tmpl-a")); err != nil {
		t.Fatalf("first CreateActorTemplate failed: %v", err)
	}
	if _, err := s.CreateActorTemplate(ctx, newTestActorTemplate("team-a", "tmpl-a")); !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestGetActorTemplate_NotFound(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.GetActorTemplate(ctx, resources.ActorTemplateRef{Atespace: "team-a", Name: "nope"}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestActorTemplateExists(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace() failed: %v", err)
	}

	if ok, err := s.ActorTemplateExists(ctx, resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-a"}); err != nil || ok {
		t.Fatalf("ActorTemplateExists before create = (%v, %v), want (false, nil)", ok, err)
	}
	if _, err := s.CreateActorTemplate(ctx, newTestActorTemplate("team-a", "tmpl-a")); err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	if ok, err := s.ActorTemplateExists(ctx, resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-a"}); err != nil || !ok {
		t.Fatalf("ActorTemplateExists after create = (%v, %v), want (true, nil)", ok, err)
	}
}

func TestUpdateActorTemplate_NotFound(t *testing.T) {
	_, s, ctx := setupTest(t)
	// A well-formed precondition, so the call gets as far as the read it is
	// meant to fail on.
	guard := store.Precondition{UID: "9a2b1c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d", Version: 1}
	_, err := s.UpdateActorTemplate(ctx, resources.ActorTemplateRef{Atespace: "team-a", Name: "non-existent"}, guard, func(dbTemplate *ateapipb.ActorTemplate) error {
		t.Error("mutate must not run for a missing template")
		return nil
	})
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected store.ErrNotFound, got %v", err)
	}
}

func TestDeleteActorTemplate_NotFound(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.DeleteActorTemplate(ctx, resources.ActorTemplateRef{Atespace: "team-a", Name: "nope"}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestListActorTemplates_Pagination(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace() failed: %v", err)
	}

	for i := 0; i < 5; i++ {
		if _, err := s.CreateActorTemplate(ctx, newTestActorTemplate("team-a", fmt.Sprintf("tmpl-%d", i))); err != nil {
			t.Fatalf("failed to create template %d: %v", i, err)
		}
	}

	var all []*ateapipb.ActorTemplate
	pageToken := ""
	for {
		page, err := s.ListActorTemplates(ctx, "team-a", store.ListOptions{PageSize: 2, PageToken: pageToken})
		if err != nil {
			t.Fatalf("ListActorTemplates failed: %v", err)
		}
		all = append(all, page.Items...)
		pageToken = page.NextPageToken
		if pageToken == "" {
			break
		}
	}

	if len(all) != 5 {
		t.Fatalf("expected 5 templates total, got %d", len(all))
	}
	seen := make(map[string]bool)
	for _, tmpl := range all {
		if seen[tmpl.GetMetadata().GetName()] {
			t.Errorf("duplicate template found in paginated results: %s", tmpl.GetMetadata().GetName())
		}
		seen[tmpl.GetMetadata().GetName()] = true
	}
}

func TestActorTemplates_AtespaceIsolation(t *testing.T) {
	_, s, ctx := setupTest(t)

	// The same name in two atespaces is two distinct resources.
	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace(%s) failed: %v", "team-a", err)
	}
	inA, err := s.CreateActorTemplate(ctx, newTestActorTemplate("team-a", "tmpl"))
	if err != nil {
		t.Fatalf("CreateActorTemplate in team-a failed: %v", err)
	}
	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-b")); err != nil {
		t.Fatalf("CreateAtespace(%s) failed: %v", "team-b", err)
	}
	inB, err := s.CreateActorTemplate(ctx, newTestActorTemplate("team-b", "tmpl"))
	if err != nil {
		t.Fatalf("CreateActorTemplate in team-b = %v, want nil: the name is only taken in team-a", err)
	}
	if inA.GetMetadata().GetUid() == inB.GetMetadata().GetUid() {
		t.Fatalf("templates in different atespaces share uid %q", inA.GetMetadata().GetUid())
	}

	got, err := s.GetActorTemplate(ctx, resources.ActorTemplateRef{Atespace: "team-b", Name: "tmpl"})
	if err != nil {
		t.Fatalf("GetActorTemplate(team-b) failed: %v", err)
	}
	if got.GetMetadata().GetUid() != inB.GetMetadata().GetUid() {
		t.Errorf("GetActorTemplate(team-b) returned uid %q, want team-b's %q", got.GetMetadata().GetUid(), inB.GetMetadata().GetUid())
	}

	// The wrong atespace is a clean NotFound, not an internal error.
	if _, err := s.GetActorTemplate(ctx, resources.ActorTemplateRef{Atespace: "team-c", Name: "tmpl"}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetActorTemplate(team-c) = %v, want ErrNotFound", err)
	}
	if ok, err := s.ActorTemplateExists(ctx, resources.ActorTemplateRef{Atespace: "team-c", Name: "tmpl"}); err != nil || ok {
		t.Errorf("ActorTemplateExists(team-c) = (%v, %v), want (false, nil)", ok, err)
	}

	// Deleting in one atespace leaves the other's untouched.
	if _, err := s.DeleteActorTemplate(ctx, resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl"}); err != nil {
		t.Fatalf("DeleteActorTemplate(team-a) failed: %v", err)
	}
	if _, err := s.GetActorTemplate(ctx, resources.ActorTemplateRef{Atespace: "team-b", Name: "tmpl"}); err != nil {
		t.Errorf("GetActorTemplate(team-b) after deleting team-a's = %v, want nil", err)
	}
}

func TestListActorTemplates_AtespaceFilter(t *testing.T) {
	_, s, ctx := setupTest(t)

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace(%s) failed: %v", "team-a", err)
	}
	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-b")); err != nil {
		t.Fatalf("CreateAtespace(%s) failed: %v", "team-b", err)
	}

	for _, tmpl := range []struct{ atespace, name string }{
		{"team-a", "tmpl-1"}, {"team-a", "tmpl-2"}, {"team-b", "tmpl-3"},
	} {
		if _, err := s.CreateActorTemplate(ctx, newTestActorTemplate(tmpl.atespace, tmpl.name)); err != nil {
			t.Fatalf("CreateActorTemplate(%s/%s) failed: %v", tmpl.atespace, tmpl.name, err)
		}
	}

	scopedResp, err := s.ListActorTemplates(ctx, "team-a", store.ListOptions{PageSize: 1000})
	if err != nil {
		t.Fatalf("ListActorTemplates(team-a) failed: %v", err)
	}
	scoped := scopedResp.Items
	if len(scoped) != 2 {
		t.Errorf("ListActorTemplates(team-a) returned %d templates, want 2", len(scoped))
	}
	for _, tmpl := range scoped {
		if got := tmpl.GetMetadata().GetAtespace(); got != "team-a" {
			t.Errorf("scoped list leaked template %q from atespace %q", tmpl.GetMetadata().GetName(), got)
		}
	}

	allResp, err := s.ListActorTemplates(ctx, "", store.ListOptions{PageSize: 1000})
	if err != nil {
		t.Fatalf("ListActorTemplates(all) failed: %v", err)
	}
	if len(allResp.Items) != 3 {
		t.Errorf("ListActorTemplates(all) returned %d templates, want 3", len(allResp.Items))
	}
}
