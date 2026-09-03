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
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/objectstore/objectstoretest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// seedTagSource stores a suspended actor whose external snapshot is made of
// objectNames, the state a tag can be taken from. It returns the actor and the
// URI of that snapshot.
func seedTagSource(t *testing.T, ctx context.Context, persistence store.Interface, objects *objectstoretest.Fake, template *ateapipb.ActorTemplate, name string, objectNames ...string) (*ateapipb.Actor, resources.SnapshotURI) {
	t.Helper()
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: template.GetMetadata().GetName()},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	uri := mustActorSnapshotURI(t, template, actor, name+"-snapshot")
	objects.PutSnapshot(t, uri, objectNames...)
	actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		s.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: uri.String(), ContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL}
	})
	return actor, uri
}

// tagToCreate is the part of an ActorSnapshotTag a client owns on create.
func tagToCreate(name string) *ateapipb.ActorSnapshotTag {
	return &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	}
}

// mustUpdateActorState moves actor to state, the way a resume or a suspend
// leaves it, and returns the stored actor.
func mustUpdateActorState(t *testing.T, ctx context.Context, persistence store.Interface, actor *ateapipb.Actor, state ateapipb.ActorState) *ateapipb.Actor {
	t.Helper()
	updated, err := persistence.UpdateActor(ctx, resources.ActorRefFromActor(actor), store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = state
		return nil
	})
	if err != nil {
		t.Fatalf("moving actor %s to %v: %v", resources.ActorRefFromActor(actor), state, err)
	}
	return updated
}

// mustParseSnapshotURI is [resources.ParseSnapshotURI] for a URI a test has
// already established is well formed.
func mustParseSnapshotURI(t *testing.T, uri string) resources.SnapshotURI {
	t.Helper()
	parsed, err := resources.ParseSnapshotURI(uri)
	if err != nil {
		t.Fatalf("ParseSnapshotURI(%q): %v", uri, err)
	}
	return parsed
}

// TestTagActorSnapshot verifies the whole tag workflow: the tag ends up owning
// a copy of the actor's external snapshot, not the actor's own, recording where
// that copy came from, and leaves the actor's snapshot alone.
func TestTagActorSnapshot(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, objects := newFinalizeWorkflow(persistence)

	actor, actorSnapshot := seedTagSource(t, ctx, persistence, objects, template, "actor-1", "manifest.json", "memory.zst")
	actorRef := resources.ActorRefFromActor(actor)

	tag, err := w.TagActorSnapshot(ctx, actorRef, tagToCreate("v1"))
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}

	tagSnapshot := tag.GetStatus().GetSnapshot().GetSnapshotUri()
	if tagSnapshot == "" || tagSnapshot == actorSnapshot.String() {
		t.Fatalf("tag snapshot uri = %q, want a copy of its own rather than the actor's %q", tagSnapshot, actorSnapshot)
	}
	if got := tag.GetStatus().GetInProgressSnapshotUri(); got != "" {
		t.Errorf("in-progress snapshot uri = %q, want cleared once the tag is finished", got)
	}
	if got, want := tag.GetStatus().GetSourceActorUid(), actor.GetMetadata().GetUid(); got != want {
		t.Errorf("source actor uid = %q, want %q", got, want)
	}
	if got, want := tag.GetStatus().GetActorTemplateUid(), template.GetMetadata().GetUid(); got != want {
		t.Errorf("actor template uid = %q, want %q", got, want)
	}
	if got, want := tag.GetStatus().GetSnapshot().GetContentScope(), ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL; got != want {
		t.Errorf("content scope = %v, want the source's %v", got, want)
	}

	// Both prefixes hold the same objects: the tag copied rather than moved.
	wantObjects := []string{"manifest.json", "memory.zst"}
	if diff := cmp.Diff(wantObjects, objects.Snapshot(t, mustParseSnapshotURI(t, tagSnapshot))); diff != "" {
		t.Errorf("the tag's external snapshot mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantObjects, objects.Snapshot(t, actorSnapshot)); diff != "" {
		t.Errorf("the actor's external snapshot mismatch (-want +got):\n%s", diff)
	}
}

// TestTagActorSnapshot_Preconditions verifies what a tag may be taken from:
// a suspended actor's finished external snapshot, and nothing else.
func TestTagActorSnapshot_Preconditions(t *testing.T) {
	tests := []struct {
		name         string
		state        ateapipb.ActorState
		withSnapshot bool
	}{
		{
			name:         "the actor is not suspended",
			state:        ateapipb.ActorState_ACTOR_STATE_RUNNING,
			withSnapshot: true,
		},
		{
			name:  "the actor holds no external snapshot",
			state: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
			w, objects := newFinalizeWorkflow(persistence)

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
				Status:        &ateapipb.ActorStatus{State: tt.state},
			})
			if tt.withSnapshot {
				uri := mustActorSnapshotURI(t, template, actor, "actor-1-snapshot")
				objects.PutSnapshot(t, uri, "manifest.json")
				mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
					s.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: uri.String()}
				})
			}

			_, err := w.TagActorSnapshot(ctx, actorRef, tagToCreate("v1"))
			if code := status.Code(err); code != codes.FailedPrecondition {
				t.Fatalf("TagActorSnapshot error = %v (code %v), want code FailedPrecondition", err, code)
			}
			// A rejected create takes the name with it.
			if _, err := persistence.GetActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "v1"}); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("GetActorSnapshotTag = %v, want ErrNotFound: the rejected create reserved the name", err)
			}
		})
	}
}

// TestTagActorSnapshot_RetriesAfterCopyFailure verifies what a create that dies
// mid-copy leaves behind, and what a retry does with it. The row survives as a
// pending tag naming the prefix the copy was writing into, so the objects it
// stranded are reachable; the retry adopts that row and finishes it in place
// rather than taking a second prefix.
func TestTagActorSnapshot_RetriesAfterCopyFailure(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, objects := newFinalizeWorkflow(persistence)

	actor, _ := seedTagSource(t, ctx, persistence, objects, template, "actor-1", "manifest.json", "memory.zst")
	actorRef := resources.ActorRefFromActor(actor)
	tagRef := resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "v1"}

	// The copy dies halfway through: the first object lands, the second does not.
	objects.OnCopy = func(_, srcObject, _, _ string) error {
		if strings.HasSuffix(srcObject, "memory.zst") {
			return errObjectStore
		}
		return nil
	}
	if _, err := w.TagActorSnapshot(ctx, actorRef, tagToCreate("v1")); !errors.Is(err, errObjectStore) {
		t.Fatalf("TagActorSnapshot = %v, want an error wrapping %v", err, errObjectStore)
	}

	pending, err := persistence.GetActorSnapshotTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetActorSnapshotTag after the external store failure: %v", err)
	}
	if got := pending.GetStatus().GetSnapshot().GetSnapshotUri(); got != "" {
		t.Errorf("snapshot uri after the failure = %q, want unset: the copy never finished", got)
	}
	stranded := pending.GetStatus().GetInProgressSnapshotUri()
	if stranded == "" {
		t.Fatal("in-progress snapshot uri after the failure is empty, want the prefix the copy stranded")
	}
	strandedURI := mustParseSnapshotURI(t, stranded)
	if diff := cmp.Diff([]string{"manifest.json"}, objects.Snapshot(t, strandedURI)); diff != "" {
		t.Errorf("the stranded partial copy mismatch (-want +got):\n%s", diff)
	}

	// The retry adopts the pending row: same destination, now complete.
	objects.OnCopy = nil
	tag, err := w.TagActorSnapshot(ctx, actorRef, tagToCreate("v1"))
	if err != nil {
		t.Fatalf("retried TagActorSnapshot: %v", err)
	}
	if got := tag.GetStatus().GetSnapshot().GetSnapshotUri(); got != stranded {
		t.Errorf("snapshot uri after the retry = %q, want the prefix the pending row already named, %q", got, stranded)
	}
	if got := tag.GetStatus().GetInProgressSnapshotUri(); got != "" {
		t.Errorf("in-progress snapshot uri after the retry = %q, want cleared", got)
	}
	if diff := cmp.Diff([]string{"manifest.json", "memory.zst"}, objects.Snapshot(t, strandedURI)); diff != "" {
		t.Errorf("the tag's external snapshot after the retry mismatch (-want +got):\n%s", diff)
	}

	// A further retry finds a finished tag. Its own copy's URI says nothing
	// about which suspend it was taken from, so the name is simply taken.
	_, err = w.TagActorSnapshot(ctx, actorRef, tagToCreate("v1"))
	if code := status.Code(err); code != codes.AlreadyExists {
		t.Errorf("TagActorSnapshot over the finished tag = %v (code %v), want code AlreadyExists", err, code)
	}
}

// TestTagActorSnapshot_RetryAfterResuspendDropsStaleObjects verifies that
// adopting a pending row clears its prefix before re-copying into it. The actor
// was resumed and suspended again between the attempts, so the snapshot it now
// holds has its own object set: overwriting alone would leave the objects only
// the old source had in place, and the tag would name a blend of two snapshots.
func TestTagActorSnapshot_RetryAfterResuspendDropsStaleObjects(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, objects := newFinalizeWorkflow(persistence)

	// The snapshot the second suspend left: a smaller memory image, so one part
	// fewer than the snapshot the first attempt was copying.
	actor, actorSnapshot := seedTagSource(t, ctx, persistence, objects, template, "actor-1", "manifest.json", "memory-0000.zst")
	actorRef := resources.ActorRefFromActor(actor)
	tagRef := resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "v1"}

	// What the first attempt left behind: a pending row, and the objects it had
	// copied out of the snapshot the actor held before it was resumed.
	stranded := mustTagSnapshotURI(t, template, "team-a", "v1-tag-snapshot")
	objects.PutSnapshot(t, stranded, "manifest.json", "memory-0000.zst", "memory-0001.zst")
	storetest.MustCreateActorSnapshotTag(t, ctx, persistence, &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: tagRef.Atespace, Name: tagRef.Name},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		Status: &ateapipb.ActorSnapshotTagStatus{
			ActorTemplateUid:      template.GetMetadata().GetUid(),
			InProgressSnapshotUri: stranded.String(),
			SourceActorUid:        actor.GetMetadata().GetUid(),
		},
	})

	tag, err := w.TagActorSnapshot(ctx, actorRef, tagToCreate("v1"))
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}
	if got := tag.GetStatus().GetSnapshot().GetSnapshotUri(); got != stranded.String() {
		t.Errorf("snapshot uri = %q, want the prefix the pending row already named, %q", got, stranded)
	}
	// memory-0001.zst belongs to a snapshot the actor no longer holds. Kept, it
	// would make the tag restore into two memory images spliced together.
	wantObjects := []string{"manifest.json", "memory-0000.zst"}
	if diff := cmp.Diff(wantObjects, objects.Snapshot(t, stranded)); diff != "" {
		t.Errorf("the tag's external snapshot mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantObjects, objects.Snapshot(t, actorSnapshot)); diff != "" {
		t.Errorf("the actor's external snapshot mismatch (-want +got):\n%s", diff)
	}
}

// TestTagActorSnapshot_RetriesAfterClearFailure verifies that a create recovers
// from a failure in the clear that precedes the re-copy. Nothing about the
// adoption is carried between attempts: it is re-derived from the pending row
// every time, so the clear a failed attempt never reached simply runs on the
// next one.
func TestTagActorSnapshot_RetriesAfterClearFailure(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, objects := newFinalizeWorkflow(persistence)

	actor, _ := seedTagSource(t, ctx, persistence, objects, template, "actor-1", "manifest.json", "memory.zst")
	actorRef := resources.ActorRefFromActor(actor)
	tagRef := resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "v1"}

	// The first attempt dies mid-copy, leaving a pending row over a partial copy.
	objects.OnCopy = func(_, srcObject, _, _ string) error {
		if strings.HasSuffix(srcObject, "memory.zst") {
			return errObjectStore
		}
		return nil
	}
	if _, err := w.TagActorSnapshot(ctx, actorRef, tagToCreate("v1")); !errors.Is(err, errObjectStore) {
		t.Fatalf("TagActorSnapshot = %v, want an error wrapping %v", err, errObjectStore)
	}
	pending, err := persistence.GetActorSnapshotTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetActorSnapshotTag after the failure: %v", err)
	}
	stranded := pending.GetStatus().GetInProgressSnapshotUri()
	strandedURI := mustParseSnapshotURI(t, stranded)

	// The second attempt adopts that row but cannot clear the partial copy.
	objects.OnCopy = nil
	objects.OnDelete = func(string, string) error { return errObjectStore }
	if _, err := w.TagActorSnapshot(ctx, actorRef, tagToCreate("v1")); !errors.Is(err, errObjectStore) {
		t.Fatalf("TagActorSnapshot over the failing clear = %v, want an error wrapping %v", err, errObjectStore)
	}
	stillPending, err := persistence.GetActorSnapshotTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetActorSnapshotTag after the failed clear: %v", err)
	}
	if got := stillPending.GetStatus().GetInProgressSnapshotUri(); got != stranded {
		t.Errorf("in-progress snapshot uri after the failed clear = %q, want the prefix the row already named, %q", got, stranded)
	}
	if diff := cmp.Diff([]string{"manifest.json"}, objects.Snapshot(t, strandedURI)); diff != "" {
		t.Errorf("the partial copy after the failed clear mismatch (-want +got):\n%s", diff)
	}

	// A third attempt runs the clear the second one never got through, and finishes.
	objects.OnDelete = nil
	tag, err := w.TagActorSnapshot(ctx, actorRef, tagToCreate("v1"))
	if err != nil {
		t.Fatalf("retried TagActorSnapshot: %v", err)
	}
	if got := tag.GetStatus().GetSnapshot().GetSnapshotUri(); got != stranded {
		t.Errorf("snapshot uri after the retry = %q, want the prefix the pending row already named, %q", got, stranded)
	}
	if diff := cmp.Diff([]string{"manifest.json", "memory.zst"}, objects.Snapshot(t, strandedURI)); diff != "" {
		t.Errorf("the tag's external snapshot after the retry mismatch (-want +got):\n%s", diff)
	}
}

// TestTagActorSnapshot_RetryWhileActorRunning verifies what a pending tag
// survives when the actor is resumed before the create is retried. The retry is
// refused, because a running actor's snapshot is stale rather than complete,
// but nothing about the pending row is cleaned up on the way out: it still
// names the objects the failed attempt stranded, so a later suspend can finish
// it and a delete can collect them.
func TestTagActorSnapshot_RetryWhileActorRunning(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, objects := newFinalizeWorkflow(persistence)

	actor, _ := seedTagSource(t, ctx, persistence, objects, template, "actor-1", "manifest.json", "memory.zst")
	actorRef := resources.ActorRefFromActor(actor)
	tagRef := resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "v1"}

	// The first attempt dies mid-copy, leaving a pending row over a partial copy.
	objects.OnCopy = func(_, srcObject, _, _ string) error {
		if strings.HasSuffix(srcObject, "memory.zst") {
			return errObjectStore
		}
		return nil
	}
	if _, err := w.TagActorSnapshot(ctx, actorRef, tagToCreate("v1")); !errors.Is(err, errObjectStore) {
		t.Fatalf("TagActorSnapshot = %v, want an error wrapping %v", err, errObjectStore)
	}
	objects.OnCopy = nil
	pending, err := persistence.GetActorSnapshotTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetActorSnapshotTag after the failure: %v", err)
	}
	stranded := pending.GetStatus().GetInProgressSnapshotUri()
	strandedURI := mustParseSnapshotURI(t, stranded)

	// The actor is resumed before the retry. It keeps the external snapshot it
	// restored from, so only its state rules the retry out.
	running := mustUpdateActorState(t, ctx, persistence, actor, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	if _, err := w.TagActorSnapshot(ctx, actorRef, tagToCreate("v1")); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("TagActorSnapshot over the running actor = %v (code %v), want code FailedPrecondition", err, status.Code(err))
	}
	stillPending, err := persistence.GetActorSnapshotTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetActorSnapshotTag after the refused retry: %v", err)
	}
	if got := stillPending.GetStatus().GetInProgressSnapshotUri(); got != stranded {
		t.Errorf("in-progress snapshot uri after the refused retry = %q, want the prefix the row already named, %q", got, stranded)
	}
	if got := stillPending.GetStatus().GetSnapshot().GetSnapshotUri(); got != "" {
		t.Errorf("snapshot uri after the refused retry = %q, want unset: the tag is still pending", got)
	}
	if diff := cmp.Diff([]string{"manifest.json"}, objects.Snapshot(t, strandedURI)); diff != "" {
		t.Errorf("the stranded partial copy after the refused retry mismatch (-want +got):\n%s", diff)
	}

	// Suspended again, the actor finishes the row it left pending.
	mustUpdateActorState(t, ctx, persistence, running, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	tag, err := w.TagActorSnapshot(ctx, actorRef, tagToCreate("v1"))
	if err != nil {
		t.Fatalf("TagActorSnapshot once the actor is suspended again: %v", err)
	}
	if got := tag.GetStatus().GetSnapshot().GetSnapshotUri(); got != stranded {
		t.Errorf("snapshot uri = %q, want the prefix the pending row already named, %q", got, stranded)
	}
	if diff := cmp.Diff([]string{"manifest.json", "memory.zst"}, objects.Snapshot(t, strandedURI)); diff != "" {
		t.Errorf("the tag's external snapshot mismatch (-want +got):\n%s", diff)
	}
}

// TestTagActorSnapshot_NameTakenByAnotherActor verifies that a pending tag is
// only ever resumed by the actor it was reserved for. Adopting another actor's
// unfinished create would union two sources into one prefix, so the name is
// reported as taken and the pending copy is left exactly as it was.
func TestTagActorSnapshot_NameTakenByAnotherActor(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, objects := newFinalizeWorkflow(persistence)

	first, _ := seedTagSource(t, ctx, persistence, objects, template, "actor-1", "manifest.json", "memory.zst")
	second, secondSnapshot := seedTagSource(t, ctx, persistence, objects, template, "actor-2", "other.json")
	tagRef := resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "v1"}

	// The first actor reserves the name, then dies in the copy.
	objects.OnCopy = func(_, srcObject, _, _ string) error {
		if strings.HasSuffix(srcObject, "memory.zst") {
			return errObjectStore
		}
		return nil
	}
	if _, err := w.TagActorSnapshot(ctx, resources.ActorRefFromActor(first), tagToCreate("v1")); !errors.Is(err, errObjectStore) {
		t.Fatalf("TagActorSnapshot(actor-1) = %v, want an error wrapping %v", err, errObjectStore)
	}
	pending, err := persistence.GetActorSnapshotTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetActorSnapshotTag: %v", err)
	}
	strandedURI := mustParseSnapshotURI(t, pending.GetStatus().GetInProgressSnapshotUri())

	// The second actor asks for the same name, with object storage healthy: it
	// must not inherit the first actor's prefix.
	objects.OnCopy = nil
	_, err = w.TagActorSnapshot(ctx, resources.ActorRefFromActor(second), tagToCreate("v1"))
	if code := status.Code(err); code != codes.AlreadyExists {
		t.Fatalf("TagActorSnapshot(actor-2) = %v (code %v), want code AlreadyExists", err, code)
	}
	if diff := cmp.Diff([]string{"manifest.json"}, objects.Snapshot(t, strandedURI)); diff != "" {
		t.Errorf("the rejected create wrote into the pending tag's prefix (-want +got):\n%s", diff)
	}
	stored, err := persistence.GetActorSnapshotTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetActorSnapshotTag after the rejected create: %v", err)
	}
	if got, want := stored.GetStatus().GetSourceActorUid(), first.GetMetadata().GetUid(); got != want {
		t.Errorf("source actor uid = %q, want the first actor's %q", got, want)
	}
	// The second actor kept its own snapshot; the failed tag collected nothing.
	if diff := cmp.Diff([]string{"other.json"}, objects.Snapshot(t, secondSnapshot)); diff != "" {
		t.Errorf("the second actor's external snapshot mismatch (-want +got):\n%s", diff)
	}

	// The actor the row belongs to can still finish it.
	if _, err := w.TagActorSnapshot(ctx, resources.ActorRefFromActor(first), tagToCreate("v1")); err != nil {
		t.Fatalf("TagActorSnapshot(actor-1) retry: %v", err)
	}
}
