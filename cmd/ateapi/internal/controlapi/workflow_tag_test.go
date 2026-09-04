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
	"sync"
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
		s.CurrentActorTemplateUid = template.GetMetadata().GetUid()
	})
	return actor, uri
}

// tagToCreate is the part of a Tag a client owns on create.
func tagToCreate(sourceActor resources.ActorRef, name string) *ateapipb.Tag {
	return &ateapipb.Tag{
		Metadata:    &ateapipb.ResourceMetadata{Name: name},
		Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: sourceActor.ToObjectRef(),
	}
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

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(actorRef, "v1"))
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

func TestTagActorSnapshot_ActorRepointedToAnotherTemplate(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	builtOn := seedSubstrateTemplate(t, ctx, persistence, "tmpl-a")
	repointedTo := seedSubstrateTemplate(t, ctx, persistence, "tmpl-b")
	w, objects := newFinalizeWorkflow(persistence)

	actor, _ := seedTagSource(t, ctx, persistence, objects, builtOn, "actor-1", "manifest.json", "memory.zst")
	actorRef := resources.ActorRefFromActor(actor)

	// Repoint the suspended actor, the way UpdateActor does. Its recorded
	// built-on UID stays at tmpl-a: only a resume moves that.
	if _, err := persistence.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.ActorTemplate = &ateapipb.ObjectRef{Atespace: "team-a", Name: "tmpl-b"}
		return nil
	}); err != nil {
		t.Fatalf("repointing actor %s: %v", actorRef, err)
	}

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(actorRef, "v1"))
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}

	if builtOn.GetMetadata().GetUid() == repointedTo.GetMetadata().GetUid() {
		t.Fatal("the two templates share a UID; the assertion below would be vacuous")
	}
	if got, want := tag.GetStatus().GetActorTemplateUid(), builtOn.GetMetadata().GetUid(); got != want {
		t.Errorf("actor template uid = %q, want the template the snapshot was built under %q (the actor now points at %q)",
			got, want, repointedTo.GetMetadata().GetUid())
	}
}

// TestTagActorSnapshot_Preconditions verifies what a tag may be taken from:
// a suspended actor's finished external snapshot, and nothing else.
func TestTagActorSnapshot_Preconditions(t *testing.T) {
	tests := []struct {
		name         string
		state        ateapipb.ActorState
		withSnapshot bool
		// builtOnTemplate records the template the seeded snapshot was taken
		// under, as every real path to holding one does. Only the case that
		// exercises the missing record leaves it false.
		builtOnTemplate bool
		wantCode        codes.Code
	}{
		{
			name:            "the actor is not suspended",
			state:           ateapipb.ActorState_ACTOR_STATE_RUNNING,
			withSnapshot:    true,
			builtOnTemplate: true,
			wantCode:        codes.FailedPrecondition,
		},
		{
			name:     "the actor holds no external snapshot",
			state:    ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			wantCode: codes.FailedPrecondition,
		},
		{
			name:         "the actor records no template its snapshot was built under",
			state:        ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			withSnapshot: true,
			wantCode:     codes.Internal,
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
					if tt.builtOnTemplate {
						s.CurrentActorTemplateUid = template.GetMetadata().GetUid()
					}
				})
			}

			_, err := w.TagActorSnapshot(ctx, tagToCreate(actorRef, "v1"))
			if code := status.Code(err); code != tt.wantCode {
				t.Fatalf("TagActorSnapshot error = %v (code %v), want code %v", err, code, tt.wantCode)
			}
			// A rejected create takes the name with it.
			if _, err := persistence.GetTag(ctx, resources.TagRef{Atespace: "team-a", Name: "v1"}); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("GetTag = %v, want ErrNotFound: the rejected create reserved the name", err)
			}
		})
	}
}

// TestTagActorSnapshot_RecreateAfterCopyFailure verifies what a create that
// dies mid-copy leaves behind, and how a client gets past it. The row survives
// as a pending tag naming the prefix the copy was writing into, so the objects
// it stranded are reachable; the name it holds is taken until the tag is
// deleted, and only then does a create under that name run again.
func TestTagActorSnapshot_RecreateAfterCopyFailure(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, objects := newFinalizeWorkflow(persistence)
	svc := &RPCService{impl: persistence, objectStore: objects}

	actor, _ := seedTagSource(t, ctx, persistence, objects, template, "actor-1", "manifest.json", "memory.zst")
	actorRef := resources.ActorRefFromActor(actor)
	tagRef := resources.TagRef{Atespace: "team-a", Name: "v1"}

	// The copy dies halfway through: the first object lands, the second does not.
	objects.OnCopy = func(_, srcObject, _, _ string) error {
		if strings.HasSuffix(srcObject, "memory.zst") {
			return errObjectStore
		}
		return nil
	}
	if _, err := w.TagActorSnapshot(ctx, tagToCreate(actorRef, "v1")); !errors.Is(err, errObjectStore) {
		t.Fatalf("TagActorSnapshot = %v, want an error wrapping %v", err, errObjectStore)
	}
	objects.OnCopy = nil

	pending, err := persistence.GetTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetTag after the external store failure: %v", err)
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

	// Creating under the same name again is refused, even for the actor the
	// pending row was reserved for, and leaves that row exactly as it was.
	_, err = w.TagActorSnapshot(ctx, tagToCreate(actorRef, "v1"))
	if code := status.Code(err); code != codes.AlreadyExists {
		t.Fatalf("TagActorSnapshot over the pending tag = %v (code %v), want code AlreadyExists", err, code)
	}
	stillPending, err := persistence.GetTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetTag after the refused create: %v", err)
	}
	if got := stillPending.GetStatus().GetInProgressSnapshotUri(); got != stranded {
		t.Errorf("in-progress snapshot uri after the refused create = %q, want the prefix the row already named, %q", got, stranded)
	}
	if diff := cmp.Diff([]string{"manifest.json"}, objects.Snapshot(t, strandedURI)); diff != "" {
		t.Errorf("the stranded partial copy after the refused create mismatch (-want +got):\n%s", diff)
	}

	// Deleting the tag collects the partial copy and frees the name.
	if _, err := svc.DeleteTag(ctx, &ateapipb.DeleteTagRequest{Tag: tagRef.ToObjectRef()}); err != nil {
		t.Fatalf("DeleteTag on the pending tag: %v", err)
	}
	if got := objects.Snapshot(t, strandedURI); len(got) != 0 {
		t.Errorf("the stranded prefix after the delete = %v, want collected", got)
	}

	// The create now runs from scratch, into a prefix of its own.
	tag, err := w.TagActorSnapshot(ctx, tagToCreate(actorRef, "v1"))
	if err != nil {
		t.Fatalf("TagActorSnapshot after the delete: %v", err)
	}
	recreated := tag.GetStatus().GetSnapshot().GetSnapshotUri()
	if recreated == "" || recreated == stranded {
		t.Fatalf("snapshot uri after the delete = %q, want a fresh prefix rather than the stranded %q", recreated, stranded)
	}
	if got := tag.GetStatus().GetInProgressSnapshotUri(); got != "" {
		t.Errorf("in-progress snapshot uri after the delete = %q, want cleared", got)
	}
	if diff := cmp.Diff([]string{"manifest.json", "memory.zst"}, objects.Snapshot(t, mustParseSnapshotURI(t, recreated))); diff != "" {
		t.Errorf("the tag's external snapshot after the delete mismatch (-want +got):\n%s", diff)
	}

	// A create over the finished tag is refused the same way.
	_, err = w.TagActorSnapshot(ctx, tagToCreate(actorRef, "v1"))
	if code := status.Code(err); code != codes.AlreadyExists {
		t.Errorf("TagActorSnapshot over the finished tag = %v (code %v), want code AlreadyExists", err, code)
	}
}

// TestTagActorSnapshot_RacesDelete verifies that deleting a tag that's
// still being created is aborted.
func TestTagActorSnapshot_RacesDelete(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, objects := newFinalizeWorkflow(persistence)
	svc := &RPCService{impl: persistence, objectStore: objects}

	actor, _ := seedTagSource(t, ctx, persistence, objects, template, "actor-1", "manifest.json", "memory.zst")
	actorRef := resources.ActorRefFromActor(actor)
	tagRef := resources.TagRef{Atespace: "team-a", Name: "v1"}

	var once sync.Once
	var pendingURI string
	var deleteErr error
	// Simulate deleting the tag that's just being created during
	// the file copy to GCS/S3.
	objects.OnCopy = func(_, _, _, _ string) error {
		once.Do(func() {
			reserved, err := persistence.GetTag(ctx, tagRef)
			if err != nil {
				t.Errorf("GetTag during the copy: %v", err)
				return
			}
			pendingURI = reserved.GetStatus().GetInProgressSnapshotUri()
			_, deleteErr = svc.DeleteTag(ctx, &ateapipb.DeleteTagRequest{Tag: tagRef.ToObjectRef()})
		})
		return nil
	}

	tag, createErr := w.TagActorSnapshot(ctx, tagToCreate(actorRef, "v1"))
	if pendingURI == "" {
		t.Fatalf("the create never reserved a row to race with (TagActorSnapshot = %v)", createErr)
	}
	if createErr != nil {
		t.Fatalf("TagActorSnapshot: %v", createErr)
	}
	// The delete is refused rather than queued behind the copy: the create
	// holds the tag's lease until it has finished writing.
	if code := status.Code(deleteErr); code != codes.Aborted {
		t.Errorf("DeleteTag during the copy = %v (code %v), want code Aborted", deleteErr, code)
	}

	// Nothing was collected out from under the copy, and the row in the store it
	// is still there.
	if got := tag.GetStatus().GetSnapshot().GetSnapshotUri(); got != pendingURI {
		t.Errorf("snapshot uri = %q, want the prefix the create reserved, %q", got, pendingURI)
	}
	if diff := cmp.Diff([]string{"manifest.json", "memory.zst"}, objects.Snapshot(t, mustParseSnapshotURI(t, pendingURI))); diff != "" {
		t.Errorf("the tag's external snapshot mismatch (-want +got):\n%s", diff)
	}
	if _, err := persistence.GetTag(ctx, tagRef); err != nil {
		t.Errorf("GetTag after the race: %v", err)
	}
}

// TestTagActorSnapshot_NameTakenByAnotherActor verifies that a name held by a
// pending tag is taken from every other actor's point of view too: the create
// is refused and the pending copy is left exactly as it was, rather than a
// second source writing into the prefix the first one is named after.
func TestTagActorSnapshot_NameTakenByAnotherActor(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, objects := newFinalizeWorkflow(persistence)

	first, _ := seedTagSource(t, ctx, persistence, objects, template, "actor-1", "manifest.json", "memory.zst")
	second, secondSnapshot := seedTagSource(t, ctx, persistence, objects, template, "actor-2", "other.json")
	tagRef := resources.TagRef{Atespace: "team-a", Name: "v1"}

	// The first actor reserves the name, then dies in the copy.
	objects.OnCopy = func(_, srcObject, _, _ string) error {
		if strings.HasSuffix(srcObject, "memory.zst") {
			return errObjectStore
		}
		return nil
	}
	if _, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(first), "v1")); !errors.Is(err, errObjectStore) {
		t.Fatalf("TagActorSnapshot(actor-1) = %v, want an error wrapping %v", err, errObjectStore)
	}
	pending, err := persistence.GetTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	strandedURI := mustParseSnapshotURI(t, pending.GetStatus().GetInProgressSnapshotUri())

	// The second actor asks for the same name, with object storage healthy: it
	// must not inherit the first actor's prefix.
	objects.OnCopy = nil
	_, err = w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(second), "v1"))
	if code := status.Code(err); code != codes.AlreadyExists {
		t.Fatalf("TagActorSnapshot(actor-2) = %v (code %v), want code AlreadyExists", err, code)
	}
	if diff := cmp.Diff([]string{"manifest.json"}, objects.Snapshot(t, strandedURI)); diff != "" {
		t.Errorf("the rejected create wrote into the pending tag's prefix (-want +got):\n%s", diff)
	}
	stored, err := persistence.GetTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetTag after the rejected create: %v", err)
	}
	if got, want := stored.GetStatus().GetSourceActorUid(), first.GetMetadata().GetUid(); got != want {
		t.Errorf("source actor uid = %q, want the first actor's %q", got, want)
	}
	// The second actor kept its own snapshot; the failed tag collected nothing.
	if diff := cmp.Diff([]string{"other.json"}, objects.Snapshot(t, secondSnapshot)); diff != "" {
		t.Errorf("the second actor's external snapshot mismatch (-want +got):\n%s", diff)
	}
}
