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

package functionaltest

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

// seedTag seeds a suspended actor holding an external snapshot plus
// a finished tag over a copy of it: the state CreateTag leaves
// behind. actorName must be unique within the test, since a tag records the
// actor it was taken from; opts adjust the tag before it is written.
func seedTag(t *testing.T, tc *testContext, actorName, tagName string, opts ...func(*ateapi.Tag)) *ateapi.Tag {
	t.Helper()
	ctx := context.Background()
	actor := storetest.MustCreateActor(t, ctx, tc.persistence, &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: actorName},
		ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
	})
	// The actor's snapshot sits under its own prefix, keyed on the UID the store
	// assigns, so it can only be recorded once the row exists.
	actorSnapshotURI, err := resources.NewActorSnapshotURI(testStorageLocation, testAtespace, actor.GetMetadata().GetUid(), actorName)
	if err != nil {
		t.Fatalf("NewActorSnapshotURI: %v", err)
	}
	actor, err = tc.persistence.UpdateActor(ctx, resources.ActorRefFromActor(actor), store.PreconditionFrom(actor), func(toUpdate *ateapi.Actor) error {
		toUpdate.Status.LastAssignedGeneration = 1
		toUpdate.Status.Snapshots = []*ateapi.Snapshot{
			newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", actorSnapshotURI.Name(), actorSnapshotURI.String()),
		}
		return nil
	})
	if err != nil {
		t.Fatalf("recording the actor's external snapshot: %v", err)
	}
	tag := &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: testAtespace, Name: tagName},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: resources.ActorRefFromActor(actor).ToObjectRef(),
		Status: &ateapi.TagStatus{
			Snapshot: newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG, durableSnapshot(actor.GetStatus()).GetFidelity(), "", "", ""),
		},
	}
	for _, opt := range opts {
		opt(tag)
	}
	// The tag's snapshot sits under its own prefix, keyed on the UID the store
	// assigns, so it can only be recorded once the row exists.
	snapshot := tag.Status.Snapshot
	tag.Status.Snapshot = nil
	reserved := storetest.MustCreateTag(t, ctx, tc.persistence, tag)
	uri, err := resources.NewTagSnapshotURI(testStorageLocation, testAtespace, reserved.GetMetadata().GetUid())
	if err != nil {
		t.Fatalf("NewTagSnapshotURI: %v", err)
	}
	snapshot.Uuid = uri.Name()
	snapshot.DurableSnapshot.Object.SnapshotUri = uri.String()
	stored, err := tc.persistence.UpdateTag(ctx, resources.TagRefFromTag(reserved), store.PreconditionFrom(reserved), func(toUpdate *ateapi.Tag) error {
		toUpdate.Status.Snapshot = snapshot
		return nil
	})
	if err != nil {
		t.Fatalf("recording the tag's external snapshot: %v", err)
	}
	return stored
}

// TestCreateTag_ReusedTagName tests that a tag does not move
// between external snapshots. A tag name that is already taken must be reported
// as taken, and the objects the existing tag names must be exactly what its own
// creator wrote, because the rejected create reserves the name before it copies anything.
func TestCreateTag_ReusedTagName(t *testing.T) {
	ns := namespaceForTest("ns-tag-reused-name")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	// Actor A is suspended and tagged, the ordinary way.
	const tagName = "before-upgrade"
	actorFirstSnapshotURI := suspendActorForTest(t, tc, workerName, "actor-a")
	first, err := tc.client.CreateTag(context.Background(), &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: testAtespace, Name: tagName},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "actor-a"},
		},
	})
	if err != nil {
		t.Fatalf("CreateTag(actor-a) failed: %v", err)
	}
	tagSnapshotURI := tagSnapshotURI(first)
	// Tag owns a snapshot. So it must not be empty.
	wantObjects := snapshotObjectNames(t, tc, tagSnapshotURI)
	if len(wantObjects) == 0 {
		t.Fatalf("the tag's external snapshot %s is empty", tagSnapshotURI)
	}

	// Create & Suspend actor B. Then, try to tag its snapshot with the same name
	// that was already tagged by actor A.
	secondSnapshotURI := suspendActorForTest(t, tc, workerName, "actor-b")
	if secondSnapshotURI == actorFirstSnapshotURI {
		t.Fatalf("both actors suspended to %s, want distinct external snapshots", secondSnapshotURI)
	}
	_, err = tc.client.CreateTag(context.Background(), &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			// Same tagName that was written before.
			Metadata:    &ateapi.ResourceMetadata{Atespace: testAtespace, Name: tagName},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "actor-b"},
		},
	})
	assertGrpcError(t, err, codes.AlreadyExists, fmt.Sprintf("Tag %s/%s already exists; delete it and create it again to retry", testAtespace, tagName))

	// The row still contains actor A's tag
	stored, err := tc.client.GetTag(context.Background(), &ateapi.GetTagRequest{
		Tag: &ateapi.ObjectRef{Atespace: testAtespace, Name: tagName},
	})
	if err != nil {
		t.Fatalf("GetTag failed: %v", err)
	}
	if diff := cmp.Diff(first, stored, protocmp.Transform()); diff != "" {
		t.Errorf("the rejected create modified the tag (-before +after):\n%s", diff)
	}
	// ...and so do the objects pointed by it.
	if diff := cmp.Diff(wantObjects, snapshotObjectNames(t, tc, tagSnapshotURI)); diff != "" {
		t.Errorf("the rejected create rewrote the tag's external snapshot (-before +after):\n%s", diff)
	}
	// Actor B kept its own snapshot: the failed tag didn't garbage collect anything
	assertSnapshotPresent(t, tc, secondSnapshotURI)

	// The collision is not a dead end: a free name works.
	if _, err := tc.client.CreateTag(context.Background(), &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "after-upgrade"},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "actor-b"},
		},
	}); err != nil {
		t.Fatalf("CreateTag under a free name failed: %v", err)
	}
}

// TestCreateTag_FromPausedActor verifies that tagging a suspended actor whose
// local snapshot is not uploaded yet has the atelet on its node upload the
// node-local snapshot first, and tags that snapshot. The actor keeps the
// snapshot on its node.
func TestCreateTag_FromPausedActor(t *testing.T) {
	ns := namespaceForTest("ns-tag-paused")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	ctx := context.Background()
	ref := &ateapi.ObjectRef{Atespace: testAtespace, Name: "actor-paused"}
	if _, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: ref.GetAtespace(), Name: ref.GetName()},
		ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   ref,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
	}); err != nil {
		t.Fatalf("pause failed: %v", err)
	}
	paused, err := tc.client.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	pausedSnap := localSnapshot(paused.GetStatus())
	if pausedSnap == nil {
		t.Fatal("paused actor has no local snapshot")
	}

	tag, err := tc.client.CreateTag(ctx, &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "from-paused"},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: ref,
		},
	})
	if err != nil {
		t.Fatalf("CreateTag failed: %v", err)
	}

	if got := tc.fakeAtelet.PromoteRequest.GetSnapshot().GetSnapshotUuid(); got != pausedSnap.GetUuid() {
		t.Errorf("PromoteSnapshot snapshot uuid = %q, want the paused snapshot %q", got, pausedSnap.GetUuid())
	}
	if len(snapshotObjectNames(t, tc, tagSnapshotURI(tag))) == 0 {
		t.Errorf("the tag's external snapshot %s is empty", tagSnapshotURI(tag))
	}
	stored, err := tc.client.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	// The node-local snapshot is now durable and stays on its node.
	if got := durableSnapshot(stored.GetStatus()); got.GetUuid() != pausedSnap.GetUuid() {
		t.Errorf("latest durable snapshot = %v, want the paused snapshot %q", got, pausedSnap.GetUuid())
	}
	if got := localSnapshot(stored.GetStatus()); got.GetUuid() != pausedSnap.GetUuid() {
		t.Errorf("local snapshot = %v, want the paused snapshot %q kept on its node", got, pausedSnap.GetUuid())
	}
}

// suspendActorForTest creates, resumes and suspends an actor on workerName,
// and returns the URI of the external snapshot the suspend wrote.
func suspendActorForTest(t *testing.T, tc *testContext, workerName, name string) string {
	t.Helper()
	ctx := context.Background()
	if _, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}); err != nil {
		t.Fatalf("CreateActor(%s) failed: %v", name, err)
	}
	return runAndSuspendActorForTest(t, tc, workerName, name)
}

// runAndSuspendActorForTest resumes an existing actor on workerName and
// suspends it, returning the URI of the external snapshot the suspend wrote.
func runAndSuspendActorForTest(t *testing.T, tc *testContext, workerName, name string) string {
	t.Helper()
	ctx := context.Background()
	// Successive actors share the one worker, and scheduling reads the worker
	// cache: the preceding suspend released the worker in the store, but the
	// cache only learns of it on its next watch poll.
	waitForWorkerAvailable(t, tc, workerName)
	if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: name},
	}); err != nil {
		t.Fatalf("ResumeActor(%s) failed: %v", name, err)
	}
	suspended, err := tc.client.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   &ateapi.ObjectRef{Atespace: testAtespace, Name: name},
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("SuspendActor(%s) failed: %v", name, err)
	}
	uri := durableSnapshotURI(suspended.GetActor().GetStatus())
	if uri == "" {
		t.Fatalf("SuspendActor(%s) wrote no external snapshot: %v", name, suspended)
	}
	return uri
}

// TestDeleteTag_RefusedWhileCloneBorrowsSnapshot walks the whole loop over the
// wire: a tag cannot be deleted while an actor cloned from it is still running
// on the tag's snapshot, and can be once that clone has suspended into one of
// its own and resumed off that one.
//
//  1. Tag actor-a's snapshot and create clone-1 from the tag.
//  2. DeleteTag fails, leaving the tag and its snapshot in place.
//  3. Run and suspend clone-1, then resume it off its own snapshot.
//  4. DeleteTag succeeds and leaves clone-1's own snapshot in place.
func TestDeleteTag_RefusedWhileCloneBorrowsSnapshot(t *testing.T) {
	ns := namespaceForTest("ns-delete-tag-borrowed")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	ctx := context.Background()
	createTemplate(t, tc, ns)
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	suspendActorForTest(t, tc, workerName, "actor-a")

	const tagName = "v1"
	tagRef := &ateapi.ObjectRef{Atespace: testAtespace, Name: tagName}
	tag, err := tc.client.CreateTag(ctx, &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: testAtespace, Name: tagName},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "actor-a"},
		},
	})
	if err != nil {
		t.Fatalf("CreateTag failed: %v", err)
	}
	tagSnapshotURI := tagSnapshotURI(tag)

	if _, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "clone-1"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		SourceTag:     tagRef,
	}}); err != nil {
		t.Fatalf("CreateActor(clone-1) from tag %s failed: %v", tagName, err)
	}

	_, err = tc.client.DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: tagRef})
	if got, want := status.Code(err), codes.FailedPrecondition; got != want {
		t.Fatalf("DeleteTag while clone-1 borrows its snapshot = %v (code %v), want %v", err, got, want)
	}
	if _, err := tc.client.GetTag(ctx, &ateapi.GetTagRequest{Tag: tagRef}); err != nil {
		t.Fatalf("GetTag after the refusal: %v", err)
	}
	if got := snapshotObjectNames(t, tc, tagSnapshotURI); len(got) == 0 {
		t.Error("the refused delete collected the tag's external snapshot anyway")
	}

	// The clone's first suspend writes a snapshot of its own, which is what
	// ends the borrow and frees the tag.
	cloneSnapshotURI := runAndSuspendActorForTest(t, tc, workerName, "clone-1")
	if cloneSnapshotURI == tagSnapshotURI {
		t.Fatalf("clone-1 suspended back into the tag's snapshot %s", cloneSnapshotURI)
	}

	// Resume actor: check that the tag can be deleted, while the actor is up,
	// since the actor no longer borrows the tag's snapshot.
	waitForWorkerAvailable(t, tc, workerName)
	resumed, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "clone-1"}})
	if err != nil {
		t.Fatalf("ResumeActor(clone-1) failed: %v", err)
	}
	if got := durableSnapshotURI(resumed.GetActor().GetStatus()); got != cloneSnapshotURI {
		t.Fatalf("clone-1 resumed holding snapshot %q, want its own %q", got, cloneSnapshotURI)
	}
	if got, want := resumed.GetActor().GetStatus().GetState(), ateapi.ActorState_ACTOR_STATE_RUNNING; got != want {
		t.Fatalf("clone-1 state after the resume = %v, want %v", got, want)
	}

	if _, err := tc.client.DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: tagRef}); err != nil {
		t.Fatalf("DeleteTag once the borrow ended: %v", err)
	}
	if _, err := tc.client.GetTag(ctx, &ateapi.GetTagRequest{Tag: tagRef}); status.Code(err) != codes.NotFound {
		t.Errorf("GetTag after the delete = %v, want NotFound", err)
	}
	if got := snapshotObjectNames(t, tc, cloneSnapshotURI); len(got) == 0 {
		t.Error("deleting the tag collected the clone's own external snapshot")
	}
}

// TestDeleteTag_RefusedWhileGoldenCloneBorrowsSnapshot is the same loop for an
// actor created with no source tag, which starts out on the template's golden
// snapshot.
//
//  1. Create actor-a, which inherits the golden tag's snapshot.
//  2. DeleteTag(golden) and DeleteActorTemplate both fail, leaving the
//     template, the golden tag, and its snapshot in place.
//  3. Run and suspend actor-a, then resume it off its own snapshot.
//  4. DeleteActorTemplate succeeds, collecting the golden tag and its snapshot
//     and leaving actor-a's own snapshot in place.
func TestDeleteTag_RefusedWhileGoldenCloneBorrowsSnapshot(t *testing.T) {
	ns := namespaceForTest("ns-delete-golden-tag-borrowed")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	ctx := context.Background()
	template := createTemplate(t, tc, ns)
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	templateRef := resources.ActorTemplateRefFromActorTemplate(template).ToObjectRef()
	goldenRef := template.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()

	created, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "actor-a"},
		ActorTemplate: templateRef,
	}})
	if err != nil {
		t.Fatalf("CreateActor(actor-a) failed: %v", err)
	}
	goldenURI := goldenSnapshotURI(t, tc, template)
	if got := durableSnapshotURI(created.GetStatus()); got != goldenURI {
		t.Fatalf("actor-a was created holding snapshot %q, want the template's golden %q", got, goldenURI)
	}

	_, err = tc.client.DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: goldenRef})
	if got, want := status.Code(err), codes.FailedPrecondition; got != want {
		t.Fatalf("DeleteTag(golden) while actor-a borrows its snapshot = %v (code %v), want %v", err, got, want)
	}
	// Deleting the template is the way a golden tag is normally collected, so
	// the borrow has to hold that back too, leaving both resources in place.
	_, err = tc.client.DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: templateRef})
	if got, want := status.Code(err), codes.FailedPrecondition; got != want {
		t.Fatalf("DeleteActorTemplate while actor-a borrows the golden snapshot = %v (code %v), want %v", err, got, want)
	}
	if _, err := tc.client.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: templateRef}); err != nil {
		t.Fatalf("GetActorTemplate after the refusal: %v", err)
	}
	if _, err := tc.client.GetTag(ctx, &ateapi.GetTagRequest{Tag: goldenRef}); err != nil {
		t.Fatalf("GetTag(golden) after the refusal: %v", err)
	}
	assertSnapshotPresent(t, tc, goldenURI)

	// The first suspend writes a snapshot under the actor's own prefix and the
	// resume restores that one, so the golden tag is free from the suspend on.
	ownSnapshotURI := runAndSuspendActorForTest(t, tc, workerName, "actor-a")
	assertSnapshotOwnedByActor(t, created, ownSnapshotURI)
	waitForWorkerAvailable(t, tc, workerName)
	resumed, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "actor-a"}})
	if err != nil {
		t.Fatalf("ResumeActor(actor-a) failed: %v", err)
	}
	if got := durableSnapshotURI(resumed.GetActor().GetStatus()); got != ownSnapshotURI {
		t.Fatalf("actor-a resumed holding snapshot %q, want its own %q", got, ownSnapshotURI)
	}

	if _, err := tc.client.DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: templateRef}); err != nil {
		t.Fatalf("DeleteActorTemplate once the borrow ended: %v", err)
	}
	if _, err := tc.client.GetTag(ctx, &ateapi.GetTagRequest{Tag: goldenRef}); status.Code(err) != codes.NotFound {
		t.Errorf("GetTag(golden) after the delete = %v, want NotFound", err)
	}
	assertSnapshotCollected(t, tc, goldenURI)
	if got := snapshotObjectNames(t, tc, ownSnapshotURI); len(got) == 0 {
		t.Error("deleting the template collected the actor's own external snapshot")
	}
}

// TestUpdateTag_Preconditions verifies the required version and uid
// guards carried in the tag's metadata.
func TestUpdateTag_Preconditions(t *testing.T) {
	ns := namespaceForTest("ns-update-tag-preconditions")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	ctx := context.Background()
	const tagName = "before-upgrade"

	// Each call to update() flips the scope, so every accepted update is an
	// observable write that bumps the version.
	// The tag under test is seeded from actor-2; source_actor is immutable, so
	// every update has to echo it back.
	update := func(meta *ateapi.ResourceMetadata, scope ateapi.TagScope) (*ateapi.Tag, error) {
		meta.Atespace, meta.Name = testAtespace, tagName
		return tc.client.UpdateTag(context.Background(), &ateapi.UpdateTagRequest{
			Tag: &ateapi.Tag{
				Metadata:    meta,
				Scope:       scope,
				SourceActor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "actor-2"},
			},
		})
	}

	// Delete and recreate the same atespace/name tag, so the first lifecycle's
	// uid becomes stale.
	staleUID := seedTag(t, tc, "actor-1", tagName).GetMetadata().GetUid()
	if _, err := tc.client.DeleteTag(ctx, &ateapi.DeleteTagRequest{
		Tag: &ateapi.ObjectRef{Atespace: testAtespace, Name: tagName},
	}); err != nil {
		t.Fatalf("DeleteTag failed: %v", err)
	}

	tagged := seedTag(t, tc, "actor-2", tagName)
	staleVersion := tagged.GetMetadata().GetVersion()
	uid := tagged.GetMetadata().GetUid()
	if uid == staleUID {
		t.Fatalf("recreated tag reused uid %s, want a fresh one", uid)
	}
	// No preconditions
	_, err := update(&ateapi.ResourceMetadata{}, ateapi.TagScope_TAG_SCOPE_PUBLISHED)
	assertGrpcError(t, err, codes.InvalidArgument, fmt.Sprintf("while updating tag %s/%s: persistence: precondition required: uid", testAtespace, tagName))

	// The uid from the deleted lifecycle must be rejected, even though the
	// atespace/name it was observed under still resolves and the version it
	// guards on matches the recreated tag's.
	_, err = update(&ateapi.ResourceMetadata{Uid: staleUID, Version: staleVersion}, ateapi.TagScope_TAG_SCOPE_PUBLISHED)
	assertGrpcError(t, err, codes.Aborted, fmt.Sprintf("Tag %s/%s not found with uid %s", testAtespace, tagName, staleUID))

	// Both guards matching the observed state: the update goes through, and
	// moves the tag past the version observed above.
	first, err := update(&ateapi.ResourceMetadata{Uid: uid, Version: staleVersion}, ateapi.TagScope_TAG_SCOPE_PUBLISHED)
	if err != nil {
		t.Fatalf("UpdateTag(matching guards) failed: %v", err)
	}
	currentVersion := first.GetMetadata().GetVersion()
	if currentVersion <= staleVersion {
		t.Fatalf("version = %d, want greater than %d after an update", currentVersion, staleVersion)
	}
	if got, want := first.GetScope(), ateapi.TagScope_TAG_SCOPE_PUBLISHED; got != want {
		t.Errorf("scope = %v, want %v", got, want)
	}

	// The version observed before that write is now stale: rejected rather than
	// silently overwriting the concurrent change.
	_, err = update(&ateapi.ResourceMetadata{Uid: uid, Version: staleVersion}, ateapi.TagScope_TAG_SCOPE_ATESPACE)
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")

	// Guarding on the version the last write produced succeeds again.
	updated, err := update(&ateapi.ResourceMetadata{Uid: uid, Version: currentVersion}, ateapi.TagScope_TAG_SCOPE_ATESPACE)
	if err != nil {
		t.Fatalf("UpdateTag(matching guards) failed: %v", err)
	}
	if got, want := updated.GetScope(), ateapi.TagScope_TAG_SCOPE_ATESPACE; got != want {
		t.Errorf("scope = %v, want %v", got, want)
	}
	if updated.GetMetadata().GetVersion() <= currentVersion {
		t.Errorf("version = %d, want greater than %d", updated.GetMetadata().GetVersion(), currentVersion)
	}

	// The guard the client just satisfied is now stale in turn.
	_, err = update(&ateapi.ResourceMetadata{Uid: uid, Version: currentVersion}, ateapi.TagScope_TAG_SCOPE_PUBLISHED)
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")
}

func TestUpdateTag_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-update-tag-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.UpdateTag(context.Background(), &ateapi.UpdateTagRequest{
		Tag: &ateapi.Tag{
			Metadata: &ateapi.ResourceMetadata{
				Atespace: testAtespace,
				Name:     "does-not-exist",
				// Well-formed guards to pass preconditions validation
				Uid:     "9a2b1c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d",
				Version: 1,
			},
			Scope:       ateapi.TagScope_TAG_SCOPE_PUBLISHED,
			SourceActor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "actor-1"},
		},
	})
	assertGrpcError(t, err, codes.NotFound, "Tag test-atespace/does-not-exist not found")
}

func TestDeleteTag(t *testing.T) {
	ns := namespaceForTest("ns-delete-tag")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	createTemplate(t, tc, ns)

	created := seedTag(t, tc, "actor-1", "v1")
	tagRef := resources.TagRefFromTag(created)
	snapshotURI, err := resources.ParseSnapshotURI(tagSnapshotURI(created))
	if err != nil {
		t.Fatalf("ParseSnapshotURI: %v", err)
	}
	tc.objectStore.PutSnapshot(t, snapshotURI, "manifest.json")

	deleted, err := tc.client.DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: tagRef.ToObjectRef()})
	if err != nil {
		t.Fatalf("DeleteTag failed: %v", err)
	}
	if diff := cmp.Diff(created, deleted, protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("DeleteTag response mismatch (-created +deleted):\n%s", diff)
	}
	if deleted.GetMetadata().GetDeleteTime() == nil {
		t.Errorf("the deleted tag carries no delete_time")
	}
	assertSnapshotCollected(t, tc, snapshotURI.String())
	_, err = tc.client.GetTag(ctx, &ateapi.GetTagRequest{Tag: tagRef.ToObjectRef()})
	assertGrpcError(t, err, codes.NotFound, "Tag "+tagRef.String()+" not found")
}

func TestDeleteTag_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-delete-tag-missing")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	tagRef := resources.TagRef{Atespace: testAtespace, Name: "missing"}
	_, err := tc.client.DeleteTag(context.Background(), &ateapi.DeleteTagRequest{Tag: tagRef.ToObjectRef()})
	assertGrpcError(t, err, codes.NotFound, "Tag "+tagRef.String()+" not found")
}

func TestDeleteTag_Preconditions(t *testing.T) {
	ns := namespaceForTest("ns-delete-tag-preconditions")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	createTemplate(t, tc, ns)
	tagRef := resources.TagRef{Atespace: testAtespace, Name: "v1"}
	ref := tagRef.ToObjectRef()
	del := func(opts *ateapi.DeleteOptions) error {
		_, err := tc.client.DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: ref, Options: opts})
		return err
	}

	tag := seedTag(t, tc, "actor-1", tagRef.Name)
	uid, version := tag.GetMetadata().GetUid(), tag.GetMetadata().GetVersion()

	assertGrpcError(t, del(&ateapi.DeleteOptions{Version: version + 1}), codes.Aborted, "concurrent update conflict, please retry")
	assertGrpcError(t, del(&ateapi.DeleteOptions{Uid: uid, Version: version + 1}), codes.Aborted, "concurrent update conflict, please retry")
	assertGrpcError(t, del(&ateapi.DeleteOptions{Uid: foreignUID}), codes.Aborted, "Tag "+tagRef.String()+" does not have uid "+foreignUID)
	assertGrpcError(t, del(&ateapi.DeleteOptions{Uid: foreignUID, Version: version}), codes.Aborted, "Tag "+tagRef.String()+" does not have uid "+foreignUID)
	if _, err := tc.client.GetTag(ctx, &ateapi.GetTagRequest{Tag: ref}); err != nil {
		t.Fatalf("a refused delete removed the tag: %v", err)
	}

	if err := del(&ateapi.DeleteOptions{Version: version}); err != nil {
		t.Fatalf("DeleteTag with the matching version: %v", err)
	}
	tag = seedTag(t, tc, "actor-2", tagRef.Name)
	if err := del(&ateapi.DeleteOptions{Uid: tag.GetMetadata().GetUid()}); err != nil {
		t.Fatalf("DeleteTag with the matching uid: %v", err)
	}
	tag = seedTag(t, tc, "actor-3", tagRef.Name)
	if err := del(&ateapi.DeleteOptions{Uid: tag.GetMetadata().GetUid(), Version: tag.GetMetadata().GetVersion()}); err != nil {
		t.Fatalf("DeleteTag with both guards: %v", err)
	}
	_, err := tc.client.GetTag(ctx, &ateapi.GetTagRequest{Tag: ref})
	assertGrpcError(t, err, codes.NotFound, "Tag "+tagRef.String()+" not found")
}
