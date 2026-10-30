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
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/functionaltest/harness"
)

// Creates a tag from a suspended actor and verifies that the whole resource
// has the expected shape.
func TestCreateTag(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)
	source := harness.ActorRef(h.SuspendedActor(as, tmpl))

	created, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-a"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: source,
	}})
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}
	want := &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-a"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: source,
		Status: &ateapi.TagStatus{
			Snapshot: &ateapi.Snapshot{
				Generation: 1,
				Owner:      ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG,
				Fidelity:   ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				DurableSnapshot: &ateapi.SnapshotStorage{
					State:  ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED,
					Object: &ateapi.ObjectSnapshot{},
				},
				SandboxRuntime: harness.SandboxRuntime(),
			},
			ActorTemplateUid: tmpl.GetMetadata().GetUid(),
		},
	}
	// The tag's snapshot is named by ate-api; it only has to hold the copy.
	ignoreSnapshotName := cmp.Options{
		protocmp.IgnoreFields(&ateapi.Snapshot{}, "uuid"),
		protocmp.IgnoreFields(&ateapi.ObjectSnapshot{}, "snapshot_uri"),
	}
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreServerMetadata, ignoreSnapshotName); diff != "" {
		t.Errorf("created tag (-want +got):\n%s", diff)
	}
	uri := created.GetStatus().GetSnapshot().GetDurableSnapshot().GetObject().GetSnapshotUri()
	if names, err := h.ObjectStore.Snapshot(uri); err != nil || len(names) == 0 {
		t.Errorf("tag's snapshot %q in object storage = %v, %v; want it present", uri, names, err)
	}
}

// Creating a tag for an existing name fails with ALREADY_EXISTS and leaves
// the existing one unchanged.
func TestCreateTag_AlreadyExists(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	source := harness.ActorRef(h.SuspendedActor(as, h.NewTemplate(as, pool)))
	tag := &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-a"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: source,
	}
	created, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: tag})
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}

	_, err = h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: tag})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("CreateTag of an existing name = %v, want AlreadyExists", err)
	}
	got, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: &ateapi.ObjectRef{Atespace: as, Name: "tag-a"}})
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("tag after a refused create (-want +got):\n%s", diff)
	}
}

// Creating a tag with an invalid name fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestCreateTag_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "Not_A_Valid_Name"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: &ateapi.ObjectRef{Atespace: as, Name: "actor-a"},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateTag with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "tag.metadata.name") {
		t.Errorf("CreateTag error %q does not name the invalid field tag.metadata.name", msg)
	}
}

// Creating a tag from a source actor that doesn't exist fails with
// FAILED_PRECONDITION, and no tag is created.
func TestCreateTag_SourceActorNotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-a"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: &ateapi.ObjectRef{Atespace: as, Name: "actor-missing"},
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateTag from a missing source actor = %v, want FailedPrecondition", err)
	}
	listed, err := h.Control.ListTags(t.Context(), &ateapi.ListTagsRequest{Atespace: as})
	if err != nil {
		t.Fatalf("ListTags: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListTagsResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("tags after a refused create (-want +got):\n%s", diff)
	}
}

// Getting an existing tag returns exactly the resource CreateTag returned.
func TestGetTag(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	source := harness.ActorRef(h.SuspendedActor(as, h.NewTemplate(as, pool)))
	created, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-a"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: source,
	}})
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}

	got, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: &ateapi.ObjectRef{Atespace: as, Name: "tag-a"}})
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetTag (-created +got):\n%s", diff)
	}
}

// Getting a tag that doesn't exist fails with NOT_FOUND.
func TestGetTag_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: &ateapi.ObjectRef{Atespace: as, Name: "tag-missing"}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetTag of a missing tag = %v, want NotFound", err)
	}
}

// Getting a tag with an invalid name fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestGetTag_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("GetTag with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "tag.name") {
		t.Errorf("GetTag error %q does not name the invalid field tag.name", msg)
	}
}

// Creates N tags in one atespace and one in another, then lists the first
// atespace with pages smaller than N. The pages together hold exactly its N
// tags.
func TestListTags(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	other := h.CreateAtespace("team-b")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	// The template's golden tag lives in the other atespace, so the listed
	// one holds only the tags the test creates.
	tmpl := h.NewTemplate(other, pool)
	source := harness.ActorRef(h.SuspendedActor(as, tmpl))
	otherSource := harness.ActorRef(h.SuspendedActor(other, tmpl))
	const n, pageSize = 5, 2
	var created []*ateapi.Tag
	for i := range n {
		tag, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: fmt.Sprintf("tag-%d", i)},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: source,
		}})
		if err != nil {
			t.Fatalf("CreateTag: %v", err)
		}
		created = append(created, tag)
	}
	if _, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: other, Name: "tag-other"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: otherSource,
	}}); err != nil {
		t.Fatalf("CreateTag: %v", err)
	}

	var listed []*ateapi.Tag
	req := &ateapi.ListTagsRequest{Atespace: as, Options: &ateapi.ListOptions{PageSize: pageSize}}
	for pages := 1; ; pages++ {
		if pages > n {
			t.Fatalf("listing took more than %d pages", n)
		}
		resp, err := h.Control.ListTags(t.Context(), req)
		if err != nil {
			t.Fatalf("ListTags page %d: %v", pages, err)
		}
		if got := len(resp.GetTags()); got > pageSize {
			t.Errorf("page %d holds %d tags, want at most %d", pages, got, pageSize)
		}
		listed = append(listed, resp.GetTags()...)
		if resp.GetNextPageToken() == "" {
			break
		}
		req.Options.PageToken = resp.GetNextPageToken()
	}
	byName := cmpopts.SortSlices(func(a, b *ateapi.Tag) bool { return a.GetMetadata().GetName() < b.GetMetadata().GetName() })
	if diff := cmp.Diff(created, listed, protocmp.Transform(), byName); diff != "" {
		t.Errorf("listed tags (-want +got):\n%s", diff)
	}
}

// Listing tags with a negative page size fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestListTags_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.ListTags(t.Context(), &ateapi.ListTagsRequest{Options: &ateapi.ListOptions{PageSize: -1}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListTags with a negative page size = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "page_size") {
		t.Errorf("ListTags error %q does not name the invalid field page_size", msg)
	}
}

// Listing tags with a malformed page token fails with INVALID_ARGUMENT.
func TestListTags_InvalidPageToken(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.ListTags(t.Context(), &ateapi.ListTagsRequest{Options: &ateapi.ListOptions{PageToken: "not-a-real-token"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListTags with a malformed page token = %v, want InvalidArgument", err)
	}
}

// Listing tags in an atespace that doesn't exist succeeds with an empty
// list.
func TestListTags_AtespaceNotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	listed, err := h.Control.ListTags(t.Context(), &ateapi.ListTagsRequest{Atespace: "team-missing"})
	if err != nil {
		t.Fatalf("ListTags in a missing atespace: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListTagsResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("ListTags in a missing atespace (-want +got):\n%s", diff)
	}
}

// Updates the scope of an existing tag, sending it as read. The result, and
// a later Get, is the resource as read with that field changed and a higher
// version.
func TestUpdateTag(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	source := harness.ActorRef(h.SuspendedActor(as, h.NewTemplate(as, pool)))
	ref := &ateapi.ObjectRef{Atespace: as, Name: "tag-a"}
	before, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-a"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: source,
	}})
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}

	toUpdate := proto.Clone(before).(*ateapi.Tag)
	toUpdate.Scope = ateapi.TagScope_TAG_SCOPE_PUBLISHED
	updated, err := h.Control.UpdateTag(t.Context(), &ateapi.UpdateTagRequest{Tag: toUpdate})
	if err != nil {
		t.Fatalf("UpdateTag: %v", err)
	}
	want := proto.Clone(before).(*ateapi.Tag)
	want.Scope = ateapi.TagScope_TAG_SCOPE_PUBLISHED
	if diff := cmp.Diff(want, updated, protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("updated tag (-want +got):\n%s", diff)
	}
	if got, was := updated.GetMetadata().GetVersion(), before.GetMetadata().GetVersion(); got <= was {
		t.Errorf("updated tag has version %d, want higher than %d", got, was)
	}
	got, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: ref})
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	if diff := cmp.Diff(updated, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetTag after update (-updated +got):\n%s", diff)
	}
}

// Updating a tag that doesn't exist fails with NOT_FOUND.
func TestUpdateTag_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.UpdateTag(t.Context(), &ateapi.UpdateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-missing", Uid: foreignUID, Version: 1},
		Scope:       ateapi.TagScope_TAG_SCOPE_PUBLISHED,
		SourceActor: &ateapi.ObjectRef{Atespace: as, Name: "actor-a"},
	}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("UpdateTag of a missing tag = %v, want NotFound", err)
	}
}

// Updating a tag with the wrong UID fails with ABORTED, and with the right
// UID but the wrong version fails with ABORTED; both leave it unchanged.
// Updating it without a UID or a version fails with INVALID_ARGUMENT.
func TestUpdateTag_WithPreconditions(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	source := harness.ActorRef(h.SuspendedActor(as, h.NewTemplate(as, pool)))
	ref := &ateapi.ObjectRef{Atespace: as, Name: "tag-a"}
	before, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-a"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: source,
	}})
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}
	update := func(uid string, version int64) error {
		toUpdate := proto.Clone(before).(*ateapi.Tag)
		toUpdate.Metadata.Uid, toUpdate.Metadata.Version = uid, version
		toUpdate.Scope = ateapi.TagScope_TAG_SCOPE_PUBLISHED
		_, err := h.Control.UpdateTag(t.Context(), &ateapi.UpdateTagRequest{Tag: toUpdate})
		return err
	}

	if err := update(foreignUID, before.GetMetadata().GetVersion()); status.Code(err) != codes.Aborted {
		t.Errorf("UpdateTag with the wrong uid = %v, want Aborted", err)
	}
	if err := update(before.GetMetadata().GetUid(), before.GetMetadata().GetVersion()+1); status.Code(err) != codes.Aborted {
		t.Errorf("UpdateTag with the right uid and the wrong version = %v, want Aborted", err)
	}
	got, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: ref})
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	if diff := cmp.Diff(before, got, protocmp.Transform()); diff != "" {
		t.Errorf("tag after refused updates (-want +got):\n%s", diff)
	}
	if err := update("", 0); status.Code(err) != codes.InvalidArgument {
		t.Errorf("UpdateTag without a uid or version = %v, want InvalidArgument", err)
	}
}

// Updating a tag with an invalid name fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestUpdateTag_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.UpdateTag(t.Context(), &ateapi.UpdateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "Not_A_Valid_Name", Uid: foreignUID, Version: 1},
		Scope:       ateapi.TagScope_TAG_SCOPE_PUBLISHED,
		SourceActor: &ateapi.ObjectRef{Atespace: as, Name: "actor-a"},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("UpdateTag with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "tag.metadata.name") {
		t.Errorf("UpdateTag error %q does not name the invalid field tag.metadata.name", msg)
	}
}

// Deletes an existing tag. The response is the deleted resource, and a later
// Get fails with NOT_FOUND.
func TestDeleteTag(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	source := harness.ActorRef(h.SuspendedActor(as, h.NewTemplate(as, pool)))
	ref := &ateapi.ObjectRef{Atespace: as, Name: "tag-a"}
	created, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-a"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: source,
	}})
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}

	deleted, err := h.Control.DeleteTag(t.Context(), &ateapi.DeleteTagRequest{Tag: ref})
	if err != nil {
		t.Fatalf("DeleteTag: %v", err)
	}
	if diff := cmp.Diff(created, deleted, protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreDeleteTime); diff != "" {
		t.Errorf("DeleteTag response (-created +deleted):\n%s", diff)
	}
	if _, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetTag after delete = %v, want NotFound", err)
	}
}

// Deleting a tag that doesn't exist fails with NOT_FOUND.
func TestDeleteTag_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.DeleteTag(t.Context(), &ateapi.DeleteTagRequest{Tag: &ateapi.ObjectRef{Atespace: as, Name: "tag-missing"}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("DeleteTag of a missing tag = %v, want NotFound", err)
	}
}

// Deleting a tag with the wrong UID fails with ABORTED, and with the right
// UID but the wrong version fails with ABORTED; both leave it unchanged.
// With the right UID and version the delete succeeds.
func TestDeleteTag_WithPreconditions(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	source := harness.ActorRef(h.SuspendedActor(as, h.NewTemplate(as, pool)))
	ref := &ateapi.ObjectRef{Atespace: as, Name: "tag-a"}
	created, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-a"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: source,
	}})
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}
	uid, version := created.GetMetadata().GetUid(), created.GetMetadata().GetVersion()

	_, err = h.Control.DeleteTag(t.Context(), &ateapi.DeleteTagRequest{Tag: ref, Options: &ateapi.DeleteOptions{Uid: foreignUID}})
	if status.Code(err) != codes.Aborted {
		t.Errorf("DeleteTag with the wrong uid = %v, want Aborted", err)
	}
	_, err = h.Control.DeleteTag(t.Context(), &ateapi.DeleteTagRequest{Tag: ref, Options: &ateapi.DeleteOptions{Uid: uid, Version: version + 1}})
	if status.Code(err) != codes.Aborted {
		t.Errorf("DeleteTag with the right uid and the wrong version = %v, want Aborted", err)
	}
	got, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: ref})
	if err != nil {
		t.Fatalf("GetTag after refused deletes: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("tag after refused deletes (-want +got):\n%s", diff)
	}

	if _, err := h.Control.DeleteTag(t.Context(), &ateapi.DeleteTagRequest{Tag: ref, Options: &ateapi.DeleteOptions{Uid: uid, Version: version}}); err != nil {
		t.Errorf("DeleteTag with the right uid and version: %v", err)
	}
}

// Deleting a tag with an invalid name fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestDeleteTag_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.DeleteTag(t.Context(), &ateapi.DeleteTagRequest{Tag: &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("DeleteTag with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "tag.name") {
		t.Errorf("DeleteTag error %q does not name the invalid field tag.name", msg)
	}
}

// Creates a tag from an actor suspended asynchronously, whose latest snapshot
// is local to its node and not uploaded. The request succeeds (OK). The atelet
// received exactly one PromoteSnapshot and still holds the local snapshot.
// The source actor's snapshot is now uploaded and otherwise unchanged, and
// object storage holds both the actor's snapshot and the tag's copy.
func TestCreateTag_FromAsyncSuspendedActor(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	source := h.AsyncSuspendedActor(as, h.NewTemplate(as, pool))
	sourceRef := harness.ActorRef(source)
	before := h.GetActor(sourceRef)
	objectsBefore := h.ObjectStore.Objects()
	callsBefore := len(node.Atelet.Calls())

	tag, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-a"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: sourceRef,
	}})
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}
	if calls := node.Atelet.Calls()[callsBefore:]; len(calls) != 1 || calls[0].Op != harness.AteletPromoteSnapshot {
		t.Errorf("atelet received %v, want exactly one PromoteSnapshot", calls)
	}
	local := before.GetStatus().GetSnapshots()[len(before.GetStatus().GetSnapshots())-1]
	if got := node.Atelet.LocalSnapshot(source.GetMetadata().GetUid()); got != local.GetUuid() {
		t.Errorf("local snapshot on node a = %q, want %q", got, local.GetUuid())
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.Snapshots[len(want.Status.Snapshots)-1].DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
	if diff := cmp.Diff(want, h.GetActor(sourceRef), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("source actor after the tag (-want +got):\n%s", diff)
	}
	var wantObjects []string
	wantObjects = append(wantObjects, objectsBefore...)
	for _, uri := range []string{local.GetDurableSnapshot().GetObject().GetSnapshotUri(), tag.GetStatus().GetSnapshot().GetDurableSnapshot().GetObject().GetSnapshotUri()} {
		prefix := strings.TrimPrefix(uri, "gs://")
		wantObjects = append(wantObjects, prefix+"/manifest.json", prefix+"/memory.zst")
	}
	if diff := cmp.Diff(wantObjects, h.ObjectStore.Objects(), cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Errorf("object storage after the tag (-want +got):\n%s", diff)
	}
}

// Creating a tag with the name of an existing tag, from a different source
// actor, fails with ALREADY_EXISTS. The existing tag and object storage are
// unchanged.
func TestCreateTag_AlreadyExistsFromAnotherActor(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)
	first := harness.ActorRef(h.SuspendedActor(as, tmpl))
	second := harness.ActorRef(h.SuspendedActor(as, tmpl))
	ref := &ateapi.ObjectRef{Atespace: as, Name: "tag-a"}
	created, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-a"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: first,
	}})
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}
	objectsBefore := h.ObjectStore.Objects()

	_, err = h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-a"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: second,
	}})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("CreateTag of an existing name from another actor = %v, want AlreadyExists", err)
	}
	got, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: ref})
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("tag after a refused create (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(objectsBefore, h.ObjectStore.Objects()); diff != "" {
		t.Errorf("object storage after a refused create (-want +got):\n%s", diff)
	}
}

// Updating a tag whose delete failed partway, leaving it marked for deletion,
// fails with FAILED_PRECONDITION and leaves the tag unchanged.
func TestUpdateTag_Deleting(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	source := harness.ActorRef(h.SuspendedActor(as, h.NewTemplate(as, pool)))
	ref := &ateapi.ObjectRef{Atespace: as, Name: "tag-a"}
	if _, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-a"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: source,
	}}); err != nil {
		t.Fatalf("CreateTag: %v", err)
	}
	h.ObjectStore.On(harness.ObjectStoreList).FailNext(harness.ErrStorageFailed)
	if _, err := h.Control.DeleteTag(t.Context(), &ateapi.DeleteTagRequest{Tag: ref}); err == nil {
		t.Fatal("DeleteTag succeeded despite failing object storage")
	}
	before, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: ref})
	if err != nil {
		t.Fatalf("GetTag after a failed delete: %v", err)
	}

	toUpdate := proto.Clone(before).(*ateapi.Tag)
	toUpdate.Scope = ateapi.TagScope_TAG_SCOPE_PUBLISHED
	_, err = h.Control.UpdateTag(t.Context(), &ateapi.UpdateTagRequest{Tag: toUpdate})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("UpdateTag of a deleting tag = %v, want FailedPrecondition", err)
	}
	got, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: ref})
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	if diff := cmp.Diff(before, got, protocmp.Transform()); diff != "" {
		t.Errorf("tag after a refused update (-want +got):\n%s", diff)
	}
}
