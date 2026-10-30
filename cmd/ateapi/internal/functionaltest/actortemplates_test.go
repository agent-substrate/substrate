// Copyright 2026 The Agent Substrate Authors
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
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/functionaltest/harness"
	"go.ate.dev/substrate/internal/resources"
)

// templateSpec is a valid ActorTemplate named name in atespace.
func templateSpec(atespace, name string) *ateapi.ActorTemplate {
	return &ateapi.ActorTemplate{
		Metadata:       &ateapi.ResourceMetadata{Atespace: atespace, Name: name},
		Containers:     []*ateapi.Container{{Name: "main", Image: harness.Image, Command: []string{"/main"}}},
		WorkerSelector: &ateapi.Selector{MatchLabels: map[string]string{"tier": "1"}},
		Resources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(harness.TemplateCPUMillicores, harness.TemplateMemoryBytes)},
	}
}

// Creates an actor template, sending output-only metadata and status, and
// verifies that the whole resource has the expected shape: ate-api ignores what
// was sent and assigns its own.
func TestCreateActorTemplate(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	in := templateSpec(as, "tmpl-a")
	// The caller sends output-only fields, which ate-api ignores.
	setOutputOnlyMetadata(in.Metadata)
	in.Status = &ateapi.ActorTemplateStatus{GoldenSnapshotStatus: &ateapi.GoldenSnapshotStatus{GoldenTag: &ateapi.ObjectRef{Atespace: as, Name: "tag-a"}}}
	created, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: in})
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	want := &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{Atespace: as, Name: "tmpl-a"},
		SnapshotConfig: &ateapi.SnapshotConfig{
			PreferredFidelity:    ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
		},
		Containers:     []*ateapi.Container{{Name: "main", Image: harness.Image, Command: []string{"/main"}}},
		WorkerSelector: &ateapi.Selector{MatchLabels: map[string]string{"tier": "1"}},
		Resources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(harness.TemplateCPUMillicores, harness.TemplateMemoryBytes)},
		Status:         &ateapi.ActorTemplateStatus{},
	}
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreCreatedMetadata); diff != "" {
		t.Errorf("created actor template (-want +got):\n%s", diff)
	}
	assertAssignedMetadata(t, created.GetMetadata())
}

// Creating an actor template for an existing name fails with ALREADY_EXISTS
// and leaves the existing one unchanged.
func TestCreateActorTemplate_AlreadyExists(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	created, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: templateSpec(as, "tmpl-a")})
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}

	_, err = h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: templateSpec(as, "tmpl-a")})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("CreateActorTemplate of an existing name = %v, want AlreadyExists", err)
	}
	got, err := h.Control.GetActorTemplate(t.Context(), &ateapi.GetActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: "tmpl-a"}})
	if err != nil {
		t.Fatalf("GetActorTemplate: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("actor template after a refused create (-want +got):\n%s", diff)
	}
}

// Creating an actor template with an invalid name fails with
// INVALID_ARGUMENT and the error names the invalid field.
func TestCreateActorTemplate_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: templateSpec(as, "Not_A_Valid_Name")})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateActorTemplate with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.HasPrefix(msg, "actor_template.metadata.name: Invalid value") {
		t.Errorf("CreateActorTemplate error %q does not start with %q", msg, "actor_template.metadata.name: Invalid value")
	}
}

// Creating an actor template in an atespace that doesn't exist fails with
// FAILED_PRECONDITION, and no template is created.
func TestCreateActorTemplate_AtespaceNotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: templateSpec("team-missing", "tmpl-a")})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActorTemplate in a missing atespace = %v, want FailedPrecondition", err)
	}
	listed, err := h.Control.ListActorTemplates(t.Context(), &ateapi.ListActorTemplatesRequest{})
	if err != nil {
		t.Fatalf("ListActorTemplates: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListActorTemplatesResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("actor templates after a refused create (-want +got):\n%s", diff)
	}
}

// Getting an existing actor template returns exactly the resource
// CreateActorTemplate returned.
func TestGetActorTemplate(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	created, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: templateSpec(as, "tmpl-a")})
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}

	got, err := h.Control.GetActorTemplate(t.Context(), &ateapi.GetActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: "tmpl-a"}})
	if err != nil {
		t.Fatalf("GetActorTemplate: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetActorTemplate (-created +got):\n%s", diff)
	}
}

// Getting an actor template that doesn't exist fails with NOT_FOUND.
func TestGetActorTemplate_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.GetActorTemplate(t.Context(), &ateapi.GetActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: "tmpl-missing"}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetActorTemplate of a missing template = %v, want NotFound", err)
	}
}

// Getting an actor template with an invalid name fails with INVALID_ARGUMENT
// and the error names the invalid field.
func TestGetActorTemplate_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.GetActorTemplate(t.Context(), &ateapi.GetActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("GetActorTemplate with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.HasPrefix(msg, "actor_template.name: Invalid value") {
		t.Errorf("GetActorTemplate error %q does not start with %q", msg, "actor_template.name: Invalid value")
	}
}

// Creates N actor templates in one atespace and one in another, then lists
// the first atespace with pages smaller than N. The pages together hold
// exactly its N actor templates. Listing every atespace, with an empty
// atespace and the same page size, returns pages that together hold exactly
// all N+1 actor templates.
func TestListActorTemplates(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	other := h.CreateAtespace("team-b")
	const n, pageSize = 5, 2
	var created []*ateapi.ActorTemplate
	for i := range n {
		tmpl, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: templateSpec(as, fmt.Sprintf("tmpl-%d", i))})
		if err != nil {
			t.Fatalf("CreateActorTemplate: %v", err)
		}
		created = append(created, tmpl)
	}
	createdOther, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: templateSpec(other, "tmpl-other")})
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}

	listAll := func(atespace string) []*ateapi.ActorTemplate {
		t.Helper()
		var listed []*ateapi.ActorTemplate
		req := &ateapi.ListActorTemplatesRequest{Atespace: atespace, Options: &ateapi.ListOptions{PageSize: pageSize}}
		for pages := 1; ; pages++ {
			if pages > n+1 {
				t.Fatalf("listing atespace %q took more than %d pages", atespace, n+1)
			}
			resp, err := h.Control.ListActorTemplates(t.Context(), req)
			if err != nil {
				t.Fatalf("ListActorTemplates of atespace %q, page %d: %v", atespace, pages, err)
			}
			if got := len(resp.GetActorTemplates()); got > pageSize {
				t.Errorf("page %d of atespace %q holds %d templates, want at most %d", pages, atespace, got, pageSize)
			}
			listed = append(listed, resp.GetActorTemplates()...)
			if resp.GetNextPageToken() == "" {
				return listed
			}
			req.Options.PageToken = resp.GetNextPageToken()
		}
	}
	byName := cmpopts.SortSlices(func(a, b *ateapi.ActorTemplate) bool { return a.GetMetadata().GetName() < b.GetMetadata().GetName() })
	if diff := cmp.Diff(created, listAll(as), protocmp.Transform(), byName); diff != "" {
		t.Errorf("listed actor templates of %s (-want +got):\n%s", as, diff)
	}
	if diff := cmp.Diff(append(created, createdOther), listAll(""), protocmp.Transform(), byName); diff != "" {
		t.Errorf("listed actor templates of every atespace (-want +got):\n%s", diff)
	}
}

// Listing actor templates with a negative page size fails with
// INVALID_ARGUMENT and the error names the invalid field.
func TestListActorTemplates_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.ListActorTemplates(t.Context(), &ateapi.ListActorTemplatesRequest{Options: &ateapi.ListOptions{PageSize: -1}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListActorTemplates with a negative page size = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.HasPrefix(msg, "options.page_size: Invalid value") {
		t.Errorf("ListActorTemplates error %q does not start with %q", msg, "options.page_size: Invalid value")
	}
}

// Listing actor templates with a malformed page token fails with
// INVALID_ARGUMENT.
func TestListActorTemplates_InvalidPageToken(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.ListActorTemplates(t.Context(), &ateapi.ListActorTemplatesRequest{Options: &ateapi.ListOptions{PageToken: "not-a-real-token"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListActorTemplates with a malformed page token = %v, want InvalidArgument", err)
	}
}

// Listing actor templates in an atespace that doesn't exist succeeds with an
// empty list.
func TestListActorTemplates_AtespaceNotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	listed, err := h.Control.ListActorTemplates(t.Context(), &ateapi.ListActorTemplatesRequest{Atespace: "team-missing"})
	if err != nil {
		t.Fatalf("ListActorTemplates in a missing atespace: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListActorTemplatesResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("ListActorTemplates in a missing atespace (-want +got):\n%s", diff)
	}
}

// Deletes an existing actor template, retrying while the delete fails with
// ABORTED. The response is the deleted resource, and a later Get fails
// with NOT_FOUND.
func TestDeleteActorTemplate(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	ref := &ateapi.ObjectRef{Atespace: as, Name: "tmpl-a"}
	created, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: templateSpec(as, "tmpl-a")})
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}

	var deleted *ateapi.ActorTemplate
	if err := harness.RetryAborted(t.Context(), func() (err error) {
		deleted, err = h.Control.DeleteActorTemplate(t.Context(), &ateapi.DeleteActorTemplateRequest{ActorTemplate: ref})
		return err
	}); err != nil {
		t.Fatalf("DeleteActorTemplate: %v", err)
	}
	if diff := cmp.Diff(created, deleted, protocmp.Transform(), ignoreVersion, ignoreCreateUpdateTime, ignoreDeleteTime); diff != "" {
		t.Errorf("DeleteActorTemplate response (-created +deleted):\n%s", diff)
	}
	if deleted.GetMetadata().GetDeleteTime() == nil {
		t.Error("DeleteActorTemplate response has no delete_time")
	}
	if _, err := h.Control.GetActorTemplate(t.Context(), &ateapi.GetActorTemplateRequest{ActorTemplate: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActorTemplate after delete = %v, want NotFound", err)
	}
}

// Deleting an actor template that doesn't exist fails with NOT_FOUND.
func TestDeleteActorTemplate_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.DeleteActorTemplate(t.Context(), &ateapi.DeleteActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: "tmpl-missing"}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("DeleteActorTemplate of a missing template = %v, want NotFound", err)
	}
}

// Deleting an actor template with golden snapshots disabled, which no
// background reconciler touches, with the wrong UID fails with ABORTED, and
// with the right UID but the wrong version fails with ABORTED; both leave it
// unchanged. With the right UID and version the delete succeeds (OK).
func TestDeleteActorTemplate_WithPreconditions(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	ref := &ateapi.ObjectRef{Atespace: as, Name: "tmpl-a"}
	spec := templateSpec(as, "tmpl-a")
	spec.SnapshotConfig = &ateapi.SnapshotConfig{GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED}}
	created, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: spec})
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	uid, version := created.GetMetadata().GetUid(), created.GetMetadata().GetVersion()

	_, err = h.Control.DeleteActorTemplate(t.Context(), &ateapi.DeleteActorTemplateRequest{ActorTemplate: ref, Options: &ateapi.DeleteOptions{Uid: foreignUID}})
	if status.Code(err) != codes.Aborted {
		t.Errorf("DeleteActorTemplate with the wrong uid = %v, want Aborted", err)
	}
	_, err = h.Control.DeleteActorTemplate(t.Context(), &ateapi.DeleteActorTemplateRequest{ActorTemplate: ref, Options: &ateapi.DeleteOptions{Uid: uid, Version: version + 1}})
	if status.Code(err) != codes.Aborted {
		t.Errorf("DeleteActorTemplate with the right uid and the wrong version = %v, want Aborted", err)
	}
	got, err := h.Control.GetActorTemplate(t.Context(), &ateapi.GetActorTemplateRequest{ActorTemplate: ref})
	if err != nil {
		t.Fatalf("GetActorTemplate after refused deletes: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("actor template after refused deletes (-want +got):\n%s", diff)
	}

	if _, err := h.Control.DeleteActorTemplate(t.Context(), &ateapi.DeleteActorTemplateRequest{ActorTemplate: ref, Options: &ateapi.DeleteOptions{Uid: uid, Version: version}}); err != nil {
		t.Errorf("DeleteActorTemplate with the right uid and version: %v", err)
	}
}

// Deleting an actor template with any of the invalid requests below fails
// with INVALID_ARGUMENT, and the error names the invalid field.
func TestDeleteActorTemplate_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	for _, tc := range []struct {
		name   string
		req    *ateapi.DeleteActorTemplateRequest
		prefix string
	}{
		{
			name:   "an invalid name",
			req:    &ateapi.DeleteActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"}},
			prefix: "actor_template.name: Invalid value",
		},
		{
			name: "a precondition uid that is not a UUID",
			req: &ateapi.DeleteActorTemplateRequest{
				ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: "tmpl-a"},
				Options:       &ateapi.DeleteOptions{Uid: "not-a-uuid"},
			},
			prefix: "options.uid: Invalid value",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.Control.DeleteActorTemplate(t.Context(), tc.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("DeleteActorTemplate = %v, want InvalidArgument", err)
			}
			if msg := status.Convert(err).Message(); !strings.HasPrefix(msg, tc.prefix) {
				t.Errorf("DeleteActorTemplate error %q does not start with %q", msg, tc.prefix)
			}
		})
	}
}

// A new actor template eventually gets a golden tag, holding an uploaded
// snapshot, and getting its golden actor fails with NOT_FOUND. Deleting the template, retrying while the delete fails with
// ABORTED, succeeds (OK); getting its golden tag fails with NOT_FOUND, and
// object storage no longer holds the tag's snapshot.
func TestActorTemplate_GoldenTag(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)
	tagRef := tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()
	tag, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: tagRef})
	if err != nil {
		t.Fatalf("GetTag of the golden tag: %v", err)
	}
	uri := tag.GetStatus().GetSnapshot().GetDurableSnapshot().GetObject().GetSnapshotUri()
	if names, err := h.ObjectStore.Snapshot(uri); err != nil || len(names) == 0 {
		t.Fatalf("golden snapshot %q in object storage = %v, %v; want it present", uri, names, err)
	}
	if _, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: tagRef}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActor of the golden actor = %v, want NotFound", err)
	}

	ref := &ateapi.ObjectRef{Atespace: as, Name: tmpl.GetMetadata().GetName()}
	if err := harness.RetryAborted(t.Context(), func() error {
		_, err := h.Control.DeleteActorTemplate(t.Context(), &ateapi.DeleteActorTemplateRequest{ActorTemplate: ref})
		return err
	}); err != nil {
		t.Fatalf("DeleteActorTemplate: %v", err)
	}
	if _, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: tagRef}); status.Code(err) != codes.NotFound {
		t.Errorf("GetTag of the golden tag after delete = %v, want NotFound", err)
	}
	if names, err := h.ObjectStore.Snapshot(uri); err != nil || len(names) != 0 {
		t.Errorf("golden snapshot %q in object storage after delete = %v, %v; want none", uri, names, err)
	}
}

// An actor created before its template has a golden tag holds no snapshot, and
// still holds none once the template eventually gets one. Resuming it succeeds
// (OK) with exactly one atelet Run. The resume retries while ate-api catches up.
func TestActorTemplate_ActorCreatedBeforeGoldenTag(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	spec := templateSpec(as, "tmpl-a")
	spec.WorkerSelector = &ateapi.Selector{MatchLabels: pool.Labels}
	spec.Containers[0].WakeupProbe = &ateapi.WakeupProbe{HttpGet: &ateapi.HttpGetAction{Path: "/healthz", Port: 8080}}
	if _, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: spec}); err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	tmplRef := &ateapi.ObjectRef{Atespace: as, Name: "tmpl-a"}
	actor, err := h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "actor-a"},
		ActorTemplate: tmplRef,
	}})
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	if snaps := actor.GetStatus().GetSnapshots(); len(snaps) != 0 {
		t.Errorf("new actor holds snapshots %v, want none", snaps)
	}

	node := h.Node("a")
	h.AddWorker(pool, node)
	h.Eventually("the template's golden tag", func() bool {
		got, err := h.Control.GetActorTemplate(t.Context(), &ateapi.GetActorTemplateRequest{ActorTemplate: tmplRef})
		return err == nil && got.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() != nil
	})
	ref := harness.ActorRef(actor)
	if snaps := h.GetActor(ref).GetStatus().GetSnapshots(); len(snaps) != 0 {
		t.Errorf("actor holds snapshots %v after the golden tag, want none", snaps)
	}
	rec := node.Atelet.RecordCalls()

	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	assertCalls(t, rec, harness.AteletRun)
}

// An actor created from a template with golden snapshots disabled holds no
// snapshot. Resuming it succeeds (OK) with exactly one atelet Run, and a
// synchronous suspend then succeeds (OK) with a snapshot of its own. The
// resume retries while ate-api catches up.
func TestActorTemplate_GoldenSnapshotDisabled(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	tmpl := templateSpec(as, "tmpl-a")
	tmpl.WorkerSelector = &ateapi.Selector{MatchLabels: pool.Labels}
	tmpl.SnapshotConfig = &ateapi.SnapshotConfig{GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED}}
	if _, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: tmpl}); err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	actor, err := h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "actor-a"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: "tmpl-a"},
	}})
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	if snaps := actor.GetStatus().GetSnapshots(); len(snaps) != 0 {
		t.Errorf("new actor holds snapshots %v, want none", snaps)
	}
	ref := &ateapi.ObjectRef{Atespace: as, Name: "actor-a"}
	rec := node.Atelet.RecordCalls()

	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	assertCalls(t, rec, harness.AteletRun)
	resp, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{
		Actor:   ref,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	snaps := resp.GetActor().GetStatus().GetSnapshots()
	if len(snaps) != 1 || snaps[0].GetOwner() != ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR {
		t.Errorf("suspended actor holds snapshots %v, want one of its own", snaps)
	}
}

// goldenEgressPolicyTemplate is a valid ActorTemplate with a golden egress
// policy, a preview field.
func goldenEgressPolicyTemplate(atespace string) *ateapi.ActorTemplate {
	tmpl := templateSpec(atespace, "tmpl-a")
	tmpl.GoldenEgressPolicy = &ateapi.EgressPolicyTemplate{Rules: []*ateapi.EgressRule{{Http: &ateapi.HttpRule{Hostnames: []string{"api.example.com"}}}}}
	return tmpl
}

// storageLocationTemplate is a valid ActorTemplate with its own snapshot
// storage location, a preview field.
func storageLocationTemplate(atespace string) *ateapi.ActorTemplate {
	tmpl := templateSpec(atespace, "tmpl-a")
	tmpl.SnapshotConfig = &ateapi.SnapshotConfig{StorageLocation: "gs://other-bucket/snapshots"}
	return tmpl
}

// Creates, opted in to preview features, an actor template with a golden
// egress policy, and verifies that the whole resource has the expected shape.
func TestCreateActorTemplate_GoldenEgressPolicy(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	tmpl := goldenEgressPolicyTemplate(as)

	created, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: tmpl})
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	want := &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{Atespace: as, Name: "tmpl-a"},
		SnapshotConfig: &ateapi.SnapshotConfig{
			PreferredFidelity:    ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
		},
		Containers:     []*ateapi.Container{{Name: "main", Image: harness.Image, Command: []string{"/main"}}},
		WorkerSelector: &ateapi.Selector{MatchLabels: map[string]string{"tier": "1"}},
		Resources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(harness.TemplateCPUMillicores, harness.TemplateMemoryBytes)},
		GoldenEgressPolicy: &ateapi.EgressPolicyTemplate{Rules: []*ateapi.EgressRule{{Http: &ateapi.HttpRule{
			Hostnames: []string{"api.example.com"},
			Ports:     &ateapi.Ports{Numbers: []int32{80}},
		}}}},
		Status: &ateapi.ActorTemplateStatus{},
	}
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreCreatedMetadata); diff != "" {
		t.Errorf("created actor template (-want +got):\n%s", diff)
	}
}

// Creating an actor template with a golden egress policy, as a client that
// does not opt in to preview features, fails with INVALID_ARGUMENT, the error
// names the invalid field, and no template is created.
func TestCreateActorTemplate_GoldenEgressPolicyWithoutPreview(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")

	_, err := h.ControlWithoutPreview().CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: goldenEgressPolicyTemplate(as)})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateActorTemplate with a golden egress policy without preview opt-in = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.HasPrefix(msg, "actor_template.golden_egress_policy: Forbidden") {
		t.Errorf("CreateActorTemplate error %q does not start with %q", msg, "actor_template.golden_egress_policy: Forbidden")
	}
	listed, err := h.Control.ListActorTemplates(t.Context(), &ateapi.ListActorTemplatesRequest{Atespace: as})
	if err != nil {
		t.Fatalf("ListActorTemplates: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListActorTemplatesResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("actor templates after a refused create (-want +got):\n%s", diff)
	}
}

// Creates, opted in to preview features, an actor template with its own
// snapshot storage location, and verifies that the whole resource has the
// expected shape.
func TestCreateActorTemplate_StorageLocation(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")

	created, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: storageLocationTemplate(as)})
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	want := &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{Atespace: as, Name: "tmpl-a"},
		SnapshotConfig: &ateapi.SnapshotConfig{
			PreferredFidelity:    ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
			StorageLocation:      "gs://other-bucket/snapshots",
		},
		Containers:     []*ateapi.Container{{Name: "main", Image: harness.Image, Command: []string{"/main"}}},
		WorkerSelector: &ateapi.Selector{MatchLabels: map[string]string{"tier": "1"}},
		Resources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(harness.TemplateCPUMillicores, harness.TemplateMemoryBytes)},
		Status:         &ateapi.ActorTemplateStatus{},
	}
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreCreatedMetadata); diff != "" {
		t.Errorf("created actor template (-want +got):\n%s", diff)
	}
}

// Creating an actor template with its own snapshot storage location, as a
// client that does not opt in to preview features, fails with
// INVALID_ARGUMENT, the error names the invalid field, and no template is
// created.
func TestCreateActorTemplate_StorageLocationWithoutPreview(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")

	_, err := h.ControlWithoutPreview().CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: storageLocationTemplate(as)})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateActorTemplate with a storage location without preview opt-in = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.HasPrefix(msg, "actor_template.snapshot_config.storage_location: Forbidden") {
		t.Errorf("CreateActorTemplate error %q does not start with %q", msg, "actor_template.snapshot_config.storage_location: Forbidden")
	}
	listed, err := h.Control.ListActorTemplates(t.Context(), &ateapi.ListActorTemplatesRequest{Atespace: as})
	if err != nil {
		t.Fatalf("ListActorTemplates: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListActorTemplatesResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("actor templates after a refused create (-want +got):\n%s", diff)
	}
}
