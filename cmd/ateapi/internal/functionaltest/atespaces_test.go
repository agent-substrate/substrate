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
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/functionaltest/harness"
	"go.ate.dev/substrate/internal/resources"
)

// Creates an atespace and verifies that the whole resource has the expected
// shape.
func TestCreateAtespace(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	created, err := h.Control.CreateAtespace(t.Context(), &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{
		Metadata: &ateapi.ResourceMetadata{Name: "team-a"},
	}})
	if err != nil {
		t.Fatalf("CreateAtespace: %v", err)
	}
	want := &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "team-a"}}
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreServerMetadata); diff != "" {
		t.Errorf("created atespace (-want +got):\n%s", diff)
	}
}

// Creating an atespace with the name of an existing one fails with
// ALREADY_EXISTS.
func TestCreateAtespace_AlreadyExists(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	created, err := h.Control.CreateAtespace(t.Context(), &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{
		Metadata: &ateapi.ResourceMetadata{Name: "team-a"},
	}})
	if err != nil {
		t.Fatalf("CreateAtespace: %v", err)
	}

	_, err = h.Control.CreateAtespace(t.Context(), &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{
		Metadata: &ateapi.ResourceMetadata{Name: "team-a"},
	}})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("CreateAtespace of an existing name = %v, want AlreadyExists", err)
	}
	got, err := h.Control.GetAtespace(t.Context(), &ateapi.GetAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}})
	if err != nil {
		t.Fatalf("GetAtespace: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("atespace after a refused create (-want +got):\n%s", diff)
	}
}

// Creating an atespace with an invalid name fails with INVALID_ARGUMENT.
func TestCreateAtespace_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.CreateAtespace(t.Context(), &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{
		Metadata: &ateapi.ResourceMetadata{Name: "Not_A_Valid_Name"},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateAtespace with an invalid name = %v, want InvalidArgument", err)
	}
	listed, err := h.Control.ListAtespaces(t.Context(), &ateapi.ListAtespacesRequest{})
	if err != nil {
		t.Fatalf("ListAtespaces: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListAtespacesResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("atespaces after a refused create (-want +got):\n%s", diff)
	}
}

// Creating an atespace as a principal no access policy names fails with
// PERMISSION_DENIED. Granted the viewer role by the global access policy,
// the principal still fails with PERMISSION_DENIED. In both cases no
// atespace is created: getting it fails with NOT_FOUND. Granted the owner
// role instead, the principal eventually creates the atespace (OK).
func TestCreateAtespace_Authorization(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	ref := &ateapi.ObjectRef{Name: "team-a"}
	alice := h.ClientAs("alice")
	create := func() error {
		_, err := alice.CreateAtespace(t.Context(), &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{
			Metadata: &ateapi.ResourceMetadata{Name: ref.GetName()},
		}})
		return err
	}
	refusedCreate := func(holding string) {
		t.Helper()
		if err := create(); status.Code(err) != codes.PermissionDenied {
			t.Errorf("CreateAtespace holding %s = %v, want PermissionDenied", holding, err)
		}
		if _, err := h.Control.GetAtespace(t.Context(), &ateapi.GetAtespaceRequest{Atespace: ref}); status.Code(err) != codes.NotFound {
			t.Errorf("GetAtespace after a refused create holding %s = %v, want NotFound", holding, err)
		}
	}

	refusedCreate("no role")
	setGlobalBinding(t, h, "viewer", harness.Member("alice"))
	refusedCreate("viewer")
	setGlobalBinding(t, h, "owner", harness.Member("alice"))
	eventuallyAllowed(t, h, "CreateAtespace holding owner", codes.OK, create)
}

// Getting an existing atespace returns exactly the resource CreateAtespace
// returned.
func TestGetAtespace(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	created, err := h.Control.CreateAtespace(t.Context(), &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{
		Metadata: &ateapi.ResourceMetadata{Name: "team-a"},
	}})
	if err != nil {
		t.Fatalf("CreateAtespace: %v", err)
	}

	got, err := h.Control.GetAtespace(t.Context(), &ateapi.GetAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}})
	if err != nil {
		t.Fatalf("GetAtespace: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetAtespace (-created +got):\n%s", diff)
	}
}

// Getting an atespace that doesn't exist fails with NOT_FOUND.
func TestGetAtespace_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.GetAtespace(t.Context(), &ateapi.GetAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "team-missing"}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetAtespace of a missing atespace = %v, want NotFound", err)
	}
}

// Getting an atespace with an invalid name fails with INVALID_ARGUMENT and
// the error names the invalid field.
func TestGetAtespace_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.GetAtespace(t.Context(), &ateapi.GetAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("GetAtespace with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "atespace.name") {
		t.Errorf("GetAtespace error %q does not name the invalid field atespace.name", msg)
	}
}

// Getting an atespace as a principal no access policy names fails with
// PERMISSION_DENIED, both for an atespace that exists and for one that does
// not. Granted the viewer role, then the editor role, then the owner role,
// by the atespace's access policy, the principal eventually gets the
// atespace (OK) each time.
func TestGetAtespace_Authorization(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	ref := &ateapi.ObjectRef{Name: h.CreateAtespace("team-a")}
	alice := h.ClientAs("alice")
	get := func() error {
		_, err := alice.GetAtespace(t.Context(), &ateapi.GetAtespaceRequest{Atespace: ref})
		return err
	}

	if err := get(); status.Code(err) != codes.PermissionDenied {
		t.Errorf("GetAtespace holding no role = %v, want PermissionDenied", err)
	}
	if _, err := alice.GetAtespace(t.Context(), &ateapi.GetAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "team-missing"}}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("GetAtespace of a missing atespace holding no role = %v, want PermissionDenied", err)
	}
	for _, role := range []string{"viewer", "editor", "owner"} {
		setAtespaceBinding(t, h, ref, role, harness.Member("alice"))
		eventuallyAllowed(t, h, fmt.Sprintf("GetAtespace holding %s", role), codes.OK, get)
	}
}

// A principal granted the viewer role by an atespace's access policy
// eventually gets the atespace (OK). After the admin deletes the atespace
// and creates it again under the same name, getting it as the principal
// eventually fails with PERMISSION_DENIED, and the new atespace has no
// access policy: getting the policy fails with NOT_FOUND.
func TestGetAtespace_AtespaceRecreated(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	ref := &ateapi.ObjectRef{Name: h.CreateAtespace("team-a")}
	alice := h.ClientAs("alice")
	get := func() error {
		_, err := alice.GetAtespace(t.Context(), &ateapi.GetAtespaceRequest{Atespace: ref})
		return err
	}
	setAtespaceBinding(t, h, ref, "viewer", harness.Member("alice"))
	eventuallyAllowed(t, h, "GetAtespace holding viewer", codes.OK, get)

	if _, err := h.Control.DeleteAtespace(t.Context(), &ateapi.DeleteAtespaceRequest{Atespace: ref}); err != nil {
		t.Fatalf("DeleteAtespace: %v", err)
	}
	h.CreateAtespace(ref.GetName())
	h.Eventually("GetAtespace of the recreated atespace to be denied", func() bool {
		return status.Code(get()) == codes.PermissionDenied
	})
	if _, err := h.Control.GetAtespaceAccessPolicy(t.Context(), &ateapi.GetAtespaceAccessPolicyRequest{Name: "default", Atespace: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetAtespaceAccessPolicy of the recreated atespace = %v, want NotFound", err)
	}
}

// Creates N atespaces and lists them with pages smaller than N.
func TestListAtespaces(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	const n, pageSize = 5, 2
	var created []*ateapi.Atespace
	for i := range n {
		atespace, err := h.Control.CreateAtespace(t.Context(), &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{
			Metadata: &ateapi.ResourceMetadata{Name: fmt.Sprintf("team-%d", i)},
		}})
		if err != nil {
			t.Fatalf("CreateAtespace: %v", err)
		}
		created = append(created, atespace)
	}

	var listed []*ateapi.Atespace
	req := &ateapi.ListAtespacesRequest{Options: &ateapi.ListOptions{PageSize: pageSize}}
	for pages := 1; ; pages++ {
		if pages > n {
			t.Fatalf("listing took more than %d pages", n)
		}
		resp, err := h.Control.ListAtespaces(t.Context(), req)
		if err != nil {
			t.Fatalf("ListAtespaces page %d: %v", pages, err)
		}
		if got := len(resp.GetAtespaces()); got > pageSize {
			t.Errorf("page %d holds %d atespaces, want at most %d", pages, got, pageSize)
		}
		listed = append(listed, resp.GetAtespaces()...)
		if resp.GetNextPageToken() == "" {
			break
		}
		req.Options.PageToken = resp.GetNextPageToken()
	}
	byName := cmpopts.SortSlices(func(a, b *ateapi.Atespace) bool { return a.GetMetadata().GetName() < b.GetMetadata().GetName() })
	if diff := cmp.Diff(created, listed, protocmp.Transform(), byName); diff != "" {
		t.Errorf("listed atespaces (-want +got):\n%s", diff)
	}
}

// Listing atespaces with a negative page size fails with INVALID_ARGUMENT
// and the error names the invalid field.
func TestListAtespaces_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.ListAtespaces(t.Context(), &ateapi.ListAtespacesRequest{Options: &ateapi.ListOptions{PageSize: -1}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListAtespaces with a negative page size = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "page_size") {
		t.Errorf("ListAtespaces error %q does not name the invalid field page_size", msg)
	}
}

// Listing atespaces with a malformed page token fails with INVALID_ARGUMENT.
func TestListAtespaces_InvalidPageToken(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.ListAtespaces(t.Context(), &ateapi.ListAtespacesRequest{Options: &ateapi.ListOptions{PageToken: "not-a-real-token"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListAtespaces with a malformed page token = %v, want InvalidArgument", err)
	}
}

// Listing atespaces as a principal no access policy names fails with
// PERMISSION_DENIED. Granted the viewer role, then the owner role, by the
// global access policy, the principal eventually lists atespaces (OK) each
// time.
func TestListAtespaces_Authorization(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	h.CreateAtespace("team-a")
	alice := h.ClientAs("alice")
	list := func() error {
		_, err := alice.ListAtespaces(t.Context(), &ateapi.ListAtespacesRequest{})
		return err
	}

	if err := list(); status.Code(err) != codes.PermissionDenied {
		t.Errorf("ListAtespaces holding no role = %v, want PermissionDenied", err)
	}
	for _, role := range []string{"viewer", "owner"} {
		setGlobalBinding(t, h, role, harness.Member("alice"))
		eventuallyAllowed(t, h, fmt.Sprintf("ListAtespaces holding %s", role), codes.OK, list)
	}
}

// Deletes an empty atespace. The response is the deleted atespace, and a
// later Get fails with NOT_FOUND.
func TestDeleteAtespace(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	ref := &ateapi.ObjectRef{Name: "team-a"}
	created, err := h.Control.CreateAtespace(t.Context(), &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{
		Metadata: &ateapi.ResourceMetadata{Name: "team-a"},
	}})
	if err != nil {
		t.Fatalf("CreateAtespace: %v", err)
	}

	deleted, err := h.Control.DeleteAtespace(t.Context(), &ateapi.DeleteAtespaceRequest{Atespace: ref})
	if err != nil {
		t.Fatalf("DeleteAtespace: %v", err)
	}
	if diff := cmp.Diff(created, deleted, protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreDeleteTime); diff != "" {
		t.Errorf("DeleteAtespace response (-created +deleted):\n%s", diff)
	}
	if _, err := h.Control.GetAtespace(t.Context(), &ateapi.GetAtespaceRequest{Atespace: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetAtespace after delete = %v, want NotFound", err)
	}
}

// Deleting an atespace that doesn't exist fails with NOT_FOUND.
func TestDeleteAtespace_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.DeleteAtespace(t.Context(), &ateapi.DeleteAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "team-missing"}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("DeleteAtespace of a missing atespace = %v, want NotFound", err)
	}
}

// Deleting an atespace with the wrong UID fails with ABORTED, and with the
// right UID but the wrong version fails with ABORTED. With the right UID and
// version the delete succeeds, and deleting it again fails with NOT_FOUND.
func TestDeleteAtespace_WithPreconditions(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	ref := &ateapi.ObjectRef{Name: "team-a"}
	created, err := h.Control.CreateAtespace(t.Context(), &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{
		Metadata: &ateapi.ResourceMetadata{Name: "team-a"},
	}})
	if err != nil {
		t.Fatalf("CreateAtespace: %v", err)
	}
	uid, version := created.GetMetadata().GetUid(), created.GetMetadata().GetVersion()
	const wrongUID = "0f0e0d0c-0b0a-4908-8706-050403020100"

	_, err = h.Control.DeleteAtespace(t.Context(), &ateapi.DeleteAtespaceRequest{Atespace: ref, Options: &ateapi.DeleteOptions{Uid: wrongUID}})
	if status.Code(err) != codes.Aborted {
		t.Errorf("DeleteAtespace with the wrong uid = %v, want Aborted", err)
	}
	_, err = h.Control.DeleteAtespace(t.Context(), &ateapi.DeleteAtespaceRequest{Atespace: ref, Options: &ateapi.DeleteOptions{Uid: uid, Version: version + 1}})
	if status.Code(err) != codes.Aborted {
		t.Errorf("DeleteAtespace with the right uid and the wrong version = %v, want Aborted", err)
	}
	got, err := h.Control.GetAtespace(t.Context(), &ateapi.GetAtespaceRequest{Atespace: ref})
	if err != nil {
		t.Fatalf("GetAtespace after refused deletes: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("atespace after refused deletes (-want +got):\n%s", diff)
	}

	if _, err := h.Control.DeleteAtespace(t.Context(), &ateapi.DeleteAtespaceRequest{Atespace: ref, Options: &ateapi.DeleteOptions{Uid: uid, Version: version}}); err != nil {
		t.Fatalf("DeleteAtespace with the right uid and version: %v", err)
	}
	_, err = h.Control.DeleteAtespace(t.Context(), &ateapi.DeleteAtespaceRequest{Atespace: ref, Options: &ateapi.DeleteOptions{Uid: uid, Version: version}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("DeleteAtespace after delete = %v, want NotFound", err)
	}
}

// Deleting an atespace that holds an actor template fails with
// FAILED_PRECONDITION.
func TestDeleteAtespace_HasChildren(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	ref := &ateapi.ObjectRef{Name: "team-a"}
	created, err := h.Control.CreateAtespace(t.Context(), &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{
		Metadata: &ateapi.ResourceMetadata{Name: "team-a"},
	}})
	if err != nil {
		t.Fatalf("CreateAtespace: %v", err)
	}
	if _, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: &ateapi.ActorTemplate{
		Metadata:       &ateapi.ResourceMetadata{Atespace: "team-a", Name: "tmpl-a"},
		Containers:     []*ateapi.Container{{Name: "main", Image: harness.Image, Command: []string{"/main"}}},
		WorkerSelector: &ateapi.Selector{MatchLabels: map[string]string{"pool": "pool1"}},
		Resources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(harness.TemplateCPUMillicores, harness.TemplateMemoryBytes)},
	}}); err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}

	_, err = h.Control.DeleteAtespace(t.Context(), &ateapi.DeleteAtespaceRequest{Atespace: ref})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("DeleteAtespace holding an actor template = %v, want FailedPrecondition", err)
	}
	got, err := h.Control.GetAtespace(t.Context(), &ateapi.GetAtespaceRequest{Atespace: ref})
	if err != nil {
		t.Fatalf("GetAtespace after a refused delete: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("atespace after a refused delete (-want +got):\n%s", diff)
	}
}

// Deleting an atespace with an invalid name fails with INVALID_ARGUMENT and
// the error names the invalid field.
func TestDeleteAtespace_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.DeleteAtespace(t.Context(), &ateapi.DeleteAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("DeleteAtespace with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "atespace.name") {
		t.Errorf("DeleteAtespace error %q does not name the invalid field atespace.name", msg)
	}
}

// Deleting an atespace as a principal no access policy names fails with
// PERMISSION_DENIED, both for an atespace that exists and for one that does
// not. Granted the viewer role, then the editor role, by the atespace's
// access policy, the principal still fails with PERMISSION_DENIED, and the
// atespace is unchanged. Granted the owner role instead, the principal
// eventually deletes the atespace (OK).
func TestDeleteAtespace_Authorization(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	ref := &ateapi.ObjectRef{Name: h.CreateAtespace("team-a")}
	alice := h.ClientAs("alice")
	refusedDelete := func(holding string) {
		t.Helper()
		before, err := h.Control.GetAtespace(t.Context(), &ateapi.GetAtespaceRequest{Atespace: ref})
		if err != nil {
			t.Fatalf("GetAtespace: %v", err)
		}
		if _, err := alice.DeleteAtespace(t.Context(), &ateapi.DeleteAtespaceRequest{Atespace: ref}); status.Code(err) != codes.PermissionDenied {
			t.Errorf("DeleteAtespace holding %s = %v, want PermissionDenied", holding, err)
		}
		after, err := h.Control.GetAtespace(t.Context(), &ateapi.GetAtespaceRequest{Atespace: ref})
		if err != nil {
			t.Fatalf("GetAtespace after a refused delete holding %s: %v", holding, err)
		}
		if diff := cmp.Diff(before, after, protocmp.Transform()); diff != "" {
			t.Errorf("atespace after a refused delete holding %s (-want +got):\n%s", holding, diff)
		}
	}

	refusedDelete("no role")
	if _, err := alice.DeleteAtespace(t.Context(), &ateapi.DeleteAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "team-missing"}}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("DeleteAtespace of a missing atespace holding no role = %v, want PermissionDenied", err)
	}
	for _, role := range []string{"viewer", "editor"} {
		setAtespaceBinding(t, h, ref, role, harness.Member("alice"))
		refusedDelete(role)
	}
	setAtespaceBinding(t, h, ref, "owner", harness.Member("alice"))
	eventuallyAllowed(t, h, "DeleteAtespace holding owner", codes.OK, func() error {
		_, err := alice.DeleteAtespace(t.Context(), &ateapi.DeleteAtespaceRequest{Atespace: ref})
		return err
	})
}

// Deletes an empty atespace while another atespace holds an actor template.
// The request succeeds (OK), and a later Get fails with NOT_FOUND.
func TestDeleteAtespace_OtherAtespaceNotEmpty(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	ref := &ateapi.ObjectRef{Name: h.CreateAtespace("team-a")}
	other := h.CreateAtespace("team-b")
	if _, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: &ateapi.ActorTemplate{
		Metadata:       &ateapi.ResourceMetadata{Atespace: other, Name: "tmpl-a"},
		Containers:     []*ateapi.Container{{Name: "main", Image: harness.Image, Command: []string{"/main"}}},
		WorkerSelector: &ateapi.Selector{MatchLabels: map[string]string{"pool": "pool1"}},
		Resources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(harness.TemplateCPUMillicores, harness.TemplateMemoryBytes)},
	}}); err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}

	if _, err := h.Control.DeleteAtespace(t.Context(), &ateapi.DeleteAtespaceRequest{Atespace: ref}); err != nil {
		t.Fatalf("DeleteAtespace: %v", err)
	}
	if _, err := h.Control.GetAtespace(t.Context(), &ateapi.GetAtespaceRequest{Atespace: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetAtespace after delete = %v, want NotFound", err)
	}
}
