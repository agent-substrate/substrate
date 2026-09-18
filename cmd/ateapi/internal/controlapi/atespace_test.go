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
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/authz"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateCreateAtespaceRequest(t *testing.T) {
	// This test verifies validation of user input for creation.
	validReq := func(atespace *ateapipb.Atespace, mods ...func(atespace *ateapipb.CreateAtespaceRequest)) *ateapipb.CreateAtespaceRequest {
		req := &ateapipb.CreateAtespaceRequest{
			Atespace: atespace,
		}
		for _, m := range mods {
			m(req)
		}
		return req
	}
	withMetadata := withAtespaceMetadata

	tests := []struct {
		name string
		req  *ateapipb.CreateAtespaceRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(validAtespace()),
		nil,
	}, {
		"missing atespace",
		&ateapipb.CreateAtespaceRequest{Atespace: nil},
		field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		"missing atespace.metadata",
		validReq(validAtespace(func(a *ateapipb.Atespace) { a.Metadata = nil })),
		field.ErrorList{field.Required(field.NewPath("atespace", "metadata"), "")},
	}, {
		"atespace.metadata.atespace must be empty",
		validReq(validAtespace(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Atespace = "as" }))),
		field.ErrorList{field.Forbidden(field.NewPath("atespace", "metadata", "atespace"), "")},
	}, {
		"missing atespace.metadata.name",
		validReq(validAtespace(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "" }))),
		field.ErrorList{field.Required(field.NewPath("atespace", "metadata", "name"), "")},
	}, {
		"invalid metadata.name",
		validReq(validAtespace(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "invalid value" }))),
		field.ErrorList{field.Invalid(field.NewPath("atespace", "metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateCreateAtespaceRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateGetAtespaceRequest(t *testing.T) {
	// This test verifies validation of user input for get.
	validReq := func(mods ...func(atespace *ateapipb.GetAtespaceRequest)) *ateapipb.GetAtespaceRequest {
		req := &ateapipb.GetAtespaceRequest{
			Atespace: &ateapipb.ObjectRef{Name: "team1"},
		}
		for _, m := range mods {
			m(req)
		}
		return req
	}

	tests := []struct {
		name string
		req  *ateapipb.GetAtespaceRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(),
		nil,
	}, {
		"missing atespace",
		&ateapipb.GetAtespaceRequest{},
		field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		"atespace.atespace must be empty",
		validReq(func(r *ateapipb.GetAtespaceRequest) { r.Atespace.Atespace = "as" }),
		field.ErrorList{field.Forbidden(field.NewPath("atespace", "atespace"), "")},
	}, {
		"missing atespace.name",
		validReq(func(r *ateapipb.GetAtespaceRequest) { r.Atespace.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("atespace", "name"), "")},
	}, {
		"invalid atespace.name",
		validReq(func(r *ateapipb.GetAtespaceRequest) { r.Atespace.Name = "invalid value" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateGetAtespaceRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateListAtespacesRequest(t *testing.T) {
	// This test verifies validation of user input for list.
	validReq := func(mods ...func(atespace *ateapipb.ListAtespacesRequest)) *ateapipb.ListAtespacesRequest {
		req := &ateapipb.ListAtespacesRequest{ /* default values */ }
		for _, m := range mods {
			m(req)
		}
		return req
	}

	tests := []struct {
		name string
		req  *ateapipb.ListAtespacesRequest
		want field.ErrorList
	}{{
		"valid, no page_size",
		validReq(),
		nil,
	}, {
		"valid, positive page_size",
		validReq(func(r *ateapipb.ListAtespacesRequest) { r.PageSize = 10 }),
		nil,
	}, {
		"negative page_size",
		validReq(func(r *ateapipb.ListAtespacesRequest) { r.PageSize = -1 }),
		field.ErrorList{field.Invalid(field.NewPath("page_size"), int32(-1), "").WithOrigin("minimum")},
	}, {
		"valid page_token",
		validReq(func(r *ateapipb.ListAtespacesRequest) { r.PageToken = strings.Repeat("x", 256) }),
		nil,
	}, {
		"too-large page_token",
		validReq(func(r *ateapipb.ListAtespacesRequest) { r.PageToken = strings.Repeat("x", 257) }),
		field.ErrorList{field.TooLongCharacters(field.NewPath("page_token"), "", 256).WithOrigin("maxLength")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateListAtespacesRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateDeleteAtespaceRequest(t *testing.T) {
	// This test verifies validation of user input for delete.
	validReq := func(mods ...func(atespace *ateapipb.DeleteAtespaceRequest)) *ateapipb.DeleteAtespaceRequest {
		req := &ateapipb.DeleteAtespaceRequest{
			Atespace: &ateapipb.ObjectRef{Name: "team1"},
		}
		for _, m := range mods {
			m(req)
		}
		return req
	}

	tests := []struct {
		name string
		req  *ateapipb.DeleteAtespaceRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(),
		nil,
	}, {
		"missing atespace",
		&ateapipb.DeleteAtespaceRequest{Atespace: nil},
		field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		"atespace.atespace must be empty",
		validReq(func(r *ateapipb.DeleteAtespaceRequest) { r.Atespace.Atespace = "as" }),
		field.ErrorList{field.Forbidden(field.NewPath("atespace", "atespace"), "")},
	}, {
		"missing atespace.name",
		validReq(func(r *ateapipb.DeleteAtespaceRequest) { r.Atespace.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("atespace", "name"), "")},
	}, {
		"invalid atespace.name",
		validReq(func(r *ateapipb.DeleteAtespaceRequest) { r.Atespace.Name = "invalid value" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateDeleteAtespaceRequest(context.Background(), tt.req), tt.want)
		})
	}
}

// validAtespace returns a minimal Atespace which should pass input validation.
func validAtespace(mods ...func(*ateapipb.Atespace)) *ateapipb.Atespace {
	a := &ateapipb.Atespace{
		Metadata: &ateapipb.ResourceMetadata{Name: "team1"},
	}
	for _, m := range mods {
		m(a)
	}
	return a
}

// withAtespaceMetadata returns a modifier func (see validAtespace) which sets
// the atespace's resource metadata to a valid value.
func withAtespaceMetadata(mutate func(*ateapipb.ResourceMetadata)) func(*ateapipb.Atespace) {
	return func(a *ateapipb.Atespace) { mutate(a.Metadata) }
}

type fakeAuthorizer struct {
	allow           bool
	listAll         bool
	listAllowed     map[string]bool
	onCreateErr     error
	onDeleteErr     error
	checkedRelation string
	checkedObject   string
	ensuredNames    []string
	createdNames    []string
	deletedNames    []string
}

func (f *fakeAuthorizer) CheckPermission(ctx context.Context, relation, object string) error {
	f.checkedRelation = relation
	f.checkedObject = object
	if !f.allow {
		return status.Error(codes.PermissionDenied, "denied by fakeAuthorizer")
	}
	return nil
}

func (f *fakeAuthorizer) ListAccessibleAtespaces(ctx context.Context) (bool, map[string]bool, error) {
	if !f.allow {
		return false, nil, status.Error(codes.PermissionDenied, "denied by fakeAuthorizer")
	}
	return f.listAll, f.listAllowed, nil
}

func (f *fakeAuthorizer) EnsureParentGlobal(ctx context.Context, name string) error {
	f.ensuredNames = append(f.ensuredNames, name)
	return nil
}

func (f *fakeAuthorizer) OnCreateAtespace(ctx context.Context, name string) error {
	if f.onCreateErr != nil {
		return f.onCreateErr
	}
	f.createdNames = append(f.createdNames, name)
	return nil
}

func (f *fakeAuthorizer) OnDeleteAtespace(ctx context.Context, name string) error {
	if f.onDeleteErr != nil {
		return f.onDeleteErr
	}
	f.deletedNames = append(f.deletedNames, name)
	return nil
}

func TestAtespace_Authorization(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	fakeAuthz := &fakeAuthorizer{allow: false, listAll: true}
	svc := &RPCService{
		impl:       newServiceImpl(persistence, nil),
		authorizer: fakeAuthz,
	}

	// 1. Denied CreateAtespace
	_, err := svc.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: validAtespace(),
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied on CreateAtespace, got %v", err)
	}
	if fakeAuthz.checkedRelation != authz.RelationCanCreateAtespace || fakeAuthz.checkedObject != authz.GlobalRootObject {
		t.Errorf("CreateAtespace checked (%q, %q), want (%q, %q)",
			fakeAuthz.checkedRelation, fakeAuthz.checkedObject, authz.RelationCanCreateAtespace, authz.GlobalRootObject)
	}

	// 2. Rollback on OnCreateAtespace failure
	fakeAuthz.allow = true
	fakeAuthz.onCreateErr = status.Error(codes.Internal, "simulated OpenFGA write error")
	_, err = svc.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: validAtespace(),
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal error on CreateAtespace when OnCreateAtespace fails, got %v", err)
	}
	// Verify DB row was rolled back
	if _, err := svc.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{
		Atespace: &ateapipb.ObjectRef{Name: "team1"},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound after rollback, got %v", err)
	}
	fakeAuthz.onCreateErr = nil

	// 3. Allowed CreateAtespace records tuple via OnCreateAtespace
	fakeAuthz.allow = true
	created, err := svc.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: validAtespace(),
	})
	if err != nil {
		t.Fatalf("expected CreateAtespace to succeed, got %v", err)
	}
	if len(fakeAuthz.createdNames) != 1 || fakeAuthz.createdNames[0] != created.GetMetadata().GetName() {
		t.Errorf("OnCreateAtespace createdNames = %v, want [%s]", fakeAuthz.createdNames, created.GetMetadata().GetName())
	}

	// 4. Duplicate CreateAtespace (AlreadyExists) calls EnsureParentGlobal, NOT OnCreateAtespace
	fakeAuthz.ensuredNames = nil
	_, err = svc.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: validAtespace(),
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists on duplicate CreateAtespace, got %v", err)
	}
	if len(fakeAuthz.createdNames) != 1 {
		t.Errorf("expected OnCreateAtespace not called on duplicate create, createdNames = %v", fakeAuthz.createdNames)
	}
	if len(fakeAuthz.ensuredNames) != 1 || fakeAuthz.ensuredNames[0] != "team1" {
		t.Errorf("expected EnsureParentGlobal called on AlreadyExists, got %v", fakeAuthz.ensuredNames)
	}

	// Create team2 for list filtering tests
	if _, err := svc.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: validAtespace(withAtespaceMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "team2" })),
	}); err != nil {
		t.Fatalf("expected CreateAtespace(team2) to succeed, got %v", err)
	}

	// 5. Denied ListAtespaces
	fakeAuthz.allow = false
	_, err = svc.ListAtespaces(ctx, &ateapipb.ListAtespacesRequest{})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied on ListAtespaces, got %v", err)
	}

	// 6. Allowed Global ListAtespaces (sees all)
	fakeAuthz.allow = true
	fakeAuthz.listAll = true
	listResp, err := svc.ListAtespaces(ctx, &ateapipb.ListAtespacesRequest{})
	if err != nil {
		t.Fatalf("expected ListAtespaces to succeed, got %v", err)
	}
	if len(listResp.GetAtespaces()) != 2 {
		t.Errorf("expected 2 listed atespaces for global list, got %d", len(listResp.GetAtespaces()))
	}

	// 7. Scoped ListAtespaces (sees only team1)
	fakeAuthz.listAll = false
	fakeAuthz.listAllowed = map[string]bool{authz.AtespaceObject("team1"): true}
	listResp, err = svc.ListAtespaces(ctx, &ateapipb.ListAtespacesRequest{})
	if err != nil {
		t.Fatalf("expected scoped ListAtespaces to succeed, got %v", err)
	}
	if len(listResp.GetAtespaces()) != 1 || listResp.GetAtespaces()[0].GetMetadata().GetName() != "team1" {
		t.Errorf("expected only [team1] in scoped list, got %v", listResp.GetAtespaces())
	}

	// 7b. Scoped ListAtespaces pagination with PageSize=1 when page 1 (team1) is unauthorized and page 2 (team2) is authorized
	fakeAuthz.listAllowed = map[string]bool{authz.AtespaceObject("team2"): true}
	listResp, err = svc.ListAtespaces(ctx, &ateapipb.ListAtespacesRequest{PageSize: 1})
	if err != nil {
		t.Fatalf("expected scoped ListAtespaces(PageSize=1) to succeed, got %v", err)
	}
	if len(listResp.GetAtespaces()) != 1 || listResp.GetAtespaces()[0].GetMetadata().GetName() != "team2" {
		t.Errorf("expected [team2] when page 1 is filtered out with PageSize=1, got %v", listResp.GetAtespaces())
	}

	// 8. Denied GetAtespace
	fakeAuthz.allow = false
	_, err = svc.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{
		Atespace: &ateapipb.ObjectRef{Name: "team1"},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied on GetAtespace, got %v", err)
	}
	if fakeAuthz.checkedRelation != authz.RelationCanGet || fakeAuthz.checkedObject != authz.AtespaceObject("team1") {
		t.Errorf("GetAtespace checked (%q, %q), want (%q, %q)",
			fakeAuthz.checkedRelation, fakeAuthz.checkedObject, authz.RelationCanGet, authz.AtespaceObject("team1"))
	}

	// 9. Allowed GetAtespace
	fakeAuthz.allow = true
	if _, err := svc.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{
		Atespace: &ateapipb.ObjectRef{Name: "team1"},
	}); err != nil {
		t.Fatalf("expected GetAtespace to succeed, got %v", err)
	}

	// 10. Denied DeleteAtespace
	fakeAuthz.allow = false
	_, err = svc.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{
		Atespace: &ateapipb.ObjectRef{Name: "team1"},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied on DeleteAtespace, got %v", err)
	}
	if fakeAuthz.checkedRelation != authz.RelationCanDelete || fakeAuthz.checkedObject != authz.AtespaceObject("team1") {
		t.Errorf("DeleteAtespace checked (%q, %q), want (%q, %q)",
			fakeAuthz.checkedRelation, fakeAuthz.checkedObject, authz.RelationCanDelete, authz.AtespaceObject("team1"))
	}

	// 11. Partial delete failure: DB row is deleted, but OnDeleteAtespace fails (orphaned tuples)
	fakeAuthz.allow = true
	fakeAuthz.deletedNames = nil
	fakeAuthz.onDeleteErr = status.Error(codes.Internal, "simulated OpenFGA delete error")
	_, err = svc.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{
		Atespace: &ateapipb.ObjectRef{Name: "team1"},
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal error when OnDeleteAtespace fails after DB deletion, got %v", err)
	}

	// 12. While tuples are orphaned in OpenFGA, ListAtespaces does NOT return deleted team1
	fakeAuthz.listAll = false
	fakeAuthz.listAllowed = map[string]bool{
		authz.AtespaceObject("team1"): true,
		authz.AtespaceObject("team2"): true,
	}
	listResp, err = svc.ListAtespaces(ctx, &ateapipb.ListAtespacesRequest{})
	if err != nil {
		t.Fatalf("expected ListAtespaces to succeed, got %v", err)
	}
	if len(listResp.GetAtespaces()) != 1 || listResp.GetAtespaces()[0].GetMetadata().GetName() != "team2" {
		t.Errorf("expected only [team2] when team1 is deleted in DB but orphaned in OpenFGA, got %v", listResp.GetAtespaces())
	}

	// 13. While tuples are orphaned in OpenFGA, GetAtespace returns NotFound and does NOT re-create parent_global
	fakeAuthz.ensuredNames = nil
	_, err = svc.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{
		Atespace: &ateapipb.ObjectRef{Name: "team1"},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound on GetAtespace for DB-deleted atespace, got %v", err)
	}
	if len(fakeAuthz.ensuredNames) != 0 {
		t.Errorf("expected EnsureParentGlobal NOT called on NotFound GetAtespace, got %v", fakeAuthz.ensuredNames)
	}

	// 14. Retry DeleteAtespace (NotFound in DB): sweeps orphaned tuples via OnDeleteAtespace and returns NotFound
	fakeAuthz.onDeleteErr = nil
	fakeAuthz.deletedNames = nil
	_, err = svc.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{
		Atespace: &ateapipb.ObjectRef{Name: "team1"},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound on retry DeleteAtespace after sweeping tuples, got %v", err)
	}
	if len(fakeAuthz.deletedNames) != 1 || fakeAuthz.deletedNames[0] != "team1" {
		t.Errorf("expected retry DeleteAtespace to invoke OnDeleteAtespace([team1]), got %v", fakeAuthz.deletedNames)
	}
}
