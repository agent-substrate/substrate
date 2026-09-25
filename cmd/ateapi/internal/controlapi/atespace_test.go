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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/grpc"
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

func TestAtespace_EndToEndOpenFGAScenarios(t *testing.T) {
	ctx := context.Background()
	persistence := storetest.SetupPostgresPersistence(t)
	pool := persistence.Pool()

	fgaServer, err := authz.NewOpenFGAServer(pool)
	if err != nil {
		t.Fatalf("authz.NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaServer.Close)

	authorizer, policyManager, err := authz.New(ctx, pool, fgaServer)
	if err != nil {
		t.Fatalf("authz.New failed: %v", err)
	}
	persistence.SetPolicyManager(policyManager)

	storesResp, err := fgaServer.ListStores(ctx, &openfgav1.ListStoresRequest{})
	if err != nil || len(storesResp.GetStores()) == 0 {
		t.Fatalf("fgaServer.ListStores failed: %v", err)
	}
	storeID := storesResp.GetStores()[0].GetId()

	svc := &RPCService{
		impl: newServiceImpl(persistence, nil),
	}
	interceptor := authz.UnaryServerInterceptor(authorizer)

	asUser := func(id string) context.Context {
		return principal.InjectContext(ctx, principal.PrincipalInfo{
			ID:   id,
			Kind: principal.KindJWT,
		})
	}
	rootCtx := asUser("root-admin")
	aliceCtx := asUser("alice")
	bobCtx := asUser("bob")

	callCreate := func(c context.Context, name string) (*ateapipb.Atespace, error) {
		req := &ateapipb.CreateAtespaceRequest{
			Atespace: validAtespace(withAtespaceMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = name })),
		}
		resp, err := interceptor(c, req, &grpc.UnaryServerInfo{FullMethod: ateapipb.Control_CreateAtespace_FullMethodName}, func(hc context.Context, r any) (any, error) {
			return svc.CreateAtespace(hc, r.(*ateapipb.CreateAtespaceRequest))
		})
		if err != nil {
			return nil, err
		}
		return resp.(*ateapipb.Atespace), nil
	}
	callGet := func(c context.Context, name string) (*ateapipb.Atespace, error) {
		req := &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: name}}
		resp, err := interceptor(c, req, &grpc.UnaryServerInfo{FullMethod: ateapipb.Control_GetAtespace_FullMethodName}, func(hc context.Context, r any) (any, error) {
			return svc.GetAtespace(hc, r.(*ateapipb.GetAtespaceRequest))
		})
		if err != nil {
			return nil, err
		}
		return resp.(*ateapipb.Atespace), nil
	}
	callDelete := func(c context.Context, name string) (*ateapipb.Atespace, error) {
		req := &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: name}}
		resp, err := interceptor(c, req, &grpc.UnaryServerInfo{FullMethod: ateapipb.Control_DeleteAtespace_FullMethodName}, func(hc context.Context, r any) (any, error) {
			return svc.DeleteAtespace(hc, r.(*ateapipb.DeleteAtespaceRequest))
		})
		if err != nil {
			return nil, err
		}
		return resp.(*ateapipb.Atespace), nil
	}

	grantObjectRole := func(user, role, object string) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("pool.Begin failed: %v", err)
		}
		defer tx.Rollback(ctx) //nolint:errcheck
		_, err = fgaServer.Write(authz.ContextWithTx(ctx, tx), &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					{
						User:     "user:" + user,
						Relation: role,
						Object:   object,
					},
				},
				OnDuplicate: "ignore",
			},
		})
		if err != nil {
			t.Fatalf("grantObjectRole(%s, %s, %s) failed: %v", user, role, object, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("tx.Commit failed in grantObjectRole: %v", err)
		}
	}
	grantRole := func(user, role, atespaceName string) {
		grantObjectRole(user, role, authz.AtespaceObject(atespaceName))
	}
	grantObjectRole("root-admin", "owner", authz.GlobalRootObject)

	// --- Scenario 1: Duplicate CreateAtespace on live team-1 preserves alice and bob's permissions ---
	if _, err := callCreate(rootCtx, "team-1"); err != nil {
		t.Fatalf("CreateAtespace(team-1) failed: %v", err)
	}
	grantRole("alice", "owner", "team-1")
	grantRole("bob", "editor", "team-1")

	// Verify both alice and bob can GetAtespace(team-1), and bob cannot DeleteAtespace(team-1).
	if _, err := callGet(aliceCtx, "team-1"); err != nil {
		t.Fatalf("expected alice to GetAtespace(team-1), got %v", err)
	}
	if _, err := callGet(bobCtx, "team-1"); err != nil {
		t.Fatalf("expected bob to GetAtespace(team-1), got %v", err)
	}
	if _, err := callDelete(bobCtx, "team-1"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected bob denied DeleteAtespace(team-1), got %v", err)
	}

	// Attempt duplicate CreateAtespace(team-1) -> AlreadyExists, and alice & bob keep permissions.
	if _, err := callCreate(rootCtx, "team-1"); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists on duplicate CreateAtespace(team-1), got %v", err)
	}
	if _, err := callGet(aliceCtx, "team-1"); err != nil {
		t.Fatalf("expected alice to still have access after duplicate CreateAtespace, got %v", err)
	}
	if _, err := callGet(bobCtx, "team-1"); err != nil {
		t.Fatalf("expected bob to still have access after duplicate CreateAtespace, got %v", err)
	}

	// --- Scenario 2: Non-empty DeleteAtespace fails with FailedPrecondition and preserves permissions ---
	tmpl, err := persistence.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-1", Name: "tmpl-1"},
	})
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	if _, err := callDelete(aliceCtx, "team-1"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition when deleting non-empty team-1, got %v", err)
	}
	// Verify alice and bob still have their permissions on team-1.
	if _, err := callGet(aliceCtx, "team-1"); err != nil {
		t.Fatalf("expected alice to retain access after FailedPrecondition delete, got %v", err)
	}
	if _, err := callGet(bobCtx, "team-1"); err != nil {
		t.Fatalf("expected bob to retain access after FailedPrecondition delete, got %v", err)
	}

	// --- Scenario 3: Atomic rollback if DeleteAtespacePolicies fails after DELETE FROM atespaces ---
	if _, err := persistence.DeleteActorTemplate(ctx, resources.ActorTemplateRefFromActorTemplate(tmpl), store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteActorTemplate failed: %v", err)
	}
	// Install a temporary PostgreSQL BEFORE DELETE trigger on OpenFGA's `tuple` table
	// so that `DELETE FROM atespaces WHERE name = 'team-1'` and `fgaServer.Read` both
	// succeed inside `tx`, and then `fgaServer.Write` (`DELETE FROM tuple`) fails.
	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION fail_tuple_delete() RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'simulated failure deleting OpenFGA tuple';
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER trg_fail_tuple_delete BEFORE DELETE ON tuple FOR EACH ROW EXECUTE FUNCTION fail_tuple_delete();
	`); err != nil {
		t.Fatalf("installing fail_tuple_delete trigger failed: %v", err)
	}
	if _, err := callDelete(aliceCtx, "team-1"); err == nil {
		t.Fatalf("expected DeleteAtespace(team-1) to fail when DeleteAtespacePolicies fails")
	}
	if _, err := pool.Exec(ctx, `DROP TRIGGER trg_fail_tuple_delete ON tuple; DROP FUNCTION fail_tuple_delete();`); err != nil {
		t.Fatalf("dropping fail_tuple_delete trigger failed: %v", err)
	}
	// Because `DELETE FROM atespaces` and `DeleteAtespacePolicies` share a single pgx.Tx,
	// rolling back on DeleteAtespacePolicies failure must restore the `team-1` row in `atespaces`
	// as well as alice and bob's tuples.
	if _, err := callGet(aliceCtx, "team-1"); err != nil {
		t.Fatalf("expected atespace team-1 and alice's access to be rolled back and intact after DeleteAtespacePolicies failure, got %v", err)
	}

	// --- Scenario 4: Delete team-1 and recreate team-1 -> alice and bob have zero access to new team-1 ---
	if _, err := callDelete(aliceCtx, "team-1"); err != nil {
		t.Fatalf("expected alice (owner) to DeleteAtespace(team-1), got %v", err)
	}
	if _, err := callCreate(rootCtx, "team-1"); err != nil {
		t.Fatalf("recreating team-1 failed: %v", err)
	}
	if _, err := callGet(aliceCtx, "team-1"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected alice denied on recreated team-1, got %v", err)
	}
	if _, err := callGet(bobCtx, "team-1"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected bob denied on recreated team-1, got %v", err)
	}
}
