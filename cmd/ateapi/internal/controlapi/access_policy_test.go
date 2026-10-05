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
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func TestAccessPolicy_GlobalAndAtespaceGovernance(t *testing.T) {
	persistence := storetest.SetupPostgresPersistence(t)
	ctx := context.Background()

	pool := persistence.Pool()
	fgaServer, err := authz.NewOpenFGAServer(pool)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaServer.Close)

	// alice@example.com is a bootstrap owner: a global owner through server
	// configuration, never through a stored AccessPolicy.
	authorizer, policyManager, err := authz.New(ctx, pool, fgaServer, []string{"alice@example.com"})
	if err != nil {
		t.Fatalf("authz.New failed: %v", err)
	}
	persistence.SetPolicyManager(policyManager)

	svc := NewRPCService(persistence, nil, nil, nil, nil, nil, nil, "", nil, nil, "", nil, nil)
	interceptor := authz.UnaryServerInterceptor(authorizer, true)

	userCtx := func(id string) context.Context {
		return principal.InjectContext(ctx, principal.PrincipalInfo{
			ID:   id,
			Kind: principal.KindJWT,
		})
	}
	aliceCtx := userCtx("alice@example.com")
	bobCtx := userCtx("bob@example.com")
	charlieCtx := userCtx("charlie@example.com")
	daveCtx := userCtx("dave@example.com")

	invoke := func(c context.Context, method string, req any, handler func(context.Context, any) (any, error)) (any, error) {
		return interceptor(c, req, &grpc.UnaryServerInfo{FullMethod: method}, handler)
	}
	getGlobal := func(c context.Context) (*ateapipb.AccessPolicy, error) {
		got, err := invoke(c, ateapipb.Control_GetGlobalAccessPolicy_FullMethodName, &ateapipb.GetGlobalAccessPolicyRequest{}, func(c context.Context, r any) (any, error) {
			return svc.GetGlobalAccessPolicy(c, r.(*ateapipb.GetGlobalAccessPolicyRequest))
		})
		if err != nil {
			return nil, err
		}
		return got.(*ateapipb.AccessPolicy), nil
	}
	createGlobal := func(c context.Context, req *ateapipb.CreateGlobalAccessPolicyRequest) (*ateapipb.AccessPolicy, error) {
		got, err := invoke(c, ateapipb.Control_CreateGlobalAccessPolicy_FullMethodName, req, func(c context.Context, r any) (any, error) {
			return svc.CreateGlobalAccessPolicy(c, r.(*ateapipb.CreateGlobalAccessPolicyRequest))
		})
		if err != nil {
			return nil, err
		}
		return got.(*ateapipb.AccessPolicy), nil
	}
	updateGlobal := func(c context.Context, req *ateapipb.UpdateGlobalAccessPolicyRequest) (*ateapipb.AccessPolicy, error) {
		got, err := invoke(c, ateapipb.Control_UpdateGlobalAccessPolicy_FullMethodName, req, func(c context.Context, r any) (any, error) {
			return svc.UpdateGlobalAccessPolicy(c, r.(*ateapipb.UpdateGlobalAccessPolicyRequest))
		})
		if err != nil {
			return nil, err
		}
		return got.(*ateapipb.AccessPolicy), nil
	}

	// 1. No global policy exists at startup. Alice (bootstrap owner) is
	// authorized and sees NotFound; Bob (unprivileged) is denied.
	if _, err := getGlobal(bobCtx); apierror.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied for Bob on GetGlobalAccessPolicy, got %v", err)
	}
	if _, err := getGlobal(aliceCtx); apierror.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound for Alice before CreateGlobalAccessPolicy, got %v", err)
	}

	// 2. CreateGlobalAccessPolicy: Bob is denied; Alice creates a policy that
	// does not list her, granting Dave owner and Bob viewer.
	createGlobalReq := &ateapipb.CreateGlobalAccessPolicyRequest{
		AccessPolicy: &ateapipb.AccessPolicy{
			Metadata: &ateapipb.ResourceMetadata{Name: "default"},
			Bindings: []*ateapipb.Binding{
				{Role: authz.RoleOwner, Members: []string{"user:dave@example.com"}},
				{Role: authz.RoleViewer, Members: []string{"user:bob@example.com"}},
			},
		},
	}
	if _, err := createGlobal(bobCtx, createGlobalReq); apierror.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied for Bob on CreateGlobalAccessPolicy, got %v", err)
	}
	globalPol, err := createGlobal(aliceCtx, createGlobalReq)
	if err != nil {
		t.Fatalf("CreateGlobalAccessPolicy as Alice failed: %v", err)
	}
	if globalPol.GetMetadata().GetName() != "default" || globalPol.GetMetadata().GetVersion() != 1 {
		t.Fatalf("unexpected created global policy metadata: %+v", globalPol.GetMetadata())
	}
	if _, err := createGlobal(aliceCtx, createGlobalReq); apierror.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists for second CreateGlobalAccessPolicy, got %v", err)
	}

	// 3. Bob (stored global viewer) can read but not update the policy.
	if _, err := getGlobal(bobCtx); err != nil {
		t.Fatalf("expected Bob (global viewer) to be allowed GetGlobalAccessPolicy, got %v", err)
	}
	updateGlobalReq := &ateapipb.UpdateGlobalAccessPolicyRequest{
		AccessPolicy: &ateapipb.AccessPolicy{
			Metadata: globalPol.GetMetadata(),
			Bindings: []*ateapipb.Binding{
				{Role: authz.RoleOwner, Members: []string{"user:dave@example.com"}},
				{Role: authz.RoleViewer, Members: []string{"user:bob@example.com", "user:charlie@example.com"}},
			},
		},
	}
	if _, err := updateGlobal(bobCtx, updateGlobalReq); apierror.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected Bob (global viewer) to be denied UpdateGlobalAccessPolicy, got %v", err)
	}

	// 4. Dave (stored global owner) updates the policy; a stale write is Aborted.
	updatedGlobal, err := updateGlobal(daveCtx, updateGlobalReq)
	if err != nil {
		t.Fatalf("UpdateGlobalAccessPolicy as Dave failed: %v", err)
	}
	if updatedGlobal.GetMetadata().GetVersion() != 2 {
		t.Fatalf("expected global policy version 2, got %d", updatedGlobal.GetMetadata().GetVersion())
	}
	if _, err := updateGlobal(daveCtx, updateGlobalReq); apierror.Code(err) != codes.Aborted {
		t.Fatalf("expected Aborted for stale version on UpdateGlobalAccessPolicy, got %v", err)
	}

	// 5. Empty bindings are allowed and revoke every stored grant. Alice keeps
	// access as a bootstrap owner, so nobody is locked out.
	emptied, err := updateGlobal(aliceCtx, &ateapipb.UpdateGlobalAccessPolicyRequest{
		AccessPolicy: &ateapipb.AccessPolicy{Metadata: updatedGlobal.GetMetadata()},
	})
	if err != nil {
		t.Fatalf("UpdateGlobalAccessPolicy with empty bindings failed: %v", err)
	}
	if len(emptied.GetBindings()) != 0 || emptied.GetMetadata().GetVersion() != 3 {
		t.Fatalf("unexpected emptied global policy: %+v", emptied)
	}
	if _, err := getGlobal(bobCtx); apierror.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected Bob denied after empty update, got %v", err)
	}
	if _, err := getGlobal(daveCtx); apierror.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected Dave denied after empty update, got %v", err)
	}
	if _, err := getGlobal(aliceCtx); err != nil {
		t.Fatalf("expected Alice (bootstrap owner) allowed after empty update, got %v", err)
	}

	// A bootstrap owner may also be listed in the stored policy; the
	// contextual grant then duplicates a stored tuple, which must not break
	// the check.
	withAlice, err := updateGlobal(aliceCtx, &ateapipb.UpdateGlobalAccessPolicyRequest{
		AccessPolicy: &ateapipb.AccessPolicy{
			Metadata: emptied.GetMetadata(),
			Bindings: []*ateapipb.Binding{{Role: authz.RoleOwner, Members: []string{"user:alice@example.com"}}},
		},
	})
	if err != nil {
		t.Fatalf("UpdateGlobalAccessPolicy listing Alice failed: %v", err)
	}
	if _, err := getGlobal(aliceCtx); err != nil {
		t.Fatalf("expected Alice (bootstrap and stored owner) allowed, got %v", err)
	}

	// 6. Removing Alice from the bootstrap owners (a restart with a different
	// configuration) leaves only her stored grant; once that is gone too she
	// is denied, while the new bootstrap owner is allowed.
	restarted, _, err := authz.New(ctx, pool, fgaServer, []string{"charlie@example.com"})
	if err != nil {
		t.Fatalf("authz.New after restart failed: %v", err)
	}
	restartedInterceptor := authz.UnaryServerInterceptor(restarted, true)
	getGlobalAfterRestart := func(c context.Context) error {
		_, err := restartedInterceptor(c, &ateapipb.GetGlobalAccessPolicyRequest{}, &grpc.UnaryServerInfo{FullMethod: ateapipb.Control_GetGlobalAccessPolicy_FullMethodName}, func(c context.Context, r any) (any, error) {
			return svc.GetGlobalAccessPolicy(c, r.(*ateapipb.GetGlobalAccessPolicyRequest))
		})
		return err
	}
	if err := getGlobalAfterRestart(aliceCtx); err != nil {
		t.Fatalf("expected Alice allowed through her stored grant after removal from bootstrap owners, got %v", err)
	}
	if _, err := updateGlobal(aliceCtx, &ateapipb.UpdateGlobalAccessPolicyRequest{
		AccessPolicy: &ateapipb.AccessPolicy{Metadata: withAlice.GetMetadata()},
	}); err != nil {
		t.Fatalf("UpdateGlobalAccessPolicy removing Alice failed: %v", err)
	}
	if err := getGlobalAfterRestart(aliceCtx); apierror.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected Alice denied after removal from bootstrap owners and the policy, got %v", err)
	}
	if err := getGlobalAfterRestart(charlieCtx); err != nil {
		t.Fatalf("expected Charlie (new bootstrap owner) allowed, got %v", err)
	}

	// 7. With the original configuration, Alice inherits owner on every
	// atespace from her bootstrap global owner grant. CreateAtespace does not
	// create an AccessPolicy row; GetAtespaceAccessPolicy returns NotFound
	// until CreateAtespaceAccessPolicy is called.
	createSpaceReq := &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{Name: "team-alpha"},
		},
	}
	if _, err := invoke(aliceCtx, ateapipb.Control_CreateAtespace_FullMethodName, createSpaceReq, func(c context.Context, r any) (any, error) {
		return svc.CreateAtespace(c, r.(*ateapipb.CreateAtespaceRequest))
	}); err != nil {
		t.Fatalf("CreateAtespace(team-alpha) failed: %v", err)
	}

	getSpacePolReq := &ateapipb.GetAtespaceAccessPolicyRequest{
		Atespace: &ateapipb.ObjectRef{Name: "team-alpha"},
	}
	if _, err := invoke(aliceCtx, ateapipb.Control_GetAtespaceAccessPolicy_FullMethodName, getSpacePolReq, func(c context.Context, r any) (any, error) {
		return svc.GetAtespaceAccessPolicy(c, r.(*ateapipb.GetAtespaceAccessPolicyRequest))
	}); apierror.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound before CreateAtespaceAccessPolicy, got %v", err)
	}

	// 8. CreateAtespaceAccessPolicy: Alice creates team-alpha's policy granting Charlie editor.
	createSpacePolReq := &ateapipb.CreateAtespaceAccessPolicyRequest{
		Atespace: &ateapipb.ObjectRef{Name: "team-alpha"},
		AccessPolicy: &ateapipb.AccessPolicy{
			Metadata: &ateapipb.ResourceMetadata{Name: "default"},
			Bindings: []*ateapipb.Binding{
				{Role: authz.RoleOwner, Members: []string{"user:alice@example.com"}},
				{Role: authz.RoleEditor, Members: []string{"user:charlie@example.com"}},
			},
		},
	}
	createdSpacePolAny, err := invoke(aliceCtx, ateapipb.Control_CreateAtespaceAccessPolicy_FullMethodName, createSpacePolReq, func(c context.Context, r any) (any, error) {
		return svc.CreateAtespaceAccessPolicy(c, r.(*ateapipb.CreateAtespaceAccessPolicyRequest))
	})
	if err != nil {
		t.Fatalf("CreateAtespaceAccessPolicy(team-alpha) failed: %v", err)
	}
	createdSpacePol := createdSpacePolAny.(*ateapipb.AccessPolicy)
	if createdSpacePol.GetMetadata().GetVersion() != 1 {
		t.Fatalf("expected Atespace policy version 1, got %d", createdSpacePol.GetMetadata().GetVersion())
	}

	// Charlie (editor on team-alpha) can GetAtespaceAccessPolicy (`can_get_access_policy`),
	// but is denied UpdateAtespaceAccessPolicy (`can_update_access_policy` requires owner)
	// and DeleteAtespaceAccessPolicy (`can_delete_access_policy` requires owner).
	if _, err := invoke(charlieCtx, ateapipb.Control_GetAtespaceAccessPolicy_FullMethodName, getSpacePolReq, func(c context.Context, r any) (any, error) {
		return svc.GetAtespaceAccessPolicy(c, r.(*ateapipb.GetAtespaceAccessPolicyRequest))
	}); err != nil {
		t.Fatalf("expected Charlie (atespace editor) allowed GetAtespaceAccessPolicy, got %v", err)
	}
	updateSpacePolReq := &ateapipb.UpdateAtespaceAccessPolicyRequest{
		Atespace: &ateapipb.ObjectRef{Name: "team-alpha"},
		AccessPolicy: &ateapipb.AccessPolicy{
			Metadata: createdSpacePol.GetMetadata(),
			Bindings: []*ateapipb.Binding{
				{Role: authz.RoleOwner, Members: []string{"user:alice@example.com"}},
				{Role: authz.RoleViewer, Members: []string{"user:charlie@example.com"}},
			},
		},
	}
	if _, err := invoke(charlieCtx, ateapipb.Control_UpdateAtespaceAccessPolicy_FullMethodName, updateSpacePolReq, func(c context.Context, r any) (any, error) {
		return svc.UpdateAtespaceAccessPolicy(c, r.(*ateapipb.UpdateAtespaceAccessPolicyRequest))
	}); apierror.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected Charlie (atespace editor) denied UpdateAtespaceAccessPolicy, got %v", err)
	}
	deleteSpacePolReq := &ateapipb.DeleteAtespaceAccessPolicyRequest{
		Atespace: &ateapipb.ObjectRef{Name: "team-alpha"},
	}
	if _, err := invoke(charlieCtx, ateapipb.Control_DeleteAtespaceAccessPolicy_FullMethodName, deleteSpacePolReq, func(c context.Context, r any) (any, error) {
		return svc.DeleteAtespaceAccessPolicy(c, r.(*ateapipb.DeleteAtespaceAccessPolicyRequest))
	}); apierror.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected Charlie (atespace editor) denied DeleteAtespaceAccessPolicy, got %v", err)
	}

	// 9. UpdateAtespaceAccessPolicy as Alice succeeds and bumps version to 2.
	updatedSpacePolAny, err := invoke(aliceCtx, ateapipb.Control_UpdateAtespaceAccessPolicy_FullMethodName, updateSpacePolReq, func(c context.Context, r any) (any, error) {
		return svc.UpdateAtespaceAccessPolicy(c, r.(*ateapipb.UpdateAtespaceAccessPolicyRequest))
	})
	if err != nil {
		t.Fatalf("UpdateAtespaceAccessPolicy(team-alpha) failed: %v", err)
	}
	updatedSpacePol := updatedSpacePolAny.(*ateapipb.AccessPolicy)
	if updatedSpacePol.GetMetadata().GetVersion() != 2 {
		t.Fatalf("expected Atespace policy version 2, got %d", updatedSpacePol.GetMetadata().GetVersion())
	}

	// 10. DeleteAtespaceAccessPolicy removes both the policy row and OpenFGA tuples.
	if _, err := invoke(aliceCtx, ateapipb.Control_DeleteAtespaceAccessPolicy_FullMethodName, deleteSpacePolReq, func(c context.Context, r any) (any, error) {
		return svc.DeleteAtespaceAccessPolicy(c, r.(*ateapipb.DeleteAtespaceAccessPolicyRequest))
	}); err != nil {
		t.Fatalf("DeleteAtespaceAccessPolicy(team-alpha) failed: %v", err)
	}
	// Charlie's tuple on team-alpha was removed, so Charlie can no longer GetAtespaceAccessPolicy.
	if _, err := invoke(charlieCtx, ateapipb.Control_GetAtespaceAccessPolicy_FullMethodName, getSpacePolReq, func(c context.Context, r any) (any, error) {
		return svc.GetAtespaceAccessPolicy(c, r.(*ateapipb.GetAtespaceAccessPolicyRequest))
	}); apierror.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected Charlie denied after DeleteAtespaceAccessPolicy, got %v", err)
	}

	// 11. Recreate team-alpha's AccessPolicy; it gets a new UID, so an Update carrying
	// the old UID fails with Aborted (UID conflict).
	recreatedPolAny, err := invoke(aliceCtx, ateapipb.Control_CreateAtespaceAccessPolicy_FullMethodName, createSpacePolReq, func(c context.Context, r any) (any, error) {
		return svc.CreateAtespaceAccessPolicy(c, r.(*ateapipb.CreateAtespaceAccessPolicyRequest))
	})
	if err != nil {
		t.Fatalf("re-creating team-alpha access policy failed: %v", err)
	}
	recreatedPol := recreatedPolAny.(*ateapipb.AccessPolicy)
	if recreatedPol.GetMetadata().GetUid() == createdSpacePol.GetMetadata().GetUid() {
		t.Fatalf("expected new UID on recreated access policy, got same UID %s", recreatedPol.GetMetadata().GetUid())
	}
	if _, err := invoke(aliceCtx, ateapipb.Control_UpdateAtespaceAccessPolicy_FullMethodName, updateSpacePolReq, func(c context.Context, r any) (any, error) {
		return svc.UpdateAtespaceAccessPolicy(c, r.(*ateapipb.UpdateAtespaceAccessPolicyRequest))
	}); apierror.Code(err) != codes.Aborted || !strings.Contains(err.Error(), "UID conflict") {
		t.Fatalf("expected Aborted UID conflict for UpdateAtespaceAccessPolicy with old UID, got %v", err)
	}
}
