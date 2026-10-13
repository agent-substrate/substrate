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

package authz

import (
	"context"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func setupTestAuthorizer(t *testing.T) *Authorizer {
	t.Helper()
	ctx := context.Background()
	pool := startPostgres(t)

	fgaServer, err := NewOpenFGAServer(pool)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaServer.Close)

	authorizer, policyManager, err := New(ctx, pool, fgaServer, nil)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	writeTestTuple(t, ctx, pool, policyManager, "alice@example.com", "owner", GlobalRootObject)
	return authorizer
}

func TestUnaryServerInterceptor_QuickRejectionAndDispatch(t *testing.T) {
	authorizer := setupTestAuthorizer(t)

	allowedCtx := principal.InjectContext(context.Background(), principal.PrincipalInfo{
		ID:   "alice@example.com",
		Kind: principal.KindJWT,
	})
	deniedCtx := principal.InjectContext(context.Background(), principal.PrincipalInfo{
		ID:   "bob@example.com",
		Kind: principal.KindJWT,
	})

	interceptor := UnaryServerInterceptor(authorizer, true)

	tests := []struct {
		name        string
		fullMethod  string
		req         any
		ctx         context.Context
		wantCode    codes.Code
		wantHandler bool
	}{
		{
			name:        "CreateAtespace denied before handler",
			fullMethod:  ateapipb.Control_CreateAtespace_FullMethodName,
			req:         &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team1"}}},
			ctx:         deniedCtx,
			wantCode:    codes.PermissionDenied,
			wantHandler: false,
		},
		{
			name:        "CreateAtespace allowed",
			fullMethod:  ateapipb.Control_CreateAtespace_FullMethodName,
			req:         &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team1"}}},
			ctx:         allowedCtx,
			wantCode:    codes.OK,
			wantHandler: true,
		},
		{
			name:        "ListAtespaces denied before handler",
			fullMethod:  ateapipb.Control_ListAtespaces_FullMethodName,
			req:         &ateapipb.ListAtespacesRequest{},
			ctx:         deniedCtx,
			wantCode:    codes.PermissionDenied,
			wantHandler: false,
		},
		{
			name:        "ListAtespaces allowed",
			fullMethod:  ateapipb.Control_ListAtespaces_FullMethodName,
			req:         &ateapipb.ListAtespacesRequest{},
			ctx:         allowedCtx,
			wantCode:    codes.OK,
			wantHandler: true,
		},
		{
			name:        "GetAtespace denied before handler",
			fullMethod:  ateapipb.Control_GetAtespace_FullMethodName,
			req:         &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team1"}},
			ctx:         deniedCtx,
			wantCode:    codes.PermissionDenied,
			wantHandler: false,
		},
		{
			name:        "GetAtespace allowed",
			fullMethod:  ateapipb.Control_GetAtespace_FullMethodName,
			req:         &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team1"}},
			ctx:         allowedCtx,
			wantCode:    codes.OK,
			wantHandler: true,
		},
		{
			name:        "DeleteAtespace denied before handler",
			fullMethod:  ateapipb.Control_DeleteAtespace_FullMethodName,
			req:         &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team1"}},
			ctx:         deniedCtx,
			wantCode:    codes.PermissionDenied,
			wantHandler: false,
		},
		{
			name:        "DeleteAtespace allowed",
			fullMethod:  ateapipb.Control_DeleteAtespace_FullMethodName,
			req:         &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team1"}},
			ctx:         allowedCtx,
			wantCode:    codes.OK,
			wantHandler: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handlerCalled := false
			_, err := interceptor(tc.ctx, tc.req, &grpc.UnaryServerInfo{FullMethod: tc.fullMethod}, func(ctx context.Context, req any) (any, error) {
				handlerCalled = true
				return "ok", nil
			})
			if apierror.Code(err) != tc.wantCode {
				t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", apierror.Code(err), tc.wantCode, err)
			}
			if handlerCalled != tc.wantHandler {
				t.Fatalf("handlerCalled = %v, want %v", handlerCalled, tc.wantHandler)
			}
		})
	}
}

func TestUnaryServerInterceptor_MalformedRequestRequiresPrincipalThenFailsClosed(t *testing.T) {
	authorizer := setupTestAuthorizer(t)
	interceptor := UnaryServerInterceptor(authorizer, true)

	// 1. Unauthenticated caller with empty atespace name -> Unauthenticated
	_, err := interceptor(context.Background(), &ateapipb.GetAtespaceRequest{}, &grpc.UnaryServerInfo{
		FullMethod: ateapipb.Control_GetAtespace_FullMethodName,
	}, func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler must not be invoked for unauthenticated request")
		return nil, nil
	})
	if apierror.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated for missing principal, got %v", err)
	}

	// 2. Authenticated caller with empty or invalid atespace name ->
	// InvalidArgument without calling the handler or OpenFGA.
	authCtx := principal.InjectContext(context.Background(), principal.PrincipalInfo{
		ID:   "alice@example.com",
		Kind: principal.KindJWT,
	})
	for _, name := range []string{"", "team\n1"} {
		_, err = interceptor(authCtx, &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: name}}, &grpc.UnaryServerInfo{
			FullMethod: ateapipb.Control_GetAtespace_FullMethodName,
		}, func(ctx context.Context, req any) (any, error) {
			t.Fatalf("handler must not be invoked for atespace name %q", name)
			return nil, nil
		})
		if apierror.Code(err) != codes.InvalidArgument {
			t.Fatalf("atespace name %q: expected InvalidArgument, got %v", name, err)
		}
	}

	// 3. Unexpected request type for a registered RPC -> fails closed with codes.Internal without calling handler
	_, err = interceptor(authCtx, "wrong-type", &grpc.UnaryServerInfo{
		FullMethod: ateapipb.Control_GetAtespace_FullMethodName,
	}, func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler must not be invoked when request type assertion fails")
		return nil, nil
	})
	if apierror.Code(err) != codes.Internal {
		t.Fatalf("expected Internal for unexpected request type, got %v", err)
	}

}

func TestUnaryServerInterceptor_EnforcementDisabled(t *testing.T) {
	authorizer := setupTestAuthorizer(t)
	interceptor := UnaryServerInterceptor(authorizer, false)
	bobCtx := principal.InjectContext(context.Background(), principal.PrincipalInfo{
		ID:   "bob@example.com",
		Kind: principal.KindJWT,
	})
	aliceCtx := principal.InjectContext(context.Background(), principal.PrincipalInfo{
		ID:   "alice@example.com",
		Kind: principal.KindJWT,
	})

	tests := []struct {
		name       string
		ctx        context.Context
		fullMethod string
		req        any
		wantCode   codes.Code
	}{
		{
			name:       "regular RPC skips the check",
			ctx:        bobCtx,
			fullMethod: ateapipb.Control_CreateAtespace_FullMethodName,
			req:        &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team1"}}},
			wantCode:   codes.OK,
		},
		{
			name:       "governance RPC is still denied",
			ctx:        bobCtx,
			fullMethod: ateapipb.Control_UpdateGlobalAccessPolicy_FullMethodName,
			req:        &ateapipb.UpdateGlobalAccessPolicyRequest{},
			wantCode:   codes.PermissionDenied,
		},
		{
			name:       "governance RPC is still allowed for an owner",
			ctx:        aliceCtx,
			fullMethod: ateapipb.Control_UpdateGlobalAccessPolicy_FullMethodName,
			req:        &ateapipb.UpdateGlobalAccessPolicyRequest{},
			wantCode:   codes.OK,
		},
		{
			name:       "governance RPC still requires a principal",
			ctx:        context.Background(),
			fullMethod: ateapipb.Control_GetAtespaceAccessPolicy_FullMethodName,
			req:        &ateapipb.GetAtespaceAccessPolicyRequest{},
			wantCode:   codes.Unauthenticated,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handlerCalled := false
			_, err := interceptor(tc.ctx, tc.req, &grpc.UnaryServerInfo{FullMethod: tc.fullMethod}, func(ctx context.Context, req any) (any, error) {
				handlerCalled = true
				return "ok", nil
			})
			if apierror.Code(err) != tc.wantCode {
				t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", apierror.Code(err), tc.wantCode, err)
			}
			if wantHandler := tc.wantCode == codes.OK; handlerCalled != wantHandler {
				t.Fatalf("handlerCalled = %v, want %v", handlerCalled, wantHandler)
			}
		})
	}
}

// TestAccessPolicyRPCsAlwaysEnforced guards against an AccessPolicy RPC being
// added without a governance rule, which would leave it unchecked while
// enforcement is disabled.
func TestAccessPolicyRPCsAlwaysEnforced(t *testing.T) {
	desc := ateapipb.Control_ServiceDesc
	found := 0
	for _, m := range desc.Methods {
		if !strings.Contains(m.MethodName, "AccessPolicy") {
			continue
		}
		found++
		fullMethod := "/" + desc.ServiceName + "/" + m.MethodName
		rule, ok := defaultRPCPermissions[fullMethod]
		if !ok {
			t.Errorf("%s has no permission rule", fullMethod)
			continue
		}
		if !rule.alwaysEnforce {
			t.Errorf("%s is not marked alwaysEnforce", fullMethod)
		}
	}
	if found == 0 {
		t.Fatal("found no AccessPolicy RPCs in Control_ServiceDesc")
	}
}

func TestUnaryServerInterceptor_ActorAndTemplateChecks(t *testing.T) {
	ctx := context.Background()
	pool := startPostgres(t)
	fgaServer, err := NewOpenFGAServer(pool)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaServer.Close)
	// bootstrap-owner is a global owner through configuration only; it has no
	// stored tuples.
	authorizer, policyManager, err := New(ctx, pool, fgaServer, []string{"bootstrap-owner"})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Actors run in team-a. The shared atespace holds templates that team-a's
	// actors may reference.
	for _, g := range []struct{ user, role, object string }{
		{"global-owner", "owner", GlobalRootObject},
		{"global-viewer", "viewer", GlobalRootObject},
		{"editor", "editor", AtespaceObject("team-a")},
		{"editor-with-shared", "editor", AtespaceObject("team-a")},
		{"editor-with-shared", "viewer", AtespaceObject("shared")},
		{"viewer", "viewer", AtespaceObject("team-a")},
		{"shared-viewer", "viewer", AtespaceObject("shared")},
	} {
		writeTestTuple(t, ctx, pool, policyManager, g.user, g.role, g.object)
	}

	type rpcCall struct {
		fullMethod string
		req        any
	}
	local := &ateapipb.ObjectRef{Atespace: "team-a", Name: "tmpl"}
	shared := &ateapipb.ObjectRef{Atespace: "shared", Name: "tmpl"}
	actor := &ateapipb.ObjectRef{Atespace: "team-a", Name: "runner"}
	actorWithTemplate := func(template *ateapipb.ObjectRef) *ateapipb.Actor {
		return &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: actor.GetAtespace(), Name: actor.GetName()},
			ActorTemplate: template,
		}
	}
	createActor := func(template *ateapipb.ObjectRef) rpcCall {
		return rpcCall{ateapipb.Control_CreateActor_FullMethodName, &ateapipb.CreateActorRequest{Actor: actorWithTemplate(template)}}
	}
	updateActor := func(template *ateapipb.ObjectRef) rpcCall {
		return rpcCall{ateapipb.Control_UpdateActor_FullMethodName, &ateapipb.UpdateActorRequest{Actor: actorWithTemplate(template)}}
	}
	getActor := rpcCall{ateapipb.Control_GetActor_FullMethodName, &ateapipb.GetActorRequest{Actor: actor}}
	deleteActor := rpcCall{ateapipb.Control_DeleteActor_FullMethodName, &ateapipb.DeleteActorRequest{Actor: actor}}
	listActors := func(atespace string) rpcCall {
		return rpcCall{ateapipb.Control_ListActors_FullMethodName, &ateapipb.ListActorsRequest{Atespace: atespace}}
	}
	createTemplate := rpcCall{ateapipb.Control_CreateActorTemplate_FullMethodName, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{Atespace: local.GetAtespace(), Name: local.GetName()}},
	}}
	getTemplate := func(template *ateapipb.ObjectRef) rpcCall {
		return rpcCall{ateapipb.Control_GetActorTemplate_FullMethodName, &ateapipb.GetActorTemplateRequest{ActorTemplate: template}}
	}
	deleteTemplate := rpcCall{ateapipb.Control_DeleteActorTemplate_FullMethodName, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: local}}
	listTemplates := func(atespace string) rpcCall {
		return rpcCall{ateapipb.Control_ListActorTemplates_FullMethodName, &ateapipb.ListActorTemplatesRequest{Atespace: atespace}}
	}

	tests := []struct {
		name     string
		user     string
		call     rpcCall
		wantCode codes.Code
	}{
		// CreateActor needs can_create_actor on team-a and can_use_template on the template.
		{"editor creates actor from local template", "editor", createActor(local), codes.OK},
		{"viewer cannot create actor", "viewer", createActor(local), codes.PermissionDenied},
		{"global viewer cannot create actor", "global-viewer", createActor(local), codes.PermissionDenied},
		{"editor cannot create actor from template they cannot use", "editor", createActor(shared), codes.PermissionDenied},
		{"editor creates actor from shared template they can use", "editor-with-shared", createActor(shared), codes.OK},
		{"shared viewer cannot create actor without editor on team-a", "shared-viewer", createActor(shared), codes.PermissionDenied},
		{"global owner creates actor from any template", "global-owner", createActor(shared), codes.OK},

		// UpdateActor needs can_update on the actor and can_use_template on the template.
		{"editor updates actor on local template", "editor", updateActor(local), codes.OK},
		{"viewer cannot update actor", "viewer", updateActor(local), codes.PermissionDenied},
		{"editor cannot point actor at template they cannot use", "editor", updateActor(shared), codes.PermissionDenied},
		{"editor updates actor on shared template they can use", "editor-with-shared", updateActor(shared), codes.OK},

		// GetActor and DeleteActor inherit from the actor's atespace.
		{"viewer gets actor", "viewer", getActor, codes.OK},
		{"outsider cannot get actor", "shared-viewer", getActor, codes.PermissionDenied},
		{"editor deletes actor", "editor", deleteActor, codes.OK},
		{"viewer cannot delete actor", "viewer", deleteActor, codes.PermissionDenied},

		// Actor templates inherit from their atespace.
		{"editor creates template", "editor", createTemplate, codes.OK},
		{"viewer cannot create template", "viewer", createTemplate, codes.PermissionDenied},
		{"shared viewer gets shared template", "shared-viewer", getTemplate(shared), codes.OK},
		{"team-a editor cannot get shared template", "editor", getTemplate(shared), codes.PermissionDenied},
		{"editor deletes template", "editor", deleteTemplate, codes.OK},
		{"viewer cannot delete template", "viewer", deleteTemplate, codes.PermissionDenied},

		// Listing one atespace needs viewer on it; listing all needs global viewer.
		{"viewer lists actors in team-a", "viewer", listActors("team-a"), codes.OK},
		{"outsider cannot list actors in team-a", "shared-viewer", listActors("team-a"), codes.PermissionDenied},
		{"atespace viewer cannot list all actors", "viewer", listActors(""), codes.PermissionDenied},
		{"global viewer lists all actors", "global-viewer", listActors(""), codes.OK},
		{"shared viewer lists templates in shared", "shared-viewer", listTemplates("shared"), codes.OK},
		{"atespace viewer cannot list all templates", "shared-viewer", listTemplates(""), codes.PermissionDenied},
		{"global owner lists all templates", "global-owner", listTemplates(""), codes.OK},

		// Global owners reach actors and templates through the contextual
		// actor -> atespace -> global:root links. The bootstrap owner has no
		// stored tuples at all, so every hop of its path is contextual.
		{"global owner gets actor", "global-owner", getActor, codes.OK},
		{"global owner updates actor", "global-owner", updateActor(local), codes.OK},
		{"global owner deletes actor", "global-owner", deleteActor, codes.OK},
		{"global owner gets template", "global-owner", getTemplate(local), codes.OK},
		{"global owner deletes template", "global-owner", deleteTemplate, codes.OK},
		{"bootstrap owner creates actor", "bootstrap-owner", createActor(shared), codes.OK},
		{"bootstrap owner gets actor", "bootstrap-owner", getActor, codes.OK},
		{"bootstrap owner updates actor", "bootstrap-owner", updateActor(shared), codes.OK},
		{"bootstrap owner deletes actor", "bootstrap-owner", deleteActor, codes.OK},
		{"bootstrap owner gets template", "bootstrap-owner", getTemplate(shared), codes.OK},
		{"bootstrap owner deletes template", "bootstrap-owner", deleteTemplate, codes.OK},
		{"bootstrap owner lists all actors", "bootstrap-owner", listActors(""), codes.OK},
		{"global viewer gets actor", "global-viewer", getActor, codes.OK},
		{"global viewer cannot update actor", "global-viewer", updateActor(local), codes.PermissionDenied},
		{"global viewer cannot delete actor", "global-viewer", deleteActor, codes.PermissionDenied},
		{"global viewer cannot delete template", "global-viewer", deleteTemplate, codes.PermissionDenied},
	}

	interceptor := UnaryServerInterceptor(authorizer, true)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			userCtx := principal.InjectContext(ctx, principal.PrincipalInfo{ID: tc.user, Kind: principal.KindJWT})
			handlerCalled := false
			_, err := interceptor(userCtx, tc.call.req, &grpc.UnaryServerInfo{FullMethod: tc.call.fullMethod}, func(context.Context, any) (any, error) {
				handlerCalled = true
				return "ok", nil
			})
			if apierror.Code(err) != tc.wantCode {
				t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", apierror.Code(err), tc.wantCode, err)
			}
			if wantHandler := tc.wantCode == codes.OK; handlerCalled != wantHandler {
				t.Fatalf("handlerCalled = %v, want %v", handlerCalled, wantHandler)
			}
		})
	}
}

func TestUnaryServerInterceptor_LifecycleEgressAndWorkerChecks(t *testing.T) {
	ctx := context.Background()
	pool := startPostgres(t)
	fgaServer, err := NewOpenFGAServer(pool)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaServer.Close)
	// bootstrap-owner is a global owner through configuration only; it has no
	// stored tuples.
	authorizer, policyManager, err := New(ctx, pool, fgaServer, []string{"bootstrap-owner"})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	for _, g := range []struct{ user, role, object string }{
		{"global-owner", "owner", GlobalRootObject},
		{"global-viewer", "viewer", GlobalRootObject},
		{"editor", "editor", AtespaceObject("team-a")},
		{"viewer", "viewer", AtespaceObject("team-a")},
		{"outsider", "editor", AtespaceObject("team-b")},
	} {
		writeTestTuple(t, ctx, pool, policyManager, g.user, g.role, g.object)
	}

	type rpcCall struct {
		fullMethod string
		req        any
	}
	actor := &ateapipb.ObjectRef{Atespace: "team-a", Name: "runner"}
	worker := &ateapipb.ObjectRef{Name: "w-1"}
	pause := rpcCall{ateapipb.Control_PauseActor_FullMethodName, &ateapipb.PauseActorRequest{Actor: actor}}
	resume := rpcCall{ateapipb.Control_ResumeActor_FullMethodName, &ateapipb.ResumeActorRequest{Actor: actor}}
	suspend := rpcCall{ateapipb.Control_SuspendActor_FullMethodName, &ateapipb.SuspendActorRequest{Actor: actor}}
	revert := rpcCall{ateapipb.Control_RevertActor_FullMethodName, &ateapipb.RevertActorRequest{Actor: actor}}
	getEgress := rpcCall{ateapipb.Control_GetActorEgressPolicy_FullMethodName, &ateapipb.GetActorEgressPolicyRequest{Actor: actor}}
	createEgress := rpcCall{ateapipb.Control_CreateActorEgressPolicy_FullMethodName, &ateapipb.CreateActorEgressPolicyRequest{Actor: actor}}
	updateEgress := rpcCall{ateapipb.Control_UpdateActorEgressPolicy_FullMethodName, &ateapipb.UpdateActorEgressPolicyRequest{Actor: actor}}
	deleteEgress := rpcCall{ateapipb.Control_DeleteActorEgressPolicy_FullMethodName, &ateapipb.DeleteActorEgressPolicyRequest{Actor: actor}}
	createWorker := rpcCall{ateapipb.Control_CreateWorker_FullMethodName, &ateapipb.CreateWorkerRequest{}}
	listWorkers := rpcCall{ateapipb.Control_ListWorkers_FullMethodName, &ateapipb.ListWorkersRequest{}}
	getWorker := rpcCall{ateapipb.Control_GetWorker_FullMethodName, &ateapipb.GetWorkerRequest{Worker: worker}}
	updateWorker := rpcCall{ateapipb.Control_UpdateWorker_FullMethodName, &ateapipb.UpdateWorkerRequest{Worker: &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: worker.GetName()},
	}}}
	deleteWorker := rpcCall{ateapipb.Control_DeleteWorker_FullMethodName, &ateapipb.DeleteWorkerRequest{Worker: worker}}
	drainWorker := rpcCall{ateapipb.Control_DrainWorker_FullMethodName, &ateapipb.DrainWorkerRequest{Worker: worker}}
	listAssignments := rpcCall{ateapipb.Control_ListWorkerActorAssignments_FullMethodName, &ateapipb.ListWorkerActorAssignmentsRequest{Worker: worker}}

	tests := []struct {
		name     string
		user     string
		call     rpcCall
		wantCode codes.Code
	}{
		// Lifecycle operations need editor on the actor's atespace.
		{"editor pauses actor", "editor", pause, codes.OK},
		{"editor resumes actor", "editor", resume, codes.OK},
		{"editor suspends actor", "editor", suspend, codes.OK},
		{"editor reverts actor", "editor", revert, codes.OK},
		{"viewer cannot pause actor", "viewer", pause, codes.PermissionDenied},
		{"viewer cannot resume actor", "viewer", resume, codes.PermissionDenied},
		{"viewer cannot suspend actor", "viewer", suspend, codes.PermissionDenied},
		{"viewer cannot revert actor", "viewer", revert, codes.PermissionDenied},
		{"editor of another atespace cannot resume actor", "outsider", resume, codes.PermissionDenied},
		{"global viewer cannot suspend actor", "global-viewer", suspend, codes.PermissionDenied},
		{"global owner pauses actor", "global-owner", pause, codes.OK},
		{"bootstrap owner resumes actor", "bootstrap-owner", resume, codes.OK},

		// Egress policies: viewers read, editors write.
		{"viewer gets egress policy", "viewer", getEgress, codes.OK},
		{"global viewer gets egress policy", "global-viewer", getEgress, codes.OK},
		{"outsider cannot get egress policy", "outsider", getEgress, codes.PermissionDenied},
		{"editor creates egress policy", "editor", createEgress, codes.OK},
		{"editor updates egress policy", "editor", updateEgress, codes.OK},
		{"editor deletes egress policy", "editor", deleteEgress, codes.OK},
		{"viewer cannot create egress policy", "viewer", createEgress, codes.PermissionDenied},
		{"viewer cannot update egress policy", "viewer", updateEgress, codes.PermissionDenied},
		{"viewer cannot delete egress policy", "viewer", deleteEgress, codes.PermissionDenied},
		{"global owner updates egress policy", "global-owner", updateEgress, codes.OK},
		{"bootstrap owner deletes egress policy", "bootstrap-owner", deleteEgress, codes.OK},

		// Workers are global-scoped: global viewers read, global owners write.
		{"global viewer lists workers", "global-viewer", listWorkers, codes.OK},
		{"global viewer gets worker", "global-viewer", getWorker, codes.OK},
		{"global viewer lists worker assignments", "global-viewer", listAssignments, codes.OK},
		{"global viewer cannot create worker", "global-viewer", createWorker, codes.PermissionDenied},
		{"global viewer cannot update worker", "global-viewer", updateWorker, codes.PermissionDenied},
		{"global viewer cannot delete worker", "global-viewer", deleteWorker, codes.PermissionDenied},
		{"global viewer cannot drain worker", "global-viewer", drainWorker, codes.PermissionDenied},
		{"global owner creates worker", "global-owner", createWorker, codes.OK},
		{"global owner updates worker", "global-owner", updateWorker, codes.OK},
		{"global owner deletes worker", "global-owner", deleteWorker, codes.OK},
		{"global owner drains worker", "global-owner", drainWorker, codes.OK},
		{"bootstrap owner creates worker", "bootstrap-owner", createWorker, codes.OK},
		{"bootstrap owner drains worker", "bootstrap-owner", drainWorker, codes.OK},
		{"bootstrap owner gets worker", "bootstrap-owner", getWorker, codes.OK},
		{"atespace editor cannot list workers", "editor", listWorkers, codes.PermissionDenied},
		{"atespace editor cannot get worker", "editor", getWorker, codes.PermissionDenied},
		{"atespace editor cannot drain worker", "editor", drainWorker, codes.PermissionDenied},
	}

	interceptor := UnaryServerInterceptor(authorizer, true)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			userCtx := principal.InjectContext(ctx, principal.PrincipalInfo{ID: tc.user, Kind: principal.KindJWT})
			handlerCalled := false
			_, err := interceptor(userCtx, tc.call.req, &grpc.UnaryServerInfo{FullMethod: tc.call.fullMethod}, func(context.Context, any) (any, error) {
				handlerCalled = true
				return "ok", nil
			})
			if apierror.Code(err) != tc.wantCode {
				t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", apierror.Code(err), tc.wantCode, err)
			}
			if wantHandler := tc.wantCode == codes.OK; handlerCalled != wantHandler {
				t.Fatalf("handlerCalled = %v, want %v", handlerCalled, wantHandler)
			}
		})
	}
}
