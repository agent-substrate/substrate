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
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/principal"
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

	authorizer, policyManager, err := New(ctx, pool, fgaServer, nil, testSystemPrincipals())
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

	interceptor := UnaryServerInterceptor(authorizer)

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
			fullMethod:  ateapi.Control_CreateAtespace_FullMethodName,
			req:         &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "team1"}}},
			ctx:         deniedCtx,
			wantCode:    codes.PermissionDenied,
			wantHandler: false,
		},
		{
			name:        "CreateAtespace allowed",
			fullMethod:  ateapi.Control_CreateAtespace_FullMethodName,
			req:         &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "team1"}}},
			ctx:         allowedCtx,
			wantCode:    codes.OK,
			wantHandler: true,
		},
		{
			name:        "ListAtespaces denied before handler",
			fullMethod:  ateapi.Control_ListAtespaces_FullMethodName,
			req:         &ateapi.ListAtespacesRequest{},
			ctx:         deniedCtx,
			wantCode:    codes.PermissionDenied,
			wantHandler: false,
		},
		{
			name:        "ListAtespaces allowed",
			fullMethod:  ateapi.Control_ListAtespaces_FullMethodName,
			req:         &ateapi.ListAtespacesRequest{},
			ctx:         allowedCtx,
			wantCode:    codes.OK,
			wantHandler: true,
		},
		{
			name:        "GetAtespace denied before handler",
			fullMethod:  ateapi.Control_GetAtespace_FullMethodName,
			req:         &ateapi.GetAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "team1"}},
			ctx:         deniedCtx,
			wantCode:    codes.PermissionDenied,
			wantHandler: false,
		},
		{
			name:        "GetAtespace allowed",
			fullMethod:  ateapi.Control_GetAtespace_FullMethodName,
			req:         &ateapi.GetAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "team1"}},
			ctx:         allowedCtx,
			wantCode:    codes.OK,
			wantHandler: true,
		},
		{
			name:        "DeleteAtespace denied before handler",
			fullMethod:  ateapi.Control_DeleteAtespace_FullMethodName,
			req:         &ateapi.DeleteAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "team1"}},
			ctx:         deniedCtx,
			wantCode:    codes.PermissionDenied,
			wantHandler: false,
		},
		{
			name:        "DeleteAtespace allowed",
			fullMethod:  ateapi.Control_DeleteAtespace_FullMethodName,
			req:         &ateapi.DeleteAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "team1"}},
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
	interceptor := UnaryServerInterceptor(authorizer)

	// 1. Unauthenticated caller with empty atespace name -> Unauthenticated
	_, err := interceptor(context.Background(), &ateapi.GetAtespaceRequest{}, &grpc.UnaryServerInfo{
		FullMethod: ateapi.Control_GetAtespace_FullMethodName,
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
		_, err = interceptor(authCtx, &ateapi.GetAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: name}}, &grpc.UnaryServerInfo{
			FullMethod: ateapi.Control_GetAtespace_FullMethodName,
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
		FullMethod: ateapi.Control_GetAtespace_FullMethodName,
	}, func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler must not be invoked when request type assertion fails")
		return nil, nil
	})
	if apierror.Code(err) != codes.Internal {
		t.Fatalf("expected Internal for unexpected request type, got %v", err)
	}

}

// An RPC with no rule is denied, and an RPC its handler authorizes passes
// through. Neither reaches OpenFGA, so the authorizer is nil.
func TestUnaryServerInterceptor_UnregisteredAndHandlerAuthorizedRPCs(t *testing.T) {
	const unregistered = "/substrate.ateapi.v1.Control/NotARealMethod"
	aliceCtx := principal.InjectContext(context.Background(), principal.PrincipalInfo{
		ID:   "alice@example.com",
		Kind: principal.KindJWT,
	})

	tests := []struct {
		name       string
		ctx        context.Context
		fullMethod string
		wantCode   codes.Code
	}{
		{"unregistered RPC is denied", aliceCtx, unregistered, codes.PermissionDenied},
		{"unregistered RPC is denied without a principal", context.Background(), unregistered, codes.PermissionDenied},
		{"RegisterWorker passes", context.Background(), ateapi.WorkerService_RegisterWorker_FullMethodName, codes.OK},
		{"RequestActorSuspend passes", context.Background(), ateapi.WorkerService_RequestActorSuspend_FullMethodName, codes.OK},
		{"MintWorkerActorCertificate passes", context.Background(), ateapi.WorkerService_MintWorkerActorCertificate_FullMethodName, codes.OK},
		{"MintWorkerActorJwt passes", context.Background(), ateapi.WorkerService_MintWorkerActorJwt_FullMethodName, codes.OK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			interceptor := UnaryServerInterceptor(nil)
			handlerCalled := false
			_, err := interceptor(tc.ctx, struct{}{}, &grpc.UnaryServerInfo{FullMethod: tc.fullMethod}, func(context.Context, any) (any, error) {
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
	authorizer, policyManager, err := New(ctx, pool, fgaServer, []string{"bootstrap-owner"}, testSystemPrincipals())
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
	local := &ateapi.ObjectRef{Atespace: "team-a", Name: "tmpl"}
	shared := &ateapi.ObjectRef{Atespace: "shared", Name: "tmpl"}
	actor := &ateapi.ObjectRef{Atespace: "team-a", Name: "runner"}
	actorWithTemplate := func(template *ateapi.ObjectRef) *ateapi.Actor {
		return &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: actor.GetAtespace(), Name: actor.GetName()},
			ActorTemplate: template,
		}
	}
	createActor := func(template *ateapi.ObjectRef) rpcCall {
		return rpcCall{ateapi.Control_CreateActor_FullMethodName, &ateapi.CreateActorRequest{Actor: actorWithTemplate(template)}}
	}
	updateActor := func(template *ateapi.ObjectRef) rpcCall {
		return rpcCall{ateapi.Control_UpdateActor_FullMethodName, &ateapi.UpdateActorRequest{Actor: actorWithTemplate(template)}}
	}
	getActor := rpcCall{ateapi.Control_GetActor_FullMethodName, &ateapi.GetActorRequest{Actor: actor}}
	deleteActor := rpcCall{ateapi.Control_DeleteActor_FullMethodName, &ateapi.DeleteActorRequest{Actor: actor}}
	listActors := func(atespace string) rpcCall {
		return rpcCall{ateapi.Control_ListActors_FullMethodName, &ateapi.ListActorsRequest{Atespace: atespace}}
	}
	createTemplate := rpcCall{ateapi.Control_CreateActorTemplate_FullMethodName, &ateapi.CreateActorTemplateRequest{
		ActorTemplate: &ateapi.ActorTemplate{Metadata: &ateapi.ResourceMetadata{Atespace: local.GetAtespace(), Name: local.GetName()}},
	}}
	getTemplate := func(template *ateapi.ObjectRef) rpcCall {
		return rpcCall{ateapi.Control_GetActorTemplate_FullMethodName, &ateapi.GetActorTemplateRequest{ActorTemplate: template}}
	}
	deleteTemplate := rpcCall{ateapi.Control_DeleteActorTemplate_FullMethodName, &ateapi.DeleteActorTemplateRequest{ActorTemplate: local}}
	listTemplates := func(atespace string) rpcCall {
		return rpcCall{ateapi.Control_ListActorTemplates_FullMethodName, &ateapi.ListActorTemplatesRequest{Atespace: atespace}}
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

	interceptor := UnaryServerInterceptor(authorizer)
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

func TestUnaryServerInterceptor_LifecycleEgressWorkerAndTagChecks(t *testing.T) {
	ctx := context.Background()
	pool := startPostgres(t)
	fgaServer, err := NewOpenFGAServer(pool)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaServer.Close)
	// bootstrap-owner is a global owner through configuration only; it has no
	// stored tuples.
	authorizer, policyManager, err := New(ctx, pool, fgaServer, []string{"bootstrap-owner"}, testSystemPrincipals())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	for _, g := range []struct{ user, role, object string }{
		{"global-owner", "owner", GlobalRootObject},
		{"global-viewer", "viewer", GlobalRootObject},
		{"editor", "editor", AtespaceObject("team-a")},
		{"viewer", "viewer", AtespaceObject("team-a")},
		{"outsider", "editor", AtespaceObject("team-b")},
		{"outsider-with-viewer", "editor", AtespaceObject("team-b")},
		{"outsider-with-viewer", "viewer", AtespaceObject("team-a")},
	} {
		writeTestTuple(t, ctx, pool, policyManager, g.user, g.role, g.object)
	}

	type rpcCall struct {
		fullMethod string
		req        any
	}
	actor := &ateapi.ObjectRef{Atespace: "team-a", Name: "runner"}
	worker := &ateapi.ObjectRef{Name: "w-1"}
	resume := rpcCall{ateapi.Control_ResumeActor_FullMethodName, &ateapi.ResumeActorRequest{Actor: actor}}
	suspend := rpcCall{ateapi.Control_SuspendActor_FullMethodName, &ateapi.SuspendActorRequest{Actor: actor}}
	revert := rpcCall{ateapi.Control_RevertActor_FullMethodName, &ateapi.RevertActorRequest{Actor: actor}}
	getEgress := rpcCall{ateapi.Control_GetActorEgressPolicy_FullMethodName, &ateapi.GetActorEgressPolicyRequest{Actor: actor}}
	createEgress := rpcCall{ateapi.Control_CreateActorEgressPolicy_FullMethodName, &ateapi.CreateActorEgressPolicyRequest{Actor: actor}}
	updateEgress := rpcCall{ateapi.Control_UpdateActorEgressPolicy_FullMethodName, &ateapi.UpdateActorEgressPolicyRequest{Actor: actor}}
	deleteEgress := rpcCall{ateapi.Control_DeleteActorEgressPolicy_FullMethodName, &ateapi.DeleteActorEgressPolicyRequest{Actor: actor}}
	createWorker := rpcCall{ateapi.Control_CreateWorker_FullMethodName, &ateapi.CreateWorkerRequest{}}
	listWorkers := rpcCall{ateapi.Control_ListWorkers_FullMethodName, &ateapi.ListWorkersRequest{}}
	getWorker := rpcCall{ateapi.Control_GetWorker_FullMethodName, &ateapi.GetWorkerRequest{Worker: worker}}
	updateWorker := rpcCall{ateapi.Control_UpdateWorker_FullMethodName, &ateapi.UpdateWorkerRequest{Worker: &ateapi.Worker{
		Metadata: &ateapi.ResourceMetadata{Name: worker.GetName()},
	}}}
	deleteWorker := rpcCall{ateapi.Control_DeleteWorker_FullMethodName, &ateapi.DeleteWorkerRequest{Worker: worker}}
	drainWorker := rpcCall{ateapi.Control_DrainWorker_FullMethodName, &ateapi.DrainWorkerRequest{Worker: worker}}
	markAvailable := rpcCall{ateapi.Control_MarkWorkerAvailable_FullMethodName, &ateapi.MarkWorkerAvailableRequest{Worker: worker}}
	markUnavailable := rpcCall{ateapi.Control_MarkWorkerUnavailable_FullMethodName, &ateapi.MarkWorkerUnavailableRequest{Worker: worker}}
	listAssignments := rpcCall{ateapi.Control_ListWorkerActorAssignments_FullMethodName, &ateapi.ListWorkerActorAssignmentsRequest{Worker: worker}}
	tag := &ateapi.ObjectRef{Atespace: "team-a", Name: "v1"}
	createTag := rpcCall{ateapi.Control_CreateTag_FullMethodName, &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: "team-a", Name: "v1"},
		SourceActor: actor,
	}}}
	// A tag in team-b of an actor in team-a.
	createCrossTag := rpcCall{ateapi.Control_CreateTag_FullMethodName, &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: "team-b", Name: "v1"},
		SourceActor: actor,
	}}}
	getTag := rpcCall{ateapi.Control_GetTag_FullMethodName, &ateapi.GetTagRequest{Tag: tag}}
	listTags := rpcCall{ateapi.Control_ListTags_FullMethodName, &ateapi.ListTagsRequest{Atespace: "team-a"}}
	listAllTags := rpcCall{ateapi.Control_ListTags_FullMethodName, &ateapi.ListTagsRequest{}}
	updateTag := rpcCall{ateapi.Control_UpdateTag_FullMethodName, &ateapi.UpdateTagRequest{Tag: &ateapi.Tag{
		Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "v1"},
	}}}
	deleteTag := rpcCall{ateapi.Control_DeleteTag_FullMethodName, &ateapi.DeleteTagRequest{Tag: tag}}
	mintJWT := rpcCall{ateapi.Control_MintActorJwt_FullMethodName, &ateapi.MintActorJwtRequest{Actor: actor}}
	mintCert := rpcCall{ateapi.Control_MintActorCertificate_FullMethodName, &ateapi.MintActorCertificateRequest{Actor: actor}}
	// cloneTag creates an actor in atespace from team-a's tag v1, with a
	// template in that same atespace so only the tag check can differ.
	cloneTag := func(atespace string, sourceTag *ateapi.ObjectRef) rpcCall {
		return rpcCall{ateapi.Control_CreateActor_FullMethodName, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: atespace, Name: "clone"},
			ActorTemplate: &ateapi.ObjectRef{Atespace: atespace, Name: "tmpl"},
			SourceTag:     sourceTag,
		}}}
	}
	cloneInTeamA := cloneTag("team-a", tag)
	cloneInTeamB := cloneTag("team-b", tag)
	createWithoutTag := cloneTag("team-a", nil)

	tests := []struct {
		name     string
		user     string
		call     rpcCall
		wantCode codes.Code
	}{
		// Lifecycle operations need editor on the actor's atespace.
		{"editor resumes actor", "editor", resume, codes.OK},
		{"editor suspends actor", "editor", suspend, codes.OK},
		{"editor reverts actor", "editor", revert, codes.OK},
		{"viewer cannot resume actor", "viewer", resume, codes.PermissionDenied},
		{"viewer cannot suspend actor", "viewer", suspend, codes.PermissionDenied},
		{"viewer cannot revert actor", "viewer", revert, codes.PermissionDenied},
		{"editor of another atespace cannot resume actor", "outsider", resume, codes.PermissionDenied},
		{"global viewer cannot suspend actor", "global-viewer", suspend, codes.PermissionDenied},
		{"global owner suspends actor", "global-owner", suspend, codes.OK},
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
		{"global owner marks worker available", "global-owner", markAvailable, codes.OK},
		{"global owner marks worker unavailable", "global-owner", markUnavailable, codes.OK},
		{"global viewer cannot mark worker available", "global-viewer", markAvailable, codes.PermissionDenied},
		{"global viewer cannot mark worker unavailable", "global-viewer", markUnavailable, codes.PermissionDenied},
		{"atespace editor cannot mark worker available", "editor", markAvailable, codes.PermissionDenied},

		// Tags: viewers read, editors write, all within the owning atespace.
		{"editor creates tag", "editor", createTag, codes.OK},
		{"viewer cannot create tag", "viewer", createTag, codes.PermissionDenied},
		{"outsider cannot create tag in another atespace", "outsider", createTag, codes.PermissionDenied},
		{"outsider cannot tag another atespace's actor", "outsider", createCrossTag, codes.PermissionDenied},
		{"global owner creates tag", "global-owner", createTag, codes.OK},
		{"viewer gets tag", "viewer", getTag, codes.OK},
		{"global viewer gets tag", "global-viewer", getTag, codes.OK},
		{"outsider cannot get tag", "outsider", getTag, codes.PermissionDenied},
		{"viewer lists tags", "viewer", listTags, codes.OK},
		{"outsider cannot list tags", "outsider", listTags, codes.PermissionDenied},
		{"global viewer lists all tags", "global-viewer", listAllTags, codes.OK},
		{"atespace editor cannot list all tags", "editor", listAllTags, codes.PermissionDenied},
		{"editor updates tag", "editor", updateTag, codes.OK},
		{"editor deletes tag", "editor", deleteTag, codes.OK},
		{"viewer cannot update tag", "viewer", updateTag, codes.PermissionDenied},
		{"viewer cannot delete tag", "viewer", deleteTag, codes.PermissionDenied},
		{"outsider cannot delete tag", "outsider", deleteTag, codes.PermissionDenied},
		{"bootstrap owner deletes tag", "bootstrap-owner", deleteTag, codes.OK},

		// Cloning from a tag needs editor on the tag's atespace, whatever its
		// scope. An actor created without an explicit tag only needs its template.
		{"editor clones tag", "editor", cloneInTeamA, codes.OK},
		{"viewer cannot clone tag", "viewer", cloneInTeamA, codes.PermissionDenied},
		{"editor of another atespace cannot clone tag", "outsider", cloneInTeamB, codes.PermissionDenied},
		{"viewer of tag's atespace cannot clone tag into another atespace", "outsider-with-viewer", cloneInTeamB, codes.PermissionDenied},
		{"global owner clones tag into another atespace", "global-owner", cloneInTeamB, codes.OK},
		{"bootstrap owner clones tag", "bootstrap-owner", cloneInTeamA, codes.OK},
		{"editor creates actor without a tag", "editor", createWithoutTag, codes.OK},

		// Only the egress gateway mints actor credentials; not even owners.
		{"editor cannot mint actor JWT", "editor", mintJWT, codes.PermissionDenied},
		{"global owner cannot mint actor JWT", "global-owner", mintJWT, codes.PermissionDenied},
		{"bootstrap owner cannot mint actor certificate", "bootstrap-owner", mintCert, codes.PermissionDenied},
	}

	interceptor := UnaryServerInterceptor(authorizer)
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

func TestUnaryServerInterceptor_SystemPrincipals(t *testing.T) {
	ctx := context.Background()
	pool := startPostgres(t)
	fgaServer, err := NewOpenFGAServer(pool)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaServer.Close)
	const (
		controllerID = "spiffe://cluster.local/ns/ate-system/sa/ate-controller"
		egressID     = "spiffe://cluster.local/ns/ate-system/sa/atenet-egress"
		routerID     = "spiffe://cluster.local/ns/ate-system/sa/atenet-router"
		ateletID     = "spiffe://cluster.local/ns/ate-system/sa/atelet"
		pluginID     = "spiffe://cluster.local/ns/ate-system/sa/ate-api-server"
		otherID      = "spiffe://cluster.local/ns/ate-system/sa/other"
	)
	authorizer, _, err := New(ctx, pool, fgaServer, nil, SystemPrincipals{
		ControllerSPIFFEID:       controllerID,
		EgressGatewaySPIFFEID:    egressID,
		IngressRouterSPIFFEID:    routerID,
		AteletSPIFFEID:           ateletID,
		APIServerSidecarSPIFFEID: pluginID,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	type rpcCall struct {
		fullMethod string
		req        any
	}
	actor := &ateapi.ObjectRef{Atespace: "team-a", Name: "runner"}
	worker := &ateapi.ObjectRef{Name: "w-1"}
	getActor := rpcCall{ateapi.Control_GetActor_FullMethodName, &ateapi.GetActorRequest{Actor: actor}}
	deleteActor := rpcCall{ateapi.Control_DeleteActor_FullMethodName, &ateapi.DeleteActorRequest{Actor: actor}}
	listAllActors := rpcCall{ateapi.Control_ListActors_FullMethodName, &ateapi.ListActorsRequest{}}
	listAtespaces := rpcCall{ateapi.Control_ListAtespaces_FullMethodName, &ateapi.ListAtespacesRequest{}}
	resume := rpcCall{ateapi.Control_ResumeActor_FullMethodName, &ateapi.ResumeActorRequest{Actor: actor}}
	suspend := rpcCall{ateapi.Control_SuspendActor_FullMethodName, &ateapi.SuspendActorRequest{Actor: actor}}
	getEgress := rpcCall{ateapi.Control_GetActorEgressPolicy_FullMethodName, &ateapi.GetActorEgressPolicyRequest{Actor: actor}}
	updateEgress := rpcCall{ateapi.Control_UpdateActorEgressPolicy_FullMethodName, &ateapi.UpdateActorEgressPolicyRequest{Actor: actor}}
	createWorker := rpcCall{ateapi.Control_CreateWorker_FullMethodName, &ateapi.CreateWorkerRequest{}}
	listWorkers := rpcCall{ateapi.Control_ListWorkers_FullMethodName, &ateapi.ListWorkersRequest{}}
	getWorker := rpcCall{ateapi.Control_GetWorker_FullMethodName, &ateapi.GetWorkerRequest{Worker: worker}}
	updateWorker := rpcCall{ateapi.Control_UpdateWorker_FullMethodName, &ateapi.UpdateWorkerRequest{Worker: &ateapi.Worker{
		Metadata: &ateapi.ResourceMetadata{Name: worker.GetName()},
	}}}
	deleteWorker := rpcCall{ateapi.Control_DeleteWorker_FullMethodName, &ateapi.DeleteWorkerRequest{Worker: worker}}
	drainWorker := rpcCall{ateapi.Control_DrainWorker_FullMethodName, &ateapi.DrainWorkerRequest{Worker: worker}}
	markAvailable := rpcCall{ateapi.Control_MarkWorkerAvailable_FullMethodName, &ateapi.MarkWorkerAvailableRequest{Worker: worker}}
	markUnavailable := rpcCall{ateapi.Control_MarkWorkerUnavailable_FullMethodName, &ateapi.MarkWorkerUnavailableRequest{Worker: worker}}
	listAssignments := rpcCall{ateapi.Control_ListWorkerActorAssignments_FullMethodName, &ateapi.ListWorkerActorAssignmentsRequest{Worker: worker}}
	mintJWT := rpcCall{ateapi.Control_MintActorJwt_FullMethodName, &ateapi.MintActorJwtRequest{Actor: actor}}
	mintCert := rpcCall{ateapi.Control_MintActorCertificate_FullMethodName, &ateapi.MintActorCertificateRequest{Actor: actor}}

	mtls := func(id string) principal.PrincipalInfo {
		return principal.PrincipalInfo{ID: id, Kind: principal.KindMTLS}
	}
	jwt := func(id string) principal.PrincipalInfo {
		return principal.PrincipalInfo{ID: id, Kind: principal.KindJWT}
	}

	tests := []struct {
		name      string
		principal principal.PrincipalInfo
		call      rpcCall
		wantCode  codes.Code
	}{
		// ate-controller manages workers and nothing else.
		{"controller creates worker", mtls(controllerID), createWorker, codes.OK},
		{"controller lists workers", mtls(controllerID), listWorkers, codes.OK},
		{"controller gets worker", mtls(controllerID), getWorker, codes.OK},
		{"controller updates worker", mtls(controllerID), updateWorker, codes.OK},
		{"controller deletes worker", mtls(controllerID), deleteWorker, codes.OK},
		{"controller drains worker", mtls(controllerID), drainWorker, codes.OK},
		{"controller marks worker available", mtls(controllerID), markAvailable, codes.OK},
		{"controller marks worker unavailable", mtls(controllerID), markUnavailable, codes.OK},
		{"controller cannot list worker assignments", mtls(controllerID), listAssignments, codes.PermissionDenied},
		{"controller cannot get actor", mtls(controllerID), getActor, codes.PermissionDenied},
		{"controller cannot resume actor", mtls(controllerID), resume, codes.PermissionDenied},
		{"controller cannot list atespaces", mtls(controllerID), listAtespaces, codes.PermissionDenied},

		// atenet-egress reads actors and their egress policies.
		{"egress gateway gets actor", mtls(egressID), getActor, codes.OK},
		{"egress gateway gets egress policy", mtls(egressID), getEgress, codes.OK},
		{"egress gateway cannot update egress policy", mtls(egressID), updateEgress, codes.PermissionDenied},
		{"egress gateway cannot delete actor", mtls(egressID), deleteActor, codes.PermissionDenied},
		{"egress gateway cannot resume actor", mtls(egressID), resume, codes.PermissionDenied},
		{"egress gateway lists actors", mtls(egressID), listAllActors, codes.OK},
		{"egress gateway cannot get worker", mtls(egressID), getWorker, codes.PermissionDenied},
		{"egress gateway mints actor JWT", mtls(egressID), mintJWT, codes.OK},
		{"egress gateway mints actor certificate", mtls(egressID), mintCert, codes.OK},

		// atenet-router resumes actors. Both it and atenet-egress list actors
		// as their ateapi health check.
		{"ingress router resumes actor", mtls(routerID), resume, codes.OK},
		{"ingress router lists actors", mtls(routerID), listAllActors, codes.OK},
		{"ingress router cannot get actor", mtls(routerID), getActor, codes.PermissionDenied},
		{"ingress router cannot suspend actor", mtls(routerID), suspend, codes.PermissionDenied},
		{"ingress router cannot delete actor", mtls(routerID), deleteActor, codes.PermissionDenied},
		{"ingress router cannot get egress policy", mtls(routerID), getEgress, codes.PermissionDenied},
		{"ingress router cannot create worker", mtls(routerID), createWorker, codes.PermissionDenied},
		{"ingress router cannot mint actor JWT", mtls(routerID), mintJWT, codes.PermissionDenied},
		{"controller cannot mint actor certificate", mtls(controllerID), mintCert, codes.PermissionDenied},

		// atelet reads egress policies for the worker that enforces them, and
		// nothing else.
		{"atelet gets egress policy", mtls(ateletID), getEgress, codes.OK},
		{"atelet cannot update egress policy", mtls(ateletID), updateEgress, codes.PermissionDenied},
		{"atelet cannot get actor", mtls(ateletID), getActor, codes.PermissionDenied},
		{"atelet cannot list actors", mtls(ateletID), listAllActors, codes.PermissionDenied},
		{"atelet cannot resume actor", mtls(ateletID), resume, codes.PermissionDenied},
		{"atelet cannot mint actor JWT", mtls(ateletID), mintJWT, codes.PermissionDenied},
		{"atelet cannot get worker", mtls(ateletID), getWorker, codes.PermissionDenied},
		// The objectstore control plugin reads actors to scope their storage
		// credentials, and nothing else.
		{"api server sidecar gets actor", mtls(pluginID), getActor, codes.OK},
		{"api server sidecar cannot delete actor", mtls(pluginID), deleteActor, codes.PermissionDenied},
		{"api server sidecar cannot list actors", mtls(pluginID), listAllActors, codes.PermissionDenied},
		{"api server sidecar cannot get egress policy", mtls(pluginID), getEgress, codes.PermissionDenied},
		{"api server sidecar cannot resume actor", mtls(pluginID), resume, codes.PermissionDenied},
		{"api server sidecar cannot mint actor JWT", mtls(pluginID), mintJWT, codes.PermissionDenied},
		{"api server sidecar cannot get worker", mtls(pluginID), getWorker, codes.PermissionDenied},

		// A bearer token whose subject is a granted SPIFFE ID gets nothing.
		{"jwt with controller ID cannot create worker", jwt(controllerID), createWorker, codes.PermissionDenied},
		{"jwt with egress ID cannot get actor", jwt(egressID), getActor, codes.PermissionDenied},
		{"jwt with egress ID cannot mint actor JWT", jwt(egressID), mintJWT, codes.PermissionDenied},
		{"jwt with router ID cannot resume actor", jwt(routerID), resume, codes.PermissionDenied},
		{"jwt with atelet ID cannot get egress policy", jwt(ateletID), getEgress, codes.PermissionDenied},
		{"jwt with api server sidecar ID cannot get actor", jwt(pluginID), getActor, codes.PermissionDenied},

		// Other SPIFFE IDs get nothing.
		{"other mTLS identity cannot resume actor", mtls(otherID), resume, codes.PermissionDenied},
		{"other mTLS identity cannot create worker", mtls(otherID), createWorker, codes.PermissionDenied},
	}

	interceptor := UnaryServerInterceptor(authorizer)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			userCtx := principal.InjectContext(ctx, tc.principal)
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
