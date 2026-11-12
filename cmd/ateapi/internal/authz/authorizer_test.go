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

package authz

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"go.ate.dev/substrate/internal/principal"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestContextualTuples(t *testing.T) {
	parentGlobal := func(atespace string) *openfgav1.TupleKey {
		return &openfgav1.TupleKey{User: GlobalRootObject, Relation: "parent_global", Object: atespace}
	}
	parentAtespace := func(atespace, object string) *openfgav1.TupleKey {
		return &openfgav1.TupleKey{User: atespace, Relation: "parent_atespace", Object: object}
	}

	tests := []struct {
		name   string
		object string
		want   []*openfgav1.TupleKey
	}{
		{
			name:   "global has no parent",
			object: GlobalRootObject,
			want:   nil,
		},
		{
			name:   "atespace",
			object: AtespaceObject("team-a"),
			want:   []*openfgav1.TupleKey{parentGlobal("atespace:team-a")},
		},
		{
			name:   "worker",
			object: WorkerObject("w-1"),
			want:   []*openfgav1.TupleKey{parentGlobal("worker:w-1")},
		},
		{
			name:   "actor",
			object: ActorObject("team-a", "runner"),
			want: []*openfgav1.TupleKey{
				parentAtespace("atespace:team-a", "actor:team-a/runner"),
				parentGlobal("atespace:team-a"),
			},
		},
		{
			name:   "actor template",
			object: ActorTemplateObject("shared", "tmpl"),
			want: []*openfgav1.TupleKey{
				parentAtespace("atespace:shared", "actor_template:shared/tmpl"),
				parentGlobal("atespace:shared"),
			},
		},
		{
			name:   "parent of an escaped actor is the atespace's own object",
			object: ActorObject("a/b", "c"),
			want: []*openfgav1.TupleKey{
				parentAtespace(AtespaceObject("a/b"), ActorObject("a/b", "c")),
				parentGlobal(AtespaceObject("a/b")),
			},
		},
		{
			name:   "actor without an atespace separator has no parent",
			object: "actor:runner",
			want:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := contextualTuples(tc.object)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("contextualTuples(%q) mismatch (-want +got):\n%s", tc.object, diff)
			}
		})
	}
}

func TestCanShortCircuitInGo(t *testing.T) {
	a := &Authorizer{
		pureGoShortCircuit: true,
		bootstrapOwners: map[string]struct{}{
			"user:bootstrap-admin": {},
		},
		systemPrincipals: map[string][]string{
			formatUser("spiffe://cluster.local/ns/ate-system/sa/atenet-router"): {RoleIngressRouter},
		},
	}

	// Verify all 44 allowed relations across all structural object types
	for objType, relations := range bootstrapOwnerAllowedRelations {
		var sampleObj string
		switch objType {
		case "global":
			sampleObj = GlobalRootObject
		case "atespace":
			sampleObj = "atespace:team-a"
		case "worker":
			sampleObj = "worker:w-1"
		case "actor_template":
			sampleObj = "actor_template:team-a/tmpl-1"
		case "tag":
			sampleObj = "tag:team-a/tag-1"
		case "actor":
			sampleObj = "actor:team-a/env-1"
		}
		for rel := range relations {
			if !a.canShortCircuitInGo(principal.KindJWT, "user:bootstrap-admin", rel, sampleObj) {
				t.Errorf("expected bootstrap owner to short circuit %s on %s (%s)", rel, sampleObj, objType)
			}
		}
	}

	// Bootstrap admin CANNOT short-circuit mint relations
	if a.canShortCircuitInGo(principal.KindJWT, "user:bootstrap-admin", "can_mint_actor_jwt", "actor:team-a/env-1") {
		t.Errorf("expected bootstrap owner NOT to short circuit can_mint_actor_jwt")
	}
	if a.canShortCircuitInGo(principal.KindJWT, "user:bootstrap-admin", "can_mint_worker_actor_credential", "actor:team-a/env-1") {
		t.Errorf("expected bootstrap owner NOT to short circuit can_mint_worker_actor_credential")
	}

	// Disallowed/malformed resource names fail closed
	if a.canShortCircuitInGo(principal.KindJWT, "user:bootstrap-admin", "can_resume", "actor:team-a/../escaped") {
		t.Errorf("expected malformed actor name to fail closed")
	}
	if a.canShortCircuitInGo(principal.KindJWT, "user:bootstrap-admin", "can_get", "global:not-root") {
		t.Errorf("expected non-root global object to fail closed")
	}

	// Ingress router can short-circuit can_resume on actor and can_list_actors on global
	routerUser := formatUser("spiffe://cluster.local/ns/ate-system/sa/atenet-router")
	if !a.canShortCircuitInGo(principal.KindMTLS, routerUser, RelationCanResume, "actor:team-a/env-1") {
		t.Errorf("expected ingress router to short circuit can_resume on actor")
	}
	if !a.canShortCircuitInGo(principal.KindMTLS, routerUser, RelationCanListActors, GlobalRootObject) {
		t.Errorf("expected ingress router to short circuit can_list_actors on global:root")
	}
	// Ingress router cannot short-circuit other relations or non-actor objects
	if a.canShortCircuitInGo(principal.KindMTLS, routerUser, "can_delete", "actor:team-a/env-1") {
		t.Errorf("expected ingress router NOT to short circuit can_delete")
	}
	if a.canShortCircuitInGo(principal.KindMTLS, routerUser, RelationCanResume, "worker:w-1") {
		t.Errorf("expected ingress router NOT to short circuit can_resume on worker")
	}

	// When pureGoShortCircuit is disabled, all return false
	disabled := &Authorizer{
		pureGoShortCircuit: false,
		bootstrapOwners:    a.bootstrapOwners,
		systemPrincipals:   a.systemPrincipals,
	}
	if disabled.canShortCircuitInGo(principal.KindJWT, "user:bootstrap-admin", "can_resume", "actor:team-a/env-1") {
		t.Errorf("expected short-circuit to be disabled when pureGoShortCircuit is false")
	}
}

func TestAuthorizerShortCircuitExactParityWithModelFGA(t *testing.T) {
	ctx := context.Background()
	fgaSrv, storeID, modelID := newInMemoryFGAServer(t)

	bootstrapUser := formatUser("bootstrap-admin")
	routerUser := formatUser("spiffe://cluster.local/ns/ate-system/sa/atenet-router")
	controllerUser := formatUser("spiffe://cluster.local/ns/ate-system/sa/ate-controller")

	a := &Authorizer{
		fgaServer:          fgaSrv,
		storeID:            storeID,
		modelID:            modelID,
		pureGoShortCircuit: true,
		bootstrapOwners: map[string]struct{}{
			bootstrapUser: {},
		},
		systemPrincipals: map[string][]string{
			routerUser:     {RoleIngressRouter},
			controllerUser: {RoleController},
		},
	}

	modelResp, err := fgaSrv.ReadAuthorizationModel(ctx, &openfgav1.ReadAuthorizationModelRequest{
		StoreId: storeID,
		Id:      modelID,
	})
	if err != nil {
		t.Fatalf("ReadAuthorizationModel: %v", err)
	}

	sampleObjects := map[string]string{
		"global":         GlobalRootObject,
		"atespace":       "atespace:team-a",
		"worker":         "worker:w-1",
		"actor":          "actor:team-a/env-1",
		"actor_template": "actor_template:team-a/tmpl-1",
		"tag":            "tag:team-a/tag-1",
	}

	principals := []struct {
		kind string
		user string
	}{
		{principal.KindJWT, bootstrapUser},
		{principal.KindMTLS, bootstrapUser},
		{principal.KindMTLS, routerUser},
		{principal.KindJWT, routerUser}, // mTLS-only system principal over JWT must not get mTLS role
		{principal.KindMTLS, controllerUser},
		{principal.KindJWT, "user:regular-user"},
	}

	// Verify soundness (canShortCircuitInGo => OpenFGA allows) across every type
	// and relation defined in model.fga, and exact equivalence on bootstrapOwner
	// and ingress_router roles.
	for _, td := range modelResp.GetAuthorizationModel().GetTypeDefinitions() {
		obj, hasSample := sampleObjects[td.GetType()]
		if !hasSample {
			continue
		}
		for rel := range td.GetRelations() {
			for _, p := range principals {
				goAllowed := a.canShortCircuitInGo(p.kind, p.user, rel, obj)

				var grants []*openfgav1.TupleKey
				if p.kind == principal.KindMTLS {
					for _, role := range a.systemPrincipals[p.user] {
						grants = append(grants, &openfgav1.TupleKey{
							User:     p.user,
							Relation: role,
							Object:   GlobalRootObject,
						})
					}
				}
				fgaAllowed, err := a.checkRaw(ctx, p.user, rel, obj, grants...)
				if err != nil {
					t.Fatalf("checkRaw(%s, %s, %s): %v", p.user, rel, obj, err)
				}

				// Soundness invariant: pure-Go short-circuit must NEVER allow a
				// check that OpenFGA denies (0 fail-open).
				if goAllowed && !fgaAllowed {
					t.Errorf("FAIL-OPEN for (%s, %s, %s, %s): goAllowed=true, fgaAllowed=false",
						p.kind, p.user, rel, obj)
				}

				// Completeness for can_* permission relations on bootstrap owners
				// and ingress_router mTLS calls.
				if p.user == bootstrapUser && strings.HasPrefix(rel, "can_") && goAllowed != fgaAllowed {
					t.Errorf("bootstrap parity mismatch for (%s, %s, %s): goAllowed=%v, fgaAllowed=%v",
						p.kind, rel, obj, goAllowed, fgaAllowed)
				}
				if p.kind == principal.KindMTLS && p.user == routerUser && strings.HasPrefix(rel, "can_") && goAllowed != fgaAllowed {
					t.Errorf("ingress_router parity mismatch for (%s, %s): goAllowed=%v, fgaAllowed=%v",
						rel, obj, goAllowed, fgaAllowed)
				}
			}
		}
	}
}
