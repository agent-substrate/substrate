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
	"testing"

	"github.com/google/go-cmp/cmp"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/transformer"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/principal"
	"google.golang.org/grpc/codes"
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

func TestValidateStructuralObject(t *testing.T) {
	// want is the object type, or "" if the object must be rejected.
	tests := []struct {
		object string
		want   string
	}{
		{GlobalRootObject, "global"},
		{AtespaceObject("team-a"), "atespace"},
		{WorkerObject("w-1"), "worker"},
		{ActorObject("team-a", "env-1"), "actor"},
		{ActorTemplateObject("team-a", "tmpl-1"), "actor_template"},
		{TagObject("team-a", "tag-1"), "tag"},
		{"", ""},
		{"global:", ""},
		{"global:not-root", ""},
		{"atespace:team-a/extra", ""},
		{"worker:UPPER", ""},
		{"actor:team-a", ""},
		{"actor:/env-1", ""},
		{"actor:team-a/env-1/extra", ""},
		// An escaped name is not a valid resource name, so it has no
		// short-circuit and OpenFGA decides.
		{ActorObject("team/a", "env-1"), ""},
		{"node:node-1", ""},
	}

	for _, tc := range tests {
		got, ok := validateStructuralObject(tc.object)
		if got != tc.want || ok != (tc.want != "") {
			t.Errorf("validateStructuralObject(%q) = (%q, %v), want (%q, %v)", tc.object, got, ok, tc.want, tc.want != "")
		}
	}
}

// TestCompileShortCircuitTable checks the rewrites model.fga does not use
// today, which TestAuthorizerShortCircuit therefore cannot reach.
func TestCompileShortCircuitTable(t *testing.T) {
	model, err := transformer.TransformDSLToProto(`model
  schema 1.1
type user
type global
  relations
    define owner: [user]
    define controller: [user with unexpired]
    define blocked: [user]
    define computed: owner
    define excluded: owner but not blocked
    define intersected: owner and computed
    define excluded_or_computed: (owner but not blocked) or computed
    define conditional: controller
    define cycle_a: cycle_b or owner
    define cycle_b: cycle_a
    define dead_a: dead_b
    define dead_b: dead_a
type atespace
  relations
    define parent_global: [global with enabled]
    define conditional_parent: owner from parent_global
condition unexpired(now: timestamp) {
  now < timestamp("2099-01-01T00:00:00Z")
}
condition enabled(on: bool) {
  on
}
`)
	if err != nil {
		t.Fatalf("TransformDSLToProto: %v", err)
	}
	table := compileShortCircuitTable(model)

	tests := []struct {
		role       string
		objectType string
		relation   string
		want       bool
	}{
		{RoleOwner, "global", "owner", true},
		{RoleOwner, "global", "computed", true},
		{RoleOwner, "global", "excluded", false},
		{RoleOwner, "global", "intersected", false},
		{RoleOwner, "global", "excluded_or_computed", true},
		{RoleController, "global", "controller", false},
		{RoleController, "global", "conditional", false},
		{RoleOwner, "global", "cycle_a", true},
		{RoleOwner, "global", "cycle_b", true},
		{RoleOwner, "global", "dead_a", false},
		{RoleOwner, "atespace", "conditional_parent", false},
		{RoleIngressRouter, "global", "owner", false},
	}
	for _, tc := range tests {
		if got := table[tc.role][objectTypeRel{tc.objectType, tc.relation}]; got != tc.want {
			t.Errorf("compileShortCircuitTable()[%q][%s#%s] = %v, want %v", tc.role, tc.objectType, tc.relation, got, tc.want)
		}
	}
}

const testBootstrapOwner = "bootstrap-admin@example.com"

// newShortCircuitTestAuthorizers returns an Authorizer over an empty tuple
// store with testBootstrapOwner and testSystemPrincipals configured, and a
// copy of it that sends every check to OpenFGA.
func newShortCircuitTestAuthorizers(t *testing.T) (a, openFGAOnly *Authorizer) {
	t.Helper()
	pool := startPostgres(t)
	fgaServer, err := NewOpenFGAServer(pool)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaServer.Close)
	a, _, err = New(t.Context(), pool, fgaServer, []string{testBootstrapOwner}, testSystemPrincipals())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	withoutTable := *a
	withoutTable.shortCircuit = nil
	return a, &withoutTable
}

// TestAuthorizerShortCircuit checks, for every relation in model.fga, that
// Check short-circuits exactly the checks OpenFGA allows from contextual
// tuples alone.
func TestAuthorizerShortCircuit(t *testing.T) {
	a, openFGAOnly := newShortCircuitTestAuthorizers(t)

	system := testSystemPrincipals()
	const dualRoleID = "spiffe://cluster.local/ns/ate-system/sa/router-and-egress"
	a.systemPrincipals[formatUser(dualRoleID)] = []string{RoleIngressRouter, RoleEgressGateway}
	ids := []string{
		testBootstrapOwner,
		system.ControllerSPIFFEID,
		system.EgressGatewaySPIFFEIDs[0],
		system.IngressRouterSPIFFEID,
		system.AteletSPIFFEID,
		system.APIServerSidecarSPIFFEID,
		dualRoleID,
		"regular-user",
	}
	objects := map[string]string{
		"global":         GlobalRootObject,
		"atespace":       AtespaceObject("team-a"),
		"worker":         WorkerObject("w-1"),
		"actor":          ActorObject("team-a", "env-1"),
		"actor_template": ActorTemplateObject("team-a", "tmpl-1"),
		"tag":            TagObject("team-a", "tag-1"),
	}
	model, err := transformer.TransformDSLToProto(modelDSL)
	if err != nil {
		t.Fatalf("TransformDSLToProto(model.fga): %v", err)
	}

	shortCircuited := 0
	for _, td := range model.GetTypeDefinitions() {
		object, ok := objects[td.GetType()]
		if !ok {
			continue
		}
		for relation := range td.GetRelations() {
			for _, id := range ids {
				for _, kind := range []string{principal.KindJWT, principal.KindMTLS} {
					ctx := principal.InjectContext(t.Context(), principal.PrincipalInfo{ID: id, Kind: kind})
					err := openFGAOnly.Check(ctx, relation, object)
					if err != nil && apierror.Code(err) != codes.PermissionDenied {
						t.Fatalf("OpenFGA Check(%s %q, %q, %q): %v", kind, id, relation, object, err)
					}
					got := a.canShortCircuitInGo(kind, formatUser(id), relation, object)
					if want := err == nil; got != want {
						t.Errorf("canShortCircuitInGo(%q, %q, %q, %q) = %v, but OpenFGA allowed = %v", kind, id, relation, object, got, want)
					}
					if got {
						shortCircuited++
					}
				}
			}
		}
	}
	if shortCircuited == 0 {
		t.Error("no check was short-circuited")
	}

	// A canceled request must still fail as it does when OpenFGA evaluates it.
	ctx, cancel := context.WithCancel(principal.InjectContext(t.Context(), principal.PrincipalInfo{ID: testBootstrapOwner, Kind: principal.KindJWT}))
	cancel()
	if err := a.Check(ctx, RelationCanResume, objects["actor"]); err == nil {
		t.Errorf("Check(canceled ctx, %q, %q) = nil, want an error", RelationCanResume, objects["actor"])
	}
}
