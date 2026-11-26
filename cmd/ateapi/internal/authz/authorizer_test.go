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
	"testing"

	"github.com/google/go-cmp/cmp"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/transformer"
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

func TestScopeCanonicalObject(t *testing.T) {
	tests := []struct {
		name      string
		relation  string
		object    string
		wantScope string
		wantExact bool
	}{
		{
			name:      "global root is exact",
			relation:  RelationCanCreateAtespace,
			object:    GlobalRootObject,
			wantScope: GlobalRootObject,
			wantExact: true,
		},
		{
			name:      "non-root global rejected",
			relation:  RelationCanCreateAtespace,
			object:    "global:other",
			wantScope: "",
			wantExact: false,
		},
		{
			name:      "atespace is exact",
			relation:  RelationCanGet,
			object:    AtespaceObject("team-a"),
			wantScope: "atespace:team-a",
			wantExact: true,
		},
		{
			name:      "escaped slash in atespace stays valid",
			relation:  RelationCanGet,
			object:    AtespaceObject("team/a"),
			wantScope: AtespaceObject("team/a"),
			wantExact: true,
		},
		{
			name:      "unescaped slash in atespace rejected",
			relation:  RelationCanGet,
			object:    "atespace:team/a",
			wantScope: "",
			wantExact: false,
		},
		{
			name:      "worker maps to probe",
			relation:  RelationCanGet,
			object:    WorkerObject("w-1"),
			wantScope: "worker:" + scopeProbeSuffix,
			wantExact: false,
		},
		{
			name:      "worker probe sentinel rejected",
			relation:  RelationCanGet,
			object:    "worker:" + scopeProbeSuffix,
			wantScope: "",
			wantExact: false,
		},
		{
			name:      "actor maps to atespace probe",
			relation:  RelationCanUpdate,
			object:    ActorObject("team-a", "sandbox-1"),
			wantScope: "actor:team-a/" + scopeProbeSuffix,
			wantExact: false,
		},
		{
			name:      "actor probe sentinel rejected",
			relation:  RelationCanUpdate,
			object:    "actor:team-a/" + scopeProbeSuffix,
			wantScope: "",
			wantExact: false,
		},
		{
			name:      "actor extra slash rejected",
			relation:  RelationCanUpdate,
			object:    "actor:team-a/sub/name",
			wantScope: "",
			wantExact: false,
		},
		{
			name:      "worker credential mint relation bypassed",
			relation:  "can_mint_worker_actor_credential",
			object:    ActorObject("team-a", "sandbox-1"),
			wantScope: "",
			wantExact: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotScope, gotExact := scopeCanonicalObject(tc.relation, tc.object)
			if gotScope != tc.wantScope || gotExact != tc.wantExact {
				t.Errorf("scopeCanonicalObject(%q, %q) = (%q, %v), want (%q, %v)",
					tc.relation, tc.object, gotScope, gotExact, tc.wantScope, tc.wantExact)
			}
		})
	}
}

// TestModelHasNoNonMonotonicRewrites guards scopeCanonicalObject's soundness:
// probing <type>:<atespace>/__ate_scope_probe__ implies permission on every
// sibling resource in <atespace> only while model.fga contains no Difference
// ("but not"), Intersection ("and"), or conditional tuple rules. If model.fga
// ever introduces one, affected relations must be exempted in
// scopeCanonicalObject before updating this test.
func TestModelHasNoNonMonotonicRewrites(t *testing.T) {
	model, err := transformer.TransformDSLToProto(modelDSL)
	if err != nil {
		t.Fatalf("TransformDSLToProto failed: %v", err)
	}
	if len(model.GetConditions()) > 0 {
		t.Fatalf("model.fga defines conditions %v; audit scopeCanonicalObject before allowing conditions", model.GetConditions())
	}
	var walk func(typeName, relName string, u *openfgav1.Userset)
	walk = func(typeName, relName string, u *openfgav1.Userset) {
		if u == nil {
			return
		}
		switch v := u.Userset.(type) {
		case *openfgav1.Userset_Difference:
			t.Errorf("%s#%s uses 'but not' (Difference); exempt it from scopeCanonicalObject", typeName, relName)
		case *openfgav1.Userset_Intersection:
			t.Errorf("%s#%s uses 'and' (Intersection); audit scopeCanonicalObject", typeName, relName)
		case *openfgav1.Userset_Union:
			for _, child := range v.Union.GetChild() {
				walk(typeName, relName, child)
			}
		}
	}
	for _, td := range model.GetTypeDefinitions() {
		for relName, userset := range td.GetRelations() {
			walk(td.GetType(), relName, userset)
		}
	}
}
