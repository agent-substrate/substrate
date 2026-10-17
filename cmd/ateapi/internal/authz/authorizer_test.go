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
	"testing"

	"github.com/google/go-cmp/cmp"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
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
