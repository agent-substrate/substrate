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
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateUpdateActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.UpdateActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.UpdateActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.UpdateActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.UpdateActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.UpdateActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.UpdateActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.UpdateActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}, {
		"nil worker_selector",
		&ateapipb.UpdateActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}, WorkerSelector: nil},
		nil,
	}, {
		"valid worker_selector",
		&ateapipb.UpdateActorRequest{
			Actor:          &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
			WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "1"}},
		},
		nil,
	}, {
		"invalid worker_selector label key",
		&ateapipb.UpdateActorRequest{
			Actor:          &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
			WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"bad key!": "1"}},
		},
		field.ErrorList{field.Invalid(field.NewPath("worker_selector", "match_labels").Key("bad key!"), "bad key!", "")},
	}, {
		"invalid worker_selector label value",
		&ateapipb.UpdateActorRequest{
			Actor:          &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
			WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "not valid!"}},
		},
		field.ErrorList{field.Invalid(field.NewPath("worker_selector", "match_labels").Key("tier"), "not valid!", "")},
	}, {
		"too many worker_selector.match_labels",
		&ateapipb.UpdateActorRequest{
			Actor:          &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
			WorkerSelector: &ateapipb.Selector{MatchLabels: selectorLabelsOfSize(11)},
		},
		field.ErrorList{field.TooMany(field.NewPath("worker_selector", "match_labels"), 11, 10)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateUpdateActorRequest(tt.req), tt.want)
		})
	}
}
