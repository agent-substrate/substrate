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

func TestValidateCreateAtespaceRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.CreateAtespaceRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}}},
		nil,
	}, {
		"missing atespace",
		&ateapipb.CreateAtespaceRequest{},
		field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		"metadata.atespace must be empty",
		&ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "team-a"}}},
		field.ErrorList{field.Invalid(field.NewPath("atespace", "metadata", "atespace"), "ns1", "")},
	}, {
		"missing metadata.name",
		&ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: ""}}},
		field.ErrorList{field.Required(field.NewPath("atespace", "metadata", "name"), "")},
	}, {
		"invalid metadata.name",
		&ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "Team_A"}}},
		field.ErrorList{field.Invalid(field.NewPath("atespace", "metadata", "name"), "Team_A", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateCreateAtespaceRequest(tt.req), tt.want)
		})
	}
}

func TestValidateGetAtespaceRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.GetAtespaceRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team-a"}},
		nil,
	}, {
		"missing atespace",
		&ateapipb.GetAtespaceRequest{},
		field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		"atespace.atespace must be empty",
		&ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Atespace: "ns1", Name: "team-a"}},
		field.ErrorList{field.Invalid(field.NewPath("atespace", "atespace"), "ns1", "")},
	}, {
		"missing atespace.name",
		&ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{}},
		field.ErrorList{field.Required(field.NewPath("atespace", "name"), "")},
	}, {
		"invalid atespace.name",
		&ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "TEAM-A"}},
		field.ErrorList{field.Invalid(field.NewPath("atespace", "name"), "TEAM-A", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateGetAtespaceRequest(tt.req), tt.want)
		})
	}
}

func TestValidateListAtespacesRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.ListAtespacesRequest
		want field.ErrorList
	}{{
		"valid, no page_size",
		&ateapipb.ListAtespacesRequest{},
		nil,
	}, {
		"valid, positive page_size",
		&ateapipb.ListAtespacesRequest{PageSize: 10},
		nil,
	}, {
		"negative page_size",
		&ateapipb.ListAtespacesRequest{PageSize: -1},
		field.ErrorList{field.Invalid(field.NewPath("page_size"), int32(-1), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateListAtespacesRequest(tt.req), tt.want)
		})
	}
}

func TestValidateDeleteAtespaceRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.DeleteAtespaceRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team-a"}},
		nil,
	}, {
		"missing atespace",
		&ateapipb.DeleteAtespaceRequest{},
		field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		"atespace.atespace must be empty",
		&ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Atespace: "ns1", Name: "team-a"}},
		field.ErrorList{field.Invalid(field.NewPath("atespace", "atespace"), "ns1", "")},
	}, {
		"missing atespace.name",
		&ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{}},
		field.ErrorList{field.Required(field.NewPath("atespace", "name"), "")},
	}, {
		"invalid atespace.name",
		&ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "TEAM-A"}},
		field.ErrorList{field.Invalid(field.NewPath("atespace", "name"), "TEAM-A", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateDeleteAtespaceRequest(tt.req), tt.want)
		})
	}
}
