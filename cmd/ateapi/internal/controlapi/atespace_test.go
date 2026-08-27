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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
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
