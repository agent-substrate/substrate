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

package apivalidation

import (
	"context"
	"strings"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateCreateAtespaceRequest(t *testing.T) {
	// This test verifies validation of user input for creation.
	validReq := func(atespace *ateapi.Atespace, mods ...func(atespace *ateapi.CreateAtespaceRequest)) *ateapi.CreateAtespaceRequest {
		req := &ateapi.CreateAtespaceRequest{
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
		req  *ateapi.CreateAtespaceRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(validAtespace()),
		nil,
	}, {
		"missing atespace",
		&ateapi.CreateAtespaceRequest{Atespace: nil},
		field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		"missing atespace.metadata",
		validReq(validAtespace(func(a *ateapi.Atespace) { a.Metadata = nil })),
		field.ErrorList{field.Required(field.NewPath("atespace", "metadata"), "")},
	}, {
		"atespace.metadata.atespace must be empty",
		validReq(validAtespace(withMetadata(func(m *ateapi.ResourceMetadata) { m.Atespace = "as" }))),
		field.ErrorList{field.Forbidden(field.NewPath("atespace", "metadata", "atespace"), "")},
	}, {
		"missing atespace.metadata.name",
		validReq(validAtespace(withMetadata(func(m *ateapi.ResourceMetadata) { m.Name = "" }))),
		field.ErrorList{field.Required(field.NewPath("atespace", "metadata", "name"), "")},
	}, {
		"invalid metadata.name",
		validReq(validAtespace(withMetadata(func(m *ateapi.ResourceMetadata) { m.Name = "invalid value" }))),
		field.ErrorList{field.Invalid(field.NewPath("atespace", "metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateCreateAtespaceRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateGetAtespaceRequest(t *testing.T) {
	// This test verifies validation of user input for get.
	validReq := func(mods ...func(atespace *ateapi.GetAtespaceRequest)) *ateapi.GetAtespaceRequest {
		req := &ateapi.GetAtespaceRequest{
			Atespace: &ateapi.ObjectRef{Name: "team1"},
		}
		for _, m := range mods {
			m(req)
		}
		return req
	}

	tests := []struct {
		name string
		req  *ateapi.GetAtespaceRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(),
		nil,
	}, {
		"missing atespace",
		&ateapi.GetAtespaceRequest{},
		field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		"atespace.atespace must be empty",
		validReq(func(r *ateapi.GetAtespaceRequest) { r.Atespace.Atespace = "as" }),
		field.ErrorList{field.Forbidden(field.NewPath("atespace", "atespace"), "")},
	}, {
		"missing atespace.name",
		validReq(func(r *ateapi.GetAtespaceRequest) { r.Atespace.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("atespace", "name"), "")},
	}, {
		"invalid atespace.name",
		validReq(func(r *ateapi.GetAtespaceRequest) { r.Atespace.Name = "invalid value" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateGetAtespaceRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateListAtespacesRequest(t *testing.T) {
	// This test verifies validation of user input for list.
	validReq := func(mods ...func(atespace *ateapi.ListAtespacesRequest)) *ateapi.ListAtespacesRequest {
		req := &ateapi.ListAtespacesRequest{ /* default values */ }
		for _, m := range mods {
			m(req)
		}
		return req
	}

	tests := []struct {
		name string
		req  *ateapi.ListAtespacesRequest
		want field.ErrorList
	}{{
		"valid, no page_size",
		validReq(),
		nil,
	}, {
		"valid, positive page_size",
		validReq(func(r *ateapi.ListAtespacesRequest) { r.Options = &ateapi.ListOptions{PageSize: 10} }),
		nil,
	}, {
		"negative page_size",
		validReq(func(r *ateapi.ListAtespacesRequest) { r.Options = &ateapi.ListOptions{PageSize: -1} }),
		field.ErrorList{field.Invalid(field.NewPath("options", "page_size"), int32(-1), "").WithOrigin("minimum")},
	}, {
		"valid page_token",
		validReq(func(r *ateapi.ListAtespacesRequest) {
			r.Options = &ateapi.ListOptions{PageToken: strings.Repeat("x", 256)}
		}),
		nil,
	}, {
		"too-large page_token",
		validReq(func(r *ateapi.ListAtespacesRequest) {
			r.Options = &ateapi.ListOptions{PageToken: strings.Repeat("x", 257)}
		}),
		field.ErrorList{field.TooLongCharacters(field.NewPath("options", "page_token"), "", 256).WithOrigin("maxLength")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateListAtespacesRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateDeleteAtespaceRequest(t *testing.T) {
	// This test verifies validation of user input for delete.
	validReq := func(mods ...func(atespace *ateapi.DeleteAtespaceRequest)) *ateapi.DeleteAtespaceRequest {
		req := &ateapi.DeleteAtespaceRequest{
			Atespace: &ateapi.ObjectRef{Name: "team1"},
		}
		for _, m := range mods {
			m(req)
		}
		return req
	}

	tests := []struct {
		name string
		req  *ateapi.DeleteAtespaceRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(),
		nil,
	}, {
		"missing atespace",
		&ateapi.DeleteAtespaceRequest{Atespace: nil},
		field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		"atespace.atespace must be empty",
		validReq(func(r *ateapi.DeleteAtespaceRequest) { r.Atespace.Atespace = "as" }),
		field.ErrorList{field.Forbidden(field.NewPath("atespace", "atespace"), "")},
	}, {
		"missing atespace.name",
		validReq(func(r *ateapi.DeleteAtespaceRequest) { r.Atespace.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("atespace", "name"), "")},
	}, {
		"invalid atespace.name",
		validReq(func(r *ateapi.DeleteAtespaceRequest) { r.Atespace.Name = "invalid value" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateDeleteAtespaceRequest(context.Background(), tt.req), tt.want)
		})
	}
}

// validAtespace returns a minimal Atespace which should pass input validation.
func validAtespace(mods ...func(*ateapi.Atespace)) *ateapi.Atespace {
	a := &ateapi.Atespace{
		Metadata: &ateapi.ResourceMetadata{Name: "team1"},
	}
	for _, m := range mods {
		m(a)
	}
	return a
}

// withAtespaceMetadata returns a modifier func (see validAtespace) which sets
// the atespace's resource metadata to a valid value.
func withAtespaceMetadata(mutate func(*ateapi.ResourceMetadata)) func(*ateapi.Atespace) {
	return func(a *ateapi.Atespace) { mutate(a.Metadata) }
}
