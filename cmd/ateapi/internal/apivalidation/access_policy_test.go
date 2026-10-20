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
	"fmt"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/authz"
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

var (
	accessPolicyPath = field.NewPath("access_policy")
	policyBindings   = accessPolicyPath.Child("bindings")
)

func validAccessPolicy(mutate ...func(*ateapi.AccessPolicy)) *ateapi.AccessPolicy {
	p := &ateapi.AccessPolicy{
		Metadata: &ateapi.ResourceMetadata{Name: "default"},
		Bindings: []*ateapi.Binding{
			{Role: authz.RoleOwner, Members: []string{"user:alice@example.com"}},
			{Role: authz.RoleViewer, Members: []string{"user:bob@example.com"}},
		},
	}
	for _, m := range mutate {
		m(p)
	}
	return p
}

// membersOfSize returns n distinct valid members.
func membersOfSize(n int) []string {
	members := make([]string, n)
	for i := range members {
		members[i] = fmt.Sprintf("user:u%d", i)
	}
	return members
}

func TestValidateCreateGlobalAccessPolicyRequest(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		req       *ateapi.CreateGlobalAccessPolicyRequest
		wantError field.ErrorList
	}{{
		name: "valid",
		req:  &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy()},
	}, {
		name: "valid without bindings",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings = nil
		})},
	}, {
		name:      "missing access_policy",
		req:       &ateapi.CreateGlobalAccessPolicyRequest{},
		wantError: field.ErrorList{field.Required(accessPolicyPath, "")},
	}, {
		name: "missing metadata",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Metadata = nil
		})},
		wantError: field.ErrorList{field.Required(accessPolicyPath.Child("metadata"), "")},
	}, {
		// A nil entry is rejected as a whole; validateBindings skips it.
		name: "nil binding",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings = append(p.Bindings, nil)
		})},
		wantError: field.ErrorList{field.Required(policyBindings.Index(2), "")},
	}, {
		name: "atespace set",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Metadata.Atespace = "team-a"
		})},
		wantError: field.ErrorList{field.Forbidden(accessPolicyPath.Child("metadata", "atespace"), "")},
	}, {
		name: "missing name",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Metadata.Name = ""
		})},
		wantError: field.ErrorList{field.Required(accessPolicyPath.Child("metadata", "name"), "")},
	}, {
		name: "name not default",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Metadata.Name = "other"
		})},
		wantError: field.ErrorList{field.Invalid(accessPolicyPath.Child("metadata", "name"), nil, "").WithOrigin("custom=default")},
	}, {
		// editor is an atespace-only role.
		name: "editor role",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[0].Role = authz.RoleEditor
		})},
		wantError: field.ErrorList{field.NotSupported[string](policyBindings.Index(0).Child("role"), "", nil)},
	}, {
		name: "unknown role",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[1].Role = "admin"
		})},
		wantError: field.ErrorList{field.NotSupported[string](policyBindings.Index(1).Child("role"), "", nil)},
	}, {
		// System component roles are granted only by ateapi's configuration.
		name: "system component role",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[1].Role = authz.RoleIngressRouter
		})},
		wantError: field.ErrorList{field.NotSupported[string](policyBindings.Index(1).Child("role"), "", nil)},
	}, {
		name: "duplicate role",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[1].Role = authz.RoleOwner
		})},
		wantError: field.ErrorList{field.Duplicate(policyBindings.Index(1).Child("role"), nil)},
	}, {
		name: "member without user prefix",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[0].Members = []string{"alice@example.com"}
		})},
		wantError: field.ErrorList{field.Invalid(policyBindings.Index(0).Child("members").Index(0), nil, "")},
	}, {
		name: "member with a control character",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[0].Members = []string{"user:alice\tbob"}
		})},
		wantError: field.ErrorList{field.Invalid(policyBindings.Index(0).Child("members").Index(0), nil, "")},
	}, {
		name: "wildcard member",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[1].Members = []string{"user:*"}
		})},
		wantError: field.ErrorList{field.Invalid(policyBindings.Index(1).Child("members").Index(0), nil, "")},
	}, {
		name: "members at the policy limit",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings = []*ateapi.Binding{{Role: authz.RoleViewer, Members: membersOfSize(maxMembersPerPolicy)}}
		})},
	}, {
		// Each binding is within its own limit; together they exceed the
		// policy's.
		name: "members over the policy limit across bindings",
		req: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings = []*ateapi.Binding{
				{Role: authz.RoleOwner, Members: membersOfSize(1)},
				{Role: authz.RoleViewer, Members: membersOfSize(maxMembersPerPolicy)},
			}
		})},
		wantError: field.ErrorList{field.TooMany(policyBindings, 0, 0)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateCreateGlobalAccessPolicyRequest(ctx, tt.req), tt.wantError)
		})
	}
}

func TestValidateGetGlobalAccessPolicyRequest(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		req       *ateapi.GetGlobalAccessPolicyRequest
		wantError field.ErrorList
	}{{
		name: "valid",
		req:  &ateapi.GetGlobalAccessPolicyRequest{Name: resources.SingletonName},
	}, {
		name:      "missing name",
		req:       &ateapi.GetGlobalAccessPolicyRequest{},
		wantError: field.ErrorList{field.Required(field.NewPath("name"), "")},
	}, {
		name:      "wrong name",
		req:       &ateapi.GetGlobalAccessPolicyRequest{Name: "other"},
		wantError: field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("custom=default")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateGetGlobalAccessPolicyRequest(ctx, tt.req), tt.wantError)
		})
	}
}

func TestValidateUpdateGlobalAccessPolicyRequest(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		req       *ateapi.UpdateGlobalAccessPolicyRequest
		wantError field.ErrorList
	}{{
		name: "valid",
		req:  &ateapi.UpdateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy()},
	}, {
		name:      "missing access_policy",
		req:       &ateapi.UpdateGlobalAccessPolicyRequest{},
		wantError: field.ErrorList{field.Required(accessPolicyPath, "")},
	}, {
		name: "editor role",
		req: &ateapi.UpdateGlobalAccessPolicyRequest{AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[0].Role = authz.RoleEditor
		})},
		wantError: field.ErrorList{field.NotSupported[string](policyBindings.Index(0).Child("role"), "", nil)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateUpdateGlobalAccessPolicyRequest(ctx, tt.req), tt.wantError)
		})
	}
}

func TestValidateGlobalAccessPolicyUpdate(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		newVal    *ateapi.AccessPolicy
		wantError field.ErrorList
	}{{
		name:   "valid",
		newVal: validAccessPolicy(),
	}, {
		name: "editor role",
		newVal: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[0].Role = authz.RoleEditor
		}),
		wantError: field.ErrorList{field.NotSupported[string](policyBindings.Index(0).Child("role"), "", nil)},
	}, {
		name: "invalid member",
		newVal: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[1].Members = []string{"user:"}
		}),
		wantError: field.ErrorList{field.Invalid(policyBindings.Index(1).Child("members").Index(0), nil, "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateGlobalAccessPolicyUpdate(ctx, accessPolicyPath, tt.newVal, validAccessPolicy()), tt.wantError)
		})
	}
}

func TestValidateCreateAtespaceAccessPolicyRequest(t *testing.T) {
	ctx := context.Background()
	atespace := &ateapi.ObjectRef{Name: "team-a"}
	tests := []struct {
		name      string
		req       *ateapi.CreateAtespaceAccessPolicyRequest
		wantError field.ErrorList
	}{{
		name: "valid",
		req:  &ateapi.CreateAtespaceAccessPolicyRequest{Atespace: atespace, AccessPolicy: validAccessPolicy()},
	}, {
		name: "valid with editor role",
		req: &ateapi.CreateAtespaceAccessPolicyRequest{Atespace: atespace, AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[1].Role = authz.RoleEditor
		})},
	}, {
		name:      "missing atespace",
		req:       &ateapi.CreateAtespaceAccessPolicyRequest{AccessPolicy: validAccessPolicy()},
		wantError: field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		name:      "missing access_policy",
		req:       &ateapi.CreateAtespaceAccessPolicyRequest{Atespace: atespace},
		wantError: field.ErrorList{field.Required(accessPolicyPath, "")},
	}, {
		name: "missing name",
		req: &ateapi.CreateAtespaceAccessPolicyRequest{Atespace: atespace, AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Metadata.Name = ""
		})},
		wantError: field.ErrorList{field.Required(accessPolicyPath.Child("metadata", "name"), "")},
	}, {
		name: "name not default",
		req: &ateapi.CreateAtespaceAccessPolicyRequest{Atespace: atespace, AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Metadata.Name = "other"
		})},
		wantError: field.ErrorList{field.Invalid(accessPolicyPath.Child("metadata", "name"), nil, "").WithOrigin("custom=default")},
	}, {
		name: "unknown role",
		req: &ateapi.CreateAtespaceAccessPolicyRequest{Atespace: atespace, AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[0].Role = "admin"
		})},
		wantError: field.ErrorList{field.NotSupported[string](policyBindings.Index(0).Child("role"), "", nil)},
	}, {
		name: "system component role",
		req: &ateapi.CreateAtespaceAccessPolicyRequest{Atespace: atespace, AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[0].Role = authz.RoleEgressGateway
		})},
		wantError: field.ErrorList{field.NotSupported[string](policyBindings.Index(0).Child("role"), "", nil)},
	}, {
		name: "invalid member",
		req: &ateapi.CreateAtespaceAccessPolicyRequest{Atespace: atespace, AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[0].Members = []string{"group:eng"}
		})},
		wantError: field.ErrorList{field.Invalid(policyBindings.Index(0).Child("members").Index(0), nil, "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateCreateAtespaceAccessPolicyRequest(ctx, tt.req), tt.wantError)
		})
	}
}

func TestValidateGetAtespaceAccessPolicyRequest(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		req       *ateapi.GetAtespaceAccessPolicyRequest
		wantError field.ErrorList
	}{{
		name: "valid",
		req:  &ateapi.GetAtespaceAccessPolicyRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}, Name: resources.SingletonName},
	}, {
		name:      "missing atespace",
		req:       &ateapi.GetAtespaceAccessPolicyRequest{Name: resources.SingletonName},
		wantError: field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		name:      "missing name",
		req:       &ateapi.GetAtespaceAccessPolicyRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}},
		wantError: field.ErrorList{field.Required(field.NewPath("name"), "")},
	}, {
		name:      "wrong name",
		req:       &ateapi.GetAtespaceAccessPolicyRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}, Name: "other"},
		wantError: field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("custom=default")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateGetAtespaceAccessPolicyRequest(ctx, tt.req), tt.wantError)
		})
	}
}

func TestValidateUpdateAtespaceAccessPolicyRequest(t *testing.T) {
	ctx := context.Background()
	atespace := &ateapi.ObjectRef{Name: "team-a"}
	tests := []struct {
		name      string
		req       *ateapi.UpdateAtespaceAccessPolicyRequest
		wantError field.ErrorList
	}{{
		name: "valid",
		req:  &ateapi.UpdateAtespaceAccessPolicyRequest{Atespace: atespace, AccessPolicy: validAccessPolicy()},
	}, {
		name:      "missing access_policy",
		req:       &ateapi.UpdateAtespaceAccessPolicyRequest{Atespace: atespace},
		wantError: field.ErrorList{field.Required(accessPolicyPath, "")},
	}, {
		name: "duplicate role",
		req: &ateapi.UpdateAtespaceAccessPolicyRequest{Atespace: atespace, AccessPolicy: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[1].Role = authz.RoleOwner
		})},
		wantError: field.ErrorList{field.Duplicate(policyBindings.Index(1).Child("role"), nil)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateUpdateAtespaceAccessPolicyRequest(ctx, tt.req), tt.wantError)
		})
	}
}

func TestValidateAtespaceAccessPolicyUpdate(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		newVal    *ateapi.AccessPolicy
		wantError field.ErrorList
	}{{
		name: "valid with editor role",
		newVal: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[1].Role = authz.RoleEditor
		}),
	}, {
		name: "unknown role",
		newVal: validAccessPolicy(func(p *ateapi.AccessPolicy) {
			p.Bindings[0].Role = "admin"
		}),
		wantError: field.ErrorList{field.NotSupported[string](policyBindings.Index(0).Child("role"), "", nil)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateAtespaceAccessPolicyUpdate(ctx, accessPolicyPath, tt.newVal, validAccessPolicy()), tt.wantError)
		})
	}
}

func TestValidateDeleteAtespaceAccessPolicyRequest(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		req       *ateapi.DeleteAtespaceAccessPolicyRequest
		wantError field.ErrorList
	}{{
		name: "valid",
		req:  &ateapi.DeleteAtespaceAccessPolicyRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}, Name: resources.SingletonName},
	}, {
		name:      "missing atespace",
		req:       &ateapi.DeleteAtespaceAccessPolicyRequest{Name: resources.SingletonName},
		wantError: field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		name:      "missing name",
		req:       &ateapi.DeleteAtespaceAccessPolicyRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}},
		wantError: field.ErrorList{field.Required(field.NewPath("name"), "")},
	}, {
		name:      "wrong name",
		req:       &ateapi.DeleteAtespaceAccessPolicyRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}, Name: "other"},
		wantError: field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("custom=default")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateDeleteAtespaceAccessPolicyRequest(ctx, tt.req), tt.wantError)
		})
	}
}
