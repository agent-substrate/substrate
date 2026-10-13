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
	"slices"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// maxMembersPerPolicy caps the members across all bindings of one policy.
const maxMembersPerPolicy = 1500

var (
	validGlobalRoles   = []string{authz.RoleOwner, authz.RoleViewer}
	validAtespaceRoles = []string{authz.RoleOwner, authz.RoleEditor, authz.RoleViewer}

	bindingsPath = field.NewPath("access_policy", "bindings")
)

func ValidateCreateGlobalAccessPolicyRequest(ctx context.Context, req *ateapipb.CreateGlobalAccessPolicyRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	errs := Validate_CreateGlobalAccessPolicyRequest(ctx, op, nil, req, nil)
	return append(errs, validateBindings(bindingsPath, req.GetAccessPolicy(), validGlobalRoles)...)
}

func ValidateGetGlobalAccessPolicyRequest(ctx context.Context, req *ateapipb.GetGlobalAccessPolicyRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_GetGlobalAccessPolicyRequest(ctx, op, nil, req, nil)
}

func ValidateUpdateGlobalAccessPolicyRequest(ctx context.Context, req *ateapipb.UpdateGlobalAccessPolicyRequest) field.ErrorList {
	// Modeled as a create: this validates the request itself. The result is
	// validated against the stored value by ValidateGlobalAccessPolicyUpdate.
	op := operation.Operation{Type: operation.Create}
	errs := Validate_UpdateGlobalAccessPolicyRequest(ctx, op, nil, req, nil)
	return append(errs, validateBindings(bindingsPath, req.GetAccessPolicy(), validGlobalRoles)...)
}

// ValidateGlobalAccessPolicyUpdate validates an updated global policy against
// the stored one.
func ValidateGlobalAccessPolicyUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapipb.AccessPolicy) field.ErrorList {
	op := operation.Operation{Type: operation.Update}
	errs := Validate_AccessPolicy(ctx, op, fldPath, newVal, oldVal)
	return append(errs, validateBindings(fldPath.Child("bindings"), newVal, validGlobalRoles)...)
}

func ValidateCreateAtespaceAccessPolicyRequest(ctx context.Context, req *ateapipb.CreateAtespaceAccessPolicyRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	errs := Validate_CreateAtespaceAccessPolicyRequest(ctx, op, nil, req, nil)
	return append(errs, validateBindings(bindingsPath, req.GetAccessPolicy(), validAtespaceRoles)...)
}

func ValidateGetAtespaceAccessPolicyRequest(ctx context.Context, req *ateapipb.GetAtespaceAccessPolicyRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_GetAtespaceAccessPolicyRequest(ctx, op, nil, req, nil)
}

func ValidateUpdateAtespaceAccessPolicyRequest(ctx context.Context, req *ateapipb.UpdateAtespaceAccessPolicyRequest) field.ErrorList {
	// Modeled as a create: this validates the request itself. The result is
	// validated against the stored value by ValidateAtespaceAccessPolicyUpdate.
	op := operation.Operation{Type: operation.Create}
	errs := Validate_UpdateAtespaceAccessPolicyRequest(ctx, op, nil, req, nil)
	return append(errs, validateBindings(bindingsPath, req.GetAccessPolicy(), validAtespaceRoles)...)
}

// ValidateAtespaceAccessPolicyUpdate validates an updated atespace policy
// against the stored one.
func ValidateAtespaceAccessPolicyUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapipb.AccessPolicy) field.ErrorList {
	op := operation.Operation{Type: operation.Update}
	errs := Validate_AccessPolicy(ctx, op, fldPath, newVal, oldVal)
	return append(errs, validateBindings(fldPath.Child("bindings"), newVal, validAtespaceRoles)...)
}

func ValidateDeleteAtespaceAccessPolicyRequest(ctx context.Context, req *ateapipb.DeleteAtespaceAccessPolicyRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_DeleteAtespaceAccessPolicyRequest(ctx, op, nil, req, nil)
}

// ValidateCustom_AccessPolicy_Metadata requires an access policy to be the
// unatespaced singleton named "default".
func ValidateCustom_AccessPolicy_Metadata(_ context.Context, _ operation.Operation, root *field.Path, meta, _ *ateapipb.ResourceMetadata) field.ErrorList {
	var errs field.ErrorList
	if meta.GetAtespace() != "" {
		errs = append(errs, field.Forbidden(root.Child("atespace"), "must not be set"))
	}
	return append(errs, validateSingletonName(root.Child("name"), meta.GetName())...)
}

func ValidateCustom_GetGlobalAccessPolicyRequest_Name(_ context.Context, _ operation.Operation, p *field.Path, name, _ *string) field.ErrorList {
	return validateSingletonName(p, *name)
}

func ValidateCustom_GetAtespaceAccessPolicyRequest_Name(_ context.Context, _ operation.Operation, p *field.Path, name, _ *string) field.ErrorList {
	return validateSingletonName(p, *name)
}

func ValidateCustom_DeleteAtespaceAccessPolicyRequest_Name(_ context.Context, _ operation.Operation, p *field.Path, name, _ *string) field.ErrorList {
	return validateSingletonName(p, *name)
}

// validateBindings checks that each binding names an allowed role at most once,
// that every member is a valid OpenFGA user, and that the policy stays within
// maxMembersPerPolicy.
func validateBindings(fldPath *field.Path, policy *ateapipb.AccessPolicy, allowedRoles []string) field.ErrorList {
	if policy == nil {
		return nil
	}
	var errs field.ErrorList
	seenRoles := make(map[string]bool, len(policy.GetBindings()))
	totalMembers := 0
	for i, b := range policy.GetBindings() {
		if b == nil {
			continue
		}
		bp := fldPath.Index(i)
		role := b.GetRole()
		if role != "" {
			if !slices.Contains(allowedRoles, role) {
				errs = append(errs, field.NotSupported(bp.Child("role"), role, allowedRoles))
			} else if seenRoles[role] {
				errs = append(errs, field.Duplicate(bp.Child("role"), role))
			}
			seenRoles[role] = true
		}
		membersPath := bp.Child("members")
		totalMembers += len(b.GetMembers())
		for j, member := range b.GetMembers() {
			if _, err := authz.FormatMember(member); err != nil {
				errs = append(errs, field.Invalid(membersPath.Index(j), member, err.Error()))
			}
		}
	}
	if totalMembers > maxMembersPerPolicy {
		errs = append(errs, field.TooMany(fldPath, totalMembers, maxMembersPerPolicy))
	}
	return errs
}
