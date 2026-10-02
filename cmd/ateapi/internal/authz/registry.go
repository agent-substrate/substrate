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
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// targetExtractor extracts the OpenFGA (relation, object) tuple to verify for a request.
// If relation or object is empty with a nil error (e.g., when a required resource identifier
// is missing on a malformed request), the interceptor verifies caller authentication and
// delegates to the handler so standard field-validation errors (codes.InvalidArgument) are returned.
// If err is non-nil (e.g., unexpected request protobuf type), the interceptor fails closed.
type targetExtractor func(req any) (relation string, object string, err error)

func globalRule[T any](relation string) targetExtractor {
	return func(req any) (string, string, error) {
		if _, ok := req.(T); !ok {
			return "", "", apierror.Internal("authz: unexpected request type %T", req)
		}
		return relation, GlobalRootObject, nil
	}
}

func atespaceRule[T any](relation string, getRef func(T) *ateapipb.ObjectRef) targetExtractor {
	return func(req any) (string, string, error) {
		r, ok := req.(T)
		if !ok {
			return "", "", apierror.Internal("authz: unexpected request type %T", req)
		}
		name := getRef(r).GetName()
		if name == "" {
			return "", "", nil
		}
		return relation, AtespaceObject(name), nil
	}
}

// rpcRule is the permission rule for one RPC.
type rpcRule struct {
	extract targetExtractor
	// alwaysEnforce marks RPCs that are checked even when enforcement is
	// disabled. AccessPolicy RPCs set it so that nobody can grant themselves
	// access while enforcement is off and keep that grant once it is turned on.
	alwaysEnforce bool
}

func rule(extract targetExtractor) rpcRule {
	return rpcRule{extract: extract}
}

func governance(extract targetExtractor) rpcRule {
	return rpcRule{extract: extract, alwaysEnforce: true}
}

// defaultRPCPermissions is the declarative registry mapping gRPC full method names
// to their required permission rules.
var defaultRPCPermissions = map[string]rpcRule{
	ateapipb.Control_CreateAtespace_FullMethodName:             rule(globalRule[*ateapipb.CreateAtespaceRequest](RelationCanCreateAtespace)),
	ateapipb.Control_ListAtespaces_FullMethodName:              rule(globalRule[*ateapipb.ListAtespacesRequest](RelationCanListAtespaces)),
	ateapipb.Control_GetAtespace_FullMethodName:                rule(atespaceRule(RelationCanGet, (*ateapipb.GetAtespaceRequest).GetAtespace)),
	ateapipb.Control_DeleteAtespace_FullMethodName:             rule(atespaceRule(RelationCanDelete, (*ateapipb.DeleteAtespaceRequest).GetAtespace)),
	ateapipb.Control_GetGlobalAccessPolicy_FullMethodName:      governance(globalRule[*ateapipb.GetGlobalAccessPolicyRequest](RelationCanGetAccessPolicy)),
	ateapipb.Control_CreateGlobalAccessPolicy_FullMethodName:   governance(globalRule[*ateapipb.CreateGlobalAccessPolicyRequest](RelationCanCreateAccessPolicy)),
	ateapipb.Control_UpdateGlobalAccessPolicy_FullMethodName:   governance(globalRule[*ateapipb.UpdateGlobalAccessPolicyRequest](RelationCanUpdateAccessPolicy)),
	ateapipb.Control_GetAtespaceAccessPolicy_FullMethodName:    governance(atespaceRule(RelationCanGetAccessPolicy, (*ateapipb.GetAtespaceAccessPolicyRequest).GetAtespace)),
	ateapipb.Control_CreateAtespaceAccessPolicy_FullMethodName: governance(atespaceRule(RelationCanCreateAccessPolicy, (*ateapipb.CreateAtespaceAccessPolicyRequest).GetAtespace)),
	ateapipb.Control_UpdateAtespaceAccessPolicy_FullMethodName: governance(atespaceRule(RelationCanUpdateAccessPolicy, (*ateapipb.UpdateAtespaceAccessPolicyRequest).GetAtespace)),
	ateapipb.Control_DeleteAtespaceAccessPolicy_FullMethodName: governance(atespaceRule(RelationCanDeleteAccessPolicy, (*ateapipb.DeleteAtespaceAccessPolicyRequest).GetAtespace)),
}
