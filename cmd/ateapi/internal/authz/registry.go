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
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
			return "", "", status.Errorf(codes.Internal, "authz: unexpected request type %T", req)
		}
		return relation, GlobalRootObject, nil
	}
}

func atespaceRule[T any](relation string, getRef func(T) *ateapipb.ObjectRef) targetExtractor {
	return func(req any) (string, string, error) {
		r, ok := req.(T)
		if !ok {
			return "", "", status.Errorf(codes.Internal, "authz: unexpected request type %T", req)
		}
		name := getRef(r).GetName()
		if name == "" {
			return "", "", nil
		}
		return relation, AtespaceObject(name), nil
	}
}

// defaultRPCPermissions is the declarative registry mapping gRPC full method names
// to their required permission rules.
var defaultRPCPermissions = map[string]targetExtractor{
	ateapipb.Control_CreateAtespace_FullMethodName: globalRule[*ateapipb.CreateAtespaceRequest](RelationCanCreateAtespace),
	ateapipb.Control_ListAtespaces_FullMethodName:  globalRule[*ateapipb.ListAtespacesRequest](RelationCanListAtespaces),
	ateapipb.Control_GetAtespace_FullMethodName:    atespaceRule(RelationCanGet, (*ateapipb.GetAtespaceRequest).GetAtespace),
	ateapipb.Control_DeleteAtespace_FullMethodName: atespaceRule(RelationCanDelete, (*ateapipb.DeleteAtespaceRequest).GetAtespace),
}
