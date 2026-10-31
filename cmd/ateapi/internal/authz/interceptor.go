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
	"context"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/principal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
)

// UnaryServerInterceptor returns a gRPC unary interceptor that enforces per-RPC
// permissions registered in defaultRPCPermissions using authorizer. An RPC
// with no rule is denied.
func UnaryServerInterceptor(authorizer *Authorizer) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if IsBypassed(ctx) {
			return handler(ctx, req)
		}
		rule, registered := defaultRPCPermissions[info.FullMethod]
		if registered && rule.authorizedInHandler != "" {
			return handler(ctx, req)
		}
		if !registered {
			return nil, apierror.PermissionDenied("permission denied: no authorization rule for %s", info.FullMethod)
		}

		// Authenticate first, so an unauthenticated caller gets Unauthenticated
		// rather than a validation error for its request.
		if p, ok := principal.FromContext(ctx); !ok || p.ID == "" {
			return nil, apierror.Unauthenticated("unauthenticated: missing principal in context")
		}
		checks, err := rule.extract(req)
		if err != nil {
			return nil, err
		}
		for _, c := range checks {
			if err := authorizer.Check(ctx, c.relation, c.object); err != nil {
				return nil, err
			}
		}

		return handler(ctx, req)
	}
}

// streamRules lists the streaming RPCs ate-api-server serves; any other stream
// is denied. ateapi.proto defines none (see
// TestDefaultRPCPermissions_CoversEveryServedRPC). gRPC reflection is open to
// any authenticated caller, like Kubernetes API discovery: it returns only the
// served API schema, which is public in api/ateapi/v1/ateapi.proto.
var streamRules = map[string]bool{
	grpc_reflection_v1.ServerReflection_ServerReflectionInfo_FullMethodName:      true,
	grpc_reflection_v1alpha.ServerReflection_ServerReflectionInfo_FullMethodName: true,
}

// StreamServerInterceptor returns a gRPC stream interceptor that denies every
// streaming RPC not in streamRules.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !streamRules[info.FullMethod] {
			return apierror.PermissionDenied("permission denied: no authorization rule for %s", info.FullMethod)
		}
		return handler(srv, ss)
	}
}
