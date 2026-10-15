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

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/principal"
	"google.golang.org/grpc"
)

// UnaryServerInterceptor returns a gRPC unary interceptor that enforces per-RPC
// permissions registered in defaultRPCPermissions using authorizer. When
// enforce is false, only rules marked alwaysEnforce are checked. When enforce
// is true, an RPC with no rule is denied.
func UnaryServerInterceptor(authorizer *Authorizer, enforce bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if IsBypassed(ctx) {
			return handler(ctx, req)
		}
		rule, registered := defaultRPCPermissions[info.FullMethod]
		if registered && rule.authorizedInHandler != "" {
			return handler(ctx, req)
		}
		if !enforce && !rule.alwaysEnforce {
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
