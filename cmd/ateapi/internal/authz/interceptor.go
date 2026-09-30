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

	"github.com/agent-substrate/substrate/internal/principal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryServerInterceptor returns a gRPC unary interceptor that enforces per-RPC
// permissions registered in defaultRPCPermissions using authorizer.
func UnaryServerInterceptor(authorizer *Authorizer) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if IsBypassed(ctx) {
			return handler(ctx, req)
		}
		if authorizer == nil {
			return nil, status.Error(codes.Internal, "authz: authorizer is not initialized")
		}

		extractTarget, registered := defaultRPCPermissions[info.FullMethod]
		if !registered {
			return handler(ctx, req)
		}

		relation, object, err := extractTarget(req)
		if err != nil {
			return nil, err
		}
		if relation == "" || object == "" {
			p, hasPrincipal := principal.FromContext(ctx)
			if !hasPrincipal || p.ID == "" {
				return nil, status.Error(codes.Unauthenticated, "unauthenticated: missing principal in context")
			}
			return handler(ctx, req)
		}
		if err := authorizer.Check(ctx, relation, object); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}
