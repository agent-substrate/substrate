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

package apiauthn

import (
	"context"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/principal"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// AuthenticationService serves the Authentication gRPC service. It reads the
// principal that the interceptor attached, so it must be served behind it.
type AuthenticationService struct {
	ateapipb.UnimplementedAuthenticationServer
}

func (AuthenticationService) WhoAmI(ctx context.Context, _ *ateapipb.WhoAmIRequest) (*ateapipb.WhoAmIResponse, error) {
	p, ok := principal.FromContext(ctx)
	if !ok || p.ID == "" {
		return nil, apierror.Unauthenticated("unauthenticated: missing principal in context")
	}
	kind := ateapipb.PrincipalKind_PRINCIPAL_KIND_UNSPECIFIED
	switch p.Kind {
	case principal.KindJWT:
		kind = ateapipb.PrincipalKind_PRINCIPAL_KIND_JWT
	case principal.KindMTLS:
		kind = ateapipb.PrincipalKind_PRINCIPAL_KIND_MTLS
	}
	return &ateapipb.WhoAmIResponse{Id: p.ID, Kind: kind, Issuer: p.Issuer}, nil
}
