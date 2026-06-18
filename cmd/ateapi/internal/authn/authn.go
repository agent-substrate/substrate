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

package authn

import (
	"context"
	"log/slog"

	"github.com/agent-substrate/substrate/internal/principal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// Config for additional authentication methods. We can add different types of authentication, such as JWT token based authentication.
type Config struct {
}

func NewAuthenticationInterceptor(cfg *Config) grpc.UnaryServerInterceptor {
	return newMTLSAuthenticationInterceptor
}

func hasPrincipal(ctx context.Context) (string, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		slog.ErrorContext(ctx, "Authentication failed: no peer or auth info in context.")
		return "", false
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		slog.ErrorContext(ctx, "Authentication failed: no TLS info in context.")
		return "", false
	}

	if len(tlsInfo.State.PeerCertificates) == 0 {
		slog.ErrorContext(ctx, "Authentication failed: no peer certificates in TLS info.")
		return "", false
	}

	clientCert := tlsInfo.State.PeerCertificates[0]
	if len(clientCert.URIs) == 0 {
		slog.ErrorContext(ctx, "Authentication failed: no URIs in peer certificate.")
		return "", false
	}

	id := clientCert.URIs[0].String()
	slog.InfoContext(ctx, "Authentication successful", slog.String("id", id))
	return id, true
}

func newMTLSAuthenticationInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	pInfo := principal.PrincipalInfo{
		ID: "anonymous",
	}
	if id, ok := hasPrincipal(ctx); ok {
		pInfo = principal.PrincipalInfo{
			ID: id,
		}
	}

	newCtx := principal.InjectContext(ctx, pInfo)
	return handler(newCtx, req)
}
