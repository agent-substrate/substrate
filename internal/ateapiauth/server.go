//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

// Package ateapiauth adds optional Kubernetes ServiceAccount JWT
// authentication on top of the ateapi gRPC server, and a matching client
// dial helper. It does not replace the existing TLS / mTLS path — the
// server's transport credentials still apply unchanged. Set Mode=ModeJWT
// on the server to require an `authorization: Bearer <SA token>` header
// on every RPC; Mode=ModeMTLS (the default) leaves identity to the
// transport-layer mTLS credentials.
package ateapiauth

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/k8sjwt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Mode selects whether the JWT interceptor enforces a Bearer token.
type Mode string

const (
	ModeMTLS Mode = "mtls"
	ModeJWT  Mode = "jwt"
)

// ParseMode parses a flag value into a Mode, defaulting to ModeMTLS on empty.
// ModeMTLS means identity is established by the transport-layer mTLS
// credentials; the interceptor performs no app-level checks. ModeJWT
// additionally requires a Kubernetes SA Bearer token on every RPC.
func ParseMode(s string) (Mode, error) {
	switch Mode(s) {
	case "", ModeMTLS:
		return ModeMTLS, nil
	case ModeJWT:
		return ModeJWT, nil
	default:
		return "", fmt.Errorf("unknown auth mode %q (want mtls|jwt)", s)
	}
}

// ServerConfig configures the server-side JWT interceptor.
type ServerConfig struct {
	Mode     Mode
	Issuer   string // OIDC issuer URL for JWT verification
	Audience string // expected audience claim for JWT verification

	// Now returns the current time; nil uses time.Now. Exposed for tests.
	Now func() time.Time
}

type ctxKey struct{}

// ClaimsFromContext returns the verified Kubernetes JWT claims that the
// interceptor attached to ctx, if any.
func ClaimsFromContext(ctx context.Context) (*k8sjwt.KubernetesClaims, bool) {
	c, ok := ctx.Value(ctxKey{}).(*k8sjwt.KubernetesClaims)
	return c, ok
}

func contextWithClaims(ctx context.Context, c *k8sjwt.KubernetesClaims) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}

// UnaryServerInterceptor returns a gRPC unary interceptor enforcing cfg.
func UnaryServerInterceptor(cfg ServerConfig) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		newCtx, err := authenticate(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return handler(newCtx, req)
	}
}

// StreamServerInterceptor returns a gRPC stream interceptor enforcing cfg.
func StreamServerInterceptor(cfg ServerConfig) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		newCtx, err := authenticate(ss.Context(), cfg)
		if err != nil {
			return err
		}
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: newCtx})
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

func authenticate(ctx context.Context, cfg ServerConfig) (context.Context, error) {
	if cfg.Mode == "" || cfg.Mode == ModeMTLS {
		return ctx, nil
	}

	now := time.Now
	if cfg.Now != nil {
		now = cfg.Now
	}

	bearer, ok := bearerToken(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing bearer token")
	}
	claims, err := k8sjwt.Verify(ctx, bearer, cfg.Issuer, cfg.Audience, now())
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid bearer token: %v", err)
	}
	return contextWithClaims(ctx, claims), nil
}

func bearerToken(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return "", false
	}
	const prefix = "Bearer "
	v := vals[0]
	if !strings.HasPrefix(v, prefix) {
		return "", false
	}
	tok := strings.TrimSpace(strings.TrimPrefix(v, prefix))
	if tok == "" {
		return "", false
	}
	return tok, true
}
