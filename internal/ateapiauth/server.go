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

// Package ateapiauth authenticates clients of the ateapi gRPC server, and
// provides a matching client dial helper. The server interceptor takes
// identity from the transport-layer mTLS credentials when the client
// presented a certificate, and otherwise requires an authorization
// header `Bearer <JWT Token>`. Requests with no credentials are rejected.
package ateapiauth

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/agent-substrate/substrate/internal/principal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func ValidateServerConfig(cfg ServerConfig) error {
	if cfg.VerifyBearerToken == nil {
		return fmt.Errorf("a bearer token verifier is required")
	}
	return nil
}

// ServerConfig configures the server-side auth interceptor.
type ServerConfig struct {
	// VerifyBearerToken verifies a Bearer token presented by a client and
	// returns the authenticated principal's ID (e.g. the JWT subject). It
	// authenticates clients that did not present a certificate identity
	// (e.g. kubectl-ate, which dials without a client certificate).
	VerifyBearerToken func(context.Context, string) (string, error)
}

// UnaryServerInterceptor returns a gRPC unary interceptor enforcing cfg.
func UnaryServerInterceptor(cfg ServerConfig) grpc.UnaryServerInterceptor {
	auth := newChainedAuthenticator(cfg)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		newCtx, err := auth.authenticate(ctx)
		if err != nil {
			return nil, err
		}
		return handler(newCtx, req)
	}
}

// StreamServerInterceptor returns a gRPC stream interceptor enforcing cfg.
func StreamServerInterceptor(cfg ServerConfig) grpc.StreamServerInterceptor {
	auth := newChainedAuthenticator(cfg)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		newCtx, err := auth.authenticate(ss.Context())
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

func newChainedAuthenticator(cfg ServerConfig) chainedServerAuthenticator {
	return chainedServerAuthenticator{
		jwt: jwtServerAuthenticator{
			verifyBearerToken: cfg.VerifyBearerToken,
		},
	}
}

// chainedServerAuthenticator first checks mTLS peer, then checks
// a bearer token in the header.
type chainedServerAuthenticator struct {
	// jwt authenticates clients that did not present a certificate identity
	// (e.g. kubectl-ate, which dials without a client certificate).
	jwt jwtServerAuthenticator
}

func (a chainedServerAuthenticator) authenticate(ctx context.Context) (context.Context, error) {
	if id, ok := mtlsPeerIdentity(ctx); ok {
		return principal.InjectContext(ctx, principal.PrincipalInfo{
			ID:   id,
			Kind: principal.KindMTLS,
		}), nil
	}
	return a.jwt.authenticate(ctx)
}

// mtlsPeerIdentity extracts the client identity (the first URI SAN, a SPIFFE
// ID) from the transport-authenticated peer certificate.
func mtlsPeerIdentity(ctx context.Context) (string, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		slog.DebugContext(ctx, "No mTLS peer identity: no peer or auth info in context.")
		return "", false
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		slog.DebugContext(ctx, "No mTLS peer identity: no TLS info in context.")
		return "", false
	}

	if len(tlsInfo.State.PeerCertificates) == 0 {
		slog.DebugContext(ctx, "No mTLS peer identity: no peer certificates in TLS info.")
		return "", false
	}

	clientCert := tlsInfo.State.PeerCertificates[0]
	if len(clientCert.URIs) == 0 {
		slog.DebugContext(ctx, "No mTLS peer identity: no URIs in peer certificate.")
		return "", false
	}

	id := clientCert.URIs[0].String()
	if id == "" {
		slog.DebugContext(ctx, "No mTLS peer identity: client cert URI is empty string")
		return "", false
	}
	slog.InfoContext(ctx, "Authentication successful", slog.String("id", id))
	return id, true
}

type jwtServerAuthenticator struct {
	verifyBearerToken func(context.Context, string) (string, error)
}

func (a jwtServerAuthenticator) authenticate(ctx context.Context) (context.Context, error) {
	bearer, ok := bearerToken(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing bearer token")
	}
	id, err := a.verifyBearerToken(ctx, bearer)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid bearer token: %v", err)
	}
	return principal.InjectContext(ctx, principal.PrincipalInfo{
		ID:   id,
		Kind: principal.KindJWT,
	}), nil
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
