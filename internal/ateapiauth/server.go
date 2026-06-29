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

func ValidateServerConfig(cfg ServerConfig) error {
	switch cfg.Mode {
	case "", ModeMTLS:
		return nil
	case ModeJWT:
		if cfg.VerifyBearerToken == nil {
			return fmt.Errorf("jwt mode requires bearer token verifier")
		}
		return nil
	default:
		return fmt.Errorf("unknown auth mode %q", cfg.Mode)
	}
}

// ServerConfig configures the server-side auth interceptor.
type ServerConfig struct {
	Mode Mode

	// VerifyBearerToken verifies a Bearer token presented by a client. Required
	// for ModeJWT and ignored for ModeMTLS.
	VerifyBearerToken func(context.Context, string) error
}

// UnaryServerInterceptor returns a gRPC unary interceptor enforcing cfg.
func UnaryServerInterceptor(cfg ServerConfig) grpc.UnaryServerInterceptor {
	auth := serverAuthenticatorFor(cfg)
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
	auth := serverAuthenticatorFor(cfg)
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

type serverAuthenticator interface {
	authenticate(context.Context) (context.Context, error)
}

func serverAuthenticatorFor(cfg ServerConfig) serverAuthenticator {
	switch cfg.Mode {
	case "", ModeMTLS:
		return mtlsServerAuthenticator{}
	case ModeJWT:
		return jwtServerAuthenticator{
			verifyBearerToken: cfg.VerifyBearerToken,
		}
	}

	return invalidServerAuthenticator{mode: cfg.Mode}
}

type mtlsServerAuthenticator struct{}

func (mtlsServerAuthenticator) authenticate(ctx context.Context) (context.Context, error) {
	// TODO: Extract the transport-authenticated client identity and attach it
	// to ctx once ateapi has an authorization layer.
	return ctx, nil
}

type jwtServerAuthenticator struct {
	verifyBearerToken func(context.Context, string) error
}

func (a jwtServerAuthenticator) authenticate(ctx context.Context) (context.Context, error) {
	bearer, ok := bearerToken(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing bearer token")
	}
	if err := a.verifyBearerToken(ctx, bearer); err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid bearer token: %v", err)
	}
	// TODO: Attach the verified JWT identity to ctx once ateapi has an
	// authorization layer that consumes it.
	return ctx, nil
}

type invalidServerAuthenticator struct {
	mode Mode
}

func (a invalidServerAuthenticator) authenticate(context.Context) (context.Context, error) {
	return nil, status.Errorf(codes.Internal, "invalid auth mode %q", a.mode)
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
