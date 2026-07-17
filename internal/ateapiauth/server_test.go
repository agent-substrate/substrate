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

package ateapiauth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"testing"

	"github.com/agent-substrate/substrate/internal/principal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestValidateServerConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ServerConfig
		wantErr bool
	}{
		{name: "valid", cfg: ServerConfig{VerifyBearerToken: func(context.Context, string) (string, error) { return "", nil }}},
		{name: "missing verifier", cfg: ServerConfig{}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateServerConfig(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateServerConfig(%+v) err=%v, wantErr=%v", tt.cfg, err, tt.wantErr)
			}
		})
	}
}

func TestChainedServerAuthenticatorPrincipal(t *testing.T) {
	const subject = "system:serviceaccount:ate-system:ate-client"
	spiffeID := &url.URL{Scheme: "spiffe", Host: "ate.dev", Path: "/ns/default/sa/router"}
	spiffePeer := func(ctx context.Context) context.Context {
		return peer.NewContext(ctx, &peer.Peer{
			AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{{URIs: []*url.URL{spiffeID}}},
			}},
		})
	}
	withBearer := func(ctx context.Context, token string) context.Context {
		return metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
	}
	verifyGoodToken := func(_ context.Context, bearer string) (string, error) {
		if bearer != "good-token" {
			return "", fmt.Errorf("bad token")
		}
		return subject, nil
	}

	tests := []struct {
		name string
		ctx  context.Context
		// verify is the bearer token verifier; nil means the test fails if
		// it is called (the certificate identity must take precedence).
		verify   func(context.Context, string) (string, error)
		want     principal.PrincipalInfo
		wantCode codes.Code
	}{
		{
			name:     "no peer and no token",
			ctx:      context.Background(),
			wantCode: codes.Unauthenticated,
		},
		{
			name: "peer without certificates and no token",
			ctx: peer.NewContext(context.Background(), &peer.Peer{
				AuthInfo: credentials.TLSInfo{},
			}),
			wantCode: codes.Unauthenticated,
		},
		{
			name:     "no peer with valid bearer",
			ctx:      withBearer(context.Background(), "good-token"),
			verify:   verifyGoodToken,
			want:     principal.PrincipalInfo{ID: subject, Kind: principal.KindJWT},
			wantCode: codes.OK,
		},
		{
			name:     "no peer with invalid bearer",
			ctx:      withBearer(context.Background(), "bad-token"),
			verify:   verifyGoodToken,
			wantCode: codes.Unauthenticated,
		},
		{
			name: "certificate without URI SAN with valid bearer",
			ctx: withBearer(peer.NewContext(context.Background(), &peer.Peer{
				AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
					PeerCertificates: []*x509.Certificate{{}},
				}},
			}), "good-token"),
			verify:   verifyGoodToken,
			want:     principal.PrincipalInfo{ID: subject, Kind: principal.KindJWT},
			wantCode: codes.OK,
		},
		{
			name:     "certificate with SPIFFE URI SAN",
			ctx:      spiffePeer(context.Background()),
			want:     principal.PrincipalInfo{ID: spiffeID.String(), Kind: principal.KindMTLS},
			wantCode: codes.OK,
		},
		{
			name:     "certificate takes precedence over bearer",
			ctx:      withBearer(spiffePeer(context.Background()), "good-token"),
			want:     principal.PrincipalInfo{ID: spiffeID.String(), Kind: principal.KindMTLS},
			wantCode: codes.OK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verify := tt.verify
			if verify == nil {
				verify = func(context.Context, string) (string, error) {
					t.Fatal("bearer token verifier called; certificate identity must take precedence")
					return "", nil
				}
			}
			auth := newChainedAuthenticator(ServerConfig{VerifyBearerToken: verify})
			newCtx, err := auth.authenticate(tt.ctx)
			if code := status.Code(err); code != tt.wantCode {
				t.Fatalf("authenticate: code=%v (err=%v), want %v", code, err, tt.wantCode)
			}
			if tt.wantCode != codes.OK {
				return
			}
			got, ok := principal.FromContext(newCtx)
			if !ok {
				t.Fatal("no principal in context")
			}
			if got != tt.want {
				t.Errorf("principal=%+v want %+v", got, tt.want)
			}
		})
	}
}

func TestJWTServerAuthenticatorRequiresBearer(t *testing.T) {
	auth := jwtServerAuthenticator{
		verifyBearerToken: func(context.Context, string) (string, error) {
			return "", fmt.Errorf("bad token")
		},
	}

	// Missing header -> Unauthenticated.
	_, err := auth.authenticate(context.Background())
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("missing bearer: want Unauthenticated, got %v (err=%v)", code, err)
	}

	// Garbage bearer -> Unauthenticated (k8sjwt.Verify will fail).
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer not-a-jwt"))
	_, err = auth.authenticate(ctx)
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("bad bearer: want Unauthenticated, got %v (err=%v)", code, err)
	}
}

func TestJWTServerAuthenticatorInjectsPrincipal(t *testing.T) {
	const subject = "system:serviceaccount:default:router"
	auth := jwtServerAuthenticator{
		verifyBearerToken: func(context.Context, string) (string, error) {
			return subject, nil
		},
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer good-token"))
	newCtx, err := auth.authenticate(ctx)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	got, ok := principal.FromContext(newCtx)
	if !ok {
		t.Fatal("no principal in context")
	}
	want := principal.PrincipalInfo{ID: subject, Kind: principal.KindJWT}
	if got != want {
		t.Errorf("principal=%+v want %+v", got, want)
	}
}

func TestBearerToken(t *testing.T) {
	cases := []struct {
		name  string
		hdr   string
		want  string
		found bool
	}{
		{"missing", "", "", false},
		{"no prefix", "abc", "", false},
		{"prefix", "Bearer abc", "abc", true},
		{"prefix with spaces", "Bearer    abc  ", "abc", true},
		{"empty after prefix", "Bearer ", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.hdr != "" {
				ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", tc.hdr))
			}
			got, ok := bearerToken(ctx)
			if ok != tc.found || got != tc.want {
				t.Errorf("bearerToken=(%q,%v) want (%q,%v)", got, ok, tc.want, tc.found)
			}
		})
	}
}

// Build-time check.
var _ grpc.UnaryServerInterceptor = UnaryServerInterceptor(ServerConfig{})
var _ grpc.StreamServerInterceptor = StreamServerInterceptor(ServerConfig{})
