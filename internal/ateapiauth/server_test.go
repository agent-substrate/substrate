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
	"fmt"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestParseMode(t *testing.T) {
	cases := []struct {
		in      string
		want    Mode
		wantErr bool
	}{
		{"", ModeMTLS, false},
		{"mtls", ModeMTLS, false},
		{"jwt", ModeJWT, false},
		{"none", "", true},
		{"bogus", "", true},
	}
	for _, tc := range cases {
		got, err := ParseMode(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("ParseMode(%q) err=%v wantErr=%v", tc.in, err, tc.wantErr)
		}
		if !tc.wantErr && got != tc.want {
			t.Errorf("ParseMode(%q)=%v want %v", tc.in, got, tc.want)
		}
	}
}

func TestValidateServerConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ServerConfig
		wantErr bool
	}{
		{name: "mtls zero config", cfg: ServerConfig{Mode: ModeMTLS}},
		{name: "empty mode zero config", cfg: ServerConfig{}},
		{name: "jwt valid", cfg: ServerConfig{Mode: ModeJWT, VerifyBearerToken: func(context.Context, string) error { return nil }}},
		{name: "jwt missing verifier", cfg: ServerConfig{Mode: ModeJWT}, wantErr: true},
		{name: "unknown mode", cfg: ServerConfig{Mode: Mode("bogus")}, wantErr: true},
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

func TestMTLSServerAuthenticatorAllowsAnonymous(t *testing.T) {
	_, err := (mtlsServerAuthenticator{}).authenticate(context.Background())
	if err != nil {
		t.Fatalf("ModeMTLS should not error: %v", err)
	}
}

func TestJWTServerAuthenticatorRequiresBearer(t *testing.T) {
	auth := jwtServerAuthenticator{
		verifyBearerToken: func(context.Context, string) error {
			return fmt.Errorf("bad token")
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
