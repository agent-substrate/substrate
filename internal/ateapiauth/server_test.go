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

func TestAuthenticate_MTLS_AllowsAnonymous(t *testing.T) {
	_, err := authenticate(context.Background(), ServerConfig{Mode: ModeMTLS})
	if err != nil {
		t.Fatalf("ModeMTLS should not error: %v", err)
	}
}

func TestAuthenticate_JWT_RequiresBearer(t *testing.T) {
	cfg := ServerConfig{Mode: ModeJWT, Issuer: "https://example", Audience: "ateapi"}

	// Missing header -> Unauthenticated.
	_, err := authenticate(context.Background(), cfg)
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("missing bearer: want Unauthenticated, got %v (err=%v)", code, err)
	}

	// Garbage bearer -> Unauthenticated (k8sjwt.Verify will fail).
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer not-a-jwt"))
	_, err = authenticate(ctx, cfg)
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
			var ctx context.Context = context.Background()
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
