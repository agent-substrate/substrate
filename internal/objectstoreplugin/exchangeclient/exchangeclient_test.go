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

package exchangeclient

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/utils/lru"

	"go.ate.dev/substrate/internal/objectstoreplugin/credential"
	exchangepb "go.ate.dev/substrate/internal/proto/objectstoreexchangepb"
	"go.ate.dev/substrate/pkg/objectstorage"
)

// fakeExchange answers ExchangeActorJwt with one token and records the calls.
type fakeExchange struct {
	exchangepb.CredentialExchangeClient
	token  string
	expiry time.Time
	err    error
	calls  []*exchangepb.ExchangeActorJwtRequest
}

func (f *fakeExchange) ExchangeActorJwt(_ context.Context, in *exchangepb.ExchangeActorJwtRequest, _ ...grpc.CallOption) (*exchangepb.ExchangeActorJwtResponse, error) {
	f.calls = append(f.calls, in)
	if f.err != nil {
		return nil, f.err
	}
	return &exchangepb.ExchangeActorJwtResponse{AccessToken: f.token, ExpireTime: timestamppb.New(f.expiry)}, nil
}

// fakeClient is the storage client built for a token.
type fakeClient struct {
	objectstorage.ObjectStorage
	token string
}

func newTestSource(exchange *fakeExchange, now time.Time) (*Source, *int) {
	built := 0
	s := newSource(exchange, func(_ context.Context, tok credential.Token) (objectstorage.ObjectStorage, error) {
		built++
		return &fakeClient{token: tok.AccessToken}, nil
	})
	s.now = func() time.Time { return now }
	return s, &built
}

func tokenOf(client objectstorage.ObjectStorage) string { return client.(*fakeClient).token }

func TestClient_CachesTheTokenAndClientPerJWTUntilNearExpiry(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	exchange := &fakeExchange{token: "tok-1", expiry: now.Add(time.Hour)}
	s, built := newTestSource(exchange, now)

	first, err := s.Client(t.Context(), "jwt-a", false)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.Client(t.Context(), "jwt-a", false)
	if err != nil {
		t.Fatal(err)
	}
	if again != first || tokenOf(first) != "tok-1" {
		t.Fatalf("second transfer got %v, want the same client on tok-1", again)
	}
	if len(exchange.calls) != 1 || exchange.calls[0].GetForce() || exchange.calls[0].GetActorJwt() != "jwt-a" {
		t.Fatalf("exchange calls = %d (%v), want one unforced with the JWT", len(exchange.calls), exchange.calls)
	}
	if *built != 1 {
		t.Fatalf("built %d clients, want 1 shared by both transfers", *built)
	}

	// Another JWT is a miss, even one naming the same actor: the key is the
	// JWT the exchange verified, never a claim read out of it.
	if _, err := s.Client(t.Context(), "jwt-b-same-actor", false); err != nil {
		t.Fatal(err)
	}
	if len(exchange.calls) != 2 {
		t.Fatalf("a JWT the exchange has not seen must exchange, calls = %d", len(exchange.calls))
	}

	// Under the floor the cache is not served and the entry is replaced.
	s.now = func() time.Time { return now.Add(time.Hour - credential.MinServeLifetime + time.Second) }
	exchange.token, exchange.expiry = "tok-2", now.Add(2*time.Hour)
	client, err := s.Client(t.Context(), "jwt-a", false)
	if err != nil || tokenOf(client) != "tok-2" {
		t.Fatalf("near expiry client = %v, %v, want tok-2 from a fresh exchange", client, err)
	}
	if client, err := s.Client(t.Context(), "jwt-a", false); err != nil || tokenOf(client) != "tok-2" || len(exchange.calls) != 3 {
		t.Fatalf("after the refresh: client %v, err %v, calls %d, want tok-2 from the cache", client, err, len(exchange.calls))
	}
}

func TestClient_ForceSkipsBothCaches(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	exchange := &fakeExchange{token: "tok-1", expiry: now.Add(time.Hour)}
	s, built := newTestSource(exchange, now)
	if _, err := s.Client(t.Context(), "jwt", false); err != nil {
		t.Fatal(err)
	}
	exchange.token = "tok-2"
	client, err := s.Client(t.Context(), "jwt", true)
	if err != nil || tokenOf(client) != "tok-2" {
		t.Fatalf("forced client = %v, %v, want one on tok-2", client, err)
	}
	if len(exchange.calls) != 2 || !exchange.calls[1].GetForce() {
		t.Fatalf("exchange calls = %v, want the second forced", exchange.calls)
	}
	// The forced token replaces the cached one.
	client, err = s.Client(t.Context(), "jwt", false)
	if err != nil || tokenOf(client) != "tok-2" || len(exchange.calls) != 2 {
		t.Fatalf("after force: client %v, err %v, calls %d, want tok-2 from the cache", client, err, len(exchange.calls))
	}
	if *built != 2 {
		t.Fatalf("built %d clients, want one per token", *built)
	}
}

func TestClient_DropsTheRefusedEntryEvenWhenTheForcedExchangeFails(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	exchange := &fakeExchange{token: "tok-1", expiry: now.Add(time.Hour)}
	s, _ := newTestSource(exchange, now)
	if _, err := s.Client(t.Context(), "jwt", false); err != nil {
		t.Fatal(err)
	}
	// GCS refused tok-1, and the actor has since left the node.
	exchange.err = status.Error(codes.NotFound, "actor not found")
	if _, err := s.Client(t.Context(), "jwt", true); status.Code(err) != codes.NotFound {
		t.Fatalf("forced exchange error = %v, want NotFound", err)
	}
	// The refused token must not come back from the cache.
	if _, err := s.Client(t.Context(), "jwt", false); status.Code(err) != codes.NotFound {
		t.Fatalf("after a failed force: err = %v, want NotFound from a new exchange", err)
	}
	if len(exchange.calls) != 3 {
		t.Fatalf("exchange calls = %d, want three", len(exchange.calls))
	}
}

func TestClient_DropsTheRefusedEntryWhenTheForcedTokenIsShort(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	exchange := &fakeExchange{token: "tok-1", expiry: now.Add(time.Hour)}
	s, _ := newTestSource(exchange, now)
	if _, err := s.Client(t.Context(), "jwt", false); err != nil {
		t.Fatal(err)
	}
	exchange.token, exchange.expiry = "short", now.Add(credential.MinServeLifetime-time.Minute)
	if client, err := s.Client(t.Context(), "jwt", true); err != nil || tokenOf(client) != "short" {
		t.Fatalf("forced client = %v, %v, want one on the short token", client, err)
	}
	// Neither the refused tok-1 nor the short token is cached.
	if client, err := s.Client(t.Context(), "jwt", false); err != nil || tokenOf(client) != "short" || len(exchange.calls) != 3 {
		t.Fatalf("after force: client %v, err %v, calls %d, want a third exchange", client, err, len(exchange.calls))
	}
}

func TestClient_EvictsTheOldestJWTWhenFull(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	exchange := &fakeExchange{token: "tok", expiry: now.Add(time.Hour)}
	s, _ := newTestSource(exchange, now)
	s.cache = lru.New(1)
	for _, jwt := range []string{"jwt-a", "jwt-b", "jwt-a"} {
		if _, err := s.Client(t.Context(), jwt, false); err != nil {
			t.Fatal(err)
		}
	}
	if len(exchange.calls) != 3 {
		t.Fatalf("exchange calls = %d, want 3: jwt-a was evicted by jwt-b and exchanged again", len(exchange.calls))
	}
}

func TestClient_UsesButDoesNotCacheAShortToken(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	exchange := &fakeExchange{token: "short", expiry: now.Add(credential.MinServeLifetime - time.Minute)}
	s, _ := newTestSource(exchange, now)
	for range 2 {
		client, err := s.Client(t.Context(), "jwt", false)
		if err != nil || tokenOf(client) != "short" {
			t.Fatalf("client = %v, %v", client, err)
		}
	}
	if len(exchange.calls) != 2 {
		t.Fatalf("a short token must not be cached, calls = %d", len(exchange.calls))
	}
}

func TestClient_Errors(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		jwt      string
		exchange *fakeExchange
		want     codes.Code
	}{
		{"empty jwt", "", &fakeExchange{token: "t", expiry: now.Add(time.Hour)}, codes.InvalidArgument},
		{"exchange refuses", "jwt", &fakeExchange{err: status.Error(codes.PermissionDenied, "actor is no longer on the token's node")}, codes.PermissionDenied},
		{"exchange unavailable", "jwt", &fakeExchange{err: status.Error(codes.Unavailable, "down")}, codes.Unavailable},
		{"exchange returns no token", "jwt", &fakeExchange{token: "", expiry: now.Add(time.Hour)}, codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newTestSource(tc.exchange, now)
			_, err := s.Client(t.Context(), tc.jwt, false)
			if status.Code(err) != tc.want {
				t.Fatalf("code = %v (%v), want %v", status.Code(err), err, tc.want)
			}
			if tc.jwt == "" && len(tc.exchange.calls) != 0 {
				t.Errorf("the exchange was called %d times for an empty token", len(tc.exchange.calls))
			}
		})
	}
}

func TestClient_ReportsAClientBuildFailure(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	s := newSource(&fakeExchange{token: "t", expiry: now.Add(time.Hour)}, func(context.Context, credential.Token) (objectstorage.ObjectStorage, error) {
		return nil, errors.New("no transport")
	})
	_, err := s.Client(t.Context(), "jwt", false)
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v (%v), want Internal", status.Code(err), err)
	}
}

func TestNew_ChecksItsConfig(t *testing.T) {
	if _, err := New(Config{CAFile: "x"}); err == nil {
		t.Error("New without an address succeeded")
	}
	if _, err := New(Config{Address: "api:8445", CAFile: filepath.Join(t.TempDir(), "absent.pem")}); err == nil {
		t.Error("New with a missing CA bundle succeeded")
	}
}

var _ io.Closer = (*Source)(nil)
