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

package main

import (
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/localjwtauthority"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorauthz"
	exchangepb "go.ate.dev/substrate/internal/proto/objectstoreexchangepb"
)

func TestNewExchangeService(t *testing.T) {
	t.Run("S3 answers unimplemented", func(t *testing.T) {
		t.Setenv("ATE_STORAGE_BACKEND", "s3")
		svc, release, err := newExchangeService(t.Context(), exchangeConfig{})
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		if _, ok := svc.(exchangepb.UnimplementedCredentialExchangeServer); !ok {
			t.Fatalf("S3 exchange is %T, want the unimplemented server", svc)
		}
	})
	const (
		issuer = "https://idp.ate-system.svc"
		xfer   = "xfer@proj.iam.gserviceaccount.com"
	)
	emptyCA := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(emptyCA, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tlsIssuer := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(tlsIssuer.Close)
	tlsIssuerCA := writeServerCA(t, tlsIssuer)
	// The httptest certificate is for 127.0.0.1, not localhost.
	_, tlsIssuerPort, err := net.SplitHostPort(tlsIssuer.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		cfg  exchangeConfig
		want string
	}{
		{"GCS needs the issuer", exchangeConfig{transferAccount: xfer}, "--actor-jwt-issuer"},
		{"GCS needs the transfer account", exchangeConfig{issuer: issuer}, "--transfer-service-account"},
		{"GCS needs a readable issuer CA file", exchangeConfig{issuer: issuer, issuerCAFile: filepath.Join(t.TempDir(), "absent.pem"), transferAccount: xfer}, "absent.pem"},
		{"GCS needs certificates in the issuer CA file", exchangeConfig{issuer: issuer, issuerCAFile: emptyCA, transferAccount: xfer}, "contains no certificates"},
		{"GCS needs an issuer certificate the CA file trusts", exchangeConfig{issuer: tlsIssuer.URL, transferAccount: xfer}, "--actor-jwt-issuer-ca-file"},
		{"GCS needs an issuer certificate for the issuer's host", exchangeConfig{issuer: "https://localhost:" + tlsIssuerPort, issuerCAFile: tlsIssuerCA, transferAccount: xfer}, "--actor-jwt-issuer-ca-file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ATE_STORAGE_BACKEND", "gcs")
			_, _, err := newExchangeService(t.Context(), tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to name %s", err, tc.want)
			}
		})
	}
}

// TestCheckIssuerCertificate checks that errors other than an untrusted
// certificate let the exchange start.
func TestCheckIssuerCertificate(t *testing.T) {
	trusted := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(trusted.Close)
	closed := httptest.NewTLSServer(http.NotFoundHandler())
	closed.Close()
	for _, tc := range []struct {
		name   string
		issuer string
		client *http.Client
	}{
		{"unreachable issuer", closed.URL, closed.Client()},
		{"issuer without discovery", trusted.URL, trusted.Client()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := checkIssuerCertificate(t.Context(), tc.issuer, actorauthz.NewVerifier(tc.issuer, tc.client)); err != nil {
				t.Fatalf("checkIssuerCertificate() = %v, want nil", err)
			}
		})
	}
}

// TestNewActorVerifierWarmsCache checks that the start check fills the cache
// of the verifier the exchange uses, and that a failed start check leaves the
// first exchange to fetch right away.
func TestNewActorVerifierWarmsCache(t *testing.T) {
	for _, tc := range []struct {
		name string
		// startFails makes the issuer answer 503 to the start check only.
		startFails bool
		// wantFetches is the JWKS fetch count after the start and one Verify.
		wantFetches int32
	}{
		{"start check fills the cache", false, 1},
		{"failed start check retries on first use", true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			auth, err := localjwtauthority.GenerateAuthority("ES256", "k1")
			if err != nil {
				t.Fatal(err)
			}
			pool := &localjwtauthority.ConcretePool{Authorities: []*localjwtauthority.Authority{auth}, ActiveForSigning: "k1"}
			keys, err := pool.VerificationKeys()
			if err != nil {
				t.Fatal(err)
			}
			jwks, err := localjwtauthority.JWKS(keys)
			if err != nil {
				t.Fatal(err)
			}
			var fetches atomic.Int32
			failNext := &atomic.Bool{}
			failNext.Store(tc.startFails)
			mux := http.NewServeMux()
			var srv *httptest.Server
			mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]string{"issuer": srv.URL, "jwks_uri": srv.URL + "/jwks"})
			})
			mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
				fetches.Add(1)
				if failNext.Swap(false) {
					http.Error(w, "down", http.StatusServiceUnavailable)
					return
				}
				_, _ = w.Write(jwks)
			})
			srv = httptest.NewTLSServer(mux)
			t.Cleanup(srv.Close)

			verifier, err := newActorVerifier(t.Context(), srv.URL, writeServerCA(t, srv))
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			token, err := pool.SignJWT(&actoridjwt.Claims{
				Kind:       actoridjwt.KindWorkerForActor,
				Issuer:     srv.URL,
				Subject:    actoridjwt.KindWorkerForActor.SubjectPrefix() + "/team-a/bot",
				Audiences:  []string{actorauthz.Audience},
				Expiration: now.Add(time.Hour),
				NotBefore:  now.Add(-time.Minute),
				IssuedAt:   now,
				Substrate:  actoridjwt.SubstrateClaims{Atespace: "team-a", ActorName: "bot", ActorUID: "uid", Node: "node-1"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := verifier.Verify(t.Context(), token, now); err != nil {
				t.Fatalf("Verify() error = %v", err)
			}
			if got := fetches.Load(); got != tc.wantFetches {
				t.Fatalf("JWKS fetches = %d, want %d", got, tc.wantFetches)
			}
		})
	}
}

// writeServerCA writes srv's certificate to a PEM file and returns its path.
func writeServerCA(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "issuer-ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestExchangeTransportCredentials_MissingBundle(t *testing.T) {
	if _, err := exchangeTransportCredentials(filepath.Join(t.TempDir(), "absent.pem")); err == nil {
		t.Fatal("a missing serving bundle was accepted")
	}
}
