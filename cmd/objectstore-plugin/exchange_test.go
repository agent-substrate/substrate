// Copyright 2026 The Agent Substrate Authors
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

	"go.ate.dev/substrate/internal/localjwtauthority"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorlookup"
	exchangepb "go.ate.dev/substrate/internal/proto/objectstoreexchangepb"
	"go.ate.dev/substrate/internal/testcert"
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
	otherCA := filepath.Join(t.TempDir(), "other-ca.pem")
	if err := os.WriteFile(otherCA, testcert.NewServerTLS(t, net.ParseIP("127.0.0.1")).RootCA, 0o600); err != nil {
		t.Fatal(err)
	}
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
		{"GCS needs the issuer CA file", exchangeConfig{issuer: issuer, transferAccount: xfer}, "--actor-jwt-issuer-ca-file"},
		{"GCS needs an issuer certificate the CA file or system roots trust", exchangeConfig{issuer: tlsIssuer.URL, issuerCAFile: otherCA, transferAccount: xfer}, "the system roots or " + otherCA},
		{"GCS needs an issuer certificate for the issuer's host", exchangeConfig{issuer: "https://localhost:" + tlsIssuerPort, issuerCAFile: tlsIssuerCA, transferAccount: xfer}, "the system roots or " + tlsIssuerCA},
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

// TestNewExchangeService_ClosesIssuerConnections checks that the connections
// to the actor JWT issuer are closed by the returned function, and when a later
// setup step fails.
func TestNewExchangeService_ClosesIssuerConnections(t *testing.T) {
	// An authorized_user file lets gcsissuer.New build its IAM client without
	// a network call.
	adc := filepath.Join(t.TempDir(), "adc.json")
	if err := os.WriteFile(adc, []byte(`{"type":"authorized_user","client_id":"id","client_secret":"secret","refresh_token":"token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		// noAddress makes actorlookup.New fail.
		noAddress bool
		// adc is GOOGLE_APPLICATION_CREDENTIALS. A missing file makes
		// gcsissuer.New fail.
		adc     string
		wantErr string
	}{
		{name: "release", adc: adc},
		{name: "actor lookup fails", noAddress: true, adc: adc, wantErr: "Address is required"},
		{name: "GCS issuer fails", adc: filepath.Join(t.TempDir(), "absent.json"), wantErr: "GCS credential issuer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ATE_STORAGE_BACKEND", "gcs")
			t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", tc.adc)
			conns := &connCounter{closedCh: make(chan struct{}, 1)}
			issuer := serveIssuer(t, conns.connState)
			issuerCA := writeServerCA(t, issuer)
			cfg := exchangeConfig{
				issuer:          issuer.URL,
				issuerCAFile:    issuerCA,
				transferAccount: "xfer@proj.iam.gserviceaccount.com",
				ateapi: actorlookup.Config{
					Address:          "127.0.0.1:1",
					CAFile:           issuerCA,
					ServerName:       "ate-api-server.ate-system.svc",
					ClientCredBundle: filepath.Join(t.TempDir(), "client.pem"),
				},
			}
			if tc.noAddress {
				cfg.ateapi.Address = ""
			}
			_, release, err := newExchangeService(t.Context(), cfg)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to hold %q", err, tc.wantErr)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				release()
			}
			conns.waitAllClosed(t)
		})
	}
}

// serveIssuer starts a TLS server that serves OIDC discovery and a JWKS.
// connState is the server's http.Server.ConnState.
func serveIssuer(t *testing.T, connState func(net.Conn, http.ConnState)) *httptest.Server {
	t.Helper()
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
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": srv.URL, "jwks_uri": srv.URL + "/jwks"})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(jwks) })
	srv = httptest.NewUnstartedServer(mux)
	srv.Config.ConnState = connState
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// connCounter counts the connections a server opens and closes.
type connCounter struct {
	opened, closed atomic.Int32
	closedCh       chan struct{}
}

func (c *connCounter) connState(_ net.Conn, s http.ConnState) {
	switch s {
	case http.StateNew:
		c.opened.Add(1)
	case http.StateClosed, http.StateHijacked:
		c.closed.Add(1)
		select {
		case c.closedCh <- struct{}{}:
		default:
		}
	}
}

// waitAllClosed waits until at least one connection was opened and every
// opened one is closed.
func (c *connCounter) waitAllClosed(t *testing.T) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for c.opened.Load() == 0 || c.closed.Load() < c.opened.Load() {
		select {
		case <-c.closedCh:
		case <-timeout:
			t.Fatalf("%d of %d issuer connections closed", c.closed.Load(), c.opened.Load())
		}
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
