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
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
