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

package harness

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/api/ateapi/v1"
)

func TestControlClientAuthenticates(t *testing.T) {
	t.Parallel()
	h := New(t)
	list := func(c ateapi.ControlClient) error {
		_, err := c.ListAtespaces(t.Context(), &ateapi.ListAtespacesRequest{})
		return err
	}

	if err := list(ateapi.NewControlClient(h.dialAteAPI(h.pki.clientTLS()))); status.Code(err) != codes.Unauthenticated {
		t.Errorf("ListAtespaces without a client certificate = %v, want Unauthenticated", err)
	}
	if err := list(h.Control); err != nil {
		t.Errorf("ListAtespaces as the harness client: %v", err)
	}
	if err := list(h.ControlWithoutPreview()); err != nil {
		t.Errorf("ListAtespaces as the harness client without preview opt-in: %v", err)
	}
	if err := list(h.ClientAs("alice")); status.Code(err) != codes.PermissionDenied {
		t.Errorf("ListAtespaces as a principal no policy names = %v, want PermissionDenied", err)
	}

	noIdentity, err := h.pki.issue(&x509.Certificate{
		Subject:     pkix.Name{CommonName: "no-identity"},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := list(ateapi.NewControlClient(h.dialAteAPI(h.pki.clientTLS(noIdentity)))); status.Code(err) != codes.Unauthenticated {
		t.Errorf("ListAtespaces with a certificate carrying no identity = %v, want Unauthenticated", err)
	}

	otherCA, err := newPKI()
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := otherCA.clientCert(ClientID(clientName))
	if err != nil {
		t.Fatal(err)
	}
	// GetClientCertificate presents the certificate whatever CAs ate-api asks
	// for, as a client set on impersonating the owner would.
	impersonating := h.pki.clientTLS()
	impersonating.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &foreign, nil }
	if err := list(ateapi.NewControlClient(h.dialAteAPI(impersonating))); err == nil {
		t.Error("ListAtespaces with the bootstrap owner's identity, certified by another CA, succeeded")
	}
}
