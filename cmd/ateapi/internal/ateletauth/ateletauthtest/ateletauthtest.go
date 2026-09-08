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

// Package ateletauthtest builds the peer contexts that the ateapi services
// served only to atelet authenticate against.
package ateletauthtest

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/url"
	"path"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// Cert returns a self-signed certificate carrying the given SPIFFE path and
// PodIdentity, either of which may be omitted to produce a certificate that
// authentication must reject.
func Cert(t *testing.T, spiffePath string, podIdentity *substratex509.PodIdentity) *x509.Certificate {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-caller"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	if spiffePath != "" {
		template.URIs = []*url.URL{{Scheme: "spiffe", Host: ateletauth.TrustDomain, Path: spiffePath}}
	}
	if podIdentity != nil {
		if err := substratex509.AddPodIdentityToCertificate(podIdentity, template); err != nil {
			t.Fatalf("add pod identity: %v", err)
		}
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	return cert
}

// PodIdentityOn returns a well-formed atelet PodIdentity pinned to nodeName.
func PodIdentityOn(nodeName string) *substratex509.PodIdentity {
	return &substratex509.PodIdentity{
		Namespace:          ateletauth.Namespace,
		ServiceAccountName: ateletauth.ServiceAccount,
		ServiceAccountUID:  "sa-uid",
		PodName:            "atelet-xyz",
		PodUID:             "pod-uid",
		NodeName:           nodeName,
		NodeUID:            "node-uid",
	}
}

// CertOn returns the certificate of the atelet running on nodeName.
func CertOn(t *testing.T, nodeName string) *x509.Certificate {
	t.Helper()
	return Cert(t, path.Join("ns", ateletauth.Namespace, "sa", ateletauth.ServiceAccount), PodIdentityOn(nodeName))
}

// ContextWith injects cert as the transport-authenticated peer certificate. A
// nil cert yields a context with no peer information at all, which is what an
// unauthenticated call looks like.
func ContextWith(cert *x509.Certificate) context.Context {
	ctx := context.Background()
	if cert == nil {
		return ctx
	}
	return peer.NewContext(ctx, &peer.Peer{
		AuthInfo: credentials.TLSInfo{
			State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}},
		},
	})
}
