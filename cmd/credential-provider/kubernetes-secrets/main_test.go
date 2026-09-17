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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func certWithURIs(t *testing.T, uris ...string) *x509.Certificate {
	t.Helper()
	cert := &x509.Certificate{}
	for _, u := range uris {
		parsed, err := url.Parse(u)
		if err != nil {
			t.Fatalf("parsing SAN %q: %v", u, err)
		}
		cert.URIs = append(cert.URIs, parsed)
	}
	return cert
}

func TestVerifyClientSAN(t *testing.T) {
	injector := *injectorSPIFFEID

	tests := []struct {
		name    string
		state   tls.ConnectionState
		wantErr bool
	}{
		{
			name:  "matching SAN",
			state: tls.ConnectionState{PeerCertificates: []*x509.Certificate{certWithURIs(t, injector)}},
		},
		{
			name:  "matching SAN among several",
			state: tls.ConnectionState{PeerCertificates: []*x509.Certificate{certWithURIs(t, "spiffe://cluster.local/ns/other/sa/x", injector)}},
		},
		{
			name:    "wrong SAN",
			state:   tls.ConnectionState{PeerCertificates: []*x509.Certificate{certWithURIs(t, "spiffe://cluster.local/ns/ate-system/sa/impostor")}},
			wantErr: true,
		},
		{
			name:    "no URI SANs",
			state:   tls.ConnectionState{PeerCertificates: []*x509.Certificate{certWithURIs(t)}},
			wantErr: true,
		},
		{
			name:    "no peer certificate",
			state:   tls.ConnectionState{},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := verifyClientSAN(injector)(tc.state)
			if tc.wantErr && err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestProviderMTLS(t *testing.T) {
	ca, err := localca.GenerateCA("trusted", localca.KeyTypeECDSAP256, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	untrustedCA, err := localca.GenerateCA("untrusted", localca.KeyTypeECDSAP256, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	servingCert := issueCertificate(t, ca, "")
	dir := t.TempDir()
	oldBundle, oldCAFile, oldInjector := *serverBundle, *clientCAFile, *injectorSPIFFEID
	t.Cleanup(func() { *serverBundle, *clientCAFile, *injectorSPIFFEID = oldBundle, oldCAFile, oldInjector })
	*serverBundle, *clientCAFile = filepath.Join(dir, "server.pem"), filepath.Join(dir, "ca.pem")
	*injectorSPIFFEID = "spiffe://cluster.local/ns/custom/sa/release-atenet-egress"
	key, err := x509.MarshalPKCS8PrivateKey(servingCert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	bundle := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
	bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: servingCert.Certificate[0]})...)
	if err := os.WriteFile(*serverBundle, bundle, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(*clientCAFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.RootCertificate.Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	creds, err := buildServerCreds(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "ns1"}, Data: map[string][]byte{"token": []byte("credential")},
	})
	srv := grpc.NewServer(grpc.Creds(creds))
	credproviderpb.RegisterCredentialProviderServer(srv, NewServer(client, &NamespaceAuthorizer{allowed: map[string]map[string]struct{}{"team-a": {"ns1": {}}}}))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	roots := x509.NewCertPool()
	roots.AddCert(ca.RootCertificate)
	for _, tc := range []struct {
		name    string
		certs   []tls.Certificate
		allowed bool
	}{
		{"injector", []tls.Certificate{issueCertificate(t, ca, *injectorSPIFFEID)}, true},
		{"other workload", []tls.Certificate{issueCertificate(t, ca, "spiffe://cluster.local/ns/custom/sa/other")}, false},
		{"missing certificate", nil, false},
		{"untrusted injector", []tls.Certificate{issueCertificate(t, untrustedCA, *injectorSPIFFEID)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
				RootCAs: roots, ServerName: "api.ate-system.svc", Certificates: tc.certs, MinVersion: tls.VersionTLS13,
			})))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			before := len(client.Actions())
			resp, err := credproviderpb.NewCredentialProviderClient(conn).FetchSecret(ctx, &credproviderpb.FetchSecretRequest{
				Uri: "ate-secret://kubernetes.io/ns1/api/token", ActorSpiffeId: "spiffe://substrate-actor.local/atespace/team-a/actor/a",
			})
			if tc.allowed {
				if err != nil || string(resp.GetOpaqueBytes()) != "credential" {
					t.Fatalf("FetchSecret: %v, %v", resp, err)
				}
			} else {
				if err == nil {
					t.Fatal("unauthorized peer received credentials")
				}
				if len(client.Actions()) != before {
					t.Fatal("unauthorized peer reached Kubernetes")
				}
			}
		})
	}
}

func issueCertificate(t *testing.T, ca *localca.CA, uri string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: serial, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		DNSNames: []string{"api.ate-system.svc", "localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	if uri != "" {
		parsed, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		template.URIs = []*url.URL{parsed}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.RootCertificate, &key.PublicKey, ca.SigningKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
