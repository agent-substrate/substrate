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

package csi

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"testing"
	"time"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type testCA struct {
	cert    *x509.Certificate
	certDER []byte
	key     *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key := generateKey(t)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	return &testCA{cert: cert, certDER: der, key: key}
}

func (ca *testCA) issueClientBundle(t *testing.T, spiffeID string) []byte {
	t.Helper()
	uri, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatalf("parse SPIFFE ID: %v", err)
	}
	key := generateKey(t)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		URIs:         []*url.URL{uri},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create client certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal PKCS8 key: %v", err)
	}
	return append(
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...,
	)
}

func (ca *testCA) issueServerCert(t *testing.T) tls.Certificate {
	t.Helper()
	key := generateKey(t)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "test-server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"test-server"},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create server certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func generateKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ECDSA key: %v", err)
	}
	return key
}

func TestResolveTLSConfig(t *testing.T) {
	ca := newTestCA(t)
	clientBundle := ca.issueClientBundle(t, "spiffe://cluster.local/ns/default/sa/client")

	// Parse client bundle to separate cert and key for mocking secret
	var certPEM, keyPEM []byte
	rest := clientBundle
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			certPEM = pem.EncodeToMemory(block)
		} else if block.Type == "PRIVATE KEY" {
			keyPEM = pem.EncodeToMemory(block)
		}
	}

	mockSecrets := map[string]map[string][]byte{
		"ate-system/ca-secret": {
			"ca.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certDER}),
		},
		"ate-system/client-secret": {
			"tls.crt": certPEM,
			"tls.key": keyPEM,
		},
		"ate-system/bad-secret": {
			"invalid": []byte("data"),
		},
	}

	secretGetter := func(ctx context.Context, namespace, name string) (map[string][]byte, error) {
		key := namespace + "/" + name
		if data, ok := mockSecrets[key]; ok {
			return data, nil
		}
		return nil, fmt.Errorf("secret %s not found", key)
	}

	tests := []struct {
		name    string
		tlsCfg  *atev1alpha1.CSIDriverTLSConfig
		wantNil bool
		wantErr bool
	}{
		{
			name:    "nil config",
			tlsCfg:  nil,
			wantNil: true,
		},
		{
			name: "disabled config",
			tlsCfg: &atev1alpha1.CSIDriverTLSConfig{
				Enabled: false,
			},
			wantNil: true,
		},
		{
			name: "enabled with CA",
			tlsCfg: &atev1alpha1.CSIDriverTLSConfig{
				Enabled: true,
				CACertSecretRef: &atev1alpha1.SecretReference{
					Namespace: "ate-system",
					Name:      "ca-secret",
				},
			},
			wantErr: false,
		},
		{
			name: "enabled with client cert",
			tlsCfg: &atev1alpha1.CSIDriverTLSConfig{
				Enabled: true,
				ClientCertSecretRef: &atev1alpha1.SecretReference{
					Namespace: "ate-system",
					Name:      "client-secret",
				},
			},
			wantErr: false,
		},
		{
			name: "enabled with CA and client cert (mTLS)",
			tlsCfg: &atev1alpha1.CSIDriverTLSConfig{
				Enabled: true,
				CACertSecretRef: &atev1alpha1.SecretReference{
					Namespace: "ate-system",
					Name:      "ca-secret",
				},
				ClientCertSecretRef: &atev1alpha1.SecretReference{
					Namespace: "ate-system",
					Name:      "client-secret",
				},
			},
			wantErr: false,
		},
		{
			name: "missing CA cert key",
			tlsCfg: &atev1alpha1.CSIDriverTLSConfig{
				Enabled: true,
				CACertSecretRef: &atev1alpha1.SecretReference{
					Namespace: "ate-system",
					Name:      "bad-secret",
				},
			},
			wantErr: true,
		},
		{
			name: "missing client cert key",
			tlsCfg: &atev1alpha1.CSIDriverTLSConfig{
				Enabled: true,
				ClientCertSecretRef: &atev1alpha1.SecretReference{
					Namespace: "ate-system",
					Name:      "bad-secret",
				},
			},
			wantErr: true,
		},
		{
			name: "secret not found",
			tlsCfg: &atev1alpha1.CSIDriverTLSConfig{
				Enabled: true,
				CACertSecretRef: &atev1alpha1.SecretReference{
					Namespace: "ate-system",
					Name:      "non-existent",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveTLSConfig(context.Background(), secretGetter, tt.tlsCfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveTLSConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantNil && got != nil {
				t.Errorf("resolveTLSConfig() got = %v, expected nil", got)
			}
			if !tt.wantNil && got == nil && !tt.wantErr {
				t.Errorf("resolveTLSConfig() got nil, expected non-nil")
			}
		})
	}
}

func TestCSIClient_mTLS(t *testing.T) {
	ca := newTestCA(t)
	serverCert := ca.issueServerCert(t)
	clientBundle := ca.issueClientBundle(t, "spiffe://cluster.local/ns/ate-system/sa/ate-api")

	// Start mock gRPC server with mTLS
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	// Server TLS config: requires client certs and trusts CA
	clientCAPool := x509.NewCertPool()
	clientCAPool.AddCert(ca.cert)
	serverTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAPool,
	}

	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLSConfig)))
	mockIdentity := &mockIdentityServer{}
	csi.RegisterIdentityServer(grpcServer, mockIdentity)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	// Client TLS config
	clientCAPoolForClient := x509.NewCertPool()
	clientCAPoolForClient.AddCert(ca.cert)

	// Parse client bundle
	var certPEM, keyPEM []byte
	rest := clientBundle
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			certPEM = pem.EncodeToMemory(block)
		} else if block.Type == "PRIVATE KEY" {
			keyPEM = pem.EncodeToMemory(block)
		}
	}
	clientCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("failed to load client key pair: %v", err)
	}

	clientTLSConfig := &tls.Config{
		RootCAs:      clientCAPoolForClient,
		Certificates: []tls.Certificate{clientCert},
		ServerName:   "test-server", // Matches CommonName in issueServerCert
	}

	// Connect using NewCSIClient with TLS config
	endpoint := "tcp://" + lis.Addr().String()
	client, err := NewCSIClient(endpoint, clientTLSConfig)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	// Verify connection works
	_, err = client.identity.GetPluginInfo(context.Background(), &csi.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo failed: %v", err)
	}
}
