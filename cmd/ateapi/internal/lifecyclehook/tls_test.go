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

package lifecyclehook

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.ate.dev/substrate/api/hook/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestBuildTargetTLSConfig(t *testing.T) {
	dir := t.TempDir()
	caPEM, serverCert, clientBundlePath, caFilePath := generateTestCerts(t, dir, "hook.ate-system.svc")

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer lis.Close()

	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caPEM)
	srvTLS := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.VerifyClientCertIfGiven,
	}
	grpcSrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(srvTLS)))
	hook.RegisterLifecycleHookServer(grpcSrv, &testHookServer{})
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	addr := "tcp://" + lis.Addr().String()
	tests := []struct {
		name    string
		tlsCfg  HookTLSConfig
		opts    TLSOptions
		wantErr bool
	}{
		{
			name: "caBundle with serverName and TLSOnly succeeds",
			tlsCfg: HookTLSConfig{
				CABundle:       caPEM,
				ServerName:     "hook.ate-system.svc",
				ClientAuthMode: HookClientAuthModeTLSOnly,
			},
		},
		{
			name: "caCertFile with serverName and MTLS succeeds",
			tlsCfg: HookTLSConfig{
				CACertFile:           caFilePath,
				ServerName:           "hook.ate-system.svc",
				ClientAuthMode:       HookClientAuthModeMTLS,
				ClientCredBundleFile: clientBundlePath,
			},
		},
		{
			name: "caCertFile with mismatched serverName fails handshake",
			tlsCfg: HookTLSConfig{
				CACertFile:     caFilePath,
				ServerName:     "wrong.ate-system.svc",
				ClientAuthMode: HookClientAuthModeMTLS,
			},
			opts: TLSOptions{
				DefaultClientCredBundlePath: clientBundlePath,
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			creds, err := buildTransportCredentials(tc.tlsCfg, tc.opts)
			if err != nil {
				t.Fatalf("buildTransportCredentials: %v", err)
			}
			conn, err := grpc.NewClient(normalizeDialTarget(addr), grpc.WithTransportCredentials(creds))
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err = hook.NewLifecycleHookClient(conn).OnDeleteActor(ctx, &hook.OnDeleteActorRequest{Atespace: "ns", Name: "a", Uid: "u"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("OnDeleteActor over TLS err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

type testHookServer struct {
	hook.UnimplementedLifecycleHookServer
}

func (s *testHookServer) OnDeleteActor(context.Context, *hook.OnDeleteActorRequest) (*hook.OnDeleteActorResponse, error) {
	return &hook.OnDeleteActorResponse{}, nil
}

func generateTestCerts(t *testing.T, dir, dnsName string) (caPEM []byte, serverCert tls.Certificate, clientBundlePath, caFilePath string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caFilePath = filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFilePath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{dnsName},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	srvKeyDER, err := x509.MarshalPKCS8PrivateKey(srvKey)
	if err != nil {
		t.Fatal(err)
	}
	srvCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER})
	srvKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: srvKeyDER})
	serverCert, err = tls.X509KeyPair(srvCertPEM, srvKeyPEM)
	if err != nil {
		t.Fatal(err)
	}

	cliKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cliTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "ate-api-server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	cliDER, err := x509.CreateCertificate(rand.Reader, cliTmpl, caCert, &cliKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	cliKeyDER, err := x509.MarshalPKCS8PrivateKey(cliKey)
	if err != nil {
		t.Fatal(err)
	}
	var bundle []byte
	bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: cliKeyDER})...)
	bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cliDER})...)
	clientBundlePath = filepath.Join(dir, "client-bundle.pem")
	if err := os.WriteFile(clientBundlePath, bundle, 0o600); err != nil {
		t.Fatal(err)
	}
	return caPEM, serverCert, clientBundlePath, caFilePath
}
