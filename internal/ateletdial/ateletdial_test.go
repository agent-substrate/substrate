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

package ateletdial

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/substratex509"
)

const ateletSPIFFEID = "spiffe://cluster.local/ns/ate-system/sa/atelet"

func TestTLSConfigReloadsAteletTrustBundle(t *testing.T) {
	t.Cleanup(credbundle.SetRecheckIntervalForTesting(0))
	dir := t.TempDir()
	ca := newTestCA(t)

	credentialBundlePath := filepath.Join(dir, "worker.pem")
	writeCredentialBundle(t, credentialBundlePath, ca.issueWorkerCert(t))

	trustBundlePath := filepath.Join(dir, "trust.pem")
	writeFile(t, trustBundlePath, ca.certPEM)

	tlsConfig, err := TLSConfig(credentialBundlePath, trustBundlePath, ateletSPIFFEID)
	if err != nil {
		t.Fatalf("TLSConfig() error = %v", err)
	}

	serverConfig := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{ca.issueAteletCert(t)},
	}
	if serverErr, clientErr := tlsHandshake(serverConfig, tlsConfig); serverErr != nil || clientErr != nil {
		t.Fatalf("initial handshake: server error = %v, client error = %v, want success", serverErr, clientErr)
	}

	rotatedCA := newTestCA(t)
	rotatedServerConfig := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{rotatedCA.issueAteletCert(t)},
	}
	if _, clientErr := tlsHandshake(rotatedServerConfig, tlsConfig); clientErr == nil {
		t.Fatalf("handshake against the rotated CA's cert succeeded before trusting it, want a client-side rejection")
	}

	writeFile(t, trustBundlePath, append(ca.certPEM, rotatedCA.certPEM...))

	if serverErr, clientErr := tlsHandshake(rotatedServerConfig, tlsConfig); serverErr != nil || clientErr != nil {
		t.Fatalf("handshake after the CA rotation: server error = %v, client error = %v, want success", serverErr, clientErr)
	}
}

func tlsHandshake(serverConfig, clientConfig *tls.Config) (serverErr, clientErr error) {
	serverConn, clientConn := net.Pipe()
	deadline := time.Now().Add(5 * time.Second)
	_ = serverConn.SetDeadline(deadline)
	_ = clientConn.SetDeadline(deadline)
	serverTLS := tls.Server(serverConn, serverConfig)
	clientTLS := tls.Client(clientConn, clientConfig)
	done := make(chan error, 1)
	go func() {
		err := serverTLS.Handshake()
		_ = serverConn.Close()
		done <- err
	}()
	clientErr = clientTLS.Handshake()
	_ = clientConn.Close()
	serverErr = <-done
	return serverErr, clientErr
}

type testCA struct {
	cert    *x509.Certificate
	certPEM []byte
	key     *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(now.UnixNano()),
		Subject:               pkix.Name{CommonName: "test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{
		cert:    cert,
		key:     key,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

// issueAteletCert issues a server-auth leaf for the node-local atelet,
// carrying the SPIFFE ID TestTLSConfigReloadsAteletTrustBundle dials and a
// PodIdentity matching the worker cert issueWorkerCert issues, so both the
// SPIFFE and same-node checks in TLSConfig's VerifyConnection pass.
func (ca *testCA) issueAteletCert(t *testing.T) tls.Certificate {
	t.Helper()
	template := ca.leafTemplate(t, x509.ExtKeyUsageServerAuth)
	uri, err := url.Parse(ateletSPIFFEID)
	if err != nil {
		t.Fatal(err)
	}
	template.URIs = []*url.URL{uri}
	if err := substratex509.AddPodIdentityToCertificate(&substratex509.PodIdentity{
		Namespace: "ate-system", ServiceAccountName: "atelet", ServiceAccountUID: "atelet-sa-uid",
		PodName: "atelet-0", PodUID: "atelet-pod-uid", NodeName: "node-a", NodeUID: "node-uid",
	}, template); err != nil {
		t.Fatal(err)
	}
	return ca.issue(t, template)
}

// issueWorkerCert issues the worker's own client credential, on the same node
// issueAteletCert names.
func (ca *testCA) issueWorkerCert(t *testing.T) tls.Certificate {
	t.Helper()
	template := ca.leafTemplate(t, x509.ExtKeyUsageClientAuth)
	if err := substratex509.AddPodIdentityToCertificate(&substratex509.PodIdentity{
		Namespace: "ate-workers", ServiceAccountName: "default", ServiceAccountUID: "worker-sa-uid",
		PodName: "worker-0", PodUID: "worker-pod-uid", NodeName: "node-a", NodeUID: "node-uid",
	}, template); err != nil {
		t.Fatal(err)
	}
	return ca.issue(t, template)
}

func (ca *testCA) leafTemplate(t *testing.T, usage x509.ExtKeyUsage) *x509.Certificate {
	t.Helper()
	now := time.Now()
	return &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
}

func (ca *testCA) issue(t *testing.T, template *x509.Certificate) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), ca.certPEM...),
		mustMarshalPKCS8(t, key),
	)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func mustMarshalPKCS8(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

// writeCredentialBundle writes cert as a credbundle.Parse-compatible bundle:
// the PKCS8 private key followed by the certificate chain.
func writeCredentialBundle(t *testing.T, path string, cert tls.Certificate) {
	t.Helper()
	key, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		t.Fatalf("private key has type %T", cert.PrivateKey)
	}
	bundle := mustMarshalPKCS8(t, key)
	for _, der := range cert.Certificate {
		bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	writeFile(t, path, bundle)
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
