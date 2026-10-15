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

package egresstunnels

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func parseCert(t *testing.T, data []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("not a PEM certificate: %q", data)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func verify(t *testing.T, b *TargetCertBundle, name string, at time.Time) error {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AddCert(parseCert(t, b.CA))
	_, err := parseCert(t, b.Cert).Verify(x509.VerifyOptions{
		DNSName:     name,
		Roots:       roots,
		CurrentTime: at,
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	return err
}

func TestCertificateCoversEveryEndpoint(t *testing.T) {
	now := time.Now()
	b, err := GenerateTargetCert(now)
	if err != nil {
		t.Fatal(err)
	}
	for i := range MaxEndpoints {
		if err := verify(t, b, EndpointHost(i), now); err != nil {
			t.Fatalf("certificate does not verify for %s: %v", EndpointHost(i), err)
		}
	}
}

func TestCertificateRejectsOtherNames(t *testing.T) {
	now := time.Now()
	b, err := GenerateTargetCert(now)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"a.egress-target-0." + EndpointDomain, // a wildcard covers one label only
		"egress-target-0.other-namespace.svc.cluster.local",
		"example.com",
	} {
		var hostErr x509.HostnameError
		if err := verify(t, b, name, now); !errors.As(err, &hostErr) {
			t.Errorf("verifying for %s = %v, want a HostnameError", name, err)
		}
	}
}

func TestCertificateValidityWindow(t *testing.T) {
	now := time.Now()
	b, err := GenerateTargetCert(now)
	if err != nil {
		t.Fatal(err)
	}
	name := EndpointHost(0)
	if err := verify(t, b, name, now.Add(-30*time.Minute)); err != nil {
		t.Errorf("certificate rejected half an hour before issue, inside the clock-skew allowance: %v", err)
	}
	for _, at := range []time.Time{now.Add(-2 * time.Hour), now.Add(validity + time.Hour)} {
		var invalid x509.CertificateInvalidError
		if err := verify(t, b, name, at); !errors.As(err, &invalid) || invalid.Reason != x509.Expired {
			t.Errorf("verifying at %v = %v, want it outside the validity window", at, err)
		}
	}
}

// The gateway trusts this CA for every upstream it dials, so the CA must be
// unable to vouch for any name outside the endpoints' domain, even in a
// certificate it signed itself.
func TestCARefusesNamesOutsideTheEndpointDomain(t *testing.T) {
	now := time.Now()
	_, ca, caKey, err := generateTargetCert(now)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "evil"},
		DNSNames:     []string{"evil.example.com"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	evil, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	_, err = evil.Verify(x509.VerifyOptions{DNSName: "evil.example.com", Roots: roots, CurrentTime: now})
	var invalid x509.CertificateInvalidError
	if !errors.As(err, &invalid) || invalid.Reason != x509.CANotAuthorizedForThisName {
		t.Errorf("a certificate for evil.example.com signed by the CA verified with %v, want CANotAuthorizedForThisName", err)
	}
}

func TestWrite(t *testing.T) {
	b, err := GenerateTargetCert(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "certs")
	if err := b.Write(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := tls.LoadX509KeyPair(filepath.Join(dir, CertFile), filepath.Join(dir, KeyFile)); err != nil {
		t.Errorf("written certificate and key do not load as a pair: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, KeyFile))
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("key file mode = %o, want 600", mode)
	}
	if ca, err := os.ReadFile(filepath.Join(dir, CAFile)); err != nil || string(ca) != string(b.CA) {
		t.Errorf("CA file = %q, %v; want the bundle's CA", ca, err)
	}
}
