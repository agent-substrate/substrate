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

package e2e

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"
)

func TestNewServerTLS(t *testing.T) {
	material := NewServerTLS(t, net.ParseIP("10.0.0.7"))
	pair, err := tls.X509KeyPair(material.Certificate, material.PrivateKey)
	if err != nil {
		t.Fatalf("loading serving key pair: %v", err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(material.RootCA) {
		t.Fatal("no CA certificate")
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: "10.0.0.7"}); err != nil {
		t.Fatalf("verifying Service IP: %v", err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: "10.0.0.8"}); err == nil {
		t.Fatal("accepted a different Service IP")
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: x509.NewCertPool(), DNSName: "10.0.0.7"}); err == nil {
		t.Fatal("accepted an untrusted certificate")
	}
	if leaf.IsCA {
		t.Fatal("origin received a CA certificate")
	}
}
