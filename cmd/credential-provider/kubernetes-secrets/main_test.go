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
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"
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
	const injector = injectorSPIFFEID

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
