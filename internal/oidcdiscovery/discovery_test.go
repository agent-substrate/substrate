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

package oidcdiscovery

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestDiscoveryDocument(t *testing.T) {
	got, err := DiscoveryDocument("https://idp.ate-system.svc", []string{"RS256"})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"issuer":"https://idp.ate-system.svc",` +
		`"jwks_uri":"https://idp.ate-system.svc/openid/v1/jwks",` +
		`"response_types_supported":["id_token"],` +
		`"subject_types_supported":["public"],` +
		`"id_token_signing_alg_values_supported":["RS256"],` +
		`"claims_supported":["iss","sub","aud","exp","nbf","iat","jti","ate.dev"]}`
	if diff := cmp.Diff(want, string(got)); diff != "" {
		t.Errorf("DiscoveryDocument mismatch (-want +got):\n%s", diff)
	}
}

func TestDiscoveryDocumentIssuerWithPath(t *testing.T) {
	got, err := DiscoveryDocument("https://idp.example.com/prod/", []string{"ES256"})
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Issuer != "https://idp.example.com/prod/" {
		t.Errorf("issuer = %q, want it unchanged", doc.Issuer)
	}
	if doc.JWKSURI != "https://idp.example.com/prod/openid/v1/jwks" {
		t.Errorf("jwks_uri = %q, want https://idp.example.com/prod/openid/v1/jwks", doc.JWKSURI)
	}
}

func TestDiscoveryDocumentAlgorithms(t *testing.T) {
	algorithms := []string{"RS256", "ES256", "RS256"}
	got, err := DiscoveryDocument("https://idp.example.com", algorithms)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Algorithms []string `json:"id_token_signing_alg_values_supported"`
	}
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([]string{"ES256", "RS256"}, doc.Algorithms); diff != "" {
		t.Errorf("algorithms mismatch (-want +got):\n%s", diff)
	}
	if !slices.Equal(algorithms, []string{"RS256", "ES256", "RS256"}) {
		t.Errorf("DiscoveryDocument modified its input: %v", algorithms)
	}
}

func TestDiscoveryDocumentErrors(t *testing.T) {
	tests := []struct {
		name       string
		issuer     string
		algorithms []string
	}{
		{name: "invalid issuer", issuer: "http://idp.example.com", algorithms: []string{"RS256"}},
		{name: "no algorithms", issuer: "https://idp.example.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := DiscoveryDocument(tt.issuer, tt.algorithms); err == nil {
				t.Errorf("DiscoveryDocument(%q, %v) = %s, want error", tt.issuer, tt.algorithms, got)
			}
		})
	}
}

func TestJWKSURI(t *testing.T) {
	tests := []struct {
		issuer string
		want   string
	}{
		{issuer: "https://idp.ate-system.svc", want: "https://idp.ate-system.svc/openid/v1/jwks"},
		{issuer: "https://idp.example.com/", want: "https://idp.example.com/openid/v1/jwks"},
		{issuer: "https://idp.example.com/prod", want: "https://idp.example.com/prod/openid/v1/jwks"},
	}
	for _, tt := range tests {
		if got := JWKSURI(tt.issuer); got != tt.want {
			t.Errorf("JWKSURI(%q) = %q, want %q", tt.issuer, got, tt.want)
		}
	}
}
