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

package steps

import (
	"crypto/x509"
	"maps"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/localca"
)

// ate-api-server resolves --postgres-connection-string=@env from this
// ConfigMap. It is the only key the shell installer wrote, and an empty value
// makes the apiserver exit with "--postgres-connection-string is required", so
// both the key set and the value are pinned here.
func TestBuildAPIServerEnvVars(t *testing.T) {
	const dsn = "postgresql://postgres@postgres.ate-system.svc:5432/atepg?sslmode=verify-full"

	got := buildAPIServerEnvVars(dsn)

	want := []string{"ATE_API_POSTGRES_CONNECTION_STRING"}
	if keys := slices.Sorted(maps.Keys(got)); !slices.Equal(keys, want) {
		t.Errorf("keys = %v, want %v", keys, want)
	}
	if got["ATE_API_POSTGRES_CONNECTION_STRING"] != dsn {
		t.Errorf("ATE_API_POSTGRES_CONNECTION_STRING = %q, want %q", got["ATE_API_POSTGRES_CONNECTION_STRING"], dsn)
	}
}

// The expected strings here are what the shell installer's
// create_api_authentication_config produced, so a regression shows up as a
// diff rather than as an apiserver that silently trusts the wrong issuer.
func TestBuildAuthenticationConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		issuer string
		want   string
	}{
		{
			name:   "external issuer needs no discovery credentials",
			issuer: "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c",
			want: "actorIdentityJWTProvider: kubernetes\n" +
				"jwtProviders:\n" +
				"- name: kubernetes\n" +
				"  issuer: https://container.googleapis.com/v1/projects/p/locations/l/clusters/c\n" +
				"  audiences: [api.ate-system.svc]",
		},
		{
			name:   "in-cluster issuer projects the service account CA and token",
			issuer: inClusterIssuer,
			want: "actorIdentityJWTProvider: kubernetes\n" +
				"jwtProviders:\n" +
				"- name: kubernetes\n" +
				"  issuer: https://kubernetes.default.svc\n" +
				"  audiences: [api.ate-system.svc]\n" +
				"  certificateAuthorityFile: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt\n" +
				"  discoveryTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token",
		},
		{
			name:   "cluster.local form is treated as in-cluster too",
			issuer: inClusterIssuer + ".cluster.local",
			want: "actorIdentityJWTProvider: kubernetes\n" +
				"jwtProviders:\n" +
				"- name: kubernetes\n" +
				"  issuer: https://kubernetes.default.svc.cluster.local\n" +
				"  audiences: [api.ate-system.svc]\n" +
				"  certificateAuthorityFile: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt\n" +
				"  discoveryTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := buildAuthenticationConfig(tc.issuer); got != tc.want {
				t.Errorf("buildAuthenticationConfig(%q) =\n%q\nwant\n%q", tc.issuer, got, tc.want)
			}
		})
	}
}

// The pool format is a contract with every component that reads these Secrets,
// and internal/localca is free to evolve its generation API underneath. Pinning
// the shape here — one named CA, marked active, with a usable root and the
// requested key type — turns a library change into a failing unit test rather
// than a cluster whose signers pick the wrong CA.
func TestNewCAPoolBytes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		id      string
		keyType localca.KeyType
		wantAlg x509.PublicKeyAlgorithm
	}{
		{"actor identity", poolKeyID, localca.KeyTypeED25519, x509.Ed25519},
		{"egress mitm", poolKeyID, localca.KeyTypeECDSAP256, x509.ECDSA},
	} {
		t.Run(tc.name, func(t *testing.T) {
			poolBytes, err := newCAPoolBytes(tc.id, tc.keyType)
			if err != nil {
				t.Fatalf("newCAPoolBytes() error = %v", err)
			}

			pool, err := localca.Unmarshal(poolBytes)
			if err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			if len(pool.CAs) != 1 {
				t.Fatalf("pool holds %d CAs, want 1", len(pool.CAs))
			}
			if pool.CAs[0].ID != tc.id {
				t.Errorf("CA ID = %q, want %q", pool.CAs[0].ID, tc.id)
			}
			// Without this the pool signs with its first CA only as a
			// backwards-compatibility fallback.
			if pool.ActiveForSigning != tc.id {
				t.Errorf("ActiveForSigning = %q, want %q", pool.ActiveForSigning, tc.id)
			}

			root := pool.CAs[0].RootCertificate
			if root == nil {
				t.Fatal("CA has no root certificate")
			}
			if root.PublicKeyAlgorithm != tc.wantAlg {
				t.Errorf("root key algorithm = %v, want %v", root.PublicKeyAlgorithm, tc.wantAlg)
			}
			if !root.IsCA {
				t.Error("root certificate is not a CA")
			}
			if got := root.NotAfter.Sub(root.NotBefore); got != caValidity {
				t.Errorf("root validity = %v, want %v", got, caValidity)
			}
		})
	}
}
