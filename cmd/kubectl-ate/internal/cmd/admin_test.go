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

package cmd

import (
	"testing"

	"github.com/agent-substrate/substrate/internal/localjwtauthority"
	"github.com/agent-substrate/substrate/internal/oidcdiscovery"
)

func TestNewJWTPoolSecretWithFlagDefaults(t *testing.T) {
	alg := makeJwtPoolCmd.Flags().Lookup("alg").DefValue
	keyID := makeJwtPoolCmd.Flags().Lookup("key-id").DefValue

	secret, gotKeyID, err := newJWTPoolSecret("ate-system", "actor-id-jwt-pool", alg, keyID)
	if err != nil {
		t.Fatal(err)
	}
	if secret.Namespace != "ate-system" || secret.Name != "actor-id-jwt-pool" {
		t.Errorf("secret = %s/%s, want ate-system/actor-id-jwt-pool", secret.Namespace, secret.Name)
	}
	pool, err := localjwtauthority.Unmarshal(secret.Data["pool"])
	if err != nil {
		t.Fatal(err)
	}
	if len(pool.Authorities) != 1 {
		t.Fatalf("pool has %d authorities, want 1", len(pool.Authorities))
	}
	authority := pool.Authorities[0]
	if authority.Algorithm != "ES256" {
		t.Errorf("Algorithm = %q, want ES256", authority.Algorithm)
	}
	thumbprint, err := oidcdiscovery.Thumbprint(authority.SigningKey.Public())
	if err != nil {
		t.Fatal(err)
	}
	if authority.ID != thumbprint || gotKeyID != thumbprint || pool.ActiveForSigning != thumbprint {
		t.Errorf("key ID %q, returned %q, active %q; want all to be the thumbprint %q", authority.ID, gotKeyID, pool.ActiveForSigning, thumbprint)
	}
}

func TestNewJWTPoolSecretExplicitKey(t *testing.T) {
	secret, keyID, err := newJWTPoolSecret("ate-system", "actor-id-jwt-pool", "RS256", "1")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := localjwtauthority.Unmarshal(secret.Data["pool"])
	if err != nil {
		t.Fatal(err)
	}
	if keyID != "1" || pool.ActiveForSigning != "1" || pool.Authorities[0].Algorithm != "RS256" {
		t.Errorf("got key %q, active %q, algorithm %q; want 1, 1, RS256", keyID, pool.ActiveForSigning, pool.Authorities[0].Algorithm)
	}
}

func TestNewJWTPoolSecretRejectsUnsupportedAlgorithm(t *testing.T) {
	if _, _, err := newJWTPoolSecret("ate-system", "actor-id-jwt-pool", "HS256", ""); err == nil {
		t.Error("newJWTPoolSecret(HS256) returned nil error")
	}
}
