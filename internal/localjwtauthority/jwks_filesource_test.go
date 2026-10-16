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

package localjwtauthority

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/oidcjwt"
)

const fileSourceTestIssuer = "https://idp.ate-system.svc"

func writeJWKS(t *testing.T, path string, pool *ConcretePool) {
	t.Helper()
	b, err := pool.JWKS()
	if err != nil {
		t.Fatalf("JWKS: %v", err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatalf("write JWKS: %v", err)
	}
}

func signTestJWT(t *testing.T, pool *ConcretePool, now time.Time) string {
	t.Helper()
	jwt, err := pool.SignJWT(&actoridjwt.Claims{
		Kind:       actoridjwt.KindWorkerForActor,
		Issuer:     fileSourceTestIssuer,
		Subject:    "worker-for-actor/a/b",
		Audiences:  []string{"aud"},
		Expiration: now.Add(time.Hour),
		NotBefore:  now.Add(-5 * time.Minute),
		IssuedAt:   now,
		Substrate:  actoridjwt.SubstrateClaims{Atespace: "a", ActorName: "b", ActorUID: "u"},
	})
	if err != nil {
		t.Fatalf("SignJWT: %v", err)
	}
	return jwt
}

// TestJWKS_FeedsFileKeySource proves the pool's published JWKS is what the
// verifier's file source needs: tokens the pool signs verify against it,
// across a rotation, and tokens from a stranger do not.
func TestJWKS_FeedsFileKeySource(t *testing.T) {
	first, err := GenerateAuthority("ES256", "first")
	if err != nil {
		t.Fatal(err)
	}
	pool := &ConcretePool{Authorities: []*Authority{first}, ActiveForSigning: "first"}
	path := filepath.Join(t.TempDir(), "jwks.json")
	writeJWKS(t, path, pool)

	now := time.Now()
	verifier := oidcjwt.NewVerifier(fileSourceTestIssuer, []string{"aud"}, oidcjwt.FileKeySource(path), oidcjwt.WithSubstrateClaims())
	claims, err := verifier.Verify(t.Context(), signTestJWT(t, pool, now), now)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.Subject != "worker-for-actor/a/b" {
		t.Errorf("sub = %q", claims.Subject)
	}
	if claims.Kind != actoridjwt.KindWorkerForActor || claims.Substrate == nil || claims.Substrate.Atespace != "a" {
		t.Errorf("Substrate claims = %+v, want worker-for-actor", claims.SubstrateClaims)
	}

	next, err := GenerateAuthority("ES256", "next")
	if err != nil {
		t.Fatal(err)
	}
	pool.Authorities = append(pool.Authorities, next)
	pool.ActiveForSigning = "next"
	writeJWKS(t, path, pool)
	if _, err := verifier.Verify(t.Context(), signTestJWT(t, pool, now), now); err != nil {
		t.Fatalf("Verify after rotation: %v", err)
	}

	stranger, err := GenerateAuthority("ES256", "stranger")
	if err != nil {
		t.Fatal(err)
	}
	strangerPool := &ConcretePool{Authorities: []*Authority{stranger}, ActiveForSigning: "stranger"}
	if _, err := verifier.Verify(t.Context(), signTestJWT(t, strangerPool, now.Add(2*time.Minute)), now.Add(2*time.Minute)); err == nil {
		t.Fatal("Verify of a token from an unknown authority succeeded, want error")
	}
}
