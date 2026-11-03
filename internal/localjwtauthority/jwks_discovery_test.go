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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/oidcjwt"
)

// discoveryServer serves pool's discovery document and JWK set the way
// ate-idp-server does, reading pool on every request so a rotation shows.
func discoveryServer(t *testing.T, mu *sync.Mutex, pool *ConcretePool) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(nil)
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]string{"issuer": srv.URL, "jwks_uri": srv.URL + "/jwks"})
		case "/jwks":
			mu.Lock()
			keys, err := pool.VerificationKeys()
			mu.Unlock()
			var b []byte
			if err == nil {
				b, err = JWKS(keys)
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, _ = w.Write(b)
		default:
			http.NotFound(w, r)
		}
	})
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func signTestJWT(t *testing.T, pool *ConcretePool, issuer string, now time.Time) string {
	t.Helper()
	jwt, err := pool.SignJWT(&actoridjwt.Claims{
		Kind:       actoridjwt.KindWorkerForActor,
		Issuer:     issuer,
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

// TestJWKS_ServedOverDiscovery proves the pool's published JWKS is what a
// verifier fetching it through OIDC discovery needs: tokens the pool signs
// verify against it, across a rotation, and tokens from a stranger do not.
func TestJWKS_ServedOverDiscovery(t *testing.T) {
	first, err := GenerateAuthority("ES256", "first")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	pool := &ConcretePool{Authorities: []*Authority{first}, ActiveForSigning: "first"}
	srv := discoveryServer(t, &mu, pool)
	issuer := srv.URL

	now := time.Now()
	verifier := oidcjwt.NewVerifier(issuer, []string{"aud"}, oidcjwt.HTTPKeySource(issuer, srv.Client()), oidcjwt.WithSubstrateClaims())
	claims, err := verifier.Verify(t.Context(), signTestJWT(t, pool, issuer, now), now)
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
	mu.Lock()
	pool.Authorities = append(pool.Authorities, next)
	pool.ActiveForSigning = "next"
	mu.Unlock()
	if _, err := verifier.Verify(t.Context(), signTestJWT(t, pool, issuer, now), now); err != nil {
		t.Fatalf("Verify after rotation: %v", err)
	}

	stranger, err := GenerateAuthority("ES256", "stranger")
	if err != nil {
		t.Fatal(err)
	}
	strangerPool := &ConcretePool{Authorities: []*Authority{stranger}, ActiveForSigning: "stranger"}
	later := now.Add(2 * time.Minute)
	if _, err := verifier.Verify(t.Context(), signTestJWT(t, strangerPool, issuer, later), later); err == nil {
		t.Fatal("Verify of a token from an unknown authority succeeded, want error")
	}
}
