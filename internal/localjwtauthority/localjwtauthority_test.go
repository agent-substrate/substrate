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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/agent-substrate/substrate/internal/actoridjwt"
)

func TestRefreshingPool(t *testing.T) {
	ca1, err := GenerateAuthority("ES256", "1")
	if err != nil {
		t.Fatalf("Unexpected error generating CA 1: %v", err)
	}
	pool1 := &ConcretePool{
		Authorities:      []*Authority{ca1},
		ActiveForSigning: "1",
	}
	pool1Bytes, err := Marshal(pool1)
	if err != nil {
		t.Fatalf("Unexpected error marshaling pool 1: %v", err)
	}

	ca2, err := GenerateAuthority("ES256", "2")
	if err != nil {
		t.Fatalf("Unexpected error generating CA 2: %v", err)
	}
	pool2 := &ConcretePool{
		Authorities:      []*Authority{ca1, ca2},
		ActiveForSigning: "1",
	}
	pool2Bytes, err := Marshal(pool2)
	if err != nil {
		t.Fatalf("Unexpected error marshaling pool 2: %v", err)
	}

	pool3 := &ConcretePool{
		Authorities:      []*Authority{ca1, ca2},
		ActiveForSigning: "2",
	}
	pool3Bytes, err := Marshal(pool3)
	if err != nil {
		t.Fatalf("Unexpected error marshaling pool 2: %v", err)
	}

	pool4 := &ConcretePool{
		Authorities:      []*Authority{ca2},
		ActiveForSigning: "2",
	}
	pool4Bytes, err := Marshal(pool4)
	if err != nil {
		t.Fatalf("Unexpected error marshaling pool 2: %v", err)
	}

	synctest.Test(t, func(t *testing.T) {
		tempDir := t.TempDir()
		poolFile := filepath.Join(tempDir, "pool.json")

		if err := os.WriteFile(poolFile, pool1Bytes, 0o600); err != nil {
			t.Fatalf("Unexpected error writing pool 1: %v", err)
		}

		refreshingPool, err := NewRefreshingPool(poolFile)
		if err != nil {
			t.Fatalf("Unexpected error creating refreshing pool: %v", err)
		}

		gotVerificationKeys, err := refreshingPool.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from refreshing pool: %v", err)
		}
		wantVerificationKeys, err := pool1.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from pool 1: %v", err)
		}
		if diff := cmp.Diff(gotVerificationKeys, wantVerificationKeys); diff != "" {
			t.Fatalf("Refreshing pool returned wrong trust anchors; diff (-got +want)\n%s", diff)
		}

		// Write pool2 and advance past the cache threshold.
		if err := os.WriteFile(poolFile, pool2Bytes, 0o600); err != nil {
			t.Fatalf("Unexpected error writing pool 2: %v", err)
		}
		time.Sleep(61 * time.Second)

		gotVerificationKeys, err = refreshingPool.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from refreshing pool: %v", err)
		}
		wantVerificationKeys, err = pool2.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from pool 2: %v", err)
		}
		if diff := cmp.Diff(gotVerificationKeys, wantVerificationKeys); diff != "" {
			t.Fatalf("Refreshing pool returned wrong trust anchors after file update 2; diff (-got +want)\n%s", diff)
		}

		// Write pool3 and advance past the cache threshold.
		if err := os.WriteFile(poolFile, pool3Bytes, 0o600); err != nil {
			t.Fatalf("Unexpected error writing pool 3: %v", err)
		}
		time.Sleep(61 * time.Second)
		gotVerificationKeys, err = refreshingPool.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from refreshing pool: %v", err)
		}
		wantVerificationKeys, err = pool3.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from pool 3: %v", err)
		}
		if diff := cmp.Diff(gotVerificationKeys, wantVerificationKeys); diff != "" {
			t.Fatalf("Refreshing pool returned wrong trust anchors after file update 3; diff (-got +want)\n%s", diff)
		}

		// Write pool4 and advance past the cache threshold.
		if err := os.WriteFile(poolFile, pool4Bytes, 0o600); err != nil {
			t.Fatalf("Unexpected error writing pool 4: %v", err)
		}
		time.Sleep(61 * time.Second)
		gotVerificationKeys, err = refreshingPool.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from refreshing pool: %v", err)
		}
		wantVerificationKeys, err = pool4.VerificationKeys()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from pool 4: %v", err)
		}
		if diff := cmp.Diff(gotVerificationKeys, wantVerificationKeys); diff != "" {
			t.Fatalf("Refreshing pool returned wrong trust anchors after file update; diff (-got +want)\n%s", diff)
		}
	})
}

func TestSignJWTHeader(t *testing.T) {
	authority, err := GenerateAuthority("ES256", "key-1")
	if err != nil {
		t.Fatalf("Unexpected error generating authority: %v", err)
	}
	pool := &ConcretePool{
		Authorities:      []*Authority{authority},
		ActiveForSigning: "key-1",
	}

	jwt, err := pool.SignJWT(&actoridjwt.Claims{Subject: "actor/a/b", Audiences: []string{"aud"}})
	if err != nil {
		t.Fatalf("Unexpected error signing JWT: %v", err)
	}

	headerB64, _, ok := strings.Cut(jwt, ".")
	if !ok {
		t.Fatalf("JWT %q has no header segment", jwt)
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(headerB64)
	if err != nil {
		t.Fatalf("Unexpected error decoding header: %v", err)
	}
	var got map[string]string
	if err := json.Unmarshal(headerBytes, &got); err != nil {
		t.Fatalf("Unexpected error unmarshaling header: %v", err)
	}

	want := map[string]string{"typ": "JWT", "alg": "ES256", "kid": "key-1"}
	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("Wrong JWT header; diff (-got +want)\n%s", diff)
	}
}

func authorityIDs(p *ConcretePool) []string {
	var ids []string
	for _, a := range p.Authorities {
		ids = append(ids, a.ID)
	}
	return ids
}

func testPool(t *testing.T, ids ...string) *ConcretePool {
	t.Helper()
	pool := &ConcretePool{ActiveForSigning: ids[0]}
	for _, id := range ids {
		authority, err := GenerateAuthority("ES256", id)
		if err != nil {
			t.Fatal(err)
		}
		pool.Authorities = append(pool.Authorities, authority)
	}
	return pool
}

func TestPoolRotation(t *testing.T) {
	pool := testPool(t, "old")
	next, err := GenerateAuthority("RS256", "next")
	if err != nil {
		t.Fatal(err)
	}

	if err := pool.AddAuthority(next); err != nil {
		t.Fatalf("AddAuthority: %v", err)
	}
	if pool.ActiveForSigning != "old" {
		t.Errorf("after AddAuthority, active = %q, want old", pool.ActiveForSigning)
	}
	if err := pool.Activate("next"); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if err := pool.RemoveAuthority("old"); err != nil {
		t.Fatalf("RemoveAuthority: %v", err)
	}

	if diff := cmp.Diff([]string{"next"}, authorityIDs(pool)); diff != "" {
		t.Errorf("authorities (-want +got):\n%s", diff)
	}
	jwt, err := pool.SignJWT(&actoridjwt.Claims{Subject: "actor/a/b", Audiences: []string{"aud"}})
	if err != nil {
		t.Fatalf("SignJWT: %v", err)
	}
	header, err := base64.RawURLEncoding.DecodeString(strings.Split(jwt, ".")[0])
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"typ":"JWT","alg":"RS256","kid":"next"}`; string(header) != want {
		t.Errorf("header = %s, want %s", header, want)
	}
}

func TestPoolRotationRejects(t *testing.T) {
	dup, err := GenerateAuthority("ES256", "1")
	if err != nil {
		t.Fatal(err)
	}
	noID := &Authority{Algorithm: "ES256", SigningKey: dup.SigningKey}

	for _, tc := range []struct {
		name   string
		pool   *ConcretePool
		change func(*ConcretePool) error
	}{
		{name: "add duplicate ID", pool: testPool(t, "1", "2"), change: func(p *ConcretePool) error { return p.AddAuthority(dup) }},
		{name: "add without ID", pool: testPool(t, "1"), change: func(p *ConcretePool) error { return p.AddAuthority(noID) }},
		{name: "activate absent", pool: testPool(t, "1"), change: func(p *ConcretePool) error { return p.Activate("2") }},
		{name: "remove absent", pool: testPool(t, "1"), change: func(p *ConcretePool) error { return p.RemoveAuthority("2") }},
		{name: "remove active", pool: testPool(t, "1", "2"), change: func(p *ConcretePool) error { return p.RemoveAuthority("1") }},
		{name: "remove first with none designated", pool: func() *ConcretePool {
			p := testPool(t, "1", "2")
			p.ActiveForSigning = ""
			return p
		}(), change: func(p *ConcretePool) error { return p.RemoveAuthority("1") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantIDs, wantActive := authorityIDs(tc.pool), tc.pool.ActiveForSigning
			if err := tc.change(tc.pool); err == nil {
				t.Error("got nil error")
			}
			if diff := cmp.Diff(wantIDs, authorityIDs(tc.pool)); diff != "" {
				t.Errorf("authorities changed (-want +got):\n%s", diff)
			}
			if tc.pool.ActiveForSigning != wantActive {
				t.Errorf("active = %q, want %q", tc.pool.ActiveForSigning, wantActive)
			}
		})
	}
}

func TestGenerateAuthority(t *testing.T) {
	for _, alg := range []string{"RS256", "ES256"} {
		t.Run(alg, func(t *testing.T) {
			authority, err := GenerateAuthority(alg, "")
			if err != nil {
				t.Fatal(err)
			}
			if authority.Algorithm != alg {
				t.Errorf("Algorithm = %q, want %q", authority.Algorithm, alg)
			}
			thumbprint, err := Thumbprint(authority.SigningKey.Public())
			if err != nil {
				t.Fatal(err)
			}
			if authority.ID != thumbprint {
				t.Errorf("ID = %q, want the key thumbprint %q", authority.ID, thumbprint)
			}
			switch key := authority.SigningKey.(type) {
			case *rsa.PrivateKey:
				if alg != "RS256" || key.N.BitLen() != 2048 {
					t.Errorf("got a %d-bit RSA key for %s, want 2048-bit for RS256", key.N.BitLen(), alg)
				}
			case *ecdsa.PrivateKey:
				if alg != "ES256" || key.Curve != elliptic.P256() {
					t.Errorf("got an EC key on %s for %s, want P-256 for ES256", key.Curve.Params().Name, alg)
				}
			default:
				t.Errorf("unexpected key type %T", key)
			}

			pool := &ConcretePool{Authorities: []*Authority{authority}, ActiveForSigning: authority.ID}
			poolBytes, err := Marshal(pool)
			if err != nil {
				t.Fatal(err)
			}
			loaded, err := Unmarshal(poolBytes)
			if err != nil {
				t.Fatal(err)
			}
			jwt, err := loaded.SignJWT(&actoridjwt.Claims{Subject: "actor/a/b", Audiences: []string{"aud"}})
			if err != nil {
				t.Fatal(err)
			}
			headerB64, _, _ := strings.Cut(jwt, ".")
			headerBytes, err := base64.RawURLEncoding.DecodeString(headerB64)
			if err != nil {
				t.Fatal(err)
			}
			var header map[string]string
			if err := json.Unmarshal(headerBytes, &header); err != nil {
				t.Fatal(err)
			}
			if header["alg"] != alg || header["kid"] != thumbprint {
				t.Errorf("header = %v, want alg %s and kid %s", header, alg, thumbprint)
			}
		})
	}
}

func TestGenerateAuthorityExplicitID(t *testing.T) {
	authority, err := GenerateAuthority("RS256", "my-key")
	if err != nil {
		t.Fatal(err)
	}
	if authority.ID != "my-key" {
		t.Errorf("ID = %q, want %q", authority.ID, "my-key")
	}
}

func TestGeneratePool(t *testing.T) {
	for _, tc := range []struct{ alg, id string }{{"ES256", ""}, {"RS256", "my-key"}} {
		wire, id, err := GeneratePool(tc.alg, tc.id)
		if err != nil {
			t.Fatalf("GeneratePool(%q, %q): %v", tc.alg, tc.id, err)
		}
		pool, err := Unmarshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		if len(pool.Authorities) != 1 {
			t.Fatalf("pool has %d authorities, want 1", len(pool.Authorities))
		}
		authority := pool.Authorities[0]
		if authority.Algorithm != tc.alg {
			t.Errorf("Algorithm = %q, want %q", authority.Algorithm, tc.alg)
		}
		if tc.id != "" && id != tc.id {
			t.Errorf("returned ID %q, want %q", id, tc.id)
		}
		if authority.ID != id || pool.ActiveForSigning != id {
			t.Errorf("authority %q, active %q; want both to be the returned ID %q", authority.ID, pool.ActiveForSigning, id)
		}
	}
	if _, _, err := GeneratePool("HS256", ""); err == nil {
		t.Error("GeneratePool(HS256) returned nil error")
	}
}

func TestGenerateAuthorityRejectsUnsupportedAlgorithm(t *testing.T) {
	if _, err := GenerateAuthority("HS256", ""); err == nil {
		t.Error("GenerateAuthority(HS256) returned nil error")
	}
}

func TestVerificationKeysCarryAlgorithm(t *testing.T) {
	es, err := GenerateAuthority("ES256", "es")
	if err != nil {
		t.Fatal(err)
	}
	rs, err := GenerateAuthority("RS256", "rs")
	if err != nil {
		t.Fatal(err)
	}
	pool := &ConcretePool{Authorities: []*Authority{es, rs}, ActiveForSigning: "es"}
	keys, err := pool.VerificationKeys()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, key := range keys {
		got[key.KeyID] = key.Algorithm
	}
	if diff := cmp.Diff(map[string]string{"es": "ES256", "rs": "RS256"}, got); diff != "" {
		t.Errorf("verification key algorithms (-want +got):\n%s", diff)
	}
}
