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
	"github.com/agent-substrate/substrate/internal/oidcdiscovery"
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
			thumbprint, err := oidcdiscovery.Thumbprint(authority.SigningKey.Public())
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

func TestGenerateAuthorityRejectsUnsupportedAlgorithm(t *testing.T) {
	if _, err := GenerateAuthority("HS256", ""); err == nil {
		t.Error("GenerateAuthority(HS256) returned nil error")
	}
}
