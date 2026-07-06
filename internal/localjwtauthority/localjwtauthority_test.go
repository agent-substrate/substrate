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
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"testing"
)

func TestUnmarshalPEMSigningKey(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey(): %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey(): %v", err)
	}
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))

	data, err := json.Marshal(&serializedPool{
		Authorities: []*serializedAuthority{{
			ID:            "1",
			Algorithm:     "ES256",
			SigningKeyPEM: keyPEM,
		}},
	})
	if err != nil {
		t.Fatalf("Marshal(): %v", err)
	}

	pool, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal(): %v", err)
	}
	if len(pool.Authorities) != 1 {
		t.Fatalf("Authorities length = %d, want 1", len(pool.Authorities))
	}
	if pool.Authorities[0].Algorithm != "ES256" {
		t.Fatalf("Algorithm = %q, want ES256", pool.Authorities[0].Algorithm)
	}
	if _, ok := pool.Authorities[0].SigningKey.(*ecdsa.PrivateKey); !ok {
		t.Fatalf("SigningKey type = %T, want *ecdsa.PrivateKey", pool.Authorities[0].SigningKey)
	}
}
