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

package tinyjwt

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

type jwkSetT struct {
	Keys []jwkT `json:"keys"`
}

type jwkT struct {
	KeyType string `json:"kty"`
	KeyID   string `json:"kid,omitempty"`
	RSAN    string `json:"n"`
	RSAE    string `json:"e"`
}

type testIssuer struct {
	server *httptest.Server
	jwks   jwkSetT
	rsaKey *rsa.PrivateKey
}

func newTestIssuer(t *testing.T) *testIssuer {
	t.Helper()
	ti := &testIssuer{}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	ti.rsaKey = key

	ti.jwks.Keys = append(ti.jwks.Keys, jwkT{
		KeyType: "RSA",
		KeyID:   "key-1",
		RSAN:    b64url(key.N.Bytes()),
		RSAE:    b64url(big.NewInt(int64(key.E)).Bytes()),
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		doc := map[string]string{"jwks_uri": ti.server.URL + "/jwks"}
		_ = json.NewEncoder(w).Encode(doc)
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(ti.jwks)
	})
	ti.server = httptest.NewServer(mux)
	t.Cleanup(ti.server.Close)
	return ti
}

func (ti *testIssuer) issuer() string { return ti.server.URL }

func (ti *testIssuer) mintJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	header := map[string]string{"alg": "RS256", "typ": "JWT", "kid": "key-1"}
	hb, _ := json.Marshal(header)
	cb, _ := json.Marshal(claims)
	signingInput := b64url(hb) + "." + b64url(cb)

	digest := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, ti.rsaKey, crypto.SHA256, digest[:])

	if err != nil {
		t.Fatalf("signing RSA: %v", err)
	}
	return signingInput + "." + b64url(sig)
}

func TestVerify_ValidToken(t *testing.T) {
	ti := newTestIssuer(t)
	now := time.Now()
	tok := ti.mintJWT(t, map[string]any{
		"iss":   ti.issuer(),
		"sub":   "user-123",
		"aud":   "test-aud",
		"email": "dev@example.com",
		"exp":   now.Add(time.Hour).Unix(),
	})

	claims, err := Verify(context.Background(), ti.server.Client(), tok, ti.issuer(), []string{"test-aud"}, now)
	if err != nil {
		t.Fatalf("Verify() err = %v, want nil", err)
	}
	if claims.Subject != "user-123" {
		t.Errorf("Subject = %q, want %q", claims.Subject, "user-123")
	}
	if claims.Email != "dev@example.com" {
		t.Errorf("Email = %q, want %q", claims.Email, "dev@example.com")
	}
}

func TestVerify_ExpiredToken(t *testing.T) {
	ti := newTestIssuer(t)
	now := time.Now()
	tok := ti.mintJWT(t, map[string]any{
		"iss": ti.issuer(),
		"sub": "user-123",
		"aud": "test-aud",
		"exp": now.Add(-10 * time.Minute).Unix(),
	})

	_, err := Verify(context.Background(), ti.server.Client(), tok, ti.issuer(), []string{"test-aud"}, now)
	if err == nil {
		t.Fatalf("Verify() err = nil, want expired error")
	}
}

func TestVerify_AudienceMismatch(t *testing.T) {
	ti := newTestIssuer(t)
	now := time.Now()
	tok := ti.mintJWT(t, map[string]any{
		"iss": ti.issuer(),
		"sub": "user-123",
		"aud": "other-aud",
		"exp": now.Add(time.Hour).Unix(),
	})

	_, err := Verify(context.Background(), ti.server.Client(), tok, ti.issuer(), []string{"expected-aud"}, now)
	if err == nil {
		t.Fatalf("Verify() err = nil, want audience mismatch error")
	}
}

func FuzzVerify(f *testing.F) {
	f.Add("invalid.jwt.token")
	f.Add("header.payload.signature")
	f.Add("")
	f.Add("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJmb28ifQ.c2lnbmF0dXJl")

	now := time.Now()
	f.Fuzz(func(t *testing.T, rawJWT string) {
		// Ensure Verify never panics on arbitrary malformed inputs.
		_, _ = Verify(context.Background(), http.DefaultClient, rawJWT, "https://example.com", []string{"aud"}, now)
	})
}
