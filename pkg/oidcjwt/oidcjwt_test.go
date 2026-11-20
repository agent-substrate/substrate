// Copyright 2026 The Agent Substrate Authors
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

package oidcjwt_test

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/pkg/oidcjwt"
)

const testAudience = "driver.openshell.dev"

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

type jwkT struct {
	KeyType string `json:"kty"`
	KeyID   string `json:"kid"`
	RSAN    string `json:"n,omitempty"`
	RSAE    string `json:"e,omitempty"`
}

type jwkSetT struct {
	Keys []jwkT `json:"keys"`
}

type testIssuer struct {
	server *httptest.Server
	caFile string
	jwks   jwkSetT
}

func newTestIssuer(t *testing.T) *testIssuer {
	t.Helper()
	ti := &testIssuer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":   ti.server.URL,
			"jwks_uri": ti.server.URL + "/jwks",
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ti.jwks)
	})
	ti.server = httptest.NewUnstartedServer(mux)
	ti.server.StartTLS()
	t.Cleanup(ti.server.Close)

	caCert := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: ti.server.Certificate().Raw,
	})
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, caCert, 0o600); err != nil {
		t.Fatalf("writing CA: %v", err)
	}
	ti.caFile = caPath

	return ti
}

func (ti *testIssuer) addRSAKey(kid string, pub *rsa.PublicKey) {
	ti.jwks.Keys = append(ti.jwks.Keys, jwkT{
		KeyType: "RSA",
		KeyID:   kid,
		RSAN:    b64url(pub.N.Bytes()),
		RSAE:    b64url(big.NewInt(int64(pub.E)).Bytes()),
	})
}

func mintTestJWT(t *testing.T, kid string, priv *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	header := map[string]string{"alg": "RS256", "typ": "JWT", "kid": kid}
	hb, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("marshaling header: %v", err)
	}
	cb, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshaling claims: %v", err)
	}
	signingInput := b64url(hb) + "." + b64url(cb)
	hashed := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, hashed[:])
	if err != nil {
		t.Fatalf("signing: %v", err)
	}
	return signingInput + "." + b64url(sig)
}

func TestVerifyActorToken(t *testing.T) {
	ti := newTestIssuer(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	ti.addRSAKey("test-key", &key.PublicKey)

	cfg := oidcjwt.Config{
		Issuer:       ti.server.URL,
		IssuerCAFile: ti.caFile,
		Audiences:    []string{testAudience},
	}

	verifier, err := oidcjwt.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	now := time.Now()
	claims := map[string]any{
		"iss": ti.server.URL,
		"sub": "actor/test-atespace/test-actor",
		"aud": []string{testAudience},
		"exp": now.Add(time.Hour).Unix(),
		"nbf": now.Add(-time.Minute).Unix(),
		"iat": now.Add(-time.Minute).Unix(),
		"jti": "jwt-12345",
		"actor.ate.dev": map[string]any{
			"atespace":  "test-atespace",
			"actorName": "test-actor",
			"actorUID":  "uid-9999",
		},
	}

	rawJWT := mintTestJWT(t, "test-key", key, claims)

	verified, err := verifier.Verify(t.Context(), rawJWT)
	if err != nil {
		t.Fatalf("Verify() failed: %v", err)
	}

	if verified.Issuer != ti.server.URL {
		t.Errorf("got Issuer %q, want %q", verified.Issuer, ti.server.URL)
	}
	if verified.Subject != "actor/test-atespace/test-actor" {
		t.Errorf("got Subject %q, want actor/test-atespace/test-actor", verified.Subject)
	}
	if verified.Kind != oidcjwt.KindActor {
		t.Errorf("got Kind %v, want KindActor", verified.Kind)
	}
	if verified.ActorClaims == nil {
		t.Fatal("ActorClaims is nil, expected populated claims")
	}
	if verified.ActorClaims.Atespace != "test-atespace" {
		t.Errorf("got Atespace %q, want test-atespace", verified.ActorClaims.Atespace)
	}
	if verified.ActorClaims.ActorName != "test-actor" {
		t.Errorf("got ActorName %q, want test-actor", verified.ActorClaims.ActorName)
	}
	if verified.ActorClaims.ActorUID != "uid-9999" {
		t.Errorf("got ActorUID %q, want uid-9999", verified.ActorClaims.ActorUID)
	}
	if verified.ActorClaims.Node != "" {
		t.Errorf("got Node %q, want empty for actor token", verified.ActorClaims.Node)
	}
}

func TestVerifyWorkerForActorToken(t *testing.T) {
	ti := newTestIssuer(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	ti.addRSAKey("worker-key", &key.PublicKey)

	cfg := oidcjwt.Config{
		Issuer:       ti.server.URL,
		IssuerCAFile: ti.caFile,
		Audiences:    []string{testAudience},
	}

	verifier, err := oidcjwt.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	now := time.Now()
	claims := map[string]any{
		"iss": ti.server.URL,
		"sub": "worker-for-actor/test-atespace/test-actor",
		"aud": []string{testAudience},
		"exp": now.Add(time.Hour).Unix(),
		"nbf": now.Add(-time.Minute).Unix(),
		"iat": now.Add(-time.Minute).Unix(),
		"jti": "worker-jwt-54321",
		"worker-for-actor.ate.dev": map[string]any{
			"atespace":  "test-atespace",
			"actorName": "test-actor",
			"actorUID":  "uid-9999",
			"node":      "worker-node-1",
		},
	}

	rawJWT := mintTestJWT(t, "worker-key", key, claims)

	verified, err := verifier.Verify(t.Context(), rawJWT)
	if err != nil {
		t.Fatalf("Verify() failed: %v", err)
	}

	if verified.Kind != oidcjwt.KindWorkerForActor {
		t.Errorf("got Kind %v, want KindWorkerForActor", verified.Kind)
	}
	if verified.ActorClaims == nil {
		t.Fatal("ActorClaims is nil")
	}
	if verified.ActorClaims.Node != "worker-node-1" {
		t.Errorf("got Node %q, want worker-node-1", verified.ActorClaims.Node)
	}
}

func TestVerifyRejectsUnspecifiedOrMissingActorClaims(t *testing.T) {
	ti := newTestIssuer(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	ti.addRSAKey("test-key", &key.PublicKey)

	cfg := oidcjwt.Config{
		Issuer:       ti.server.URL,
		IssuerCAFile: ti.caFile,
		Audiences:    []string{testAudience},
	}

	verifier, err := oidcjwt.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	now := time.Now()

	tests := []struct {
		name   string
		claims map[string]any
	}{
		{
			name: "token with neither actor nor worker-for-actor claim",
			claims: map[string]any{
				"iss": ti.server.URL,
				"sub": "some-subject",
				"aud": []string{testAudience},
				"exp": now.Add(time.Hour).Unix(),
			},
		},
		{
			name: "actor claim is empty object",
			claims: map[string]any{
				"iss":           ti.server.URL,
				"sub":           "actor/test-atespace/test-actor",
				"aud":           []string{testAudience},
				"exp":           now.Add(time.Hour).Unix(),
				"actor.ate.dev": map[string]any{},
			},
		},
		{
			name: "actor claim missing actorUID",
			claims: map[string]any{
				"iss": ti.server.URL,
				"sub": "actor/test-atespace/test-actor",
				"aud": []string{testAudience},
				"exp": now.Add(time.Hour).Unix(),
				"actor.ate.dev": map[string]any{
					"atespace":  "test-atespace",
					"actorName": "test-actor",
				},
			},
		},
		{
			name: "worker-for-actor claim missing node",
			claims: map[string]any{
				"iss": ti.server.URL,
				"sub": "worker-for-actor/test-atespace/test-actor",
				"aud": []string{testAudience},
				"exp": now.Add(time.Hour).Unix(),
				"worker-for-actor.ate.dev": map[string]any{
					"atespace":  "test-atespace",
					"actorName": "test-actor",
					"actorUID":  "uid-9999",
				},
			},
		},
		{
			name: "actor claim carries node",
			claims: map[string]any{
				"iss": ti.server.URL,
				"sub": "actor/test-atespace/test-actor",
				"aud": []string{testAudience},
				"exp": now.Add(time.Hour).Unix(),
				"actor.ate.dev": map[string]any{
					"atespace":  "test-atespace",
					"actorName": "test-actor",
					"actorUID":  "uid-9999",
					"node":      "forbidden-node",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rawJWT := mintTestJWT(t, "test-key", key, tt.claims)
			if claims, err := verifier.Verify(t.Context(), rawJWT); err == nil {
				t.Fatalf("expected Verify() to fail for %s, got claims: %+v", tt.name, claims)
			}
		})
	}
}

func TestVerifyRejectsEmptyAndOversizedToken(t *testing.T) {
	ti := newTestIssuer(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	ti.addRSAKey("test-key", &key.PublicKey)

	cfg := oidcjwt.Config{
		Issuer:       ti.server.URL,
		IssuerCAFile: ti.caFile,
		Audiences:    []string{testAudience},
	}

	verifier, err := oidcjwt.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("empty token", func(t *testing.T) {
		if _, err := verifier.Verify(t.Context(), ""); err == nil {
			t.Fatal("expected error for empty token, got nil")
		}
	})

	t.Run("oversized token exceeding MaxJWTSize", func(t *testing.T) {
		oversized := strings.Repeat("a", oidcjwt.MaxJWTSize+1)
		if _, err := verifier.Verify(t.Context(), oversized); err == nil {
			t.Fatal("expected error for oversized token, got nil")
		}
	})
}

func TestVerifyRejectsAudienceMismatch(t *testing.T) {
	ti := newTestIssuer(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	ti.addRSAKey("aud-key", &key.PublicKey)

	cfg := oidcjwt.Config{
		Issuer:       ti.server.URL,
		IssuerCAFile: ti.caFile,
		Audiences:    []string{"expected-audience"},
	}

	verifier, err := oidcjwt.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	claims := map[string]any{
		"iss": ti.server.URL,
		"sub": "actor/test/bot",
		"aud": []string{"wrong-audience"},
		"exp": time.Now().Add(time.Hour).Unix(),
		"actor.ate.dev": map[string]any{
			"atespace":  "test",
			"actorName": "bot",
			"actorUID":  "uid-1",
		},
	}
	rawJWT := mintTestJWT(t, "aud-key", key, claims)

	if _, err := verifier.Verify(t.Context(), rawJWT); err == nil {
		t.Fatal("expected error for audience mismatch, got nil")
	}
}

func TestVerifyRejectsExpiredToken(t *testing.T) {
	ti := newTestIssuer(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	ti.addRSAKey("exp-key", &key.PublicKey)

	cfg := oidcjwt.Config{
		Issuer:       ti.server.URL,
		IssuerCAFile: ti.caFile,
		Audiences:    []string{testAudience},
	}

	verifier, err := oidcjwt.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// More than 5 minutes skew in the past
	expiredAt := time.Now().Add(-10 * time.Minute)
	claims := map[string]any{
		"iss": ti.server.URL,
		"sub": "actor/test/bot",
		"aud": []string{testAudience},
		"exp": expiredAt.Unix(),
		"actor.ate.dev": map[string]any{
			"atespace":  "test",
			"actorName": "bot",
			"actorUID":  "uid-1",
		},
	}
	rawJWT := mintTestJWT(t, "exp-key", key, claims)

	if _, err := verifier.Verify(t.Context(), rawJWT); err == nil {
		t.Fatal("expected error for expired token, got nil")
	}
}

func TestConfigValidation(t *testing.T) {
	ctx := t.Context()

	// Missing Issuer
	if _, err := oidcjwt.New(ctx, oidcjwt.Config{IssuerCAFile: "ca.pem", Audiences: []string{"aud"}}); err == nil {
		t.Error("expected error for missing Issuer, got nil")
	}

	// Invalid Issuer scheme
	if _, err := oidcjwt.New(ctx, oidcjwt.Config{Issuer: "http://insecure.example.com", IssuerCAFile: "ca.pem", Audiences: []string{"aud"}}); err == nil {
		t.Error("expected error for non-https Issuer, got nil")
	}

	// Missing IssuerCAFile
	if _, err := oidcjwt.New(ctx, oidcjwt.Config{Issuer: "https://example.com", Audiences: []string{"aud"}}); err == nil {
		t.Error("expected error for missing IssuerCAFile, got nil")
	}

	// Missing Audiences
	if _, err := oidcjwt.New(ctx, oidcjwt.Config{Issuer: "https://example.com", IssuerCAFile: "ca.pem"}); err == nil {
		t.Error("expected error for missing Audiences, got nil")
	}
}
