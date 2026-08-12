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

package oidcauth


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

func TestOIDCAuthenticator_SubClaimMapping(t *testing.T) {
	ti := newTestIssuer(t)
	now := time.Now()
	tok := ti.mintJWT(t, map[string]any{
		"iss": ti.issuer(),
		"sub": "user-123",
		"aud": "test-aud",
		"exp": now.Add(time.Hour).Unix(),
	})

	auth := New(OIDCAuthenticatorConfig{
		IssuerURL: ti.issuer(),
		Audiences: []string{"test-aud"},
		ClaimMappings: ClaimMappings{
			Username: "claims.sub",
		},
	}, ti.server.Client())

	user, ok, err := auth.AuthenticateToken(context.Background(), tok)
	if err != nil {
		t.Fatalf("AuthenticateToken() err = %v, want nil", err)
	}
	if !ok || user == nil {
		t.Fatalf("AuthenticateToken() ok = false or user is nil, want user")
	}
	if user.ID != "user-123" {
		t.Errorf("user.ID = %q, want %q", user.ID, "user-123")
	}
	if len(user.ExtraInfo["sub"]) == 0 || user.ExtraInfo["sub"][0] != "user-123" {
		t.Errorf("user.ExtraInfo[sub] = %v, want %q", user.ExtraInfo["sub"], "user-123")
	}
	if len(user.ExtraInfo["iss"]) == 0 || user.ExtraInfo["iss"][0] != ti.issuer() {
		t.Errorf("user.ExtraInfo[iss] = %v, want %q", user.ExtraInfo["iss"], ti.issuer())
	}
}

func TestOIDCAuthenticator_EmailClaimMapping(t *testing.T) {
	ti := newTestIssuer(t)
	now := time.Now()
	tok := ti.mintJWT(t, map[string]any{
		"iss":   ti.issuer(),
		"sub":   "user-123",
		"email": "shrutinair@google.com",
		"aud":   "test-aud",
		"exp":   now.Add(time.Hour).Unix(),
	})

	auth := New(OIDCAuthenticatorConfig{
		IssuerURL: ti.issuer(),
		Audiences: []string{"test-aud"},
		ClaimMappings: ClaimMappings{
			Username: "'google:' + claims.email",
		},
	}, ti.server.Client())

	user, ok, err := auth.AuthenticateToken(context.Background(), tok)
	if err != nil {
		t.Fatalf("AuthenticateToken() err = %v, want nil", err)
	}
	if !ok || user == nil {
		t.Fatalf("AuthenticateToken() ok = false or user is nil, want user")
	}
	if user.ID != "google:shrutinair@google.com" {
		t.Errorf("user.ID = %q, want %q", user.ID, "google:shrutinair@google.com")
	}
	if len(user.ExtraInfo["email"]) == 0 || user.ExtraInfo["email"][0] != "shrutinair@google.com" {
		t.Errorf("user.ExtraInfo[email] = %v, want %q", user.ExtraInfo["email"], "shrutinair@google.com")
	}
}

func TestChain_AuthenticateToken(t *testing.T) {
	ti1 := newTestIssuer(t)
	ti2 := newTestIssuer(t)
	now := time.Now()

	tok2 := ti2.mintJWT(t, map[string]any{
		"iss":   ti2.issuer(),
		"sub":   "sub-2",
		"email": "dev@example.com",
		"aud":   "aud-2",
		"exp":   now.Add(time.Hour).Unix(),
	})

	chain := Chain{
		New(OIDCAuthenticatorConfig{
			IssuerURL: ti1.issuer(),
			Audiences: []string{"aud-1"},
			ClaimMappings: ClaimMappings{
				Username: "claims.sub",
			},
		}, ti1.server.Client()),
		New(OIDCAuthenticatorConfig{
			IssuerURL: ti2.issuer(),
			Audiences: []string{"aud-2"},
			ClaimMappings: ClaimMappings{
				Username: "claims.email",
			},
		}, ti2.server.Client()),
	}

	user, ok, err := chain.AuthenticateToken(context.Background(), tok2)
	if err != nil {
		t.Fatalf("chain.AuthenticateToken() err = %v, want nil", err)
	}
	if !ok || user == nil {
		t.Fatalf("chain.AuthenticateToken() ok = false or user is nil, want user")
	}
	if user.ID != "dev@example.com" {
		t.Errorf("user.ID = %q, want %q", user.ID, "dev@example.com")
	}
}

func TestChain_VerificationFailureStopsChain(t *testing.T) {
	ti1 := newTestIssuer(t)
	now := time.Now()

	// Token issued by ti1 but expired
	tok := ti1.mintJWT(t, map[string]any{
		"iss": ti1.issuer(),
		"sub": "sub-1",
		"aud": "aud-1",
		"exp": now.Add(-time.Hour).Unix(),
	})

	chain := Chain{
		New(OIDCAuthenticatorConfig{
			IssuerURL: ti1.issuer(),
			Audiences: []string{"aud-1"},
			ClaimMappings: ClaimMappings{
				Username: "claims.sub",
			},
		}, ti1.server.Client()),
	}

	_, ok, err := chain.AuthenticateToken(context.Background(), tok)
	if err == nil {
		t.Fatalf("chain.AuthenticateToken() err = nil, want expired token error")
	}
	if !ok {
		t.Errorf("chain.AuthenticateToken() ok = false, want true (issuer matched)")
	}
}

func TestChain_BothKubernetesAndOIDC(t *testing.T) {
	k8sIssuer := newTestIssuer(t)
	oidcIssuer := newTestIssuer(t)
	unknownIssuer := newTestIssuer(t)
	now := time.Now()

	// Build the exact authenticator chain used by ate-api-server in main.go
	chain := Chain{
		// 1. Kubernetes Bound ServiceAccount Authenticator
		New(OIDCAuthenticatorConfig{
			IssuerURL: k8sIssuer.issuer(),
			Audiences: []string{"api.ate-system.svc"},
			ClaimMappings: ClaimMappings{
				Username: "claims.sub",
			},
		}, k8sIssuer.server.Client()),
		// 2. Human Google IDP Authenticator
		New(OIDCAuthenticatorConfig{
			IssuerURL: oidcIssuer.issuer(),
			Audiences: []string{"32555940559.apps.googleusercontent.com"},
			ClaimMappings: ClaimMappings{
				Username: "'google:' + claims.email",
			},
		}, oidcIssuer.server.Client()),
	}


	// Test Case 1: Kubernetes ServiceAccount token (issued by K8s)
	k8sTok := k8sIssuer.mintJWT(t, map[string]any{
		"iss": k8sIssuer.issuer(),
		"sub": "system:serviceaccount:ate-system:atelet",
		"aud": "api.ate-system.svc",
		"exp": now.Add(time.Hour).Unix(),
		"kubernetes.io": map[string]any{
			"namespace": "ate-system",
			"serviceaccount": map[string]any{
				"name": "atelet",
				"uid":  "sa-uid-123",
			},
		},
	})

	userK8s, ok, err := chain.AuthenticateToken(context.Background(), k8sTok)
	if err != nil {
		t.Fatalf("chain.AuthenticateToken(k8sTok) err = %v, want nil", err)
	}
	if !ok || userK8s == nil {
		t.Fatalf("chain.AuthenticateToken(k8sTok) ok = false or user is nil, want user")
	}
	if userK8s.ID != "system:serviceaccount:ate-system:atelet" {
		t.Errorf("userK8s.ID = %q, want %q", userK8s.ID, "system:serviceaccount:ate-system:atelet")
	}
	if len(userK8s.ExtraInfo["kubernetes.io/namespace"]) == 0 || userK8s.ExtraInfo["kubernetes.io/namespace"][0] != "ate-system" {
		t.Errorf("userK8s.ExtraInfo[kubernetes.io/namespace] = %v, want %q", userK8s.ExtraInfo["kubernetes.io/namespace"], "ate-system")
	}
	if len(userK8s.ExtraInfo["kubernetes.io/serviceaccount/name"]) == 0 || userK8s.ExtraInfo["kubernetes.io/serviceaccount/name"][0] != "atelet" {
		t.Errorf("userK8s.ExtraInfo[kubernetes.io/serviceaccount/name] = %v, want %q", userK8s.ExtraInfo["kubernetes.io/serviceaccount/name"], "atelet")
	}
	if len(userK8s.ExtraInfo["kubernetes.io/serviceaccount/uid"]) == 0 || userK8s.ExtraInfo["kubernetes.io/serviceaccount/uid"][0] != "sa-uid-123" {
		t.Errorf("userK8s.ExtraInfo[kubernetes.io/serviceaccount/uid] = %v, want %q", userK8s.ExtraInfo["kubernetes.io/serviceaccount/uid"], "sa-uid-123")
	}

	// Test Case 2: Human OIDC token (issued by Google IDP)
	oidcTok := oidcIssuer.mintJWT(t, map[string]any{
		"iss":   oidcIssuer.issuer(),
		"sub":   "114973134352974025410",
		"email": "shrutinair@google.com",
		"aud":   "32555940559.apps.googleusercontent.com",
		"exp":   now.Add(time.Hour).Unix(),
	})

	userOIDC, ok, err := chain.AuthenticateToken(context.Background(), oidcTok)
	if err != nil {
		t.Fatalf("chain.AuthenticateToken(oidcTok) err = %v, want nil", err)
	}
	if !ok || userOIDC == nil {
		t.Fatalf("chain.AuthenticateToken(oidcTok) ok = false or user is nil, want user")
	}
	if userOIDC.ID != "google:shrutinair@google.com" {
		t.Errorf("userOIDC.ID = %q, want %q", userOIDC.ID, "google:shrutinair@google.com")
	}

	// Test Case 3: Token from an unrecognized third issuer -> skipped (ok=false)
	unknownTok := unknownIssuer.mintJWT(t, map[string]any{
		"iss": unknownIssuer.issuer(),
		"sub": "some-user",
		"aud": "some-aud",
		"exp": now.Add(time.Hour).Unix(),
	})

	_, ok, err = chain.AuthenticateToken(context.Background(), unknownTok)
	if err != nil {
		t.Fatalf("chain.AuthenticateToken(unknownTok) err = %v, want nil", err)
	}
	if ok {
		t.Errorf("chain.AuthenticateToken(unknownTok) ok = true, want false (unrecognized issuer)")
	}
}

