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

// Package oidcauth implements pluggable OIDC authentication chaining for the ate-api-server,
// mirroring Kubernetes' structured AuthenticationConfiguration API.
package oidcauth


import (
	"context"
	"net/http"
	"strings"
	"time"
)





// Authenticator evaluates a Bearer token and returns an authenticated principal ID.
//
// ok == false indicates that the token was not recognized by this authenticator
// (e.g., unrecognized issuer), so the chain should try the next authenticator.
// ok == true indicates that the token was evaluated by this authenticator; if err != nil,
// verification failed (e.g. expired token or invalid signature) and the chain stops.
type Authenticator interface {
	AuthenticateToken(ctx context.Context, token string) (id string, ok bool, err error)
}

// Chain evaluates multiple Authenticators sequentially.
type Chain []Authenticator

// AuthenticateToken calls each authenticator in sequence.
// The first authenticator whose issuer matches the token evaluates it.
// If verification succeeds, the principal ID is returned with ok=true, err=nil.
// If verification fails, authentication fails immediately with ok=true, err!=nil.
// If no authenticator recognizes the token issuer, ok=false, err=nil is returned.
func (c Chain) AuthenticateToken(ctx context.Context, token string) (string, bool, error) {
	for _, auth := range c {
		id, ok, err := auth.AuthenticateToken(ctx, token)
		if err != nil {
			return "", true, err
		}
		if ok {
			return id, true, nil
		}
	}
	return "", false, nil
}

// OIDCAuthenticatorConfig defines the configuration for an OIDC authenticator,
// mirroring the structured AuthenticationConfiguration API in Kubernetes.
type OIDCAuthenticatorConfig struct {
	// IssuerURL is the OIDC issuer URL (e.g., "https://accounts.google.com").
	IssuerURL string
	// Audiences is the list of acceptable audience values in the token.
	Audiences []string
	// UsernameClaim specifies which JWT claim to map to the principal ID ("email" or "sub").
	// If empty or if the claim is absent in the token, it falls back to the "sub" claim.
	UsernameClaim string
	// UsernamePrefix is prepended to the extracted username claim (if non-empty).
	UsernamePrefix string
}

// OIDCAuthenticator implements Authenticator for a specific OIDC issuer.
type OIDCAuthenticator struct {
	cfg        OIDCAuthenticatorConfig
	httpClient *http.Client
	now        func() time.Time
}

// New creates a new OIDCAuthenticator.
func New(cfg OIDCAuthenticatorConfig, httpClient *http.Client) *OIDCAuthenticator {
	return &OIDCAuthenticator{
		cfg:        cfg,
		httpClient: httpClient,
		now:        time.Now,
	}
}

// AuthenticateToken verifies the Bearer token against this authenticator's configured issuer and audiences.
func (a *OIDCAuthenticator) AuthenticateToken(ctx context.Context, token string) (string, bool, error) {

	if a.cfg.IssuerURL == "" {
		return "", false, nil
	}

	claims, err := Verify(ctx, a.httpClient, token, a.cfg.IssuerURL, a.cfg.Audiences, a.now())


	if err != nil {
		// If verification failed because of unexpected issuer, return ok=false so chain can continue.
		if isIssuerMismatch(err) {
			return "", false, nil
		}
		return "", true, err
	}

	username := claims.Subject
	if a.cfg.UsernameClaim == "email" && claims.Email != "" {
		username = claims.Email
	}

	if a.cfg.UsernamePrefix != "" {
		username = a.cfg.UsernamePrefix + username
	}

	return username, true, nil
}

func isIssuerMismatch(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), "unexpected issuer")
}

