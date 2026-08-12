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
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/tinyjwt"
)





// UserInfo contains authenticated principal identity information and extra claims,
// mirroring k8s.io/apiserver/pkg/authentication/user.Info.
type UserInfo struct {
	ID        string
	ExtraInfo map[string][]string
}

// Authenticator evaluates a Bearer token and returns authenticated principal user info.
//
// ok == false indicates that the token was not recognized by this authenticator
// (e.g., unrecognized issuer), so the chain should try the next authenticator.
// ok == true indicates that the token was evaluated by this authenticator; if err != nil,
// verification failed (e.g. expired token or invalid signature) and the chain stops.
type Authenticator interface {
	AuthenticateToken(ctx context.Context, token string) (user *UserInfo, ok bool, err error)
}

// Chain evaluates multiple Authenticators sequentially.
type Chain []Authenticator

// AuthenticateToken calls each authenticator in sequence.
// The first authenticator whose issuer matches the token evaluates it.
// If verification succeeds, the principal user info is returned with ok=true, err=nil.
// If verification fails, authentication fails immediately with ok=true, err!=nil.
// If no authenticator recognizes the token issuer, ok=false, err=nil is returned.
func (c Chain) AuthenticateToken(ctx context.Context, token string) (*UserInfo, bool, error) {
	for _, auth := range c {
		user, ok, err := auth.AuthenticateToken(ctx, token)
		if err != nil {
			return nil, true, err
		}
		if ok {
			return user, true, nil
		}
	}
	return nil, false, nil
}

// ClaimMappings defines how identity claims are mapped via CEL expressions,
// mirroring Kubernetes' apiserver.config.k8s.io/v1 AuthenticationConfiguration API.
type ClaimMappings struct {
	// Username defines the CEL expression for mapping the principal ID from JWT claims.
	// E.g. 'claims.sub', 'claims.email', or '"google:" + claims.email'.
	Username string
}

// OIDCAuthenticatorConfig defines the configuration for an OIDC authenticator,
// mirroring the structured AuthenticationConfiguration API in Kubernetes.
type OIDCAuthenticatorConfig struct {
	// IssuerURL is the OIDC issuer URL (e.g., "https://accounts.google.com").
	IssuerURL string
	// Audiences is the list of acceptable audience values in the token.
	Audiences []string
	// ClaimMappings specifies CEL expressions for mapping user identity attributes from claims.
	ClaimMappings ClaimMappings
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
func (a *OIDCAuthenticator) AuthenticateToken(ctx context.Context, token string) (*UserInfo, bool, error) {

	if a.cfg.IssuerURL == "" {
		return nil, false, nil
	}

	claims, err := tinyjwt.Verify(ctx, a.httpClient, token, a.cfg.IssuerURL, a.cfg.Audiences, a.now())

	if err != nil {
		// If verification failed because of unexpected issuer, return ok=false so chain can continue.
		if errors.Is(err, tinyjwt.ErrIssuerMismatch) {
			return nil, false, nil
		}
		return nil, true, err
	}

	username := evaluateUsernameExpression(a.cfg.ClaimMappings.Username, claims)

	extraInfo := make(map[string][]string)
	if claims.Issuer != "" {
		extraInfo["iss"] = []string{claims.Issuer}
	}
	if claims.Subject != "" {
		extraInfo["sub"] = []string{claims.Subject}
	}
	if claims.Email != "" {
		extraInfo["email"] = []string{claims.Email}
	}
	if claims.JTI != "" {
		extraInfo["jti"] = []string{claims.JTI}
	}
	if claims.Kubernetes != nil {
		if claims.Kubernetes.Namespace != "" {
			extraInfo["kubernetes.io/namespace"] = []string{claims.Kubernetes.Namespace}
		}
		if claims.Kubernetes.ServiceAccountName != "" {
			extraInfo["kubernetes.io/serviceaccount/name"] = []string{claims.Kubernetes.ServiceAccountName}
		}
		if claims.Kubernetes.ServiceAccountUID != "" {
			extraInfo["kubernetes.io/serviceaccount/uid"] = []string{claims.Kubernetes.ServiceAccountUID}
		}
		if claims.Kubernetes.PodName != "" {
			extraInfo["kubernetes.io/pod/name"] = []string{claims.Kubernetes.PodName}
		}
		if claims.Kubernetes.PodUID != "" {
			extraInfo["kubernetes.io/pod/uid"] = []string{claims.Kubernetes.PodUID}
		}
		if claims.Kubernetes.NodeName != "" {
			extraInfo["kubernetes.io/node/name"] = []string{claims.Kubernetes.NodeName}
		}
		if claims.Kubernetes.NodeUID != "" {
			extraInfo["kubernetes.io/node/uid"] = []string{claims.Kubernetes.NodeUID}
		}
		if claims.Kubernetes.SecretName != "" {
			extraInfo["kubernetes.io/secret/name"] = []string{claims.Kubernetes.SecretName}
		}
		if claims.Kubernetes.SecretUID != "" {
			extraInfo["kubernetes.io/secret/uid"] = []string{claims.Kubernetes.SecretUID}
		}
	}

	return &UserInfo{
		ID:        username,
		ExtraInfo: extraInfo,
	}, true, nil
}

// evaluateUsernameExpression evaluates simple username mapping expressions.
// TODO(shrutinair): Integrate full CEL evaluation (google/cel-go) once AuthenticationConfiguration YAML loading is added.
func evaluateUsernameExpression(expr string, claims *tinyjwt.Claims) string {
	switch strings.TrimSpace(expr) {
	case "claims.email":
		if claims.Email != "" {
			return claims.Email
		}
		return claims.Subject
	case `'google:' + claims.email`, `"google:" + claims.email`:
		if claims.Email != "" {
			return "google:" + claims.Email
		}
		return "google:" + claims.Subject
	case "claims.sub", "":
		return claims.Subject
	default:
		return claims.Subject
	}
}

