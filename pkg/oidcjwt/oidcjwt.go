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

// Package oidcjwt provides a high-level OIDC JWT verifier for Substrate Actor
// tokens. External sandbox gateways and compute drivers (such as out-of-tree
// microVM or OpenShell drivers) use it to verify egress-injected Actor JWTs
// against ate-idp-server OIDC discovery and JWKS endpoints and extract actor
// claims without duplicating internal JWT verification logic.
//
// This package imports packages under internal/, so that an external driver
// runs the same verification logic as in-tree components and only this package's
// API is public. Go allows it: the internal rule checks the importer's path, and
// every package under go.ate.dev/substrate may import
// go.ate.dev/substrate/internal/..., while code outside the module may import
// this package.
package oidcjwt

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/credbundle"
	internaloidc "go.ate.dev/substrate/internal/oidcjwt"
)

// MaxJWTSize is the largest token, in bytes, Verify accepts.
const MaxJWTSize = 8 << 10

// Kind represents who an actor token speaks for.
type Kind int

const (
	// KindUnspecified is the zero value indicating an unrecognized token kind.
	KindUnspecified Kind = iota

	// KindActor represents a token minted for an actor itself. Egress gateways
	// inject KindActor tokens into outbound requests matching an ActorJwtSource
	// egress rule.
	KindActor

	// KindWorkerForActor represents a token minted for a Substrate component
	// or worker acting on behalf of an actor (e.g. transfer and storage exchanges).
	KindWorkerForActor
)

func (k Kind) String() string {
	switch k {
	case KindActor:
		return "actor"
	case KindWorkerForActor:
		return "worker-for-actor"
	default:
		return "unspecified"
	}
}

// ActorClaims contains Substrate-specific actor identity and placement claims.
type ActorClaims struct {
	Atespace  string
	ActorName string
	ActorUID  string
	Node      string
}

// Claims contains the verified standard JWT claims and Substrate actor claims.
type Claims struct {
	// Standard claims from RFC 7519
	Issuer     string
	Subject    string
	Audiences  []string
	Expiration time.Time
	NotBefore  time.Time
	IssuedAt   time.Time
	JTI        string

	// Substrate-specific claims
	Kind        Kind
	ActorClaims *ActorClaims
}

// Config configures the Verifier.
type Config struct {
	// Issuer is the OIDC issuer URL (e.g. "https://ate-idp-server.ate-system.svc").
	// It is required. It must be https:// followed by a host and optional port,
	// with no path, query or fragment.
	Issuer string

	// IssuerCAFile is the path to a PEM CA bundle that verifies the issuer's
	// serving certificate. For an issuer whose host ends in .svc, it is the
	// only trusted bundle. For external issuers, system roots are trusted as
	// well. It is required. It is re-read when the file changes.
	IssuerCAFile string

	// Audiences is the list of acceptable audiences. At least one audience is required.
	Audiences []string
}

// Verifier verifies Substrate actor tokens using OIDC discovery. It is safe for
// concurrent use.
type Verifier struct {
	issuer    string
	audiences []string
	inner     *internaloidc.Verifier
}

// New returns a Verifier configured with cfg. It immediately discovers the
// issuer's OIDC configuration and fetches the initial JWKS set.
func New(ctx context.Context, cfg Config) (*Verifier, error) {
	if cfg.Issuer == "" {
		return nil, errors.New("oidcjwt: Issuer is required")
	}
	if err := actoridjwt.ValidateIssuer(cfg.Issuer); err != nil {
		return nil, fmt.Errorf("oidcjwt: %w", err)
	}
	if cfg.IssuerCAFile == "" {
		return nil, errors.New("oidcjwt: IssuerCAFile is required")
	}
	if len(cfg.Audiences) == 0 {
		return nil, errors.New("oidcjwt: at least one audience is required")
	}

	loadRoots, err := issuerRoots(cfg.Issuer, cfg.IssuerCAFile)
	if err != nil {
		return nil, fmt.Errorf("oidcjwt: %w", err)
	}
	client, err := internaloidc.NewHTTPClientWithRoots(loadRoots)
	if err != nil {
		return nil, fmt.Errorf("oidcjwt: setting up HTTP client: %w", err)
	}

	opts := []internaloidc.Option{
		internaloidc.WithSubstrateClaims(),
	}

	keySource := internaloidc.HTTPKeySource(cfg.Issuer, client)
	inner := internaloidc.NewVerifier(cfg.Issuer, cfg.Audiences, keySource, opts...)

	if err := inner.Refresh(ctx, time.Now()); err != nil {
		client.CloseIdleConnections()
		return nil, fmt.Errorf("oidcjwt: warming key source: %w", err)
	}

	return &Verifier{
		issuer:    cfg.Issuer,
		audiences: cfg.Audiences,
		inner:     inner,
	}, nil
}

// Verify validates rawJWT against the issuer's keys and returns the parsed claims.
func (v *Verifier) Verify(ctx context.Context, rawJWT string) (*Claims, error) {
	if rawJWT == "" {
		return nil, errors.New("oidcjwt: rawJWT is required")
	}
	if len(rawJWT) > MaxJWTSize {
		return nil, fmt.Errorf("oidcjwt: rawJWT is longer than %d bytes", MaxJWTSize)
	}

	parsed, err := v.inner.Verify(ctx, rawJWT, time.Now())
	if err != nil {
		return nil, err
	}

	var kind Kind
	switch parsed.SubstrateClaims.Kind {
	case actoridjwt.KindActor:
		kind = KindActor
	case actoridjwt.KindWorkerForActor:
		kind = KindWorkerForActor
	default:
		return nil, errors.New("oidcjwt: token carries neither actor.ate.dev nor worker-for-actor.ate.dev claim")
	}

	sub := parsed.SubstrateClaims.Substrate
	if sub == nil || sub.Atespace == "" || sub.ActorName == "" || sub.ActorUID == "" {
		return nil, errors.New("oidcjwt: actor token must carry atespace, actor name, and actor UID")
	}
	if kind == KindWorkerForActor && sub.Node == "" {
		return nil, errors.New("oidcjwt: worker-for-actor token must carry node")
	}
	if kind == KindActor && sub.Node != "" {
		return nil, errors.New("oidcjwt: an actor token carries no node")
	}

	actorClaims := &ActorClaims{
		Atespace:  sub.Atespace,
		ActorName: sub.ActorName,
		ActorUID:  sub.ActorUID,
		Node:      sub.Node,
	}

	return &Claims{
		Issuer:      parsed.Issuer,
		Subject:     parsed.Subject,
		Audiences:   parsed.Audiences,
		Expiration:  parsed.Expiration,
		NotBefore:   parsed.NotBefore,
		IssuedAt:    parsed.IssuedAt,
		JTI:         parsed.JTI,
		Kind:        kind,
		ActorClaims: actorClaims,
	}, nil
}

// issuerRoots loads certificates based on whether the issuer is an in-cluster .svc or external.
func issuerRoots(issuer, caFile string) (func() (*x509.CertPool, error), error) {
	u, err := url.Parse(issuer)
	if err != nil {
		return nil, fmt.Errorf("invalid issuer URL: %w", err)
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if strings.HasSuffix(host, ".svc") {
		return credbundle.PoolLoader(caFile), nil
	}
	return credbundle.SystemPoolLoader(caFile), nil
}
