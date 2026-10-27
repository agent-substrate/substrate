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

package tokenexchange

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/cache"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/resources"
)

const (
	// subjectTokenSeconds is the subject JWT's lifetime, the least
	// MintActorJwt accepts. Only the exchange uses it.
	subjectTokenSeconds = 300
	// exchangeTimeout bounds an exchange that has outlived its caller. Without
	// it, an endpoint that never answers would hold the flight for its key
	// forever.
	exchangeTimeout = 10 * time.Second
	// minReuseMargin is the least lifetime left at which a token stops being
	// reused.
	minReuseMargin = 30 * time.Second
	// unknownExpiryReuse is how long a token without expires_in is reused.
	unknownExpiryReuse = 60 * time.Second
)

// ErrMint wraps a failure to mint the subject JWT, so callers can tell it from
// a failed exchange.
var ErrMint = errors.New("minting the subject token")

// subjectMinter mints actor JWTs; *actorjwt.Minter implements it.
type subjectMinter interface {
	Token(ctx context.Context, ref resources.ActorRef, src *ateapi.ActorJwtSource) (string, error)
}

// Broker exchanges actor JWTs for access tokens and reuses each access token
// until a third of its lifetime, or 30 seconds, remains, whichever comes
// first. Tokens are cached by actor name, so an actor recreated under the
// same name can get its predecessor's token until then.
type Broker struct {
	minter subjectMinter
	client *Client
	// tokens maps a tokenKey to an access token until it stops being reused.
	tokens *cache.Expiring
	// flight collapses concurrent exchanges of one token into a single call.
	flight singleflight.Group
}

// NewBroker returns a Broker that mints subject JWTs with minter and exchanges
// them with client.
func NewBroker(minter subjectMinter, client *Client) *Broker {
	return &Broker{minter: minter, client: client, tokens: cache.NewExpiring()}
}

// Token returns an access token for the actor ref names, exchanged as src
// describes. Errors carry a gRPC status code as Client.Exchange's do, and a
// failed mint also wraps ErrMint.
func (b *Broker) Token(ctx context.Context, ref resources.ActorRef, src *ateapi.TokenExchangeSource) (string, error) {
	scopes := slices.Sorted(slices.Values(src.GetScopes()))
	key := tokenKey(ref, src, scopes)
	if token, ok := b.cached(key); ok {
		return token, nil
	}

	// The exchange outlives the caller, so a slow endpoint still fills the
	// cache for the next request.
	ch := b.flight.DoChan(key, func() (any, error) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), exchangeTimeout)
		defer cancel()
		return b.exchange(ctx, key, ref, src, scopes)
	})
	select {
	case <-ctx.Done():
		return "", status.FromContextError(ctx.Err()).Err()
	case res := <-ch:
		if res.Err != nil {
			return "", res.Err
		}
		return res.Val.(string), nil
	}
}

// exchange mints a subject JWT, exchanges it, and caches the access token
// under key, unless a flight that just finished has already stored one.
func (b *Broker) exchange(ctx context.Context, key string, ref resources.ActorRef, src *ateapi.TokenExchangeSource, scopes []string) (string, error) {
	if token, ok := b.cached(key); ok {
		return token, nil
	}
	subject, err := b.minter.Token(ctx, ref, &ateapi.ActorJwtSource{
		Audiences:         []string{src.GetAudience()},
		ExpirationSeconds: subjectTokenSeconds,
	})
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrMint, err)
	}
	token, err := b.client.Exchange(ctx, src.GetTokenUrl(), subject, src.GetAudience(), scopes)
	if err != nil {
		return "", fmt.Errorf("exchanging a token for %s at %s: %w", ref, src.GetTokenUrl(), err)
	}
	if reuse := reuseFor(token); reuse > 0 {
		b.tokens.Set(key, token.AccessToken, reuse)
	}
	return token.AccessToken, nil
}

// reuseFor is how long token is served from the cache.
func reuseFor(token Token) time.Duration {
	if token.ExpiresAt.IsZero() {
		return unknownExpiryReuse
	}
	lifetime := time.Until(token.ExpiresAt)
	return lifetime - max(lifetime/3, minReuseMargin)
}

func (b *Broker) cached(key string) (string, bool) {
	v, ok := b.tokens.Get(key)
	s, _ := v.(string)
	return s, ok
}

// tokenKey identifies the token an exchange for ref returns: everything the
// exchange request carries except the subject JWT, which is itself cached per
// actor. It is a string because singleflight keys are strings.
func tokenKey(ref resources.ActorRef, src *ateapi.TokenExchangeSource, scopes []string) string {
	return fmt.Sprintf("%q %q %q %q %q", ref.Atespace, ref.Name, src.GetTokenUrl(), src.GetAudience(), strings.Join(scopes, " "))
}
