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

// Package actorjwt mints the actor JWTs that egress policies inject.
package actorjwt

import (
	"context"
	"fmt"
	"slices"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/cache"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Minter mints actor JWTs and reuses each token until a third of its lifetime
// remains. Tokens are cached by actor name, so an actor recreated under the
// same name can get its predecessor's token until then.
type Minter struct {
	client ateapipb.ControlClient
	// tokens maps a tokenKey to a JWT until the token's refresh point.
	tokens *cache.Expiring
	// flight collapses concurrent mints of one token into a single call.
	flight singleflight.Group
}

// New returns a Minter that mints through client.
func New(client ateapipb.ControlClient) *Minter {
	return &Minter{client: client, tokens: cache.NewExpiring()}
}

// Token returns a JWT for the actor ref names, bound to src's audiences and
// lifetime. MintActorJWT errors are returned wrapped.
func (m *Minter) Token(ctx context.Context, ref resources.ActorRef, src *ateapipb.ActorJWTSource) (string, error) {
	req := &ateapipb.MintActorJWTRequest{
		Actor:             ref.ToObjectRef(),
		Audience:          slices.Sorted(slices.Values(src.GetAudiences())),
		ExpirationSeconds: src.GetExpirationSeconds(),
	}
	key, err := tokenKey(req)
	if err != nil {
		return "", err
	}
	if jwt, ok := m.cached(key); ok {
		return jwt, nil
	}

	// The mint outlives the caller: the leader of a flight going away must
	// not fail the callers that joined it. It has no timeout of its own, so a
	// slow mint still lands in the cache for the next request.
	ch := m.flight.DoChan(key, func() (any, error) {
		return m.mint(context.WithoutCancel(ctx), key, ref, req)
	})
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return "", res.Err
		}
		return res.Val.(string), nil
	}
}

// mint sends req and caches the token under key, unless a flight that just
// finished has already stored one.
func (m *Minter) mint(ctx context.Context, key string, ref resources.ActorRef, req *ateapipb.MintActorJWTRequest) (string, error) {
	if jwt, ok := m.cached(key); ok {
		return jwt, nil
	}
	resp, err := m.client.MintActorJWT(ctx, req)
	if err != nil {
		return "", fmt.Errorf("minting an actor JWT for %s: %w", ref, err)
	}
	reuseFor := time.Until(resp.GetExpiresAt().AsTime()) - time.Duration(req.GetExpirationSeconds())*time.Second/3
	m.tokens.Set(key, resp.GetActorJwt(), reuseFor)
	return resp.GetActorJwt(), nil
}

func (m *Minter) cached(key string) (string, bool) {
	v, ok := m.tokens.Get(key)
	s, _ := v.(string)
	return s, ok
}

// tokenKey identifies the token req mints by req's own encoding. Deterministic
// encoding is stable within one binary, which is all an in-process cache needs.
func tokenKey(req *ateapipb.MintActorJWTRequest) (string, error) {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("encoding the token key: %w", err)
	}
	return string(b), nil
}
