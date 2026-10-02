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

package egress

import (
	"context"
	"fmt"
	"slices"
	"time"

	"golang.org/x/sync/singleflight"
	"k8s.io/apimachinery/pkg/util/cache"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// actorJWTMintTimeout caps the UID lookup and MintActorJWT call behind one
// token. It sits under the 5s ext_proc message_timeout.
const actorJWTMintTimeout = 3 * time.Second

// actorJWTs mints the actor JWTs that egress policies inject, and reuses each
// token until a third of its lifetime remains.
type actorJWTs struct {
	client ateapipb.ControlClient
	// uidTTL is how long a recorded actor UID is used before GetActor is asked
	// again. 0 records nothing.
	uidTTL time.Duration
	// uids maps a resources.ActorRef to the actor's UID.
	uids *cache.Expiring
	// tokens maps a tokenKey to a JWT until the token's refresh point.
	tokens *cache.Expiring
	// flight collapses concurrent mints of one token into a single call.
	flight singleflight.Group
}

func newActorJWTs(client ateapipb.ControlClient, uidTTL time.Duration) *actorJWTs {
	return &actorJWTs{
		client: client,
		uidTTL: uidTTL,
		uids:   cache.NewExpiring(),
		tokens: cache.NewExpiring(),
	}
}

// recordUID notes the UID that the CONNECT leg read for ref, so requests inside
// the tunnel can mint without calling GetActor.
func (c *actorJWTs) recordUID(ref resources.ActorRef, uid string) {
	if c.uidTTL > 0 {
		c.uids.Set(ref, uid, c.uidTTL)
	}
}

// get returns a JWT for the actor ref names, bound to src's audiences and
// lifetime. Errors from GetActor and MintActorJWT are returned wrapped.
func (c *actorJWTs) get(ctx context.Context, ref resources.ActorRef, src *ateapipb.ActorJWTSource) (string, error) {
	audiences := slices.Sorted(slices.Values(src.GetAudiences()))
	lifetime := src.GetExpirationSeconds()
	if uid, ok := cachedString(c.uids, ref); ok {
		if jwt, ok := cachedString(c.tokens, tokenKey(uid, audiences, lifetime)); ok {
			return jwt, nil
		}
	}

	// The mint outlives the caller: the leader of a flight going away must
	// not fail the callers that joined it.
	ch := c.flight.DoChan(fmt.Sprintf("%s %d %q", ref, lifetime, audiences), func() (any, error) {
		mintCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), actorJWTMintTimeout)
		defer cancel()
		return c.mint(mintCtx, ref, audiences, lifetime)
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

// mint resolves ref's UID and mints a token for it, unless a flight that just
// finished has already stored one.
func (c *actorJWTs) mint(ctx context.Context, ref resources.ActorRef, audiences []string, lifetime int64) (string, error) {
	uid, ok := cachedString(c.uids, ref)
	if !ok {
		actor, err := c.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref.ToObjectRef()})
		if err != nil {
			return "", fmt.Errorf("looking up actor %s: %w", ref, err)
		}
		uid = actor.GetMetadata().GetUid()
		c.recordUID(ref, uid)
	}
	key := tokenKey(uid, audiences, lifetime)
	if jwt, ok := cachedString(c.tokens, key); ok {
		return jwt, nil
	}

	resp, err := c.client.MintActorJWT(ctx, &ateapipb.MintActorJWTRequest{
		Actor:             ref.ToObjectRef(),
		ActorUid:          uid,
		Audience:          audiences,
		ExpirationSeconds: lifetime,
	})
	if err != nil {
		return "", fmt.Errorf("minting an actor JWT for %s: %w", ref, err)
	}
	reuseFor := time.Until(resp.GetExpiresAt().AsTime()) - time.Duration(lifetime)*time.Second/3
	c.tokens.Set(key, resp.GetActorJwt(), reuseFor)
	return resp.GetActorJwt(), nil
}

func cachedString(e *cache.Expiring, key any) (string, bool) {
	v, ok := e.Get(key)
	s, _ := v.(string)
	return s, ok
}

// tokenKey identifies the token for one actor UID, sorted audience list, and
// lifetime. Keying on the UID keeps a recreated actor from getting its
// predecessor's token.
func tokenKey(uid string, audiences []string, lifetime int64) string {
	return fmt.Sprintf("%s %d %q", uid, lifetime, audiences)
}
