//go:build linux

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

package ateomtunnel

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateletdial"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
)

const (
	// egressPolicyTTL is how long an actor's egress policy is reused before it
	// is fetched again.
	//
	// TODO: tune once the cost of fetching is known at scale.
	egressPolicyTTL = 5 * time.Second

	// egressPolicyFetchTimeout bounds one fetch.
	egressPolicyFetchTimeout = 2 * time.Second
)

// actorEgressPolicy supplies an actor's egress policy to its DNS relay.
type actorEgressPolicy struct {
	fetch func(context.Context) (*ateletpb.EgressPolicy, error)
	ttl   time.Duration
	now   func() time.Time
	// log identifies the actor on the source's records.
	log *slog.Logger

	// flight shares one fetch among the queries waiting on it to avoid duplicate
	// requests for the same information.
	flight singleflight.Group

	mu      sync.Mutex
	fetched bool
	policy  *ateletpb.EgressPolicy
	expires time.Time
	// failing is set while fetches fail, so that each run of failures is
	// logged once.
	failing bool
}

// ActorEgressPolicy returns the source of actor's egress policy for its DNS
// relay.
func (t *Tunnel) ActorEgressPolicy(actor resources.ActorAttribution) *actorEgressPolicy {
	return &actorEgressPolicy{
		fetch: func(ctx context.Context) (*ateletpb.EgressPolicy, error) {
			conn, err := ateletdial.Dial(nodepath.AteomSupportSocket, t.ateletTLS)
			if err != nil {
				return nil, err
			}
			defer conn.Close()
			return getEgressPolicy(ctx, ateletpb.NewAteomSupportClient(conn), actor.Ref)
		},
		ttl: egressPolicyTTL,
		now: time.Now,
		log: slog.New(slog.Default().Handler().WithAttrs(ateattr.ActorLogAttrs(actor))),
	}
}

// EgressPolicy returns the actor's egress policy, or nil if it has none.
//
// Callers must not modify the policy as it may be shared in response to concurrent calls
// to EgressPolicy.
func (p *actorEgressPolicy) EgressPolicy(ctx context.Context) (*ateletpb.EgressPolicy, error) {
	if policy, ok := p.cached(); ok {
		return policy, nil
	}
	results := p.flight.DoChan("", func() (any, error) {
		// A fetch may have finished since this query found the policy stale.
		if policy, ok := p.cached(); ok {
			return policy, nil
		}
		// Not canceled with the query that started the fetch: the others
		// waiting on it still need its result.
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), egressPolicyFetchTimeout)
		defer cancel()
		policy, err := p.fetch(fetchCtx)
		p.record(ctx, policy, err)
		return policy, err
	})
	select {
	case res := <-results:
		if res.Err != nil {
			return nil, res.Err
		}
		policy, _ := res.Val.(*ateletpb.EgressPolicy)
		return policy, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// cached returns the policy last fetched, unless it has expired.
func (p *actorEgressPolicy) cached() (*ateletpb.EgressPolicy, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.fetched || !p.now().Before(p.expires) {
		return nil, false
	}
	return p.policy, true
}

// record keeps a fetch's result. It logs when the actor gains or loses its
// policy, and when fetches start failing.
func (p *actorEgressPolicy) record(ctx context.Context, policy *ateletpb.EgressPolicy, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err != nil {
		level := slog.LevelDebug
		if !p.failing {
			level = slog.LevelWarn
		}
		p.failing = true
		p.log.LogAttrs(ctx, level, "Could not fetch the actor's egress policy; refusing its DNS queries", slog.Any("err", err))
		return
	}

	if p.failing || !p.fetched || (policy == nil) != (p.policy == nil) {
		if policy == nil {
			p.log.InfoContext(ctx, "Actor has no egress policy; blocking all DNS queries")
		} else {
			p.log.InfoContext(ctx, "Actor has an egress policy")
		}
	}

	p.failing = false
	p.fetched = true
	p.policy = policy
	p.expires = p.now().Add(p.ttl)
}

// getEgressPolicy asks atelet for actor's egress policy, returning nil if the
// actor has none.
func getEgressPolicy(ctx context.Context, client ateletpb.AteomSupportClient, actor resources.ActorRef) (*ateletpb.EgressPolicy, error) {
	resp, err := client.GetActorEgressPolicy(ctx, &ateletpb.GetActorEgressPolicyRequest{
		ActorAtespace: actor.Atespace,
		ActorName:     actor.Name,
	})
	if err != nil {
		return nil, fmt.Errorf("while fetching the actor's egress policy: %w", err)
	}
	return resp.GetEgressPolicy(), nil
}
