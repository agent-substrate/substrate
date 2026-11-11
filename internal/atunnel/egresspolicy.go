//go:build linux

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

package atunnel

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/ateletdial"
	"go.ate.dev/substrate/internal/atunnel/dns"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
)

const (
	// egressPolicyTTL is how long an actor's egress policy is reused before it
	// is fetched again.
	//
	// TODO: tune once the cost of fetching is known at scale.
	egressPolicyTTL = 5 * time.Second

	// egressPolicyFetchTimeout bounds one fetch.
	egressPolicyFetchTimeout = 2 * time.Second

	// egressPolicyStaleWarnInterval is the minimum interval between warning
	// logs while a stale cached policy is used because atelet is unreachable.
	egressPolicyStaleWarnInterval = 5 * time.Minute
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
	// failing is set while fetches fail, so that the first failure in a run is
	// logged at error level and subsequent stale-cache warnings are rate-limited.
	failing       bool
	nextStaleWarn time.Time
	// nextRetry bounds background refresh attempts while fetches keep failing
	// to one per ttl.
	nextRetry time.Time
}

// ActorEgressPolicy returns the source of actor's egress policy for its DNS
// relay.
func (t *Tunnel) ActorEgressPolicy(actor resources.ActorAttribution) dns.PolicySource {
	return &actorEgressPolicy{
		fetch: func(ctx context.Context) (*ateletpb.EgressPolicy, error) {
			conn, err := ateletdial.Dial(nodepath.WorkerSupportSocket, t.ateletTLS)
			if err != nil {
				return nil, err
			}
			defer conn.Close()
			return getEgressPolicy(ctx, ateletpb.NewWorkerSupportClient(conn), actor.Ref)
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
	// While refreshes fail, answer with the stale entry without waiting and
	// let a background fetch retry at most once per TTL. Waiting here would
	// stall the relay: it reads the actor's queries one at a time, so the
	// queries queued behind one that waits egressPolicyFetchTimeout for a
	// fetch that keeps failing would each wait too, and each would start
	// another failing fetch.
	if policy, ok := p.staleWhileFailing(); ok {
		p.refreshInBackground()
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
		return p.record(ctx, policy, err)
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

// staleWhileFailing returns the last fetched policy while refreshes of it keep
// failing, even though the entry has expired. It is the fail-static answer the
// relay documents for an unreachable atelet, served without waiting for the
// next refresh attempt.
func (p *actorEgressPolicy) staleWhileFailing() (*ateletpb.EgressPolicy, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.fetched || !p.failing {
		return nil, false
	}
	return p.policy, true
}

// refreshInBackground starts one fetch of the policy in the background, at
// most once per ttl, while queries are being answered with the stale entry.
// Its result updates the cache like any other fetch's, so recovery takes
// effect without waiting for an entry to expire.
func (p *actorEgressPolicy) refreshInBackground() {
	p.mu.Lock()
	now := p.now()
	due := p.nextRetry.IsZero() || !now.Before(p.nextRetry)
	if due {
		p.nextRetry = now.Add(p.ttl)
	}
	p.mu.Unlock()
	if !due {
		return
	}
	go func() {
		// Not canceled with any query: no query is waiting on it.
		ctx := context.Background()
		results := p.flight.DoChan("", func() (any, error) {
			// A fetch may have finished since this retry was scheduled.
			if policy, ok := p.cached(); ok {
				return policy, nil
			}
			fetchCtx, cancel := context.WithTimeout(ctx, egressPolicyFetchTimeout)
			defer cancel()
			policy, err := p.fetch(fetchCtx)
			return p.record(ctx, policy, err)
		})
		<-results
	}()
}

// record updates the cache with a fetch's result and returns the policy and
// error EgressPolicy should report. When a refresh fails after a policy has
// already been fetched, record keeps and returns the stale entry (fail static),
// logging an error on the first failure and a warning every
// egressPolicyStaleWarnInterval while stale entries are used.
func (p *actorEgressPolicy) record(ctx context.Context, policy *ateletpb.EgressPolicy, err error) (*ateletpb.EgressPolicy, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.now()
	if err != nil {
		if !p.fetched {
			level := slog.LevelDebug
			if !p.failing {
				level = slog.LevelError
			}
			p.failing = true
			p.log.LogAttrs(ctx, level, "Could not fetch the actor's egress policy; refusing its DNS queries", slog.Any("err", err))
			return nil, err
		}

		level := slog.LevelDebug
		switch {
		case !p.failing:
			level = slog.LevelError
			p.failing = true
			p.nextStaleWarn = now.Add(egressPolicyStaleWarnInterval)
		case !now.Before(p.nextStaleWarn):
			level = slog.LevelWarn
			p.nextStaleWarn = now.Add(egressPolicyStaleWarnInterval)
		}
		p.log.LogAttrs(ctx, level, "Could not refresh the actor's egress policy; using stale cached policy", slog.Any("err", err))
		return p.policy, nil
	}

	if p.failing || !p.fetched || (policy == nil) != (p.policy == nil) {
		if policy == nil {
			p.log.InfoContext(ctx, "Actor has no egress policy; blocking all DNS queries")
		} else {
			p.log.InfoContext(ctx, "Actor has an egress policy")
		}
	}

	p.failing = false
	p.nextStaleWarn = time.Time{}
	p.fetched = true
	p.policy = policy
	p.expires = now.Add(p.ttl)
	return policy, nil
}

// getEgressPolicy asks atelet for actor's egress policy, returning nil if the
// actor has none.
func getEgressPolicy(ctx context.Context, client ateletpb.WorkerSupportClient, actor resources.ActorRef) (*ateletpb.EgressPolicy, error) {
	resp, err := client.GetActorEgressPolicy(ctx, &ateletpb.GetActorEgressPolicyRequest{
		ActorAtespace: actor.Atespace,
		ActorName:     actor.Name,
	})
	if err != nil {
		return nil, fmt.Errorf("while fetching the actor's egress policy: %w", err)
	}
	return resp.GetEgressPolicy(), nil
}
