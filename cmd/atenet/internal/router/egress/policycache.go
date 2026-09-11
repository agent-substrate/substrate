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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/egresspolicy"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// DefaultPolicyCacheTTL is how long a fetched policy stays usable, and so how
// stale a decision can be.
const DefaultPolicyCacheTTL = 10 * time.Second

// policyFetchTimeout caps one GetActorEgressPolicy call. It sits under the
// ext_proc message_timeout (5s in both manifests) so a slow control plane is a
// 503 rather than an Envoy timeout.
const policyFetchTimeout = 4 * time.Second

// errNoPolicy reports that the actor has no EgressPolicy. Cached like a
// policy, so a flood of denied requests does not hit ateapi.
var errNoPolicy = errors.New("actor has no egress policy")

// policyCache holds each actor's compiled EgressPolicy for one TTL. Every leg
// reads through it, so the CONNECT warms the entry for the requests inside.
type policyCache struct {
	client ateapipb.ControlClient
	// ttl of 0 disables caching: every call fetches.
	ttl time.Duration
	now func() time.Time

	mu      sync.Mutex
	entries map[resources.ActorRef]policyEntry
	// cleanupAt is the entry count at which the next store also removes
	// expired entries, so the map does not grow with every actor the gateway
	// ever saw. It doubles after each cleanup.
	cleanupAt int
	// flight collapses concurrent fetches for one actor into a single call.
	flight singleflight.Group
}

// minCleanupSize is the map size below which removing expired entries is not
// worth a walk of the map.
const minCleanupSize = 64

// policyEntry is one cached fetch. A nil policy records that the actor had no
// EgressPolicy when it was fetched.
type policyEntry struct {
	policy  *egresspolicy.Policy
	expires time.Time
}

func newPolicyCache(client ateapipb.ControlClient, ttl time.Duration) *policyCache {
	return &policyCache{
		client:    client,
		ttl:       ttl,
		now:       time.Now,
		entries:   make(map[resources.ActorRef]policyEntry),
		cleanupAt: minCleanupSize,
	}
}

// get returns the actor's compiled policy, fetching when there is no live
// entry, or errNoPolicy. Fetch errors are never cached.
func (c *policyCache) get(ctx context.Context, ref resources.ActorRef) (*egresspolicy.Policy, error) {
	if c.ttl > 0 {
		c.mu.Lock()
		entry, ok := c.entries[ref]
		c.mu.Unlock()
		if ok && c.now().Before(entry.expires) {
			if entry.policy == nil {
				return nil, errNoPolicy
			}
			return entry.policy, nil
		}
	}

	// The fetch outlives the caller: the leader of a flight going away must
	// not fail the callers that joined it.
	ch := c.flight.DoChan(ref.String(), func() (any, error) {
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), policyFetchTimeout)
		defer cancel()
		return c.fetch(fetchCtx, ref)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		policy, _ := res.Val.(*egresspolicy.Policy)
		if policy == nil {
			return nil, errNoPolicy
		}
		return policy, nil
	}
}

// fetch loads and compiles one actor's policy and stores the result. A nil
// policy with a nil error is the stored form of "no policy".
func (c *policyCache) fetch(ctx context.Context, ref resources.ActorRef) (*egresspolicy.Policy, error) {
	resp, err := c.client.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: ref.ToObjectRef()})
	var policy *egresspolicy.Policy
	switch {
	case err == nil:
		var compileErrs []error
		policy, compileErrs = egresspolicy.Compile(resp)
		// ateapi accepted these and this gateway cannot match them. Log once
		// here, not on every request that fails to match.
		for _, cerr := range compileErrs {
			slog.WarnContext(ctx, "egress policy has an entry this gateway cannot enforce; it will not match anything",
				slog.Any("actor", ref), slog.Any("err", cerr))
		}
	case status.Code(err) == codes.NotFound:
		policy = nil
	default:
		return nil, fmt.Errorf("fetching egress policy for %s: %w", ref, err)
	}

	if c.ttl > 0 {
		c.mu.Lock()
		now := c.now()
		c.entries[ref] = policyEntry{policy: policy, expires: now.Add(c.ttl)}
		if len(c.entries) >= c.cleanupAt {
			for k, e := range c.entries {
				if !now.Before(e.expires) {
					delete(c.entries, k)
				}
			}
			// Clean up again once the live set has doubled.
			c.cleanupAt = max(minCleanupSize, 2*len(c.entries))
		}
		c.mu.Unlock()
	}
	return policy, nil
}
