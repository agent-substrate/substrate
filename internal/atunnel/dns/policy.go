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

package dns

import (
	"context"
	"log/slog"
	"sync"

	"golang.org/x/net/dns/dnsmessage"

	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// PolicySource supplies the egress policy an actor's DNS queries are checked
// against.
type PolicySource interface {
	// EgressPolicy returns the actor's current policy.
	//
	// Callers must not modify the policy.
	EgressPolicy(ctx context.Context) (*ateletpb.EgressPolicy, error)
}

// egressPolicyCheck returns the queryPolicy that checks each DNS question against
// the actor's egress policy.
func egressPolicyCheck(ctx context.Context, source PolicySource) queryPolicy {
	if source == nil {
		return func(dnsmessage.Question) bool { return false }
	}
	var (
		mu     sync.Mutex
		cached *ateletpb.EgressPolicy
		eval   *evaluator
	)
	return func(q dnsmessage.Question) bool {
		policy, err := source.EgressPolicy(ctx)
		if err != nil {
			slog.DebugContext(ctx, "dns relay denied a query; the actor's egress policy is unavailable", slog.Any("err", err))
			return false
		}
		mu.Lock()
		if policy != cached || eval == nil {
			cached = policy
			eval = newEvaluator(policy)
		}
		e := eval
		mu.Unlock()
		return e.allowsHostname(q.Name.String())
	}
}

// allowQuery reports whether policy lets the actor ask q. An actor without a
// policy may resolve nothing, as it may reach nothing through the egress
// gateway.
func allowQuery(policy *ateletpb.EgressPolicy, q dnsmessage.Question) bool {
	return newEvaluator(policy).allowsHostname(q.Name.String())
}
