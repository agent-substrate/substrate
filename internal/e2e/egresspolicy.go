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

package e2e

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// EgressAllowAll is what a test that is not about egress policy gives its
// actor, since the gateway denies an actor with no policy at all: every name
// and address, as cleartext HTTP on any port and as intercepted HTTPS on 443.
// TLS forwarded unread is not decided by the gateway yet, and a passthrough
// rule on every port would tie with the http rule, which the API rejects.
func EgressAllowAll() []*ateapipb.EgressRule {
	return []*ateapipb.EgressRule{
		{Http: &ateapipb.HTTPRule{HostPatterns: []string{"*"}, Ports: []string{"*"}}},
		{Https: &ateapipb.HTTPSRule{HostPatterns: []string{"*"}}},
	}
}

// EgressAllowHTTP is a rule that lets an actor send cleartext HTTP to the
// hosts matching patterns (exact names, or "*." plus a name for one leftmost
// label) on port 80.
func EgressAllowHTTP(patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{Http: &ateapipb.HTTPRule{HostPatterns: patterns}}
}

// EgressAllowHTTPS is a rule that lets an actor send HTTPS, intercepted by the
// gateway, to the hosts matching patterns on port 443.
func EgressAllowHTTPS(patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{Https: &ateapipb.HTTPSRule{HostPatterns: patterns}}
}

// EnsureEgressPolicy gives actor an EgressPolicy with exactly rules, replacing
// any it had. Deleting the actor deletes the policy, so there is no cleanup.
func EnsureEgressPolicy(t *testing.T, ctx context.Context, clients *Clients, actor *ateapipb.ObjectRef, rules ...*ateapipb.EgressRule) {
	t.Helper()
	policy := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: actor.GetAtespace(), Name: "default"},
		Rules:    rules,
	}
	_, err := clients.SubstrateAPI.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{
		Actor:        actor,
		EgressPolicy: policy,
	})
	if status.Code(err) != codes.AlreadyExists {
		if err != nil {
			t.Fatalf("CreateActorEgressPolicy for %s/%s: %v", actor.GetAtespace(), actor.GetName(), err)
		}
		return
	}
	existing, err := clients.SubstrateAPI.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: actor})
	if err != nil {
		t.Fatalf("GetActorEgressPolicy for %s/%s: %v", actor.GetAtespace(), actor.GetName(), err)
	}
	policy.Metadata = existing.GetMetadata()
	if _, err := clients.SubstrateAPI.UpdateActorEgressPolicy(ctx, &ateapipb.UpdateActorEgressPolicyRequest{
		Actor:        actor,
		EgressPolicy: policy,
	}); err != nil {
		t.Fatalf("UpdateActorEgressPolicy for %s/%s: %v", actor.GetAtespace(), actor.GetName(), err)
	}
}
