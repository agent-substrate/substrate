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
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// EgressAllowAll is the rule a test that is not about egress policy gives its
// actor: the gateway denies an actor with no policy at all.
func EgressAllowAll() *ateapipb.EgressRule {
	return &ateapipb.EgressRule{All: &emptypb.Empty{}}
}

// EgressAllowHostnames is a rule that lets an actor reach the hostnames
// matching patterns (exact names, or "*." plus a name for one leftmost label).
func EgressAllowHostnames(patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{Hostnames: &ateapipb.HostnameRule{Patterns: patterns}}
}

// EgressAllowCIDRs is a rule that lets an actor reach the addresses in cidrs.
func EgressAllowCIDRs(cidrs ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{IpBlocks: &ateapipb.IPBlockRule{Cidrs: cidrs}}
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
