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

// TODO(yufan-su): Move these constructors into an internal/e2e/egresspolicy package.

// EgressAllowAll is what a test that is not about egress policy gives its
// actor, since the gateway denies an actor with no policy at all: every name
// and address, as cleartext HTTP on any port and as intercepted HTTPS on 443.
// TLS forwarded unread is not decided by the gateway yet, and a passthrough
// rule on every port would tie with the http rule, which the API rejects.
func EgressAllowAll() []*ateapipb.EgressRule {
	return []*ateapipb.EgressRule{
		{Http: &ateapipb.HTTPRule{Hostnames: []string{"*"}, Ports: &ateapipb.Ports{All: &ateapipb.AllPorts{}}}},
		{Https: &ateapipb.HTTPSRule{Hostnames: []string{"*"}}},
	}
}

// EgressAllowHTTP is a rule that lets an actor send cleartext HTTP to the
// hosts matching patterns (exact names, or "*." plus a name for one leftmost
// label) on port 80.
func EgressAllowHTTP(patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{Http: &ateapipb.HTTPRule{Hostnames: patterns}}
}

// EgressAllowHTTPS is a rule that lets an actor send HTTPS, intercepted by the
// gateway, to the hosts matching patterns on port 443.
func EgressAllowHTTPS(patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{Https: &ateapipb.HTTPSRule{Hostnames: patterns}}
}

// EgressAllowPassthrough is a rule that lets an actor send TLS to the hosts
// matching patterns on any port, relayed by the gateway without decryption.
func EgressAllowPassthrough(patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{
		TlsPassthrough: &ateapipb.TLSPassthroughRule{
			Hostnames: patterns,
			Ports:     &ateapipb.Ports{All: &ateapipb.AllPorts{}},
		},
	}
}

// EgressAllowPassthroughOnPorts is EgressAllowPassthrough confined to the
// given ports.
func EgressAllowPassthroughOnPorts(ports []int32, patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{
		TlsPassthrough: &ateapipb.TLSPassthroughRule{
			Hostnames: patterns,
			Ports:     &ateapipb.Ports{Numbers: ports},
		},
	}
}

// EgressInjectHeader is an https rule (see EgressAllowHTTPS) that also carries
// a replace_headers effect: on a match, the gateway resolves credentialURI
// through its credential provider and replaces header with prefix plus the
// credential.
func EgressInjectHeader(header, prefix, credentialURI string, patterns ...string) *ateapipb.EgressRule {
	rule := EgressAllowHTTPS(patterns...)
	rule.Https.Effects = replaceHeaderEffects(header, prefix, credentialURI)
	return rule
}

// EgressInjectHeaderHTTP is an http rule (see EgressAllowHTTP) carrying the
// same effect as EgressInjectHeader. The gateway never puts a credential on
// cleartext, so a test uses it to prove the effect is skipped there.
func EgressInjectHeaderHTTP(header, prefix, credentialURI string, patterns ...string) *ateapipb.EgressRule {
	rule := EgressAllowHTTP(patterns...)
	rule.Http.Effects = replaceHeaderEffects(header, prefix, credentialURI)
	return rule
}

func replaceHeaderEffects(header, prefix, credentialURI string) *ateapipb.HttpRuleEffects {
	return &ateapipb.HttpRuleEffects{
		ReplaceHeaders: []*ateapipb.CredentialHeader{{
			Header:        header,
			Prefix:        prefix,
			CredentialUri: credentialURI,
		}},
	}
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
