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
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

func TestEvaluator(t *testing.T) {
	if (*evaluator)(nil).allowsHostname("example.com.") {
		t.Error("nil evaluator allowed a hostname")
	}

	tests := []struct {
		name    string
		policy  *ateletpb.EgressPolicy
		allowed []string
		blocked []string
	}{
		{
			name:    "nil policy",
			policy:  nil,
			blocked: []string{"example.com.", "api.example.com.", "."},
		},
		{
			name:    "empty policy",
			policy:  &ateletpb.EgressPolicy{},
			blocked: []string{"example.com.", "api.example.com.", "."},
		},
		{
			name:    "rule with no protocol",
			policy:  &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{}}},
			blocked: []string{"example.com.", "api.example.com."},
		},
		{
			name: "exact host across http, https, and tls_passthrough",
			policy: &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{
				{Http: &ateletpb.EgressDestinations{Hostnames: []string{"http.example.com"}}},
				{Https: &ateletpb.EgressDestinations{Hostnames: []string{"https.example.com"}}},
				{TlsPassthrough: &ateletpb.EgressDestinations{Hostnames: []string{"tls.example.com"}}},
			}},
			allowed: []string{
				"http.example.com.",
				"http.example.com",
				"HTTP.Example.COM.",
				"https.example.com.",
				"tls.example.com.",
			},
			blocked: []string{
				"example.com.",
				"sub.http.example.com.",
				"other.example.com.",
				"http.example.com..",
				".",
				"",
			},
		},
		{
			name: "leftmost-label wildcard",
			policy: &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
				Https: &ateletpb.EgressDestinations{Hostnames: []string{"*.example.com"}},
			}}},
			allowed: []string{
				"api.example.com.",
				"API.Example.COM.",
				"a-b.example.com.",
				"1.example.com.",
			},
			blocked: []string{
				"example.com.",
				"a.b.example.com.",
				"notexample.com.",
				".example.com.",
				"_dmarc.example.com.",
				"-bad.example.com.",
			},
		},
		{
			name: "star wildcard",
			policy: &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
				Http: &ateletpb.EgressDestinations{Hostnames: []string{"*"}},
			}}},
			allowed: []string{
				"example.com.",
				"api.example.com.",
				"a.b.c.example.org.",
				"xn--bcher-kva.example.",
			},
			blocked: []string{
				"",
				".",
				"example.com..",
				"192.0.2.1.",
				"01.2.3.4.",
				"_dmarc.example.com.",
				"bücher.example.",
			},
		},
		{
			name: "invalid patterns are ignored while valid ones still match",
			policy: &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
				Http: &ateletpb.EgressDestinations{Hostnames: []string{
					"BAD.EXAMPLE.COM",
					"*.",
					"https://bad.example.com",
					"good.example.com",
					"*.wild.example.com",
				}},
			}}},
			allowed: []string{
				"good.example.com.",
				"a.wild.example.com.",
			},
			blocked: []string{
				"bad.example.com.",
				"wild.example.com.",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eval := newEvaluator(tc.policy)
			for _, qname := range tc.allowed {
				if !eval.allowsHostname(qname) {
					t.Errorf("allowsHostname(%q) = false, want true", qname)
				}
			}
			for _, qname := range tc.blocked {
				if eval.allowsHostname(qname) {
					t.Errorf("allowsHostname(%q) = true, want false", qname)
				}
			}
		})
	}
}
