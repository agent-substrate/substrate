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

package egressinject

import (
	"testing"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// sampleEgressPolicy is the actor egress policy the handler tests resolve for
// team-a/my-actor: a single hostname rule injecting a bearer token.
func sampleEgressPolicy() *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "default"},
		Rules: []*ateapipb.EgressRule{{
			Hostnames: &ateapipb.HostnameRule{
				Patterns: []string{"api.example.com"},
				Effects: &ateapipb.EgressRuleEffects{
					InjectStaticHeaders: []*ateapipb.CredentialHeaderInjection{{
						Header:        "Authorization",
						Prefix:        "Bearer ",
						CredentialUri: "substrate-secret://kubernetes.io/team-secrets/ns1/example-api",
					}},
				},
			},
		}},
	}
}

func TestEvaluate(t *testing.T) {
	policy := sampleEgressPolicy()

	t.Run("match injects", func(t *testing.T) {
		inj, matched := evaluate(policy, "api.example.com")
		if !matched {
			t.Fatal("evaluate did not match the hostname rule")
		}
		if len(inj) != 1 || inj[0].GetCredentialUri() != "substrate-secret://kubernetes.io/team-secrets/ns1/example-api" {
			t.Errorf("evaluate returned %+v", inj)
		}
	})

	t.Run("no rule matches", func(t *testing.T) {
		if inj, matched := evaluate(policy, "other.example.com"); matched || inj != nil {
			t.Errorf("evaluate(other host) = (%+v, %v), want (nil, false)", inj, matched)
		}
	})

	t.Run("first matching rule wins", func(t *testing.T) {
		// An earlier rule that matches but injects nothing shadows a later rule
		// that would inject: only the first match's effects apply.
		p := &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{
			{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}}},
			{Hostnames: &ateapipb.HostnameRule{
				Patterns: []string{"api.example.com"},
				Effects: &ateapipb.EgressRuleEffects{InjectStaticHeaders: []*ateapipb.CredentialHeaderInjection{{
					Header: "Authorization", CredentialUri: "substrate-secret://kubernetes.io/p/ns/s",
				}}},
			}},
		}}
		inj, matched := evaluate(p, "api.example.com")
		if !matched {
			t.Fatal("evaluate did not match")
		}
		if len(inj) != 0 {
			t.Errorf("first (injectionless) rule should win, got %+v", inj)
		}
	})

	t.Run("all matcher matches every destination", func(t *testing.T) {
		p := &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{All: &emptypb.Empty{}}}}
		if _, matched := evaluate(p, "anything.example.org"); !matched {
			t.Error("an `all` rule should match every destination")
		}
	})

	t.Run("ip_blocks rule never matches on the MITM leg", func(t *testing.T) {
		p := &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{IpBlocks: &ateapipb.IPBlockRule{Cidrs: []string{"0.0.0.0/0"}}}}}
		if _, matched := evaluate(p, "api.example.com"); matched {
			t.Error("an ip_blocks rule must not match hostname-based requests")
		}
	})
}

func TestHostnameMatches(t *testing.T) {
	tests := []struct {
		pattern string
		host    string
		want    bool
	}{
		{"api.example.com", "api.example.com", true},
		{"api.example.com", "API.EXAMPLE.COM", true},
		{"api.example.com", "api.example.com.", true},
		{"api.example.com", "other.example.com", false},
		{"*.example.com", "api.example.com", true},
		// A wildcard replaces exactly one leftmost label per the API semantics.
		{"*.example.com", "a.b.example.com", false},
		{"*.example.com", "example.com", false},
		{"*.example.com", "example.org", false},
	}
	for _, tc := range tests {
		if got := hostnameMatches(tc.pattern, tc.host); got != tc.want {
			t.Errorf("hostnameMatches(%q, %q) = %v, want %v", tc.pattern, tc.host, got, tc.want)
		}
	}
}

func TestHostFromAuthority(t *testing.T) {
	tests := []struct{ in, want string }{
		{"api.example.com", "api.example.com"},
		{"api.example.com:443", "api.example.com"},
		{"", ""},
	}
	for _, tc := range tests {
		if got := hostFromAuthority(tc.in); got != tc.want {
			t.Errorf("hostFromAuthority(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCredentialURIClass(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "full uri", raw: "substrate-secret://kubernetes.io/team-secrets/ns1/example-api", want: "kubernetes.io"},
		{name: "class prefix only", raw: "substrate-secret://kubernetes.io", want: "kubernetes.io"},
		{name: "other class", raw: "substrate-secret://vault.hashicorp.com/p/ns/s", want: "vault.hashicorp.com"},
		{name: "wrong scheme", raw: "https://kubernetes.io/x", wantErr: true},
		{name: "bare class no scheme", raw: "kubernetes.io", wantErr: true},
		{name: "empty", raw: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := credentialURIClass(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("credentialURIClass(%q) = %q, want error", tc.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("credentialURIClass(%q) unexpected error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Errorf("credentialURIClass(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestValidateInjectHeader(t *testing.T) {
	for _, name := range []string{"", ":authority", ":scheme", "Host", "host"} {
		if err := validateInjectHeader(name); err == nil {
			t.Errorf("validateInjectHeader(%q) = nil, want error", name)
		}
	}
	if err := validateInjectHeader("Authorization"); err != nil {
		t.Errorf("validateInjectHeader(Authorization) = %v, want nil", err)
	}
}

func TestParseActorURI(t *testing.T) {
	tests := []struct {
		name         string
		uri          string
		wantAtespace string
		wantActor    string
		wantErr      bool
	}{
		{
			name:         "valid",
			uri:          "spiffe://substrate-actor.local/atespace/team-a/actor/my-actor",
			wantAtespace: "team-a",
			wantActor:    "my-actor",
		},
		{name: "wrong scheme", uri: "https://substrate-actor.local/atespace/team-a/actor/my-actor", wantErr: true},
		{name: "wrong trust domain", uri: "spiffe://other.local/atespace/team-a/actor/my-actor", wantErr: true},
		{name: "wrong structure", uri: "spiffe://substrate-actor.local/ns/team-a/actor/my-actor", wantErr: true},
		{name: "short path", uri: "spiffe://substrate-actor.local/atespace/team-a", wantErr: true},
		{name: "empty", uri: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			atespace, actor, err := parseActorURI(tc.uri)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseActorURI(%q) = (%q, %q), want error", tc.uri, atespace, actor)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseActorURI(%q) unexpected error: %v", tc.uri, err)
			}
			if atespace != tc.wantAtespace || actor != tc.wantActor {
				t.Errorf("parseActorURI(%q) = (%q, %q), want (%q, %q)", tc.uri, atespace, actor, tc.wantAtespace, tc.wantActor)
			}
		})
	}
}
