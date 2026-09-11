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

package egresspolicy

import (
	"net/netip"
	"testing"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func hostnameRule(patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{Hostnames: &ateapipb.HostnameRule{Patterns: patterns}}
}

func ipBlockRule(cidrs ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{IpBlocks: &ateapipb.IPBlockRule{Cidrs: cidrs}}
}

func allRule() *ateapipb.EgressRule {
	return &ateapipb.EgressRule{All: &emptypb.Empty{}}
}

func policy(rules ...*ateapipb.EgressRule) *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{Rules: rules}
}

func mustCompile(t *testing.T, p *ateapipb.EgressPolicy) *Policy {
	t.Helper()
	compiled, errs := Compile(p)
	if len(errs) != 0 {
		t.Fatalf("Compile: %v", errs)
	}
	return compiled
}

func host(name string) Destination { return Destination{Hostname: name} }

func addr(ip string) Destination { return Destination{IP: netip.MustParseAddr(ip)} }

func TestParseHostnamePattern(t *testing.T) {
	valid := []string{
		"example.com",
		"api.example.com",
		"*.example.com",
		"*.com",
		"a-b.example.com",
		"1.example.com",
		"xn--bcher-kva.example",
	}
	for _, raw := range valid {
		if _, err := ParseHostnamePattern(raw); err != nil {
			t.Errorf("ParseHostnamePattern(%q) = %v, want ok", raw, err)
		}
	}

	invalid := []string{
		"",
		"*",
		"*.",
		"API.EXAMPLE.COM",
		"example.com.",
		"192.0.2.1",
		"01.2.3.4",
		"2001:db8::1",
		"example.com:443",
		"https://example.com",
		"api.*.example.com",
		"**.example.com",
		"*example.com",
		"foo..example.com",
		"-a.example.com",
		"bücher.example",
	}
	for _, raw := range invalid {
		if _, err := ParseHostnamePattern(raw); err == nil {
			t.Errorf("ParseHostnamePattern(%q) = ok, want error", raw)
		}
	}
}

func TestHostnamePatternMatches(t *testing.T) {
	tests := []struct {
		pattern  string
		hostname string
		want     bool
	}{
		{"example.com", "example.com", true},
		{"example.com", "api.example.com", false},
		{"example.com", "example.org", false},
		{"example.com", "notexample.com", false},
		{"*.example.com", "api.example.com", true},
		{"*.example.com", "example.com", false},
		{"*.example.com", "a.b.example.com", false},
		{"*.example.com", "notexample.com", false},
		{"*.example.com", ".example.com", false},
		{"*.com", "example.com", true},
		{"*.com", "com", false},
	}
	for _, tc := range tests {
		pattern, err := ParseHostnamePattern(tc.pattern)
		if err != nil {
			t.Fatalf("ParseHostnamePattern(%q): %v", tc.pattern, err)
		}
		if got := pattern.Matches(tc.hostname); got != tc.want {
			t.Errorf("%q.Matches(%q) = %v, want %v", tc.pattern, tc.hostname, got, tc.want)
		}
		if pattern.String() != tc.pattern {
			t.Errorf("String() = %q, want %q", pattern.String(), tc.pattern)
		}
	}
}

func TestParsePrefix(t *testing.T) {
	valid := []string{
		"192.0.2.0/24",
		"192.0.2.1/32",
		"0.0.0.0/0",
		"2001:db8::/32",
		"2001:db8::1/128",
		"::/0",
	}
	for _, raw := range valid {
		if _, err := ParsePrefix(raw); err != nil {
			t.Errorf("ParsePrefix(%q) = %v, want ok", raw, err)
		}
	}

	invalid := []string{
		"",
		"192.0.2.1",
		"192.0.2.1/24",
		"192.0.2.0/33",
		"192.0.02.0/24",
		"192.0.2.0/024",
		"2001:DB8::/32",
		"2001:0db8::/32",
		"::ffff:192.0.2.0/120",
		"fe80::%eth0/64",
		"example.com/24",
	}
	for _, raw := range invalid {
		if _, err := ParsePrefix(raw); err == nil {
			t.Errorf("ParsePrefix(%q) = ok, want error", raw)
		}
	}
}

func TestNormalizeAuthority(t *testing.T) {
	tests := []struct {
		authority string
		want      Destination
		wantErr   bool
	}{
		{authority: "example.com", want: host("example.com")},
		{authority: "example.com:8443", want: Destination{Hostname: "example.com", Port: 8443}},
		{authority: "API.Example.COM", want: host("api.example.com")},
		{authority: "example.com.", want: host("example.com")},
		{authority: "example.com.:443", want: Destination{Hostname: "example.com", Port: 443}},
		{authority: "192.0.2.1", want: addr("192.0.2.1")},
		{authority: "192.0.2.1:80", want: Destination{IP: netip.MustParseAddr("192.0.2.1"), Port: 80}},
		{authority: "[2001:db8::1]:443", want: Destination{IP: netip.MustParseAddr("2001:db8::1"), Port: 443}},
		{authority: "[2001:db8::1]", want: addr("2001:db8::1")},
		{authority: "2001:db8::1", want: addr("2001:db8::1")},
		{authority: "[::ffff:192.0.2.1]:80", want: Destination{IP: netip.MustParseAddr("192.0.2.1"), Port: 80}},
		{authority: "", wantErr: true},
		{authority: "example.com..", wantErr: true},
		{authority: "example.com:0", wantErr: true},
		{authority: "example.com:99999", wantErr: true},
		{authority: "example.com:https", wantErr: true},
		{authority: "[fe80::1%25eth0]:443", wantErr: true},
		{authority: "bücher.example", wantErr: true},
		{authority: "exa mple.com", wantErr: true},
		{authority: "http://example.com", wantErr: true},
		{authority: "example.com/path", wantErr: true},
		{authority: "_dmarc.example.com", wantErr: true},
	}
	for _, tc := range tests {
		got, err := NormalizeAuthority(tc.authority)
		if tc.wantErr {
			if err == nil {
				t.Errorf("NormalizeAuthority(%q) = %+v, want error", tc.authority, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("NormalizeAuthority(%q): %v", tc.authority, err)
			continue
		}
		if got != tc.want {
			t.Errorf("NormalizeAuthority(%q) = %+v, want %+v", tc.authority, got, tc.want)
		}
	}
}

func TestCompileReportsAndDropsInvalidEntries(t *testing.T) {
	compiled, errs := Compile(policy(
		hostnameRule("good.example.com", "BAD.example.com"),
		ipBlockRule("192.0.2.0/24", "192.0.2.1/24"),
	))
	if len(errs) != 2 {
		t.Fatalf("Compile errors = %v, want 2", errs)
	}
	if compiled.RuleCount() != 2 {
		t.Errorf("RuleCount = %d, want 2", compiled.RuleCount())
	}
	if d := compiled.Evaluate(host("good.example.com")); !d.Allowed || d.RuleIndex != 0 {
		t.Errorf("valid pattern of a partly invalid rule should still match, got %+v", d)
	}
	if d := compiled.Evaluate(host("bad.example.com")); d.Allowed {
		t.Errorf("dropped pattern must not match, got %+v", d)
	}
	if d := compiled.Evaluate(addr("192.0.2.7")); !d.Allowed || d.RuleIndex != 1 {
		t.Errorf("valid prefix of a partly invalid rule should still match, got %+v", d)
	}
}

func TestEvaluate(t *testing.T) {
	effects := &ateapipb.EgressRuleEffects{
		InjectStaticHeaders: []*ateapipb.CredentialHeaderInjection{{
			Header:        "authorization",
			Prefix:        "Bearer ",
			CredentialUri: "substrate-secret://k8s/default/token",
		}},
	}
	withEffects := &ateapipb.EgressRule{Hostnames: &ateapipb.HostnameRule{
		Patterns: []string{"api.example.com"},
		Effects:  effects,
	}}

	tests := []struct {
		name   string
		policy *ateapipb.EgressPolicy
		dest   Destination
		want   Decision
	}{
		{
			name:   "no rules denies",
			policy: policy(),
			dest:   host("example.com"),
			want:   Decision{RuleIndex: -1},
		},
		{
			name:   "exact hostname",
			policy: policy(hostnameRule("example.com")),
			dest:   host("example.com"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "hostname rule does not match another name",
			policy: policy(hostnameRule("example.com")),
			dest:   host("example.org"),
			want:   Decision{RuleIndex: -1},
		},
		{
			name:   "wildcard hostname",
			policy: policy(hostnameRule("*.example.com")),
			dest:   host("api.example.com"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "any pattern in the rule matches",
			policy: policy(hostnameRule("other.example", "example.com")),
			dest:   host("example.com"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "hostname rule never matches a destination with no hostname",
			policy: policy(hostnameRule("*.example.com")),
			dest:   addr("192.0.2.1"),
			want:   Decision{RuleIndex: -1},
		},
		{
			name:   "ip block matches the address",
			policy: policy(ipBlockRule("192.0.2.0/24")),
			dest:   addr("192.0.2.200"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "ip block does not match outside the prefix",
			policy: policy(ipBlockRule("192.0.2.0/24")),
			dest:   addr("192.0.3.1"),
			want:   Decision{RuleIndex: -1},
		},
		{
			name:   "ip block never matches a destination with no address",
			policy: policy(ipBlockRule("0.0.0.0/0")),
			dest:   host("example.com"),
			want:   Decision{RuleIndex: -1},
		},
		{
			name:   "ipv6 block",
			policy: policy(ipBlockRule("2001:db8::/32")),
			dest:   addr("2001:db8:1::1"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "ipv4 block matches a mapped address",
			policy: policy(ipBlockRule("192.0.2.0/24")),
			dest:   addr("::ffff:192.0.2.1"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "ipv4 block does not match ipv6",
			policy: policy(ipBlockRule("0.0.0.0/0")),
			dest:   addr("2001:db8::1"),
			want:   Decision{RuleIndex: -1},
		},
		{
			name:   "all matches a hostname",
			policy: policy(allRule()),
			dest:   host("example.com"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "all matches an address",
			policy: policy(allRule()),
			dest:   addr("192.0.2.1"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "all matches an empty destination",
			policy: policy(allRule()),
			dest:   Destination{},
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "first matching rule wins and carries its effects",
			policy: policy(hostnameRule("other.example"), withEffects, allRule()),
			dest:   host("api.example.com"),
			want:   Decision{Allowed: true, RuleIndex: 1, Effects: effects},
		},
		{
			name:   "a later all rule does not lend effects to an earlier match",
			policy: policy(hostnameRule("api.example.com"), withEffects),
			dest:   host("api.example.com"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "sni and address are evaluated together on a tls connection",
			policy: policy(hostnameRule("api.example.com"), ipBlockRule("10.0.0.0/8")),
			dest:   Destination{Hostname: "api.example.com", IP: netip.MustParseAddr("203.0.113.5"), Port: 443},
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "address rule authorizes a tls connection whose sni no rule names",
			policy: policy(hostnameRule("api.example.com"), ipBlockRule("10.0.0.0/8")),
			dest:   Destination{Hostname: "other.example", IP: netip.MustParseAddr("10.1.2.3"), Port: 443},
			want:   Decision{Allowed: true, RuleIndex: 1},
		},
		{
			name:   "empty rule matches nothing",
			policy: policy(&ateapipb.EgressRule{}, allRule()),
			dest:   host("example.com"),
			want:   Decision{Allowed: true, RuleIndex: 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mustCompile(t, tc.policy).Evaluate(tc.dest)
			if got.Allowed != tc.want.Allowed || got.RuleIndex != tc.want.RuleIndex || got.Effects != tc.want.Effects {
				t.Errorf("Evaluate(%+v) = %+v, want %+v", tc.dest, got, tc.want)
			}
		})
	}
}

func TestHasHostnameRules(t *testing.T) {
	tests := []struct {
		name   string
		policy *ateapipb.EgressPolicy
		want   bool
	}{
		{name: "no rules", policy: &ateapipb.EgressPolicy{}},
		{name: "ip blocks only", policy: &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{IpBlocks: &ateapipb.IPBlockRule{Cidrs: []string{"10.0.0.0/8"}}}}}},
		{name: "all only", policy: &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{All: &emptypb.Empty{}}}}},
		{name: "hostnames", policy: &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}}}}}, want: true},
		{name: "hostnames after an ip block", policy: &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{
			{IpBlocks: &ateapipb.IPBlockRule{Cidrs: []string{"10.0.0.0/8"}}},
			{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"*.example.com"}}},
		}}, want: true},
		// Every pattern was dropped at compile time, so the rule can match nothing.
		{name: "hostnames that did not compile", policy: &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"not a hostname"}}}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			policy, _ := Compile(tc.policy)
			if got := policy.HasHostnameRules(); got != tc.want {
				t.Errorf("HasHostnameRules() = %v, want %v", got, tc.want)
			}
		})
	}
}
