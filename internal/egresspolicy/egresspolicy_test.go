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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func httpRule(patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{Http: &ateapipb.HTTPRule{HostPatterns: patterns}}
}

func httpRuleOnPorts(ports []string, patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{Http: &ateapipb.HTTPRule{HostPatterns: patterns, Ports: ports}}
}

func httpsRule(patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{Https: &ateapipb.HTTPSRule{HostPatterns: patterns}}
}

func passthroughRule(ports []string, patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{TlsPassthrough: &ateapipb.TLSPassthroughRule{SniPatterns: patterns, Ports: ports}}
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
		"*",
		"a-b.example.com",
		"1.example.com",
		"xn--bcher-kva.example",
	}
	for _, raw := range valid {
		pattern, err := ParseHostnamePattern(raw)
		if err != nil {
			t.Errorf("ParseHostnamePattern(%q) = %v, want ok", raw, err)
			continue
		}
		if pattern.String() != raw {
			t.Errorf("ParseHostnamePattern(%q).String() = %q", raw, pattern.String())
		}
	}

	invalid := []string{
		"",
		"*.",
		"**",
		"API.EXAMPLE.COM",
		"example.com.",
		"192.0.2.1",
		"01.2.3.4",
		"example.123",
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
		{"*", "example.com", true},
		{"*", "a.b.example.com", true},
		{"*", "", false},
	}
	for _, tc := range tests {
		pattern, err := ParseHostnamePattern(tc.pattern)
		if err != nil {
			t.Fatalf("ParseHostnamePattern(%q): %v", tc.pattern, err)
		}
		if got := pattern.Matches(tc.hostname); got != tc.want {
			t.Errorf("%q.Matches(%q) = %v, want %v", tc.pattern, tc.hostname, got, tc.want)
		}
	}
}

func TestParsePort(t *testing.T) {
	for raw, want := range map[string]uint16{"1": 1, "80": 80, "8443": 8443, "65535": 65535} {
		if got, err := ParsePort(raw); err != nil || got != want {
			t.Errorf("ParsePort(%q) = %d, %v; want %d", raw, got, err, want)
		}
	}
	for _, raw := range []string{"", "0", "65536", "080", "+80", "-1", " 80", "80 ", "http", "*", "8_0", "0x50"} {
		if got, err := ParsePort(raw); err == nil {
			t.Errorf("ParsePort(%q) = %d, want error", raw, got)
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
		httpRule("good.example.com", "BAD.example.com"),
		passthroughRule([]string{"443", "eighty"}, "*"),
		passthroughRule(nil, "*"),
	))
	if len(errs) != 3 {
		t.Fatalf("Compile errors = %v, want 3", errs)
	}
	if compiled.RuleCount() != 3 {
		t.Errorf("RuleCount = %d, want 3", compiled.RuleCount())
	}
	if d := compiled.EvaluateRequest(host("good.example.com"), false); !d.Allowed || d.RuleIndex != 0 {
		t.Errorf("valid pattern of a partly invalid rule should still match, got %+v", d)
	}
	if d := compiled.EvaluateRequest(host("bad.example.com"), false); d.Allowed {
		t.Errorf("dropped pattern must not match, got %+v", d)
	}
}

func TestEvaluateRequest(t *testing.T) {
	effects := &ateapipb.HttpRuleEffects{
		ReplaceHeaders: []*ateapipb.CredentialHeaderInjection{{
			Header:        "authorization",
			Prefix:        "Bearer ",
			CredentialUri: "ate-secret://k8s/default/token",
		}},
	}
	withEffects := &ateapipb.EgressRule{Https: &ateapipb.HTTPSRule{
		HostPatterns: []string{"api.example.com"},
		Effects:      effects,
	}}

	tests := []struct {
		name      string
		policy    *ateapipb.EgressPolicy
		dest      Destination
		decrypted bool
		want      Decision
	}{
		{
			name:   "no rules denies",
			policy: policy(),
			dest:   host("example.com"),
			want:   Decision{RuleIndex: -1},
		},
		{
			name:   "exact hostname",
			policy: policy(httpRule("example.com")),
			dest:   host("example.com"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "http rule does not match another name",
			policy: policy(httpRule("example.com")),
			dest:   host("example.org"),
			want:   Decision{RuleIndex: -1},
		},
		{
			name:   "wildcard hostname",
			policy: policy(httpRule("*.example.com")),
			dest:   host("api.example.com"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "star matches every name",
			policy: policy(httpRule("*")),
			dest:   host("anything.example"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "any pattern in the rule matches",
			policy: policy(httpRule("other.example", "example.com")),
			dest:   host("example.com"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "star matches an ip literal",
			policy: policy(httpRule("*")),
			dest:   Destination{IP: netip.MustParseAddr("192.0.2.1"), Port: 80},
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "a name does not match an ip literal",
			policy: policy(httpRule("example.com", "*.example.com")),
			dest:   Destination{IP: netip.MustParseAddr("192.0.2.1"), Port: 80},
			want:   Decision{RuleIndex: -1},
		},
		{
			name:   "an empty destination matches nothing",
			policy: policy(httpRule("*")),
			dest:   Destination{Port: 80},
			want:   Decision{RuleIndex: -1},
		},
		{
			name:   "http rule does not decide a decrypted request",
			policy: policy(httpRule("api.example.com")),
			dest:   host("api.example.com"), decrypted: true,
			want: Decision{RuleIndex: -1},
		},
		{
			name:   "https rule does not decide a cleartext request",
			policy: policy(httpsRule("api.example.com")),
			dest:   host("api.example.com"),
			want:   Decision{RuleIndex: -1},
		},
		{
			name:   "https rule decides a decrypted request and carries its effects",
			policy: policy(httpRule("api.example.com"), withEffects),
			dest:   host("api.example.com"), decrypted: true,
			want: Decision{Allowed: true, RuleIndex: 1, Effects: effects},
		},
		{
			name:   "passthrough rule never decides a request",
			policy: policy(passthroughRule([]string{"*"}, "*")),
			dest:   host("api.example.com"), decrypted: true,
			want: Decision{RuleIndex: -1},
		},
		{
			name:   "dialed port outside the rule",
			policy: policy(httpRuleOnPorts([]string{"8080"}, "api.example.com")),
			dest:   Destination{Hostname: "api.example.com", Port: 80},
			want:   Decision{RuleIndex: -1},
		},
		{
			name:   "dialed port in the rule",
			policy: policy(httpRuleOnPorts([]string{"8080"}, "api.example.com")),
			dest:   Destination{Hostname: "api.example.com", Port: 8080},
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "default http port",
			policy: policy(httpRule("api.example.com")),
			dest:   Destination{Hostname: "api.example.com", Port: 80},
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "default https port",
			policy: policy(httpsRule("api.example.com")),
			dest:   Destination{Hostname: "api.example.com", Port: 443}, decrypted: true,
			want: Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "any port",
			policy: policy(httpRuleOnPorts([]string{"*"}, "api.example.com")),
			dest:   Destination{Hostname: "api.example.com", Port: 8080},
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "unknown dialed port is not enforced",
			policy: policy(httpRuleOnPorts([]string{"8080"}, "api.example.com")),
			dest:   host("api.example.com"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "exact pattern beats an earlier wildcard",
			policy: policy(httpRule("*.example.com"), httpRule("api.example.com")),
			dest:   host("api.example.com"),
			want:   Decision{Allowed: true, RuleIndex: 1},
		},
		{
			name:   "labeled wildcard beats an earlier star",
			policy: policy(httpRule("*"), httpRule("*.example.com")),
			dest:   host("api.example.com"),
			want:   Decision{Allowed: true, RuleIndex: 1},
		},
		{
			name:   "named port beats an earlier any port on the same pattern",
			policy: policy(httpRuleOnPorts([]string{"*"}, "api.example.com"), httpRuleOnPorts([]string{"80"}, "api.example.com")),
			dest:   host("api.example.com"),
			want:   Decision{Allowed: true, RuleIndex: 1},
		},
		{
			name:   "name specificity outranks port specificity",
			policy: policy(httpRuleOnPorts([]string{"80"}, "*.example.com"), httpRuleOnPorts([]string{"*"}, "api.example.com")),
			dest:   host("api.example.com"),
			want:   Decision{Allowed: true, RuleIndex: 1},
		},
		{
			name:   "a full tie keeps policy order",
			policy: policy(httpRule("*.example.com"), httpRule("*.example.com")),
			dest:   host("api.example.com"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "empty rule matches nothing",
			policy: policy(&ateapipb.EgressRule{}, httpRule("example.com")),
			dest:   host("example.com"),
			want:   Decision{Allowed: true, RuleIndex: 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := mustCompile(t, tc.policy).EvaluateRequest(tc.dest, tc.decrypted); got != tc.want {
				t.Errorf("EvaluateRequest(%+v, %v) = %+v, want %+v", tc.dest, tc.decrypted, got, tc.want)
			}
		})
	}
}
