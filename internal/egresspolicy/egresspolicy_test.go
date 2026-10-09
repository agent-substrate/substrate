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
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func httpRule(patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{Http: &ateapipb.HTTPRule{Hostnames: patterns}}
}

func httpRuleOnPorts(ports *ateapipb.Ports, patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{Http: &ateapipb.HTTPRule{Hostnames: patterns, Ports: ports}}
}

func httpsRule(patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{Https: &ateapipb.HTTPSRule{Hostnames: patterns}}
}

func passthroughRule(ports *ateapipb.Ports, patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{TlsPassthrough: &ateapipb.TLSPassthroughRule{Hostnames: patterns, Ports: ports}}
}

func ports(numbers ...int32) *ateapipb.Ports { return &ateapipb.Ports{Numbers: numbers} }

func allPorts() *ateapipb.Ports { return &ateapipb.Ports{All: &ateapipb.AllPorts{}} }

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
		passthroughRule(ports(443, 0), "*"),
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
		ReplaceHeaders: []*ateapipb.CredentialHeader{{
			Header:        "authorization",
			Prefix:        "Bearer ",
			CredentialUri: "ate-secret://k8s/default/token",
		}},
	}
	withEffects := &ateapipb.EgressRule{Https: &ateapipb.HTTPSRule{
		Hostnames: []string{"api.example.com"},
		Effects:   effects,
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
			policy: policy(passthroughRule(allPorts(), "*")),
			dest:   host("api.example.com"), decrypted: true,
			want: Decision{RuleIndex: -1},
		},
		{
			name:   "dialed port outside the rule",
			policy: policy(httpRuleOnPorts(ports(8080), "api.example.com")),
			dest:   Destination{Hostname: "api.example.com", Port: 80},
			want:   Decision{RuleIndex: -1},
		},
		{
			name:   "dialed port in the rule",
			policy: policy(httpRuleOnPorts(ports(8080), "api.example.com")),
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
			policy: policy(httpRuleOnPorts(allPorts(), "api.example.com")),
			dest:   Destination{Hostname: "api.example.com", Port: 8080},
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "unknown dialed port is not enforced",
			policy: policy(httpRuleOnPorts(ports(8080), "api.example.com")),
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
			policy: policy(httpRuleOnPorts(allPorts(), "api.example.com"), httpRuleOnPorts(ports(80), "api.example.com")),
			dest:   host("api.example.com"),
			want:   Decision{Allowed: true, RuleIndex: 1},
		},
		{
			name:   "name specificity outranks port specificity",
			policy: policy(httpRuleOnPorts(ports(80), "*.example.com"), httpRuleOnPorts(allPorts(), "api.example.com")),
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
			// A rule ranks by its best matching pattern.
			name:   "a rule holding star and an exact name wins the exact name",
			policy: policy(httpRule("*", "api.google.com"), httpRule("*.google.com")),
			dest:   host("api.google.com"),
			want:   Decision{Allowed: true, RuleIndex: 0},
		},
		{
			name:   "a rule holding star and an exact name loses other names to a labeled wildcard",
			policy: policy(httpRule("*", "api.google.com"), httpRule("*.google.com")),
			dest:   host("admin.google.com"),
			want:   Decision{Allowed: true, RuleIndex: 1},
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

func httpsRuleOnPorts(ports *ateapipb.Ports, patterns ...string) *ateapipb.EgressRule {
	return &ateapipb.EgressRule{Https: &ateapipb.HTTPSRule{Hostnames: patterns, Ports: ports}}
}

func TestEgressRules(t *testing.T) {
	cleartext := func(patterns ...string) []EgressRule {
		rules := make([]EgressRule, len(patterns))
		for i, p := range patterns {
			rules[i] = EgressRule{Pattern: p, Mode: EgressModeCleartext}
		}
		return rules
	}
	mitm := func(patterns ...string) []EgressRule {
		rules := make([]EgressRule, len(patterns))
		for i, p := range patterns {
			rules[i] = EgressRule{Pattern: p, Mode: EgressModeMITM}
		}
		return rules
	}
	passthrough := func(patterns ...string) []EgressRule {
		rules := make([]EgressRule, len(patterns))
		for i, p := range patterns {
			rules[i] = EgressRule{Pattern: p, Mode: EgressModePassthrough}
		}
		return rules
	}
	tests := []struct {
		name   string
		policy *ateapipb.EgressPolicy
		port   uint16
		want   []EgressRule
	}{
		{name: "no rules", policy: &ateapipb.EgressPolicy{}, port: 443},
		{name: "http on its default port", policy: policy(httpRule("api.example.com", "*.example.org")), port: 80, want: cleartext("api.example.com", "*.example.org")},
		{name: "http default port covers no other", policy: policy(httpRule("api.example.com")), port: 8080},
		{name: "http on a named port", policy: policy(httpRuleOnPorts(ports(8080, 9090), "api.example.com")), port: 9090, want: cleartext("api.example.com")},
		{name: "http on all ports", policy: policy(httpRuleOnPorts(allPorts(), "api.example.com")), port: 12345, want: cleartext("api.example.com")},
		{name: "tls_passthrough on a named port", policy: policy(passthroughRule(ports(443), "tls.example.com", "*")), port: 443, want: passthrough("tls.example.com", "*")},
		{name: "tls_passthrough on all ports", policy: policy(passthroughRule(allPorts(), "tls.example.com")), port: 8443, want: passthrough("tls.example.com")},
		{name: "https on its default port", policy: policy(httpsRule("api.example.com", "*.example.org")), port: 443, want: mitm("api.example.com", "*.example.org")},
		{name: "https default port covers no other", policy: policy(httpsRule("api.example.com")), port: 8443},
		{name: "https on a named port", policy: policy(httpsRuleOnPorts(ports(8443, 9443), "api.example.com")), port: 9443, want: mitm("api.example.com")},
		{name: "https on all ports", policy: policy(httpsRuleOnPorts(allPorts(), "api.example.com")), port: 12345, want: mitm("api.example.com")},
		{
			name: "http, https, and tls_passthrough rules for the dialed port",
			policy: policy(
				httpsRule("api.example.com"),
				httpsRuleOnPorts(ports(8443), "alt.example.com"),
				httpRule("plain.example.com"),
				httpRuleOnPorts(ports(443), "http-on-443.example.com"),
				passthroughRule(ports(443), "pinned.example.com"),
				passthroughRule(ports(8443), "other-pinned.example.com"),
			),
			port: 443,
			want: []EgressRule{
				{Pattern: "api.example.com", Mode: EgressModeMITM},
				{Pattern: "http-on-443.example.com", Mode: EgressModeCleartext},
				{Pattern: "pinned.example.com", Mode: EgressModePassthrough},
			},
		},
		{
			// Name specificity outranks port specificity across http, https, and tls_passthrough.
			name: "most specific first across http, https, and tls_passthrough",
			policy: policy(
				httpsRule("*"),
				passthroughRule(allPorts(), "a.example.com"),
				httpsRule("*.example.com"),
				httpsRule("b.example.com"),
				httpRuleOnPorts(ports(443), "c.example.com"),
				passthroughRule(ports(443), "*.example.org"),
				httpsRuleOnPorts(allPorts(), "*.example.net"),
			),
			port: 443,
			want: []EgressRule{
				{Pattern: "b.example.com", Mode: EgressModeMITM},
				{Pattern: "c.example.com", Mode: EgressModeCleartext},
				{Pattern: "a.example.com", Mode: EgressModePassthrough},
				{Pattern: "*.example.com", Mode: EgressModeMITM},
				{Pattern: "*.example.org", Mode: EgressModePassthrough},
				{Pattern: "*.example.net", Mode: EgressModeMITM},
				{Pattern: "*", Mode: EgressModeMITM},
			},
		},
		{
			// Patterns rank individually, not per rule, matching
			// EvaluateRequest.
			name:   "patterns of one rule are split by rank",
			policy: policy(httpsRule("*", "api.google.com"), httpsRule("*.google.com")),
			port:   443,
			want:   mitm("api.google.com", "*.google.com", "*"),
		},
		{
			name:   "ties keep policy order",
			policy: policy(httpsRule("b.example.com", "a.example.com"), passthroughRule(ports(443), "c.example.com")),
			port:   443,
			want: []EgressRule{
				{Pattern: "b.example.com", Mode: EgressModeMITM},
				{Pattern: "a.example.com", Mode: EgressModeMITM},
				{Pattern: "c.example.com", Mode: EgressModePassthrough},
			},
		},
		{name: "invalid patterns dropped", policy: policy(httpsRule("good.example.com", "not a hostname")), port: 443, want: mitm("good.example.com")},
		{
			name: "http and https rules with effects set HasEffects",
			policy: policy(
				&ateapipb.EgressRule{Http: &ateapipb.HTTPRule{
					Hostnames: []string{"http-effects.example.com"},
					Ports:     ports(443),
					Effects: &ateapipb.HttpRuleEffects{
						ReplaceHeaders: []*ateapipb.CredentialHeader{{Header: "authorization", CredentialUri: "ate-secret://k8s/default/token"}},
					},
				}},
				&ateapipb.EgressRule{Https: &ateapipb.HTTPSRule{
					Hostnames: []string{"https-effects.example.com"},
					Effects: &ateapipb.HttpRuleEffects{
						ReplaceHeaders: []*ateapipb.CredentialHeader{{Header: "authorization", CredentialUri: "ate-secret://k8s/default/token"}},
					},
				}},
				httpsRule("no-effects.example.com"),
			),
			port: 443,
			want: []EgressRule{
				{Pattern: "http-effects.example.com", Mode: EgressModeCleartext, HasEffects: true},
				{Pattern: "https-effects.example.com", Mode: EgressModeMITM, HasEffects: true},
				{Pattern: "no-effects.example.com", Mode: EgressModeMITM, HasEffects: false},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			compiled, _ := Compile(tc.policy)
			if got := compiled.EgressRules(tc.port); !slices.Equal(got, tc.want) {
				t.Errorf("EgressRules(%d) = %v, want %v", tc.port, got, tc.want)
			}
		})
	}
}
