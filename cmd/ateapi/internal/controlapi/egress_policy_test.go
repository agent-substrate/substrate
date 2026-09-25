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

package controlapi

import (
	"context"
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func validEgressPolicy() *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "default"},
		Rules: []*ateapipb.EgressRule{{
			Http: &ateapipb.HTTPRule{
				HostPatterns: []string{"api.example.com"},
				Effects: &ateapipb.HttpRuleEffects{
					ReplaceHeaders: []*ateapipb.CredentialHeaderInjection{{
						Header:        "Authorization",
						Prefix:        "Bearer ",
						CredentialUri: "ate-secret://kubernetes.io/provider/ns/name",
					}},
				},
			},
		}},
	}
}

func TestValidateCreateActorEgressPolicyRequest(t *testing.T) {
	validReq := func() *ateapipb.CreateActorEgressPolicyRequest {
		return &ateapipb.CreateActorEgressPolicyRequest{
			Actor:        &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor"},
			EgressPolicy: validEgressPolicy(),
		}
	}

	tests := []struct {
		name string
		req  *ateapipb.CreateActorEgressPolicyRequest
		want field.ErrorList
	}{{
		name: "valid",
		req:  validReq(),
	}, {
		name: "missing actor",
		req: func() *ateapipb.CreateActorEgressPolicyRequest {
			r := validReq()
			r.Actor = nil
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("actor"), ""),
		},
	}, {
		name: "missing actor atespace",
		req: func() *ateapipb.CreateActorEgressPolicyRequest {
			r := validReq()
			r.Actor.Atespace = ""
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("actor", "atespace"), ""),
		},
	}, {
		name: "invalid actor atespace",
		req: func() *ateapipb.CreateActorEgressPolicyRequest {
			r := validReq()
			r.Actor.Atespace = "invalid value"
			return r
		}(),
		want: field.ErrorList{
			field.Invalid(field.NewPath("actor", "atespace"), nil, "").WithOrigin("format=k8s-short-name"),
			field.Invalid(field.NewPath("egress_policy", "metadata", "atespace"), nil, ""),
		},
	}, {
		name: "missing actor name",
		req: func() *ateapipb.CreateActorEgressPolicyRequest {
			r := validReq()
			r.Actor.Name = ""
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("actor", "name"), ""),
		},
	}, {
		name: "invalid actor name",
		req: func() *ateapipb.CreateActorEgressPolicyRequest {
			r := validReq()
			r.Actor.Name = "invalid value"
			return r
		}(),
		want: field.ErrorList{
			field.Invalid(field.NewPath("actor", "name"), nil, "").WithOrigin("format=k8s-short-name"),
		},
	}, {
		name: "missing policy",
		req: func() *ateapipb.CreateActorEgressPolicyRequest {
			r := validReq()
			r.EgressPolicy = nil
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("egress_policy"), ""),
		},
	}, {
		name: "missing metadata",
		req: func() *ateapipb.CreateActorEgressPolicyRequest {
			r := validReq()
			r.EgressPolicy.Metadata = nil
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("egress_policy", "metadata"), ""),
		},
	}, {
		name: "missing policy atespace",
		req: func() *ateapipb.CreateActorEgressPolicyRequest {
			r := validReq()
			r.EgressPolicy.Metadata.Atespace = ""
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("egress_policy", "metadata", "atespace"), ""),
		},
	}, {
		name: "missing default name",
		req: func() *ateapipb.CreateActorEgressPolicyRequest {
			r := validReq()
			r.EgressPolicy.Metadata.Name = ""
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("egress_policy", "metadata", "name"), ""),
		},
	}, {
		name: "wrong policy name",
		req: func() *ateapipb.CreateActorEgressPolicyRequest {
			r := validReq()
			r.EgressPolicy.Metadata.Name = "other"
			return r
		}(),
		want: field.ErrorList{
			field.Invalid(field.NewPath("egress_policy", "metadata", "name"), "other", `must be "default"`).WithOrigin("custom=default"),
		},
	}, {
		name: "mismatched policy atespace",
		req: func() *ateapipb.CreateActorEgressPolicyRequest {
			r := validReq()
			r.EgressPolicy.Metadata.Atespace = "other"
			return r
		}(),
		want: field.ErrorList{
			field.Invalid(field.NewPath("egress_policy", "metadata", "atespace"), "other", "must match actor.atespace"),
		},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertValidateErr(t, validateCreateActorEgressPolicyRequest(context.Background(), tc.req), tc.want)
		})
	}
}

func TestValidateGetActorEgressPolicyRequest(t *testing.T) {
	validReq := func() *ateapipb.GetActorEgressPolicyRequest {
		return &ateapipb.GetActorEgressPolicyRequest{
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor"},
		}
	}
	tests := []struct {
		name string
		req  *ateapipb.GetActorEgressPolicyRequest
		want field.ErrorList
	}{{
		name: "valid",
		req:  validReq(),
	}, {
		name: "missing actor",
		req:  &ateapipb.GetActorEgressPolicyRequest{},
		want: field.ErrorList{
			field.Required(field.NewPath("actor"), ""),
		},
	}, {
		name: "missing atespace",
		req: func() *ateapipb.GetActorEgressPolicyRequest {
			r := validReq()
			r.Actor.Atespace = ""
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("actor", "atespace"), ""),
		},
	}, {
		name: "invalid atespace",
		req: func() *ateapipb.GetActorEgressPolicyRequest {
			r := validReq()
			r.Actor.Atespace = "invalid value"
			return r
		}(),
		want: field.ErrorList{
			field.Invalid(field.NewPath("actor", "atespace"), nil, "").WithOrigin("format=k8s-short-name"),
		},
	}, {
		name: "missing name",
		req: func() *ateapipb.GetActorEgressPolicyRequest {
			r := validReq()
			r.Actor.Name = ""
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("actor", "name"), ""),
		},
	}, {
		name: "invalid name",
		req: func() *ateapipb.GetActorEgressPolicyRequest {
			r := validReq()
			r.Actor.Name = "invalid value"
			return r
		}(),
		want: field.ErrorList{
			field.Invalid(field.NewPath("actor", "name"), nil, "").WithOrigin("format=k8s-short-name"),
		},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertValidateErr(t, validateGetActorEgressPolicyRequest(context.Background(), tc.req), tc.want)
		})
	}
}

func TestValidateUpdateActorEgressPolicyRequest(t *testing.T) {
	validReq := func() *ateapipb.UpdateActorEgressPolicyRequest {
		return &ateapipb.UpdateActorEgressPolicyRequest{
			Actor:        &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor"},
			EgressPolicy: validEgressPolicy(),
		}
	}
	tests := []struct {
		name string
		req  *ateapipb.UpdateActorEgressPolicyRequest
		want field.ErrorList
	}{{
		name: "valid",
		req:  validReq(),
	}, {
		name: "missing actor",
		req: func() *ateapipb.UpdateActorEgressPolicyRequest {
			r := validReq()
			r.Actor = nil
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("actor"), ""),
		},
	}, {
		name: "missing policy",
		req: func() *ateapipb.UpdateActorEgressPolicyRequest {
			r := validReq()
			r.EgressPolicy = nil
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("egress_policy"), ""),
		},
	}, {
		name: "mismatched atespace",
		req: func() *ateapipb.UpdateActorEgressPolicyRequest {
			r := validReq()
			r.EgressPolicy.Metadata.Atespace = "other"
			return r
		}(),
		want: field.ErrorList{
			field.Invalid(field.NewPath("egress_policy", "metadata", "atespace"), "other", "must match actor.atespace"),
		},
	}, {
		name: "invalid rule",
		req: func() *ateapipb.UpdateActorEgressPolicyRequest {
			r := validReq()
			r.EgressPolicy.Rules[0].Http.HostPatterns = nil
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("egress_policy", "rules").Index(0).Child("http", "host_patterns"), ""),
		},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertValidateErr(t, validateUpdateActorEgressPolicyRequest(context.Background(), tc.req), tc.want)
		})
	}
}

func TestValidateDeleteActorEgressPolicyRequest(t *testing.T) {
	validReq := func() *ateapipb.DeleteActorEgressPolicyRequest {
		return &ateapipb.DeleteActorEgressPolicyRequest{
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor"},
		}
	}
	tests := []struct {
		name string
		req  *ateapipb.DeleteActorEgressPolicyRequest
		want field.ErrorList
	}{{
		name: "valid",
		req:  validReq(),
	}, {
		name: "missing actor",
		req:  &ateapipb.DeleteActorEgressPolicyRequest{},
		want: field.ErrorList{
			field.Required(field.NewPath("actor"), ""),
		},
	}, {
		name: "missing atespace",
		req: func() *ateapipb.DeleteActorEgressPolicyRequest {
			r := validReq()
			r.Actor.Atespace = ""
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("actor", "atespace"), ""),
		},
	}, {
		name: "invalid atespace",
		req: func() *ateapipb.DeleteActorEgressPolicyRequest {
			r := validReq()
			r.Actor.Atespace = "invalid value"
			return r
		}(),
		want: field.ErrorList{
			field.Invalid(field.NewPath("actor", "atespace"), nil, "").WithOrigin("format=k8s-short-name"),
		},
	}, {
		name: "missing name",
		req: func() *ateapipb.DeleteActorEgressPolicyRequest {
			r := validReq()
			r.Actor.Name = ""
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("actor", "name"), ""),
		},
	}, {
		name: "invalid name",
		req: func() *ateapipb.DeleteActorEgressPolicyRequest {
			r := validReq()
			r.Actor.Name = "invalid value"
			return r
		}(),
		want: field.ErrorList{
			field.Invalid(field.NewPath("actor", "name"), nil, "").WithOrigin("format=k8s-short-name"),
		},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertValidateErr(t, validateDeleteActorEgressPolicyRequest(context.Background(), tc.req), tc.want)
		})
	}
}

func TestValidateEgressPolicyRules(t *testing.T) {
	root := field.NewPath("egress_policy")
	rules := root.Child("rules")
	rule := rules.Index(0)
	httpRule := rule.Child("http")
	pattern := httpRule.Child("host_patterns").Index(0)
	ports := httpRule.Child("ports")
	staticHeader := httpRule.Child("effects", "replace_headers").Index(0)
	const badPattern = `must be a DNS hostname, optionally with a complete leftmost-label wildcard, or "*"`
	const badPort = `must be a port number from 1 to 65535, or "*"`
	validReq := func() *ateapipb.CreateActorEgressPolicyRequest {
		return &ateapipb.CreateActorEgressPolicyRequest{
			Actor:        &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor"},
			EgressPolicy: validEgressPolicy(),
		}
	}
	withoutEffects := func(p *ateapipb.EgressPolicy) { p.Rules[0].Http.Effects = nil }
	trivialHTTPRule := func(hostname string) *ateapipb.EgressRule {
		return &ateapipb.EgressRule{Http: &ateapipb.HTTPRule{HostPatterns: []string{hostname}}}
	}
	httpsRule := func(ports []string, patterns ...string) *ateapipb.EgressRule {
		return &ateapipb.EgressRule{Https: &ateapipb.HTTPSRule{HostPatterns: patterns, Ports: ports}}
	}
	passthroughRule := func(ports []string, patterns ...string) *ateapipb.EgressRule {
		return &ateapipb.EgressRule{TlsPassthrough: &ateapipb.TLSPassthroughRule{SniPatterns: patterns, Ports: ports}}
	}

	tests := []struct {
		name   string
		mutate func(*ateapipb.EgressPolicy)
		want   field.ErrorList
	}{{
		name: "valid",
	}, {
		name: "no rules",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = nil
		},
	}, {
		name: "many rules",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = nil
			for i := range 256 {
				p.Rules = append(p.Rules, trivialHTTPRule(fmt.Sprintf("api%d.example.com", i)))
			}
		},
	}, {
		name: "too many rules",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = nil
			for i := range 257 {
				p.Rules = append(p.Rules, trivialHTTPRule(fmt.Sprintf("api%d.example.com", i)))
			}
		},
		want: field.ErrorList{
			field.TooMany(rules, 257, 256).WithOrigin("maxItems"),
		},
	}, {
		name: "nil rule",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = nil
		},
		want: field.ErrorList{
			field.Required(rule, ""),
		},
	}, {
		name: "no protocol",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = &ateapipb.EgressRule{}
		},
		want: field.ErrorList{
			field.Invalid(rule, nil, "one of").WithOrigin("union"),
		},
	}, {
		name: "two protocols",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Https = &ateapipb.HTTPSRule{HostPatterns: []string{"api.example.com"}}
		},
		want: field.ErrorList{
			field.Invalid(rule, nil, "one of").WithOrigin("union"),
		},
	}, {
		name: "https rule",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = httpsRule([]string{"443", "8443"}, "api.example.com", "*.example.org")
		},
	}, {
		name: "tls passthrough rule",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = passthroughRule([]string{"443"}, "api.example.com", "*")
		},
	}, {
		name: "tls passthrough rule without ports",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = passthroughRule(nil, "api.example.com")
		},
		want: field.ErrorList{
			field.Required(rule.Child("tls_passthrough", "ports"), ""),
		},
	}, {
		name: "empty pattern list",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.HostPatterns = nil
			withoutEffects(p)
		},
		want: field.ErrorList{
			field.Required(httpRule.Child("host_patterns"), ""),
		},
	}, {
		name: "long pattern list",
		mutate: func(p *ateapipb.EgressPolicy) {
			var pats []string
			for i := range 256 {
				pats = append(pats, fmt.Sprintf("api%d.example.com", i))
			}
			p.Rules[0].Http.HostPatterns = pats
			withoutEffects(p)
		},
	}, {
		name: "too-long pattern list",
		mutate: func(p *ateapipb.EgressPolicy) {
			var pats []string
			for i := range 257 {
				pats = append(pats, fmt.Sprintf("api%d.example.com", i))
			}
			p.Rules[0].Http.HostPatterns = pats
			withoutEffects(p)
		},
		want: field.ErrorList{
			field.TooMany(httpRule.Child("host_patterns"), 257, 256).WithOrigin("maxItems"),
		},
	}, {
		name: "missing pattern",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.HostPatterns[0] = ""
			withoutEffects(p)
		},
		want: field.ErrorList{
			field.Required(pattern, ""),
		},
	}, {
		name: "duplicate pattern",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.HostPatterns = append(p.Rules[0].Http.HostPatterns, "api.example.com")
		},
		want: field.ErrorList{
			field.Duplicate(httpRule.Child("host_patterns").Index(1), "api.example.com"),
		},
	}, {
		name: "invalid pattern",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.HostPatterns[0] = "https://example.com"
		},
		want: field.ErrorList{
			field.Invalid(pattern, "https://example.com", badPattern),
		},
	}, {
		name: "uppercase pattern",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.HostPatterns[0] = "API.EXAMPLE.COM"
		},
		want: field.ErrorList{
			field.Invalid(pattern, "API.EXAMPLE.COM", badPattern),
		},
	}, {
		name: "pattern with trailing dot",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.HostPatterns[0] = "api.example.com."
		},
		want: field.ErrorList{
			field.Invalid(pattern, "api.example.com.", badPattern),
		},
	}, {
		name: "IP literal pattern",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.HostPatterns[0] = "192.0.2.1"
		},
		want: field.ErrorList{
			field.Invalid(pattern, "192.0.2.1", badPattern),
		},
	}, {
		name: "pattern with port",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.HostPatterns[0] = "example.com:443"
		},
		want: field.ErrorList{
			field.Invalid(pattern, "example.com:443", badPattern),
		},
	}, {
		name: "wildcard without effects",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.HostPatterns[0] = "*.example.com"
			withoutEffects(p)
		},
	}, {
		name: "wildcard with effects",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.HostPatterns[0] = "*.example.com"
		},
	}, {
		name: "star pattern",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.HostPatterns[0] = "*"
		},
	}, {
		name: "invalid wildcard",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.HostPatterns[0] = "api.*.example.com"
		},
		want: field.ErrorList{
			field.Invalid(pattern, "api.*.example.com", badPattern),
		},
	}, {
		name: "invalid sni pattern",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = passthroughRule([]string{"443"}, "*example.com")
		},
		want: field.ErrorList{
			field.Invalid(rule.Child("tls_passthrough", "sni_patterns").Index(0), "*example.com", badPattern),
		},
	}, {
		name: "ports",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = []string{"80", "8080"}
		},
	}, {
		name: "any port",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = []string{"*"}
		},
	}, {
		name: "any port among others",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = []string{"80", "*"}
		},
		want: field.ErrorList{
			field.Invalid(ports.Index(1), "*", `"*" must be the only entry`),
		},
	}, {
		name: "port zero",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = []string{"0"}
		},
		want: field.ErrorList{
			field.Invalid(ports.Index(0), "0", badPort),
		},
	}, {
		name: "port out of range",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = []string{"65536"}
		},
		want: field.ErrorList{
			field.Invalid(ports.Index(0), "65536", badPort),
		},
	}, {
		name: "port with leading zero",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = []string{"080"}
		},
		want: field.ErrorList{
			field.Invalid(ports.Index(0), "080", badPort),
		},
	}, {
		name: "port by name",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = []string{"http"}
		},
		want: field.ErrorList{
			field.Invalid(ports.Index(0), "http", badPort),
		},
	}, {
		name: "duplicate port",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = []string{"80", "80"}
		},
		want: field.ErrorList{
			field.Duplicate(ports.Index(1), "80"),
		},
	}, {
		name: "too many ports",
		mutate: func(p *ateapipb.EgressPolicy) {
			var many []string
			for i := range 17 {
				many = append(many, fmt.Sprintf("%d", 8000+i))
			}
			p.Rules[0].Http.Ports = many
		},
		want: field.ErrorList{
			field.TooMany(ports, 17, 16).WithOrigin("maxItems"),
		},
	}, {
		name: "invalid https port",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = httpsRule([]string{"-1"}, "api.example.com")
		},
		want: field.ErrorList{
			field.Invalid(rule.Child("https", "ports").Index(0), "-1", badPort),
		},
	}, {
		name: "invalid tls passthrough port",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = passthroughRule([]string{"443 "}, "*")
		},
		want: field.ErrorList{
			field.Invalid(rule.Child("tls_passthrough", "ports").Index(0), "443 ", badPort),
		},
	}, {
		name: "same pattern on different ports",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = append(p.Rules, &ateapipb.EgressRule{Http: &ateapipb.HTTPRule{HostPatterns: []string{"api.example.com"}, Ports: []string{"8080"}}})
		},
	}, {
		name: "same pattern on the default port ties",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = append(p.Rules, trivialHTTPRule("api.example.com"))
		},
		want: field.ErrorList{
			field.Invalid(rules.Index(1).Child("http", "host_patterns").Index(0), "api.example.com", `ties with egress_policy.rules[0].http.host_patterns[0] on port "80"`),
		},
	}, {
		name: "http and https on their default ports do not tie",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = append(p.Rules, httpsRule(nil, "api.example.com"))
		},
	}, {
		name: "http and https on any port tie",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = []string{"*"}
			p.Rules = append(p.Rules, httpsRule([]string{"*"}, "api.example.com"))
		},
		want: field.ErrorList{
			field.Invalid(rules.Index(1).Child("https", "host_patterns").Index(0), "api.example.com", `ties with egress_policy.rules[0].http.host_patterns[0] on port "*"`),
		},
	}, {
		name: "http and tls passthrough on every port tie",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = []*ateapipb.EgressRule{
				{Http: &ateapipb.HTTPRule{HostPatterns: []string{"*"}, Ports: []string{"*"}}},
				passthroughRule([]string{"*"}, "*"),
			}
		},
		want: field.ErrorList{
			field.Invalid(rules.Index(1).Child("tls_passthrough", "sni_patterns").Index(0), "*", `ties with egress_policy.rules[0].http.host_patterns[0] on port "*"`),
		},
	}, {
		name: "http on its default port and tls passthrough on every port",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = []*ateapipb.EgressRule{trivialHTTPRule("*"), passthroughRule([]string{"*"}, "*")}
		},
	}, {
		name: "https and tls passthrough on the same port tie",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = append(p.Rules, httpsRule(nil, "*.example.com"), passthroughRule([]string{"443"}, "*.example.com"))
		},
		want: field.ErrorList{
			field.Invalid(rules.Index(2).Child("tls_passthrough", "sni_patterns").Index(0), "*.example.com", `ties with egress_policy.rules[1].https.host_patterns[0] on port "443"`),
		},
	}, {
		name: "missing static header",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0].Header = ""
		},
		want: field.ErrorList{
			field.Required(staticHeader.Child("header"), ""),
		},
	}, {
		name: "invalid static header",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0].Header = "bad header"
		},
		want: field.ErrorList{
			field.Invalid(staticHeader.Child("header"), "bad header", "must be an HTTP header name"),
		},
	}, {
		name: "duplicate header",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders = append(
				p.Rules[0].Http.Effects.ReplaceHeaders,
				&ateapipb.CredentialHeaderInjection{Header: "authorization", CredentialUri: "ate-secret://example.com/provider/secret"},
			)
		},
		want: field.ErrorList{
			field.Duplicate(httpRule.Child("effects", "replace_headers").Index(1).Child("header"), "authorization"),
		},
	}, {
		name: "same header in later rule",
		mutate: func(p *ateapipb.EgressPolicy) {
			later := proto.Clone(p.Rules[0]).(*ateapipb.EgressRule)
			later.Http.HostPatterns = []string{"other.example.com"}
			p.Rules = append(p.Rules, later)
		},
	}, {
		name: "many headers",
		mutate: func(p *ateapipb.EgressPolicy) {
			var injections []*ateapipb.CredentialHeaderInjection
			for i := range 16 {
				injections = append(injections, &ateapipb.CredentialHeaderInjection{
					Header:        fmt.Sprintf("X-Header-%d", i),
					CredentialUri: "ate-secret://example.com/provider/secret",
				})
			}
			p.Rules[0].Http.Effects.ReplaceHeaders = injections
		},
	}, {
		name: "too many headers",
		mutate: func(p *ateapipb.EgressPolicy) {
			var injections []*ateapipb.CredentialHeaderInjection
			for i := range 17 {
				injections = append(injections, &ateapipb.CredentialHeaderInjection{
					Header:        fmt.Sprintf("X-Header-%d", i),
					CredentialUri: "ate-secret://example.com/provider/secret",
				})
			}
			p.Rules[0].Http.Effects.ReplaceHeaders = injections
		},
		want: field.ErrorList{
			field.TooMany(httpRule.Child("effects", "replace_headers"), 17, 16).WithOrigin("maxItems"),
		},
	}, {
		name: "invalid prefix",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0].Prefix = "Bearer\r"
		},
		want: field.ErrorList{
			field.Invalid(staticHeader.Child("prefix"), "Bearer\r", "must be a valid HTTP field value prefix"),
		},
	}, {
		name: "missing credential URI",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0].CredentialUri = ""
		},
		want: field.ErrorList{
			field.Required(staticHeader.Child("credential_uri"), ""),
		},
	}, {
		name: "invalid credential URI",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0].CredentialUri = "https://example.com/secret"
		},
		want: field.ErrorList{
			field.Invalid(staticHeader.Child("credential_uri"), "https://example.com/secret", "must be ate-secret://<provider-class>/<provider-name>/<provider-specific-tail>"),
		},
	}, {
		name: "empty effects",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects = &ateapipb.HttpRuleEffects{}
		},
		want: field.ErrorList{
			field.Required(httpRule.Child("effects"), "at least one effect must be specified"),
		},
	}, {
		name: "effects on an https rule",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = &ateapipb.EgressRule{Https: &ateapipb.HTTPSRule{HostPatterns: []string{"api.example.com"}, Effects: &ateapipb.HttpRuleEffects{}}}
		},
		want: field.ErrorList{
			field.Required(rule.Child("https", "effects"), "at least one effect must be specified"),
		},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := validReq()
			if tc.mutate != nil {
				tc.mutate(req.EgressPolicy)
			}
			assertValidateErr(t, validateCreateActorEgressPolicyRequest(context.Background(), req), tc.want)
		})
	}
}

func TestActorEgressPolicy(t *testing.T) {
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	service := &RPCService{impl: &ServiceImpl{store: persistence}}
	if _, err := persistence.CreateAtespace(t.Context(), &ateapipb.Atespace{
		Metadata: &ateapipb.ResourceMetadata{Name: testAtespace},
	}); err != nil {
		t.Fatal(err)
	}
	_, err := persistence.CreateActor(t.Context(), &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "egress-actor"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})
	if err != nil {
		t.Fatal(err)
	}
	actorRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "egress-actor"}

	if _, err := service.GetActorEgressPolicy(t.Context(), &ateapipb.GetActorEgressPolicyRequest{
		Actor: actorRef,
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("policy before create status = %v, want NotFound", status.Code(err))
	}
	if _, err := service.GetActorEgressPolicy(t.Context(), &ateapipb.GetActorEgressPolicyRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "missing-actor"},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing parent status = %v, want NotFound", status.Code(err))
	}
	created, err := service.CreateActorEgressPolicy(t.Context(), &ateapipb.CreateActorEgressPolicyRequest{
		Actor: actorRef,
		EgressPolicy: &ateapipb.EgressPolicy{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: testAtespace,
				Name:     "default",
				Uid:      "ignored",
				Version:  99,
			}, Rules: []*ateapipb.EgressRule{{
				Http: &ateapipb.HTTPRule{
					HostPatterns: []string{"api.example.com"},
					Effects: &ateapipb.HttpRuleEffects{
						ReplaceHeaders: []*ateapipb.CredentialHeaderInjection{{
							Header:        "Authorization",
							Prefix:        "Bearer ",
							CredentialUri: "ate-secret://kubernetes.io/provider/ns/name",
						}},
					},
				},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateActorEgressPolicy(t.Context(), &ateapipb.CreateActorEgressPolicyRequest{
		Actor: actorRef,
		EgressPolicy: &ateapipb.EgressPolicy{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "default"},
		},
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("create collision status = %v, want AlreadyExists", status.Code(err))
	}
	if created.GetRules()[0].GetHttp().GetEffects().GetReplaceHeaders()[0].GetHeader() != "Authorization" {
		t.Fatalf("policy input was rewritten: %v", created)
	}
	if md := created.GetMetadata(); md.GetName() != "default" || md.GetAtespace() != testAtespace || md.GetUid() == "" || md.GetVersion() != 1 || md.GetCreateTime() == nil || md.GetUpdateTime() == nil {
		t.Fatalf("created metadata = %v", md)
	}
	got, err := service.GetActorEgressPolicy(t.Context(), &ateapipb.GetActorEgressPolicyRequest{Actor: actorRef})
	if err != nil || !proto.Equal(got, created) {
		t.Fatalf("policy after create = %v, %v; want %v", got, err, created)
	}
	if _, err := service.impl.UpdateEgressPolicy(t.Context(), resources.ActorRefFromObjectRef(actorRef), store.PreconditionFrom(created), func(policy *ateapipb.EgressPolicy) error {
		policy.Rules[0].Http.HostPatterns = nil
		return nil
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid internal update status = %v, want InvalidArgument", status.Code(err))
	}
	replacement := proto.Clone(created).(*ateapipb.EgressPolicy)
	replacement.Rules = nil
	missingPreconditions := proto.Clone(replacement).(*ateapipb.EgressPolicy)
	missingPreconditions.Metadata.Uid = ""
	missingPreconditions.Metadata.Version = 0
	if _, err := service.UpdateActorEgressPolicy(t.Context(), &ateapipb.UpdateActorEgressPolicyRequest{
		Actor:        actorRef,
		EgressPolicy: missingPreconditions,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing preconditions status = %v, want InvalidArgument", status.Code(err))
	}
	changedIdentity := proto.Clone(replacement).(*ateapipb.EgressPolicy)
	changedIdentity.Metadata.Atespace = "other"
	changedIdentity.Metadata.Name = "other"
	if _, err := service.UpdateActorEgressPolicy(t.Context(), &ateapipb.UpdateActorEgressPolicyRequest{
		Actor:        actorRef,
		EgressPolicy: changedIdentity,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("changed identity status = %v, want InvalidArgument", status.Code(err))
	}
	updated, err := service.UpdateActorEgressPolicy(t.Context(), &ateapipb.UpdateActorEgressPolicyRequest{
		Actor:        actorRef,
		EgressPolicy: replacement,
	})
	if err != nil || updated.GetMetadata().GetVersion() != 2 || len(updated.GetRules()) != 0 {
		t.Fatalf("replacement = %v, %v; want empty version 2", updated, err)
	}
	if _, err := service.UpdateActorEgressPolicy(t.Context(), &ateapipb.UpdateActorEgressPolicyRequest{
		Actor:        actorRef,
		EgressPolicy: replacement,
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale replacement status = %v, want Aborted", status.Code(err))
	}
	deleted, err := service.DeleteActorEgressPolicy(t.Context(), &ateapipb.DeleteActorEgressPolicyRequest{
		Actor: actorRef,
	})
	if err != nil || !proto.Equal(deleted, updated) {
		t.Fatalf("deleted policy = %v, %v; want %v", deleted, err, updated)
	}
	if _, err := service.GetActorEgressPolicy(t.Context(), &ateapipb.GetActorEgressPolicyRequest{
		Actor: actorRef,
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("policy after delete status = %v, want NotFound", status.Code(err))
	}
}

func TestCredentialURIValidation(t *testing.T) {
	for _, uri := range []string{
		"ate-secret://kubernetes.io/provider/ns/name",
		"ate-secret://vault.example/provider/secret",
	} {
		if !validCredentialURI(uri) {
			t.Errorf("validCredentialURI(%q) = false", uri)
		}
	}
	for _, uri := range []string{
		"https://kubernetes.io/provider/ns/name",
		"ate-secret://kubernetes.io/provider",
		"ate-secret://kubernetes.io//provider/secret",
		"ate-secret://kubernetes.io/provider/secret/",
		"ate-secret://kubernetes.io:443/provider/secret",
		"ate-secret://kubernetes.io/provider/sec%2Fret", // percent-encoded separator
		"ate-secret://kubernetes.io/provider/sec%2Dret", // percent-encoding of any kind
	} {
		if validCredentialURI(uri) {
			t.Errorf("validCredentialURI(%q) = true", uri)
		}
	}
}

func TestHeaderValueValidation(t *testing.T) {
	for _, value := range []string{"Bearer token", "value\tvalue", "\u0080\u0081"} {
		if !validHeaderValue(value) {
			t.Errorf("validHeaderValue(%q) = false", value)
		}
	}
	for _, value := range []string{"a\rb", "a\nb", "a\x00b", "a\x1fb", "a\x7fb"} {
		if validHeaderValue(value) {
			t.Errorf("validHeaderValue(%q) = true", value)
		}
	}
}

func TestHeaderNameValidation(t *testing.T) {
	for _, value := range []string{"Authorization", "x-custom_header", "!#$%&'*+-.^_`|~"} {
		if !validHeaderName(value) {
			t.Errorf("validHeaderName(%q) = false", value)
		}
	}
	for _, value := range []string{"", "bad header", "bad:header", "héader"} {
		if validHeaderName(value) {
			t.Errorf("validHeaderName(%q) = true", value)
		}
	}
}
