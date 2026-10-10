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

package apivalidation

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func validEgressPolicy() *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "default"},
		Rules: []*ateapipb.EgressRule{{
			Http: &ateapipb.HTTPRule{
				Hostnames: []string{"api.example.com"},
				Effects: &ateapipb.HttpRuleEffects{
					ReplaceHeaders: []*ateapipb.CredentialHeader{{
						Header:        "Authorization",
						Prefix:        "Bearer ",
						CredentialUri: "ate-secret://kubernetes.io/provider/ns/name",
					}},
				},
			},
		}},
	}
}

// actorJWTHeader is an Authorization header replaced with a 900-second actor
// JWT for audiences.
func actorJWTHeader(audiences ...string) *ateapipb.CredentialHeader {
	return &ateapipb.CredentialHeader{
		Header:   "Authorization",
		Prefix:   "Bearer ",
		ActorJwt: &ateapipb.ActorJWTSource{Audiences: audiences, ExpirationSeconds: 900},
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
			assertValidateErr(t, ValidateCreateActorEgressPolicyRequest(context.Background(), tc.req), tc.want)
		})
	}
}

func TestValidateGetActorEgressPolicyRequest(t *testing.T) {
	validReq := func() *ateapipb.GetActorEgressPolicyRequest {
		return &ateapipb.GetActorEgressPolicyRequest{
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor"},
			Name:  resources.SingletonName,
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
		req:  &ateapipb.GetActorEgressPolicyRequest{Name: resources.SingletonName},
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
	}, {
		name: "missing singleton name",
		req: func() *ateapipb.GetActorEgressPolicyRequest {
			r := validReq()
			r.Name = ""
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("name"), ""),
		},
	}, {
		name: "wrong singleton name",
		req: func() *ateapipb.GetActorEgressPolicyRequest {
			r := validReq()
			r.Name = "other"
			return r
		}(),
		want: field.ErrorList{
			field.Invalid(field.NewPath("name"), nil, "").WithOrigin("custom=default"),
		},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertValidateErr(t, ValidateGetActorEgressPolicyRequest(context.Background(), tc.req), tc.want)
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
			r.EgressPolicy.Rules[0].Http.Hostnames = nil
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("egress_policy", "rules").Index(0).Child("http", "hostnames"), ""),
		},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertValidateErr(t, ValidateUpdateActorEgressPolicyRequest(context.Background(), tc.req), tc.want)
		})
	}
}

func TestValidateDeleteActorEgressPolicyRequest(t *testing.T) {
	validReq := func() *ateapipb.DeleteActorEgressPolicyRequest {
		return &ateapipb.DeleteActorEgressPolicyRequest{
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor"},
			Name:  resources.SingletonName,
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
		req:  &ateapipb.DeleteActorEgressPolicyRequest{Name: resources.SingletonName},
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
	}, {
		name: "missing singleton name",
		req: func() *ateapipb.DeleteActorEgressPolicyRequest {
			r := validReq()
			r.Name = ""
			return r
		}(),
		want: field.ErrorList{
			field.Required(field.NewPath("name"), ""),
		},
	}, {
		name: "wrong singleton name",
		req: func() *ateapipb.DeleteActorEgressPolicyRequest {
			r := validReq()
			r.Name = "other"
			return r
		}(),
		want: field.ErrorList{
			field.Invalid(field.NewPath("name"), nil, "").WithOrigin("custom=default"),
		},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertValidateErr(t, ValidateDeleteActorEgressPolicyRequest(context.Background(), tc.req), tc.want)
		})
	}
}

func TestValidateEgressPolicyRules(t *testing.T) {
	root := field.NewPath("egress_policy")
	rules := root.Child("rules")
	rule := rules.Index(0)
	httpRule := rule.Child("http")
	pattern := httpRule.Child("hostnames").Index(0)
	ports := httpRule.Child("ports")
	staticHeader := httpRule.Child("effects", "replace_headers").Index(0)
	const badPattern = `must be a DNS hostname, optionally with a complete leftmost-label wildcard, or "*"`
	validReq := func() *ateapipb.CreateActorEgressPolicyRequest {
		return &ateapipb.CreateActorEgressPolicyRequest{
			Actor:        &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor"},
			EgressPolicy: validEgressPolicy(),
		}
	}
	withoutEffects := func(p *ateapipb.EgressPolicy) { p.Rules[0].Http.Effects = nil }
	trivialHTTPRule := func(hostname string) *ateapipb.EgressRule {
		return &ateapipb.EgressRule{Http: &ateapipb.HTTPRule{Hostnames: []string{hostname}}}
	}
	httpsRule := func(ports *ateapipb.Ports, patterns ...string) *ateapipb.EgressRule {
		return &ateapipb.EgressRule{Https: &ateapipb.HTTPSRule{Hostnames: patterns, Ports: ports}}
	}
	passthroughRule := func(ports *ateapipb.Ports, patterns ...string) *ateapipb.EgressRule {
		return &ateapipb.EgressRule{TlsPassthrough: &ateapipb.TLSPassthroughRule{Hostnames: patterns, Ports: ports}}
	}
	portNumbers := func(numbers ...int32) *ateapipb.Ports { return &ateapipb.Ports{Numbers: numbers} }
	allPorts := func() *ateapipb.Ports { return &ateapipb.Ports{All: &ateapipb.AllPorts{}} }

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
			p.Rules[0].Https = &ateapipb.HTTPSRule{Hostnames: []string{"api.example.com"}}
		},
		want: field.ErrorList{
			field.Invalid(rule, nil, "one of").WithOrigin("union"),
		},
	}, {
		name: "https rule",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = httpsRule(portNumbers(443, 8443), "api.example.com", "*.example.org")
		},
	}, {
		name: "tls passthrough rule",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = passthroughRule(portNumbers(443), "api.example.com", "*")
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
			p.Rules[0].Http.Hostnames = nil
			withoutEffects(p)
		},
		want: field.ErrorList{
			field.Required(httpRule.Child("hostnames"), ""),
		},
	}, {
		name: "long pattern list",
		mutate: func(p *ateapipb.EgressPolicy) {
			var pats []string
			for i := range 256 {
				pats = append(pats, fmt.Sprintf("api%d.example.com", i))
			}
			p.Rules[0].Http.Hostnames = pats
			withoutEffects(p)
		},
	}, {
		name: "too-long pattern list",
		mutate: func(p *ateapipb.EgressPolicy) {
			var pats []string
			for i := range 257 {
				pats = append(pats, fmt.Sprintf("api%d.example.com", i))
			}
			p.Rules[0].Http.Hostnames = pats
			withoutEffects(p)
		},
		want: field.ErrorList{
			field.TooMany(httpRule.Child("hostnames"), 257, 256).WithOrigin("maxItems"),
		},
	}, {
		name: "missing pattern",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Hostnames[0] = ""
			withoutEffects(p)
		},
		want: field.ErrorList{
			field.Required(pattern, ""),
		},
	}, {
		name: "duplicate pattern",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Hostnames = append(p.Rules[0].Http.Hostnames, "api.example.com")
		},
		want: field.ErrorList{
			field.Duplicate(httpRule.Child("hostnames").Index(1), "api.example.com"),
		},
	}, {
		name: "invalid pattern",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Hostnames[0] = "https://example.com"
		},
		want: field.ErrorList{
			field.Invalid(pattern, "https://example.com", badPattern),
		},
	}, {
		name: "uppercase pattern",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Hostnames[0] = "API.EXAMPLE.COM"
		},
		want: field.ErrorList{
			field.Invalid(pattern, "API.EXAMPLE.COM", badPattern),
		},
	}, {
		name: "pattern with trailing dot",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Hostnames[0] = "api.example.com."
		},
		want: field.ErrorList{
			field.Invalid(pattern, "api.example.com.", badPattern),
		},
	}, {
		name: "IP literal pattern",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Hostnames[0] = "192.0.2.1"
		},
		want: field.ErrorList{
			field.Invalid(pattern, "192.0.2.1", badPattern),
		},
	}, {
		name: "pattern with port",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Hostnames[0] = "example.com:443"
		},
		want: field.ErrorList{
			field.Invalid(pattern, "example.com:443", badPattern),
		},
	}, {
		name: "wildcard without effects",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Hostnames[0] = "*.example.com"
			withoutEffects(p)
		},
	}, {
		name: "wildcard with effects",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Hostnames[0] = "*.example.com"
		},
	}, {
		name: "star pattern",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Hostnames[0] = "*"
		},
	}, {
		name: "invalid wildcard",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Hostnames[0] = "api.*.example.com"
		},
		want: field.ErrorList{
			field.Invalid(pattern, "api.*.example.com", badPattern),
		},
	}, {
		name: "invalid sni pattern",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = passthroughRule(portNumbers(443), "*example.com")
		},
		want: field.ErrorList{
			field.Invalid(rule.Child("tls_passthrough", "hostnames").Index(0), "*example.com", badPattern),
		},
	}, {
		name: "ports",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = portNumbers(80, 8080)
		},
	}, {
		name: "any port",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = allPorts()
		},
	}, {
		name: "all and numbers together",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = &ateapipb.Ports{All: &ateapipb.AllPorts{}, Numbers: []int32{80}}
		},
		want: field.ErrorList{
			field.Invalid(ports, nil, "one of").WithOrigin("union"),
		},
	}, {
		name: "empty ports",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = &ateapipb.Ports{}
		},
		want: field.ErrorList{
			field.Invalid(ports, nil, "one of").WithOrigin("union"),
		},
	}, {
		name: "empty numbers",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = &ateapipb.Ports{Numbers: []int32{}}
		},
		want: field.ErrorList{
			field.Invalid(ports, nil, "one of").WithOrigin("union"),
		},
	}, {
		name: "port zero",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = portNumbers(0)
		},
		want: field.ErrorList{
			field.Invalid(ports.Child("numbers").Index(0), int32(0), "").WithOrigin("minimum"),
		},
	}, {
		name: "port out of range",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = portNumbers(65536)
		},
		want: field.ErrorList{
			field.Invalid(ports.Child("numbers").Index(0), int32(65536), "").WithOrigin("maximum"),
		},
	}, {
		name: "duplicate port",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = portNumbers(80, 80)
		},
		want: field.ErrorList{
			field.Duplicate(ports.Child("numbers").Index(1), int32(80)),
		},
	}, {
		name: "too many ports",
		mutate: func(p *ateapipb.EgressPolicy) {
			var many []int32
			for i := range 17 {
				many = append(many, int32(8000+i))
			}
			p.Rules[0].Http.Ports = portNumbers(many...)
		},
		want: field.ErrorList{
			field.TooMany(ports.Child("numbers"), 17, 16).WithOrigin("maxItems"),
		},
	}, {
		name: "negative https port",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = httpsRule(portNumbers(-1), "api.example.com")
		},
		want: field.ErrorList{
			field.Invalid(rule.Child("https", "ports", "numbers").Index(0), int32(-1), "").WithOrigin("minimum"),
		},
	}, {
		name: "tls passthrough with empty ports",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = passthroughRule(&ateapipb.Ports{}, "*")
		},
		want: field.ErrorList{
			field.Invalid(rule.Child("tls_passthrough", "ports"), nil, "one of").WithOrigin("union"),
		},
	}, {
		name: "same pattern on different ports",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = append(p.Rules, &ateapipb.EgressRule{Http: &ateapipb.HTTPRule{Hostnames: []string{"api.example.com"}, Ports: portNumbers(8080)}})
		},
	}, {
		name: "same pattern on the default port ties",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = append(p.Rules, trivialHTTPRule("api.example.com"))
		},
		want: field.ErrorList{
			field.Invalid(rules.Index(1).Child("http", "hostnames").Index(0), "api.example.com", `ties with egress_policy.rules[0].http.hostnames[0] on port 80`),
		},
	}, {
		name: "http and https on their default ports do not tie",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = append(p.Rules, httpsRule(nil, "api.example.com"))
		},
	}, {
		name: "http and https on any port tie",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Ports = allPorts()
			p.Rules = append(p.Rules, httpsRule(allPorts(), "api.example.com"))
		},
		want: field.ErrorList{
			field.Invalid(rules.Index(1).Child("https", "hostnames").Index(0), "api.example.com", `ties with egress_policy.rules[0].http.hostnames[0] on every port`),
		},
	}, {
		name: "http and tls passthrough on every port tie",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = []*ateapipb.EgressRule{
				{Http: &ateapipb.HTTPRule{Hostnames: []string{"*"}, Ports: allPorts()}},
				passthroughRule(allPorts(), "*"),
			}
		},
		want: field.ErrorList{
			field.Invalid(rules.Index(1).Child("tls_passthrough", "hostnames").Index(0), "*", `ties with egress_policy.rules[0].http.hostnames[0] on every port`),
		},
	}, {
		name: "http on its default port and tls passthrough on every port",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = []*ateapipb.EgressRule{trivialHTTPRule("*"), passthroughRule(allPorts(), "*")}
		},
	}, {
		name: "https and tls passthrough on the same port tie",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules = append(p.Rules, httpsRule(nil, "*.example.com"), passthroughRule(portNumbers(443), "*.example.com"))
		},
		want: field.ErrorList{
			field.Invalid(rules.Index(2).Child("tls_passthrough", "hostnames").Index(0), "*.example.com", `ties with egress_policy.rules[1].https.hostnames[0] on port 443`),
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
				&ateapipb.CredentialHeader{Header: "authorization", CredentialUri: "ate-secret://example.com/provider/secret"},
			)
		},
		want: field.ErrorList{
			field.Duplicate(httpRule.Child("effects", "replace_headers").Index(1).Child("header"), "authorization"),
		},
	}, {
		name: "same header in later rule",
		mutate: func(p *ateapipb.EgressPolicy) {
			later := proto.Clone(p.Rules[0]).(*ateapipb.EgressRule)
			later.Http.Hostnames = []string{"other.example.com"}
			p.Rules = append(p.Rules, later)
		},
	}, {
		name: "many headers",
		mutate: func(p *ateapipb.EgressPolicy) {
			var injections []*ateapipb.CredentialHeader
			for i := range 16 {
				injections = append(injections, &ateapipb.CredentialHeader{
					Header:        fmt.Sprintf("X-Header-%d", i),
					CredentialUri: "ate-secret://example.com/provider/secret",
				})
			}
			p.Rules[0].Http.Effects.ReplaceHeaders = injections
		},
	}, {
		name: "too many headers",
		mutate: func(p *ateapipb.EgressPolicy) {
			var injections []*ateapipb.CredentialHeader
			for i := range 17 {
				injections = append(injections, &ateapipb.CredentialHeader{
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
		name: "no credential source",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0].CredentialUri = ""
		},
		want: field.ErrorList{
			field.Invalid(staticHeader, nil, "one of").WithOrigin("union"),
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
		name: "actor JWT on an http rule",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0] = actorJWTHeader("https://api.example.com")
		},
	}, {
		name: "actor JWT on an https rule",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0] = &ateapipb.EgressRule{Https: &ateapipb.HTTPSRule{
				Hostnames: []string{"api.example.com"},
				Effects:   &ateapipb.HttpRuleEffects{ReplaceHeaders: []*ateapipb.CredentialHeader{actorJWTHeader("https://api.example.com")}},
			}}
		},
	}, {
		name: "actor JWT next to a credential URI",
		mutate: func(p *ateapipb.EgressPolicy) {
			jwt := actorJWTHeader("https://api.example.com")
			jwt.Header = "X-Actor-Token"
			p.Rules[0].Http.Effects.ReplaceHeaders = append(p.Rules[0].Http.Effects.ReplaceHeaders, jwt)
		},
	}, {
		name: "credential URI and actor JWT in one header",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0].ActorJwt = &ateapipb.ActorJWTSource{Audiences: []string{"https://api.example.com"}, ExpirationSeconds: 900}
		},
		want: field.ErrorList{
			field.Invalid(staticHeader, nil, "one of").WithOrigin("union"),
		},
	}, {
		name: "actor JWT without audiences",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0] = actorJWTHeader()
		},
		want: field.ErrorList{
			field.Required(staticHeader.Child("actor_jwt", "audiences"), ""),
		},
	}, {
		name: "actor JWT with an empty audience",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0] = actorJWTHeader("")
		},
		want: field.ErrorList{
			field.TooShort(staticHeader.Child("actor_jwt", "audiences").Index(0), "", 1).WithOrigin("minLength"),
		},
	}, {
		name: "actor JWT with a long audience",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0] = actorJWTHeader(strings.Repeat("a", 513))
		},
		want: field.ErrorList{
			field.TooLong(staticHeader.Child("actor_jwt", "audiences").Index(0), "", 512).WithOrigin("maxLength"),
		},
	}, {
		name: "actor JWT with a repeated audience",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0] = actorJWTHeader("a", "a")
		},
		want: field.ErrorList{
			field.Duplicate(staticHeader.Child("actor_jwt", "audiences").Index(1), "a"),
		},
	}, {
		name: "actor JWT with too many audiences",
		mutate: func(p *ateapipb.EgressPolicy) {
			var audiences []string
			for i := range 17 {
				audiences = append(audiences, fmt.Sprintf("aud-%d", i))
			}
			p.Rules[0].Http.Effects.ReplaceHeaders[0] = actorJWTHeader(audiences...)
		},
		want: field.ErrorList{
			field.TooMany(staticHeader.Child("actor_jwt", "audiences"), 17, 16).WithOrigin("maxItems"),
		},
	}, {
		name: "actor JWT without a lifetime",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0] = actorJWTHeader("a")
			p.Rules[0].Http.Effects.ReplaceHeaders[0].ActorJwt.ExpirationSeconds = 0
		},
		want: field.ErrorList{
			field.Required(staticHeader.Child("actor_jwt", "expiration_seconds"), ""),
		},
	}, {
		name: "actor JWT lifetime at its bounds",
		mutate: func(p *ateapipb.EgressPolicy) {
			short, long := actorJWTHeader("a"), actorJWTHeader("a")
			short.ActorJwt.ExpirationSeconds = 300
			long.Header, long.ActorJwt.ExpirationSeconds = "X-Actor-Token", 3600
			p.Rules[0].Http.Effects.ReplaceHeaders = []*ateapipb.CredentialHeader{short, long}
		},
	}, {
		name: "actor JWT lifetime too short",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0] = actorJWTHeader("a")
			p.Rules[0].Http.Effects.ReplaceHeaders[0].ActorJwt.ExpirationSeconds = 299
		},
		want: field.ErrorList{
			field.Invalid(staticHeader.Child("actor_jwt", "expiration_seconds"), 299, "").WithOrigin("minimum"),
		},
	}, {
		name: "actor JWT lifetime too long",
		mutate: func(p *ateapipb.EgressPolicy) {
			p.Rules[0].Http.Effects.ReplaceHeaders[0] = actorJWTHeader("a")
			p.Rules[0].Http.Effects.ReplaceHeaders[0].ActorJwt.ExpirationSeconds = 3601
		},
		want: field.ErrorList{
			field.Invalid(staticHeader.Child("actor_jwt", "expiration_seconds"), 3601, "").WithOrigin("maximum"),
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
			p.Rules[0] = &ateapipb.EgressRule{Https: &ateapipb.HTTPSRule{Hostnames: []string{"api.example.com"}, Effects: &ateapipb.HttpRuleEffects{}}}
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
			defaults.Apply(req.EgressPolicy)
			assertValidateErr(t, ValidateCreateActorEgressPolicyRequest(context.Background(), req), tc.want)
		})
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
