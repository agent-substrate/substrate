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

package validation

import (
	"context"
	"net/url"
	"strings"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateCreateActorEgressPolicyRequest(ctx context.Context, req *ateapipb.CreateActorEgressPolicyRequest) field.ErrorList {
	return Validate_CreateActorEgressPolicyRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

func ValidateGetActorEgressPolicyRequest(ctx context.Context, req *ateapipb.GetActorEgressPolicyRequest) field.ErrorList {
	return Validate_GetActorEgressPolicyRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

func ValidateUpdateActorEgressPolicyRequest(ctx context.Context, req *ateapipb.UpdateActorEgressPolicyRequest) field.ErrorList {
	return Validate_UpdateActorEgressPolicyRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

func ValidateEgressPolicyUpdate(ctx context.Context, p *field.Path, newVal, oldVal *ateapipb.EgressPolicy) field.ErrorList {
	return Validate_EgressPolicy(ctx, operation.Operation{Type: operation.Update}, p, newVal, oldVal)
}

func ValidateDeleteActorEgressPolicyRequest(ctx context.Context, req *ateapipb.DeleteActorEgressPolicyRequest) field.ErrorList {
	return Validate_DeleteActorEgressPolicyRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

func ValidateCustom_CreateActorEgressPolicyRequest(_ context.Context, _ operation.Operation, p *field.Path, req, _ *ateapipb.CreateActorEgressPolicyRequest) field.ErrorList {
	return validateEgressPolicyParentAtespace(req.GetActor(), req.GetEgressPolicy(), p)
}

func ValidateCustom_UpdateActorEgressPolicyRequest(_ context.Context, _ operation.Operation, p *field.Path, req, _ *ateapipb.UpdateActorEgressPolicyRequest) field.ErrorList {
	return validateEgressPolicyParentAtespace(req.GetActor(), req.GetEgressPolicy(), p)
}

func validateEgressPolicyParentAtespace(actor *ateapipb.ObjectRef, policy *ateapipb.EgressPolicy, p *field.Path) field.ErrorList {
	if actor == nil || actor.Atespace == "" {
		return nil // regular DV will handle it
	}
	actorAtespace := actor.GetAtespace()
	if policy == nil || policy.Metadata == nil || policy.Metadata.Atespace == "" {
		return nil // regular DV will handle it
	}
	policyAtespace := policy.GetMetadata().GetAtespace()
	if actorAtespace != policyAtespace {
		return field.ErrorList{
			field.Invalid(p.Child("egress_policy", "metadata", "atespace"), policyAtespace, "must match actor.atespace"),
		}
	}
	return nil
}

func ValidateCustom_EgressPolicy_Metadata(_ context.Context, _ operation.Operation, root *field.Path, meta, _ *ateapipb.ResourceMetadata) field.ErrorList {
	if meta == nil || meta.Name == "" {
		return nil // regular DV will handle it
	}
	if meta.Name != "default" {
		return field.ErrorList{field.Invalid(root.Child("name"), meta.Name, `must be "default"`).WithOrigin("custom=default")}
	}
	return nil
}

func ValidateCustom_HostnameRule_Patterns(_ context.Context, _ operation.Operation, p *field.Path, patterns, _ []string) field.ErrorList {
	var errs field.ErrorList
	for i, raw := range patterns {
		errs = append(errs, validateHostnamePattern(raw, p.Index(i))...)
	}
	return errs
}

func ValidateCustom_EgressRuleEffects(_ context.Context, _ operation.Operation, p *field.Path, effects, _ *ateapipb.EgressRuleEffects) field.ErrorList {
	var errs field.ErrorList
	if len(effects.GetInjectStaticHeaders()) == 0 {
		errs = append(errs, field.Required(p, "at least one effect must be specified"))
	}
	return errs
}

func ValidateCustom_EgressRuleEffects_InjectStaticHeaders(_ context.Context, _ operation.Operation, p *field.Path, injections, _ []*ateapipb.CredentialHeaderInjection) field.ErrorList {
	var errs field.ErrorList
	seenHeaders := map[string]bool{}
	for i, inj := range injections {
		if inj == nil {
			continue // handled by DV
		}
		norm := strings.ToLower(inj.Header)
		if seenHeaders[norm] {
			errs = append(errs, field.Duplicate(p.Index(i).Child("header"), inj.Header))
		}
		seenHeaders[norm] = true
	}
	return errs
}

func ValidateCustom_IPBlockRule_Cidrs(_ context.Context, _ operation.Operation, p *field.Path, cidrs, _ []string) field.ErrorList {
	var errs field.ErrorList
	for i, cidr := range cidrs {
		errs = append(errs, validation.IsValidCIDR(p.Index(i), cidr)...)
	}
	return errs
}

func validateHostnamePattern(raw string, p *field.Path) field.ErrorList {
	if raw == "" {
		return field.ErrorList{field.Required(p, "")}
	}
	name := strings.TrimPrefix(raw, "*.")
	if len(content.IsDNS1123Subdomain(name)) != 0 || len(validation.IsValidIP(p, name)) == 0 {
		return field.ErrorList{
			field.Invalid(p, raw, "must be a DNS hostname, optionally with a complete leftmost-label wildcard"),
		}
	}
	return nil
}

func ValidateCustom_CredentialHeaderInjection_Header(_ context.Context, _ operation.Operation, p *field.Path, header, _ *string) field.ErrorList {
	if !validHeaderName(*header) {
		return field.ErrorList{
			field.Invalid(p, *header, "must be an HTTP header name"),
		}
	}
	return nil
}

func ValidateCustom_CredentialHeaderInjection_Prefix(_ context.Context, _ operation.Operation, p *field.Path, prefix, _ *string) field.ErrorList {
	if !validHeaderValue(*prefix) {
		return field.ErrorList{
			field.Invalid(p, *prefix, "must be a valid HTTP field value prefix"),
		}
	}
	return nil
}

func ValidateCustom_CredentialHeaderInjection_CredentialUri(_ context.Context, _ operation.Operation, p *field.Path, uri, _ *string) field.ErrorList {
	if !validCredentialURI(*uri) {
		return field.ErrorList{
			field.Invalid(p, *uri, "must be substrate-secret://<provider-class>/<provider-name>/<provider-specific-tail>"),
		}
	}
	return nil
}

func validCredentialURI(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "substrate-secret" || u.Host == "" || u.Host != u.Hostname() || u.User != nil || u.RawQuery != "" || u.Fragment != "" || len(validation.IsDNS1123Subdomain(u.Host)) != 0 {
		return false
	}
	escapedPath := u.EscapedPath()
	if !strings.HasPrefix(escapedPath, "/") || strings.HasSuffix(escapedPath, "/") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(escapedPath, "/"), "/")
	if len(parts) < 2 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
	}
	return true
}

func validHeaderName(value string) bool {
	if value == "" {
		return false
	}
	for _, c := range []byte(value) {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c))) {
			return false
		}
	}
	return true
}

func validHeaderValue(value string) bool {
	for _, c := range []byte(value) {
		if c != '\t' && (c < ' ' || c == 0x7f) {
			return false
		}
	}
	return true
}
