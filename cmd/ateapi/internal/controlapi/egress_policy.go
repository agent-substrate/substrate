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
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateActorEgressPolicy(ctx context.Context, req *ateapipb.CreateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	policy := req.GetEgressPolicy()
	if policy != nil {
		scrubResourceMetadataForCreate(policy.Metadata)
	}
	if errs := validateCreateActorEgressPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	return s.impl.CreateEgressPolicy(ctx, actorRef, policy)
}

func (s *ServiceImpl) CreateEgressPolicy(ctx context.Context, actorRef resources.ActorRef, policy *ateapipb.EgressPolicy) (*ateapipb.EgressPolicy, error) {
	created, err := s.store.CreateEgressPolicy(ctx, actorRef, policy)
	return mapEgressPolicyWrite(created, err)
}

func validateCreateActorEgressPolicyRequest(ctx context.Context, req *ateapipb.CreateActorEgressPolicyRequest) field.ErrorList {
	return Validate_CreateActorEgressPolicyRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

func (s *RPCService) GetActorEgressPolicy(ctx context.Context, req *ateapipb.GetActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	if errs := validateGetActorEgressPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	return s.impl.GetEgressPolicy(ctx, resources.ActorRefFromObjectRef(req.GetActor()))
}

func (s *ServiceImpl) GetEgressPolicy(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.EgressPolicy, error) {
	policy, err := s.store.GetEgressPolicy(ctx, actorRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "EgressPolicy not found")
	}
	if err != nil {
		return nil, fmt.Errorf("while getting Actor egress policy: %w", err)
	}
	return policy, nil
}

func validateGetActorEgressPolicyRequest(ctx context.Context, req *ateapipb.GetActorEgressPolicyRequest) field.ErrorList {
	return Validate_GetActorEgressPolicyRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

func (s *RPCService) UpdateActorEgressPolicy(ctx context.Context, req *ateapipb.UpdateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	policy := req.GetEgressPolicy()
	if policy != nil {
		scrubResourceMetadataForUpdate(policy.Metadata)
	}
	if errs := validateUpdateActorEgressPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	return s.impl.UpdateEgressPolicy(ctx, actorRef, store.PreconditionFrom(policy), func(toUpdate *ateapipb.EgressPolicy) error {
		metadata := toUpdate.GetMetadata()
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, policy)
		toUpdate.Metadata = metadata
		return nil
	})
}

func (s *ServiceImpl) UpdateEgressPolicy(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapipb.EgressPolicy) error) (*ateapipb.EgressPolicy, error) {
	updated, err := s.store.UpdateEgressPolicy(ctx, actorRef, precondition, func(toUpdate *ateapipb.EgressPolicy) error {
		oldVal := proto.Clone(toUpdate).(*ateapipb.EgressPolicy)
		if err := mutate(toUpdate); err != nil {
			return err
		}
		if errs := validateEgressPolicyUpdate(ctx, field.NewPath("egress_policy"), toUpdate, oldVal); len(errs) > 0 {
			return toGRPCStatusError(errs)
		}
		// EgressPolicy has no status or other server-derived fields to verify.
		return nil
	})
	return mapEgressPolicyWrite(updated, err)
}

func validateUpdateActorEgressPolicyRequest(ctx context.Context, req *ateapipb.UpdateActorEgressPolicyRequest) field.ErrorList {
	return Validate_UpdateActorEgressPolicyRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

func validateEgressPolicyUpdate(ctx context.Context, p *field.Path, newVal, oldVal *ateapipb.EgressPolicy) field.ErrorList {
	return Validate_EgressPolicy(ctx, operation.Operation{Type: operation.Update}, p, newVal, oldVal)
}

func (s *RPCService) DeleteActorEgressPolicy(ctx context.Context, req *ateapipb.DeleteActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	if errs := validateDeleteActorEgressPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	return s.impl.DeleteEgressPolicy(ctx, resources.ActorRefFromObjectRef(req.GetActor()))
}

func (s *ServiceImpl) DeleteEgressPolicy(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.EgressPolicy, error) {
	deleted, err := s.store.DeleteEgressPolicy(ctx, actorRef)
	return mapEgressPolicyWrite(deleted, err)
}

func validateDeleteActorEgressPolicyRequest(ctx context.Context, req *ateapipb.DeleteActorEgressPolicyRequest) field.ErrorList {
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

func mapEgressPolicyWrite(policy *ateapipb.EgressPolicy, err error) (*ateapipb.EgressPolicy, error) {
	switch {
	case err == nil:
		return policy, nil
	case errors.Is(err, store.ErrNotFound):
		return nil, status.Error(codes.NotFound, "EgressPolicy not found")
	case errors.Is(err, store.ErrAlreadyExists):
		return nil, status.Error(codes.AlreadyExists, "EgressPolicy already exists")
	case errors.Is(err, store.ErrVersionConflict):
		return nil, status.Error(codes.Aborted, "EgressPolicy version conflict")
	case errors.Is(err, store.ErrUIDConflict):
		return nil, status.Error(codes.Aborted, "EgressPolicy UID conflict")
	case errors.Is(err, store.ErrPreconditionRequired):
		return nil, status.Error(codes.InvalidArgument, "EgressPolicy UID and version are required")
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, status.Error(codes.FailedPrecondition, "parent Actor does not exist")
	default:
		return nil, fmt.Errorf("while writing EgressPolicy: %w", err)
	}
}
