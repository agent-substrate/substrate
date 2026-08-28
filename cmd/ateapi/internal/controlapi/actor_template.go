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
	"regexp"
	"strings"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	// First scrub any fields that users are not allowed to set.
	in := req.GetActorTemplate()
	if in != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(in.Metadata)
		in.Status = nil
	}

	// Validate the request, including the object within it.
	if errs := validateCreateActorTemplateRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	templateRef := resources.ActorTemplateRefFromActorTemplate(in)

	stored, err := s.impl.CreateActorTemplate(ctx, in)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "ActorTemplate %s already exists", templateRef)
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, fmt.Errorf("while recording actor template: %w", err)
	}

	return stored, nil
}

func (s *ServiceImpl) CreateActorTemplate(ctx context.Context, inTemplate *ateapipb.ActorTemplate) (*ateapipb.ActorTemplate, error) {
	// Build the stored object: status is server-owned and starts empty.
	// TODO: check that sandbox_config.config_name matches sandbox_class.
	outTemplate := proto.Clone(inTemplate).(*ateapipb.ActorTemplate)
	outTemplate.Status = &ateapipb.ActorTemplateStatus{}

	// Validate the final value before storing it.
	if errs := validateActorTemplateUpdate(ctx, field.NewPath("actor_template"), outTemplate, inTemplate); len(errs) > 0 {
		return nil, toGRPCInternalError(errs)
	}

	return s.store.CreateActorTemplate(ctx, outTemplate)
}

func validateCreateActorTemplateRequest(ctx context.Context, req *ateapipb.CreateActorTemplateRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_CreateActorTemplateRequest(ctx, op, nil, req, nil)
}

func validateActorTemplateUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapipb.ActorTemplate) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Update}
	return Validate_ActorTemplate(ctx, op, fldPath, newVal, oldVal)
}

func (s *RPCService) GetActorTemplate(ctx context.Context, req *ateapipb.GetActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if errs := validateGetActorTemplateRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	templateRef := resources.ActorTemplateRefFromObjectRef(req.GetActorTemplate())
	template, err := s.impl.GetActorTemplate(ctx, templateRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "ActorTemplate %s not found", templateRef)
	} else if err != nil {
		return nil, fmt.Errorf("while getting actor template from DB: %w", err)
	}

	return template, nil
}

func (s *ServiceImpl) GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error) {
	// TODO: implement this
	return s.store.GetActorTemplate(ctx, templateRef)
}

func validateGetActorTemplateRequest(req *ateapipb.GetActorTemplateRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.ActorTemplate, fldPath.Child("actor_template"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	return errs
}

func (s *RPCService) ListActorTemplates(ctx context.Context, req *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	if errs := validateListActorTemplatesRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	page, err := s.impl.ListActorTemplates(ctx, req.GetAtespace(), store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, fmt.Errorf("while listing actor templates in db: %w", err)
	}
	return &ateapipb.ListActorTemplatesResponse{
		ActorTemplates: page.Items,
		NextPageToken:  page.NextPageToken,
	}, nil
}

func (s *ServiceImpl) ListActorTemplates(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorTemplate], error) {
	// TODO: implement this
	return s.store.ListActorTemplates(ctx, atespace, opts)
}

func validateListActorTemplatesRequest(req *ateapipb.ListActorTemplatesRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	// An empty atespace is allowed here and means "all atespaces".
	if val, fldPath := req.Atespace, fldPath.Child("atespace"); val != "" {
		errs = append(errs, resources.ValidateResourceName(val, fldPath)...)
	}

	if val, fldPath := req.PageSize, fldPath.Child("page_size"); val < 0 {
		errs = append(errs, field.Invalid(fldPath, val, "must be greater than or equal to 0"))
	}

	return errs
}

func (s *RPCService) DeleteActorTemplate(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if errs := validateDeleteActorTemplateRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	templateRef := resources.ActorTemplateRefFromObjectRef(req.GetActorTemplate())
	deleted, err := s.impl.DeleteActorTemplate(ctx, templateRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ActorTemplate %s not found", templateRef)
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, fmt.Errorf("while deleting actor template from DB: %w", err)
	}

	return deleted, nil
}

func (s *ServiceImpl) DeleteActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error) {
	// TODO: implement this
	return s.store.DeleteActorTemplate(ctx, templateRef)
}

func validateDeleteActorTemplateRequest(req *ateapipb.DeleteActorTemplateRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.ActorTemplate, fldPath.Child("actor_template"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	return errs
}

func (s *ServiceImpl) UpdateActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.Precondition, mutate func(dbTemplate *ateapipb.ActorTemplate) error) (*ateapipb.ActorTemplate, error) {
	// TODO: implement this
	return s.store.UpdateActorTemplate(ctx, templateRef, precondition, mutate)
}

// httpGetPathRE mirrors the ActorTemplate CRD's pattern for readyz paths:
// RFC 3986 path-segment characters only, with well-formed percent-escapes,
// and no query string or fragment.
var httpGetPathRE = regexp.MustCompile(`^/([A-Za-z0-9\-._~!$&'()*+,;=:@/]|%[0-9A-Fa-f]{2})*$`)

func ValidateCustom_HTTPGetAction_Path(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if !httpGetPathRE.MatchString(*value) {
		return field.ErrorList{field.Invalid(fldPath, *value, "must be a URL path starting with '/', using only RFC 3986 path-segment characters, without query or fragment")}
	}
	return nil
}

// mountPathBadSegmentRE matches '.' or '..' path segments.
var mountPathBadSegmentRE = regexp.MustCompile(`(^|/)[.][.]?(/|$)`)

// ValidateCustom_VolumeMount_MountPath mirrors the ActorTemplate CRD's CEL
// rule: a clean absolute Unix path that starts with '/', is not '/', and
// contains no ':', '.' or '..' segments, '//', trailing '/', or control
// characters.
func ValidateCustom_VolumeMount_MountPath(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	p := *value
	bad := !strings.HasPrefix(p, "/") || len(p) == 1 ||
		strings.HasSuffix(p, "/") || strings.Contains(p, "//") ||
		strings.Contains(p, ":") || mountPathBadSegmentRE.MatchString(p)
	if !bad {
		for _, r := range p {
			if r < 0x20 || r == 0x7f {
				bad = true
				break
			}
		}
	}
	if bad {
		return field.ErrorList{field.Invalid(fldPath, p, "must be a clean absolute Unix path: must start with '/', not be '/', and contain no ':', '..', '.', '//', trailing '/', or control characters")}
	}
	return nil
}

// ValidateCustom_ImageVolumeSource_Reference mirrors the ActorTemplate CRD's
// CEL rule: image references must be pinned by digest, because changing the
// image content under a fixed reference invalidates snapshots.
func ValidateCustom_ImageVolumeSource_Reference(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if !strings.Contains(*value, "@") {
		return field.ErrorList{field.Invalid(fldPath, *value, "must be pinned by digest (changing the image invalidates snapshots)")}
	}
	return nil
}

func ValidateCustom_ExternalVolumeTemplate_Capacity(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if _, err := resource.ParseQuantity(*value); err != nil {
		return field.ErrorList{field.Invalid(fldPath, *value, fmt.Sprintf("must be a Kubernetes resource quantity: %v", err))}
	}
	return nil
}

// cpuLimitMax mirrors the ActorTemplate CRD's bound: cpu limits must be less
// than 1000 cores.
var cpuLimitMax = resource.MustParse("1k")

// ValidateCustom_Resources_Limits mirrors the ActorTemplate CRD's CEL rules
// on ContainerResources: only cpu and memory limits are supported, each
// quantity must be greater than zero, and the cpu limit must be less than
// 1000 cores. Presence and uniqueness of names are enforced by tags.
func ValidateCustom_Resources_Limits(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []*ateapipb.Limits) field.ErrorList {
	var errs field.ErrorList
	for i, limit := range value {
		if limit == nil {
			continue
		}
		if limit.Name != "cpu" && limit.Name != "memory" {
			errs = append(errs, field.NotSupported(fldPath.Index(i).Child("name"), limit.Name, []string{"cpu", "memory"}))
			continue
		}
		if limit.Quantity == "" {
			continue // required is enforced by tags
		}
		q, err := resource.ParseQuantity(limit.Quantity)
		if err != nil {
			errs = append(errs, field.Invalid(fldPath.Index(i).Child("quantity"), limit.Quantity, fmt.Sprintf("must be a Kubernetes resource quantity: %v", err)))
			continue
		}
		if q.Sign() <= 0 {
			errs = append(errs, field.Invalid(fldPath.Index(i).Child("quantity"), limit.Quantity, "must be greater than zero"))
		}
		if limit.Name == "cpu" && q.Cmp(cpuLimitMax) >= 0 {
			errs = append(errs, field.Invalid(fldPath.Index(i).Child("quantity"), limit.Quantity, "cpu limit must be less than 1000 cores"))
		}
	}
	return errs
}

// ValidateCustom_ActorTemplate_SnapshotsConfig mirrors the ActorTemplate
// CRD's CEL rule: on_commit must be a subset of on_pause. UNSPECIFIED means
// FULL, so an unset on_commit over a DATA on_pause is rejected too.
func ValidateCustom_ActorTemplate_SnapshotsConfig(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateapipb.SnapshotsConfig) field.ErrorList {
	if value.GetOnPause() == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA &&
		value.GetOnCommit() != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
		return field.ErrorList{field.Invalid(fldPath.Child("on_commit"), value.GetOnCommit().String(), "must be a subset of on_pause")}
	}
	return nil
}
