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

	// config_name is required; the declarative validation has already
	// rejected an empty one.
	if _, err := resolveTemplateSandboxConfig(s.sandboxConfigLister, in.GetSandboxConfig()); err != nil {
		return nil, err
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
	if errs := validateGetActorTemplateRequest(ctx, req); len(errs) > 0 {
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

func validateGetActorTemplateRequest(ctx context.Context, req *ateapipb.GetActorTemplateRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_GetActorTemplateRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) ListActorTemplates(ctx context.Context, req *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	if errs := validateListActorTemplatesRequest(ctx, req); len(errs) > 0 {
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

func validateListActorTemplatesRequest(ctx context.Context, req *ateapipb.ListActorTemplatesRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_ListActorTemplatesRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) DeleteActorTemplate(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if errs := validateDeleteActorTemplateRequest(ctx, req); len(errs) > 0 {
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

func validateDeleteActorTemplateRequest(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_DeleteActorTemplateRequest(ctx, op, nil, req, nil)
}

func (s *ServiceImpl) UpdateActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.Precondition, mutate func(dbTemplate *ateapipb.ActorTemplate) error) (*ateapipb.ActorTemplate, error) {
	// ActorTemplates are immutable to clients: there is no update RPC, and
	// the only writer is the template reconciler, which updates status
	// against the store directly. The store enforces metadata immutability,
	// so this layer has nothing to add.
	return s.store.UpdateActorTemplate(ctx, templateRef, precondition, mutate)
}

// httpGetPathRE constrains readyz paths to RFC 3986 path-segment
// characters only, with well-formed percent-escapes, and no query string
// or fragment.
var httpGetPathRE = regexp.MustCompile(`^/([A-Za-z0-9\-._~!$&'()*+,;=:@/]|%[0-9A-Fa-f]{2})*$`)

func ValidateCustom_HTTPGetAction_Path(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if !httpGetPathRE.MatchString(*value) {
		return field.ErrorList{field.Invalid(fldPath, *value, "must be a URL path starting with '/', using only RFC 3986 path-segment characters, without query or fragment")}
	}
	return nil
}

// mountPathBadSegmentRE matches '.' or '..' path segments.
var mountPathBadSegmentRE = regexp.MustCompile(`(^|/)[.][.]?(/|$)`)

// ValidateCustom_VolumeMount_MountPath requires a clean absolute Unix path
// that starts with '/', is not '/', and contains no ':', '.' or '..'
// segments, '//', trailing '/', or control characters.
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

// ValidateCustom_ImageVolumeSource_Reference requires image references to
// be pinned by digest, because changing the image content under a fixed
// reference invalidates snapshots.
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

// cpuLimitMax bounds cpu limits: they must be less than 1000 cores.
var cpuLimitMax = resource.MustParse("1k")

// ValidateCustom_Resources_Limits validates the resource limits: only cpu
// and memory limits are supported, each quantity must be greater than zero,
// and the cpu limit must be less than 1000 cores. Presence and uniqueness
// of names are enforced by tags.
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

// ValidateCustom_SnapshotsConfig requires on_commit to be a
// subset of on_pause. UNSPECIFIED means FULL, so an unset on_commit over a
// DATA on_pause is rejected too.
func ValidateCustom_SnapshotsConfig(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateapipb.SnapshotsConfig) field.ErrorList {
	if value.GetOnPause() == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA &&
		value.GetOnCommit() != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
		return field.ErrorList{field.Invalid(fldPath.Child("on_commit"), value.GetOnCommit().String(), "must be a subset of on_pause")}
	}
	return nil
}

// envVarNameRE constrains env var names to any printable ASCII character
// except '='.
var envVarNameRE = regexp.MustCompile(`^[ -<>-~]+$`)

func ValidateCustom_EnvVar_Name(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if !envVarNameRE.MatchString(*value) {
		return field.ErrorList{field.Invalid(fldPath, *value, "may contain any printable ASCII character except '='")}
	}
	return nil
}

// capabilityRE constrains Linux capability names: uppercase, without the
// "CAP_" prefix (which is added when the OCI spec is written; the prefixed
// spelling would silently grant nothing).
var capabilityRE = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

func validateCapabilities(fldPath *field.Path, caps []string, allowAll bool) field.ErrorList {
	var errs field.ErrorList
	for i, c := range caps {
		p := fldPath.Index(i)
		switch {
		case c == "ALL" && !allowAll:
			errs = append(errs, field.Invalid(p, c, "add does not accept 'ALL'; name the individual capabilities the container needs"))
		case c == "ALL":
		case len(c) > 63:
			errs = append(errs, field.TooLong(p, nil, 63))
		case strings.HasPrefix(c, "CAP_"):
			errs = append(errs, field.Invalid(p, c, "must be named without the 'CAP_' prefix (e.g. 'NET_BIND_SERVICE')"))
		case !capabilityRE.MatchString(c):
			errs = append(errs, field.Invalid(p, c, "must be an uppercase capability name like 'NET_BIND_SERVICE'"))
		}
	}
	return errs
}

func ValidateCustom_Capabilities_Add(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return validateCapabilities(fldPath, value, false)
}

func ValidateCustom_Capabilities_Drop(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return validateCapabilities(fldPath, value, true)
}

// actorTemplateGetter is the storage subset template resolution needs.
type actorTemplateGetter interface {
	GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error)
}

// errActorTemplateNotFound matches (via errors.Is) resolution failures where
// the actor names a template that does not exist. Most callers return the
// error as is — it already carries FailedPrecondition — while delete
// tolerates it and cleans up without the template.
var errActorTemplateNotFound = status.New(codes.FailedPrecondition, "actor template not found").Err()

// resolveActorTemplate resolves the substrate ActorTemplate the actor's
// actor_template ref names. A missing template surfaces as
// errActorTemplateNotFound.
func resolveActorTemplate(ctx context.Context, st actorTemplateGetter, actor *ateapipb.Actor) (*ateapipb.ActorTemplate, error) {
	templateRef := resources.ActorTemplateRefFromObjectRef(actor.GetActorTemplate())
	template, err := st.GetActorTemplate(ctx, templateRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("%w; ObjectRef: %s ", errActorTemplateNotFound, templateRef)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting ActorTemplate: %w", err)
	}
	return template, nil
}

// actorTemplateObjectRef returns a fresh copy of the actor's template
// reference — fresh so records built from it never alias the actor message.
func actorTemplateObjectRef(actor *ateapipb.Actor) *ateapipb.ObjectRef {
	ref := actor.GetActorTemplate()
	if ref == nil {
		return nil
	}
	return &ateapipb.ObjectRef{Atespace: ref.GetAtespace(), Name: ref.GetName()}
}
