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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	// First scrub any fields that users are not allowed to set, then fill the
	// defaults so validation sees the final resource state.
	in := req.GetActorTemplate()
	if in != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(in.Metadata)
		in.Status = nil
		defaults.Apply(in)
	}

	// Validate the request, including the object within it.
	if errs := validateCreateActorTemplateRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToGRPCStatusError(errs)
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
		return nil, resources.ToGRPCStatusError(errs)
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
		return nil, resources.ToGRPCStatusError(errs)
	}

	page, err := s.impl.ListActorTemplates(ctx, req.GetAtespace(), store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing actor templates in db: %w", err))
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
		return nil, resources.ToGRPCStatusError(errs)
	}
	return s.actorWorkflow.DeleteActorTemplate(ctx, resources.ActorTemplateRefFromObjectRef(req.GetActorTemplate()), toDeletePreconditions(req.GetOptions()))
}

func (s *ServiceImpl) DeleteActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.DeletePreconditions) (*ateapipb.ActorTemplate, error) {
	// TODO: implement this
	return s.store.DeleteActorTemplate(ctx, templateRef, precondition)
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

func ValidateCustom_HTTPGetAction_Path(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateHTTPGetPath(fldPath, *value)
}

func ValidateCustom_VolumeMount_MountPath(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateMountPath(fldPath, *value)
}

func ValidateCustom_ActorMetadataItem_Path(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateProjectedPath(fldPath, *value)
}

func ValidateCustom_TrustBundleDataSource_Path(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateProjectedPath(fldPath, *value)
}

// ValidateCustom_SystemInfoVolumeSource_DataSources allows at most one
// actor_metadata entry and requires every projected file path to be unique
// across all data sources: atelet writes them in order into one tree, so a
// repeated path silently clobbers the earlier file.
func ValidateCustom_SystemInfoVolumeSource_DataSources(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []*ateapipb.SystemInfoDataSource) field.ErrorList {
	var errs field.ErrorList
	seen := sets.New[string]()
	sawMetadata := false
	for i, ds := range value {
		switch {
		case ds == nil:
		case ds.TrustBundle != nil:
			if seen.Has(ds.TrustBundle.Path) {
				errs = append(errs, field.Duplicate(fldPath.Index(i).Child("trust_bundle", "path"), ds.TrustBundle.Path))
			}
			seen.Insert(ds.TrustBundle.Path)
		case ds.ActorMetadata != nil:
			if sawMetadata {
				errs = append(errs, field.Forbidden(fldPath.Index(i).Child("actor_metadata"), "at most one actor_metadata entry may appear"))
			}
			sawMetadata = true
			for j, item := range ds.ActorMetadata.Items {
				if item == nil {
					continue
				}
				if seen.Has(item.Path) {
					errs = append(errs, field.Duplicate(fldPath.Index(i).Child("actor_metadata", "items").Index(j).Child("path"), item.Path))
				}
				seen.Insert(item.Path)
			}
		}
	}
	return errs
}

func ValidateCustom_ImageVolumeSource_Reference(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidatePinnedImage(fldPath, *value)
}

func ValidateCustom_Container_Image(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidatePinnedImage(fldPath, *value)
}

func ValidateCustom_ExternalVolumeTemplate_Capacity(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if _, err := resource.ParseQuantity(*value); err != nil {
		return field.ErrorList{field.Invalid(fldPath, *value, fmt.Sprintf("must be a Kubernetes resource quantity: %v", err))}
	}
	return nil
}

// ValidateCustom_Limits validates one limit with resources.ValidateLimit.
// Presence and uniqueness of names are enforced by tags.
func ValidateCustom_Limits(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateapipb.Limits) field.ErrorList {
	return resources.ValidateLimit(fldPath, value.GetName(), value.GetQuantity())
}

// ValidateCustom_SnapshotConfig_StorageLocation ensures an
// ActorTemplate's snapshotConfig.location is a well-formed
// URI with a bucket, so a bad location fails fast.
func ValidateCustom_SnapshotConfig_StorageLocation(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if err := resources.ValidateSnapshotLocation(*value); err != nil {
		return field.ErrorList{field.Invalid(fldPath, *value, err.Error())}
	}
	return nil
}

// ValidateCustom_SnapshotConfig requires on_commit to be a subset of on_pause.
func ValidateCustom_SnapshotConfig(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateapipb.SnapshotConfig) field.ErrorList {
	if value.GetOnPause() == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA &&
		value.GetOnCommit() != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
		return field.ErrorList{field.Invalid(fldPath.Child("on_commit"), value.GetOnCommit().String(), "must be a subset of on_pause")}
	}
	return nil
}

func ValidateCustom_EnvVar_Name(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateEnvVarName(fldPath, *value)
}

func ValidateCustom_Capabilities_Add(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return resources.ValidateCapabilities(fldPath, value, false)
}

func ValidateCustom_Capabilities_Drop(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return resources.ValidateCapabilities(fldPath, value, true)
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

// ValidateCustom_Container_VolumeMounts rejects mounts that nest under one
// another. Mount-path uniqueness is enforced by the list key.
func ValidateCustom_Container_VolumeMounts(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []*ateapipb.VolumeMount) field.ErrorList {
	paths := make([]string, len(value))
	for i, m := range value {
		paths[i] = m.GetMountPath()
	}
	return resources.ValidateNestedMountPaths(fldPath, paths)
}

// ValidateCustom_CreateActorTemplateRequest_ActorTemplate rejects container
// volume mounts that reference volumes the template does not declare.
func ValidateCustom_CreateActorTemplateRequest_ActorTemplate(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateapipb.ActorTemplate) field.ErrorList {
	declared := make(map[string]bool, len(value.GetVolumes()))
	for _, vol := range value.GetVolumes() {
		declared[vol.GetName()] = true
	}
	var errs field.ErrorList
	for i, ctr := range value.GetContainers() {
		for j, mount := range ctr.GetVolumeMounts() {
			name := mount.GetName()
			if name == "" {
				continue // required is enforced by tags
			}
			if !declared[name] {
				errs = append(errs, field.Invalid(
					fldPath.Child("containers").Index(i).Child("volume_mounts").Index(j).Child("name"),
					name, "must reference a volume declared in the template"))
			}
		}
	}
	return errs
}
