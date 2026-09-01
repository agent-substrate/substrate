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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if errs := validateCreateActorTemplateRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	in := req.GetActorTemplate()
	templateRef := resources.ActorTemplateRefFromActorTemplate(in)

	// Rebuild the template from the client-owned fields only: status is
	// server-owned and ignored per the CreateActorTemplateRequest contract.
	template := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: templateRef.Atespace,
			Name:     templateRef.Name,
		},
		WorkerSelector:  in.GetWorkerSelector(),
		Containers:      in.GetContainers(),
		Volumes:         in.GetVolumes(),
		SnapshotsConfig: in.GetSnapshotsConfig(),
		SandboxConfig:   in.GetSandboxConfig(),
		Resources:       in.GetResources(),
		Status:          &ateapipb.ActorTemplateStatus{},
	}
	stored, err := s.impl.CreateActorTemplate(ctx, template)
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

func (s *ServiceImpl) CreateActorTemplate(ctx context.Context, template *ateapipb.ActorTemplate) (*ateapipb.ActorTemplate, error) {
	// TODO: implement this
	return s.store.CreateActorTemplate(ctx, template)
}

func validateCreateActorTemplateRequest(req *ateapipb.CreateActorTemplateRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	template := req.GetActorTemplate()
	templatePath := fldPath.Child("actor_template")
	if template == nil {
		errs = append(errs, field.Required(templatePath, ""))
		return errs
	}

	// ActorTemplate is Atespaced: metadata.atespace and name are required + valid.
	metaPath := templatePath.Child("metadata")
	if val, p := template.GetMetadata().GetAtespace(), metaPath.Child("atespace"); val == "" {
		errs = append(errs, field.Required(p, ""))
	} else {
		errs = append(errs, resources.ValidateResourceName(val, p)...)
	}
	if val, p := template.GetMetadata().GetName(), metaPath.Child("name"); val == "" {
		errs = append(errs, field.Required(p, ""))
	} else {
		errs = append(errs, resources.ValidateResourceName(val, p)...)
	}

	if sel := template.GetWorkerSelector(); sel != nil {
		errs = append(errs, validateSelector(sel, templatePath.Child("worker_selector"))...)
	}

	containersPath := templatePath.Child("containers")
	if len(template.GetContainers()) == 0 {
		errs = append(errs, field.Required(containersPath, ""))
	}
	for i, container := range template.GetContainers() {
		if val, p := container.GetName(), containersPath.Index(i).Child("name"); val == "" {
			errs = append(errs, field.Required(p, ""))
		} else {
			for _, msg := range content.IsDNS1123Label(val) {
				errs = append(errs, field.Invalid(p, val, msg))
			}
		}
		if container.GetImage() == "" {
			errs = append(errs, field.Required(containersPath.Index(i).Child("image"), ""))
		}
	}

	snapshotsPath := templatePath.Child("snapshots_config")
	if snapshots := template.GetSnapshotsConfig(); snapshots == nil {
		errs = append(errs, field.Required(snapshotsPath, ""))
	} else {
		if snapshots.GetStorageLocation() == "" {
			errs = append(errs, field.Required(snapshotsPath.Child("storage_location"), ""))
		}
		// Mirrors the ActorTemplate CRD's CEL rule; UNSPECIFIED means FULL, so
		// an unset on_commit over a DATA on_pause is rejected too.
		if snapshots.GetOnPause() == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA &&
			snapshots.GetOnCommit() != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
			errs = append(errs, field.Invalid(snapshotsPath.Child("on_commit"), snapshots.GetOnCommit().String(), "must be a subset of on_pause"))
		}
	}

	sandboxPath := templatePath.Child("sandbox_config")
	if sandbox := template.GetSandboxConfig(); sandbox == nil {
		errs = append(errs, field.Required(sandboxPath, ""))
	} else {
		if sandbox.GetSandboxClass() == ateapipb.SandboxClass_SANDBOX_CLASS_UNSPECIFIED {
			errs = append(errs, field.Required(sandboxPath.Child("sandbox_class"), ""))
		}
		if sandbox.GetConfigName() == "" {
			errs = append(errs, field.Required(sandboxPath.Child("config_name"), ""))
		}
	}

	return errs
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

func validateSelector(sel *ateapipb.Selector, fldPath *field.Path) field.ErrorList {
	var errs field.ErrorList

	if sel.MatchLabels != nil {
		const maxSelectorMatchLabels = 10
		if n := len(sel.MatchLabels); n > maxSelectorMatchLabels {
			return field.ErrorList{field.TooMany(fldPath.Child("match_labels"), n, maxSelectorMatchLabels)}
		}

		for k, v := range sel.MatchLabels {
			for _, msg := range content.IsLabelKey(k) {
				errs = append(errs, field.Invalid(fldPath.Child("match_labels").Key(k), k, msg))
			}
			for _, msg := range content.IsLabelValue(v) {
				errs = append(errs, field.Invalid(fldPath.Child("match_labels").Key(k), v, msg))
			}
		}
	}

	return errs
}

func (s *ServiceImpl) UpdateActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.Precondition, mutate func(dbTemplate *ateapipb.ActorTemplate) error) (*ateapipb.ActorTemplate, error) {
	// TODO: implement this
	return s.store.UpdateActorTemplate(ctx, templateRef, precondition, mutate)
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
