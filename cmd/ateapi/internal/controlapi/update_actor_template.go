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
	"slices"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const defaultVersionMaskPath = "spec.default_version_on_create"

// actorTemplateMutableFields lists the ActorTemplate field paths a client may
// name in an UpdateActorTemplate update_mask.
var actorTemplateMutableFields = mutableFields[*ateapipb.ActorTemplate]{
	"spec.worker_selector": func(dst, src *ateapipb.ActorTemplate) {
		if dst.Spec == nil {
			dst.Spec = &ateapipb.ActorTemplateSpec{}
		}
		dst.Spec.WorkerSelector = src.GetSpec().GetWorkerSelector()
	},
	defaultVersionMaskPath: func(dst, src *ateapipb.ActorTemplate) {
		if dst.Spec == nil {
			dst.Spec = &ateapipb.ActorTemplateSpec{}
		}
		dst.Spec.DefaultVersionOnCreate = src.GetSpec().GetDefaultVersionOnCreate()
	},
}

func (s *Service) UpdateActorTemplate(ctx context.Context, req *ateapipb.UpdateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if errs := validateUpdateActorTemplateRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	in := req.GetActorTemplate()
	name := in.GetMetadata().GetName()

	// Setting a new default names an ActorTemplateVersion; take the template
	// lock so the reference check below cannot race a concurrent
	// DeleteActorTemplateVersion. Clearing the default or updating only the
	// selector needs no cross-resource check, so it stays lock-free.
	newDefault := in.GetSpec().GetDefaultVersionOnCreate()
	setsDefault := newDefault != nil && slices.Contains(req.GetUpdateMask().GetPaths(), defaultVersionMaskPath)
	if setsDefault {
		lock, err := s.persistence.AcquireLock(ctx, actorTemplateLockKey(name))
		if errors.Is(err, store.ErrLockConflict) {
			return nil, status.Error(codes.Aborted, "another operation is using this ActorTemplate")
		}
		if err != nil {
			return nil, fmt.Errorf("while locking ActorTemplate: %w", err)
		}
		defer lock.Close()
		ctx = lock.Context()
	}

	template, err := s.persistence.GetActorTemplate(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ActorTemplate %s not found", name)
		}
		return nil, fmt.Errorf("while getting actor template: %w", err)
	}

	// UID and version preconditions
	if uid := in.GetMetadata().GetUid(); uid != "" && uid != template.GetMetadata().GetUid() {
		return nil, status.Errorf(codes.Aborted, "ActorTemplate %s has uid %s, not %s", name, template.GetMetadata().GetUid(), uid)
	}

	expectedVersion := template.GetMetadata().GetVersion()
	if version := in.GetMetadata().GetVersion(); version != 0 {
		expectedVersion = version
	}

	// The new default must exist and belong to this template. Readiness is
	// deliberately not checked here: CreateActor enforces it when the default
	// is used, so a template can point at a version that is still building.
	if setsDefault {
		version, err := s.persistence.GetActorTemplateVersion(ctx, newDefault.GetName())
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplateVersion %s not found", newDefault.GetName())
			}
			return nil, fmt.Errorf("while getting actor template version: %w", err)
		}
		if parent := version.GetActorTemplate().GetName(); parent != name {
			return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplateVersion %s belongs to ActorTemplate %s, not %s",
				newDefault.GetName(), parent, name)
		}
	}

	applyUpdateMask(template, in, req.GetUpdateMask(), actorTemplateMutableFields)

	updated, err := s.persistence.UpdateActorTemplate(ctx, template, expectedVersion)
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while updating actor template: %w", err)
	}

	return updated, nil
}

func validateUpdateActorTemplateRequest(req *ateapipb.UpdateActorTemplateRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	template := req.GetActorTemplate()
	templatePath := fldPath.Child("actor_template")
	if template == nil {
		return field.ErrorList{field.Required(templatePath, "")}
	}

	errs = append(errs, resources.ValidateGlobalResourceMetadataRef(template.GetMetadata(), templatePath.Child("metadata"))...)

	errs = append(errs, validateUpdateMask(req.GetUpdateMask(), actorTemplateMutableFields)...)

	specPath := templatePath.Child("spec")
	if selector := template.GetSpec().GetWorkerSelector(); selector != nil {
		errs = append(errs, validateSelector(selector, specPath.Child("worker_selector"))...)
	}
	if ref := template.GetSpec().GetDefaultVersionOnCreate(); ref != nil {
		errs = append(errs, resources.ValidateGlobalObjectRef(ref, specPath.Child("default_version_on_create"))...)
	}

	return errs
}
