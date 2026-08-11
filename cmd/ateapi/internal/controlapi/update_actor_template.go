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
	"github.com/agent-substrate/substrate/internal/fieldmask"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const defaultVersionMaskPath = "default_version_on_create"

// actorTemplateMutableFields lists the ActorTemplate field paths a client may
// name in an UpdateActorTemplate update_mask.
var actorTemplateMutableFields = fieldmask.NewMutableFields(defaultVersionMaskPath)

func (s *Service) UpdateActorTemplate(ctx context.Context, req *ateapipb.UpdateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if errs := validateUpdateActorTemplateRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	in := req.GetActorTemplate()
	templateRef := resources.ActorTemplateRefFromActorTemplate(in)

	// Setting a new default names an ActorTemplateVersion; take the template
	// lock so the reference check below cannot race a concurrent
	// DeleteActorTemplateVersion. Clearing the default needs no cross-resource
	// check, so it stays lock-free.
	newDefault := in.GetDefaultVersionOnCreate()
	setsDefault := newDefault != nil && slices.Contains(req.GetUpdateMask().GetPaths(), defaultVersionMaskPath)
	if setsDefault {
		lock, err := s.persistence.AcquireLock(ctx, actorTemplateLockKey(templateRef))
		if errors.Is(err, store.ErrLockConflict) {
			return nil, status.Error(codes.Aborted, "another operation is using this ActorTemplate")
		}
		if err != nil {
			return nil, fmt.Errorf("while locking ActorTemplate: %w", err)
		}
		defer lock.Close()
		ctx = lock.Context()
	}

	// The new default must exist and belong to this template. Readiness is
	// deliberately not checked here: CreateActor enforces it when the default
	// is used, so a template can point at a version that is still building.
	if setsDefault {
		versionRef := resources.ActorTemplateVersionRefFromObjectRef(newDefault)
		version, err := s.persistence.GetActorTemplateVersion(ctx, versionRef)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplateVersion %s not found", versionRef)
			}
			return nil, fmt.Errorf("while getting actor template version: %w", err)
		}
		if parentRef := resources.ActorTemplateRefFromObjectRef(version.GetActorTemplate()); parentRef != templateRef {
			return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplateVersion %s belongs to ActorTemplate %s, not %s",
				versionRef, parentRef, templateRef)
		}
	}

	updated, err := s.persistence.UpdateActorTemplate(ctx, templateRef, store.WithPrecondition(in, func(toUpdate *ateapipb.ActorTemplate) error {
		fieldmask.Apply(toUpdate, in, req.GetUpdateMask())
		return nil
	}))
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "ActorTemplate %s not found with uid %s", templateRef, in.GetMetadata().GetUid())
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ActorTemplate %s not found", templateRef)
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

	errs = append(errs, resources.ValidateResourceMetadataRef(template.GetMetadata(), templatePath.Child("metadata"))...)

	errs = append(errs, fieldmask.Validate(req.GetUpdateMask(), actorTemplateMutableFields, fldPath.Child("update_mask"))...)

	if ref := template.GetDefaultVersionOnCreate(); ref != nil {
		errs = append(errs, resources.ValidateObjectRef(ref, templatePath.Child("default_version_on_create"))...)
	}

	return errs
}
