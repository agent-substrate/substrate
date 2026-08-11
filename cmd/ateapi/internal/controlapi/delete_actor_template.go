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
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *Service) DeleteActorTemplate(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if errs := validateDeleteActorTemplateRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	templateRef := resources.ActorTemplateRefFromObjectRef(req.GetActorTemplate())
	lock, err := s.persistence.AcquireLock(ctx, actorTemplateLockKey(templateRef))
	if errors.Is(err, store.ErrLockConflict) {
		return nil, status.Error(codes.Aborted, "another operation is using this ActorTemplate")
	}
	if err != nil {
		return nil, fmt.Errorf("while locking ActorTemplate: %w", err)
	}
	defer lock.Close()
	deleted, err := s.persistence.DeleteActorTemplate(lock.Context(), templateRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ActorTemplate %s not found", templateRef)
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplate %s still has versions", templateRef)
		}
		return nil, fmt.Errorf("while deleting actor template from DB: %w", err)
	}

	return deleted, nil
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
