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

func (s *Service) UpdateActor(ctx context.Context, req *ateapipb.UpdateActorRequest) (*ateapipb.UpdateActorResponse, error) {
	if err := validateUpdateActorRequest(req); err != nil {
		return nil, err
	}

	actor, err := s.persistence.GetActor(ctx, req.GetActor().GetAtespace(), req.GetActor().GetName())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Actor %s not found", req.GetActor().GetName())
		}
		return nil, fmt.Errorf("while getting actor: %w", err)
	}
	actor.WorkerSelector = req.GetWorkerSelector()
	// Labels are merged, not replaced: an absent/empty map leaves the actor's
	// labels untouched (so a labels-unaware caller cannot wipe selectors set by
	// someone else), a key with a non-empty value sets it, and a key with an
	// empty value deletes it. Proto3 cannot distinguish a nil map from an empty
	// one on the wire, so delete-by-empty-value is the explicit clear path.
	for k, v := range req.GetLabels() {
		if v == "" {
			delete(actor.Labels, k)
			continue
		}
		if actor.Labels == nil {
			actor.Labels = map[string]string{}
		}
		actor.Labels[k] = v
	}
	if len(actor.Labels) == 0 {
		actor.Labels = nil
	}

	updated, err := s.persistence.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())
	if err != nil {
		if errors.Is(err, store.ErrPersistenceRetry) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while updating actor: %w", err)
	}

	return &ateapipb.UpdateActorResponse{Actor: updated}, nil
}

func validateUpdateActorRequest(req *ateapipb.UpdateActorRequest) error {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.Actor, fldPath.Child("actor"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	if val := req.WorkerSelector; val != nil {
		errs = append(errs, validateSelector(val, fldPath.Child("worker_selector"))...)
	}

	errs = append(errs, validateLabels(req.GetLabels(), fldPath.Child("labels"))...)
	errs = append(errs, validateEgressPEPSelector(req.GetLabels(), fldPath.Child("labels"))...)

	if len(errs) > 0 {
		return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	return nil
}
