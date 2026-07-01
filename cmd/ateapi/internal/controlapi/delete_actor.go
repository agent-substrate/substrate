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

func (s *Service) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.DeleteActorResponse, error) {
	if err := validateDeleteActorRequest(req); err != nil {
		return nil, err
	}

	if err := s.persistence.DeleteActor(ctx, req.GetActorRef().GetAtespace(), req.GetActorRef().GetName()); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Actor %s not found", req.GetActorRef().GetName())
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			actor, getErr := s.persistence.GetActor(ctx, req.GetActorRef().GetAtespace(), req.GetActorRef().GetName())
			if getErr == nil {
				return nil, status.Errorf(codes.FailedPrecondition, "Actor %s is not suspended (status: %v)", req.GetActorRef().GetName(), actor.GetStatus())
			}
			return nil, status.Errorf(codes.FailedPrecondition, "Actor %s is not suspended", req.GetActorRef().GetName())
		}
		if errors.Is(err, store.ErrPersistenceRetry) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting actor from DB: %w", err)
	}

	return &ateapipb.DeleteActorResponse{}, nil
}

func validateDeleteActorRequest(req *ateapipb.DeleteActorRequest) error {
	var fldPath *field.Path
	var errs field.ErrorList

	if val := req.ActorRef; val == nil {
		errs = append(errs, field.Required(fldPath.Child("actor_ref"), ""))
	} else {
		errs = append(errs, resources.ValidateActorRef(val, fldPath.Child("actor_ref"))...)
	}

	if len(errs) > 0 {
		return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	return nil
}
