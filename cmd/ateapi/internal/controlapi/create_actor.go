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
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

func (s *Service) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.CreateActorResponse, error) {
	if err := validateCreateActorRequest(req); err != nil {
		return nil, err
	}
	_, err := s.actorTemplateLister.ActorTemplates(req.GetActorTemplateNamespace()).Get(req.GetActorTemplateName())
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplate %s/%s not found", req.GetActorTemplateNamespace(), req.GetActorTemplateName())
		}
		return nil, fmt.Errorf("while getting ActorTemplate: %w", err)
	}

	id := req.GetActorId()
	actor := &ateapipb.Actor{
		ActorId:                id,
		Version:                1,
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		ActorTemplateNamespace: req.GetActorTemplateNamespace(),
		ActorTemplateName:      req.GetActorTemplateName(),
	}
	err = s.persistence.CreateActor(ctx, actor)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "Actor %s already exists", id)
		}
		return nil, fmt.Errorf("while recording actor: %w", err)
	}

	storedActor, err := s.persistence.GetActor(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("while fetching recorded actor from DB: %w", err)
	}

	return &ateapipb.CreateActorResponse{
		Actor: storedActor,
	}, nil
}

func validateCreateActorRequest(req *ateapipb.CreateActorRequest) error {
	if req.GetActorTemplateNamespace() == "" {
		return status.Error(codes.InvalidArgument, "actor_template_namespace is required")
	}
	if req.GetActorTemplateName() == "" {
		return status.Error(codes.InvalidArgument, "actor_template_name is required")
	}
	if req.GetActorId() == "" {
		return status.Error(codes.InvalidArgument, "actor_id is required")
	}
	if err := resources.ValidateActorID(req.GetActorId()); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return nil
}
