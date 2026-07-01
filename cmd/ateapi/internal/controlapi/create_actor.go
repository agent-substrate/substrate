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
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation/field"
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

	// The atespace must already exist.
	exists, err := s.persistence.AtespaceExists(ctx, req.GetActorRef().GetAtespace())
	if err != nil {
		return nil, fmt.Errorf("while checking atespace: %w", err)
	}
	if !exists {
		return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s not found", req.GetActorRef().GetAtespace())
	}

	id := req.GetActorRef().GetName()
	actor := &ateapipb.Actor{
		ActorId:                id,
		Version:                1,
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		ActorTemplateNamespace: req.GetActorTemplateNamespace(),
		ActorTemplateName:      req.GetActorTemplateName(),
		WorkerSelector:         req.GetWorkerSelector(),
		Atespace:               req.GetActorRef().GetAtespace(),
	}
	err = s.persistence.CreateActor(ctx, actor)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "Actor %s already exists", id)
		}
		return nil, fmt.Errorf("while recording actor: %w", err)
	}

	storedActor, err := s.persistence.GetActor(ctx, req.GetActorRef().GetAtespace(), id)
	if err != nil {
		return nil, fmt.Errorf("while fetching recorded actor from DB: %w", err)
	}

	return &ateapipb.CreateActorResponse{
		Actor: storedActor,
	}, nil
}

func validateCreateActorRequest(req *ateapipb.CreateActorRequest) error {
	var fldPath *field.Path
	var errs field.ErrorList

	if val := req.ActorTemplateNamespace; val == "" {
		errs = append(errs, field.Required(fldPath.Child("actor_template_namespace"), ""))
	} else {
		for _, msg := range content.IsDNS1123Label(val) {
			errs = append(errs, field.Invalid(fldPath.Child("actor_template_namespace"), val, msg))
		}
	}

	if val := req.ActorTemplateName; val == "" {
		errs = append(errs, field.Required(fldPath.Child("actor_template_name"), ""))
	} else {
		for _, msg := range content.IsDNS1123Subdomain(val) {
			errs = append(errs, field.Invalid(fldPath.Child("actor_template_name"), val, msg))
		}
	}

	if val := req.ActorRef; val == nil {
		errs = append(errs, field.Required(fldPath.Child("actor_ref"), ""))
	} else {
		errs = append(errs, resources.ValidateActorRef(val, fldPath.Child("actor_ref"))...)
	}

	if val := req.WorkerSelector; val != nil {
		errs = append(errs, validateSelector(val, fldPath.Child("worker_selector"))...)
	}

	if len(errs) > 0 {
		return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	return nil
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
