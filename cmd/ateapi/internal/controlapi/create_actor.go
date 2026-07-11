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

func (s *Service) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	if err := validateCreateActorRequest(req); err != nil {
		return nil, err
	}

	in := req.GetActor()
	templateNamespace := in.GetActorTemplateNamespace()
	templateName := in.GetActorTemplateName()

	_, err := s.actorTemplateLister.ActorTemplates(templateNamespace).Get(templateName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplate %s/%s not found", templateNamespace, templateName)
		}
		return nil, fmt.Errorf("while getting ActorTemplate: %w", err)
	}

	atespace := in.GetMetadata().GetAtespace()
	name := in.GetMetadata().GetName()

	// The atespace must already exist.
	exists, err := s.persistence.AtespaceExists(ctx, atespace)
	if err != nil {
		return nil, fmt.Errorf("while checking atespace: %w", err)
	}
	if !exists {
		return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s not found", atespace)
	}

	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: atespace,
			Name:     name,
		},
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		ActorTemplateNamespace: templateNamespace,
		ActorTemplateName:      templateName,
		WorkerSelector:         in.GetWorkerSelector(),
	}
	stored, err := s.persistence.CreateActor(ctx, actor)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "Actor %s already exists", name)
		}
		return nil, fmt.Errorf("while recording actor: %w", err)
	}

	return stored, nil
}

func validateCreateActorRequest(req *ateapipb.CreateActorRequest) error {
	var fldPath *field.Path
	var errs field.ErrorList

	actor := req.GetActor()
	actorPath := fldPath.Child("actor")
	if actor == nil {
		errs = append(errs, field.Required(actorPath, ""))
		return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}

	metaPath := actorPath.Child("metadata")
	if val, p := actor.GetMetadata().GetAtespace(), metaPath.Child("atespace"); val == "" {
		errs = append(errs, field.Required(p, ""))
	} else {
		errs = append(errs, resources.ValidateResourceName(val, p)...)
	}
	if val, p := actor.GetMetadata().GetName(), metaPath.Child("name"); val == "" {
		errs = append(errs, field.Required(p, ""))
	} else {
		errs = append(errs, resources.ValidateResourceName(val, p)...)
	}

	if val, p := actor.GetActorTemplateNamespace(), actorPath.Child("actor_template_namespace"); val == "" {
		errs = append(errs, field.Required(p, ""))
	} else {
		for _, msg := range content.IsDNS1123Label(val) {
			errs = append(errs, field.Invalid(p, val, msg))
		}
	}
	if val, p := actor.GetActorTemplateName(), actorPath.Child("actor_template_name"); val == "" {
		errs = append(errs, field.Required(p, ""))
	} else {
		for _, msg := range content.IsDNS1123Subdomain(val) {
			errs = append(errs, field.Invalid(p, val, msg))
		}
	}

	if val := actor.GetWorkerSelector(); val != nil {
		errs = append(errs, validateSelector(val, actorPath.Child("worker_selector"))...)
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
