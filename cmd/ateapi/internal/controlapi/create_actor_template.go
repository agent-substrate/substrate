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

// actorTemplateLockKey names the distributed lock serializing the operations
// that maintain cross-resource invariants within one template family:
// DeleteActorTemplate vs CreateActorTemplateVersion, and
// DeleteActorTemplateVersion vs UpdateActorTemplate setting a default.
func actorTemplateLockKey(templateRef resources.ActorTemplateRef) string {
	return "lock:actor-template:" + templateRef.String()
}

func (s *Service) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if errs := validateCreateActorTemplateRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	in := req.GetActorTemplate()
	templateRef := resources.ActorTemplateRefFromActorTemplate(in)
	template := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: templateRef.Atespace,
			Name:     templateRef.Name,
		},
	}
	stored, err := s.persistence.CreateActorTemplate(ctx, template)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "ActorTemplate %s already exists", templateRef)
		}
		return nil, fmt.Errorf("while recording actor template: %w", err)
	}

	return stored, nil
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

	// No version of a brand-new template can exist yet, so a default set at
	// creation could never be valid.
	if template.GetDefaultVersionOnCreate() != nil {
		errs = append(errs, field.Forbidden(templatePath.Child("default_version_on_create"),
			"cannot be set at creation; set it via UpdateActorTemplate once the version exists"))
	}

	return errs
}
