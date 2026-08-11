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

func (s *Service) GetActorTemplateVersion(ctx context.Context, req *ateapipb.GetActorTemplateVersionRequest) (*ateapipb.ActorTemplateVersion, error) {
	if errs := validateGetActorTemplateVersionRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	versionRef := resources.ActorTemplateVersionRefFromObjectRef(req.GetActorTemplateVersion())
	version, err := s.persistence.GetActorTemplateVersion(ctx, versionRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "ActorTemplateVersion %s not found", versionRef)
	} else if err != nil {
		return nil, fmt.Errorf("while getting actor template version from DB: %w", err)
	}

	return version, nil
}

func validateGetActorTemplateVersionRequest(req *ateapipb.GetActorTemplateVersionRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.ActorTemplateVersion, fldPath.Child("actor_template_version"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	return errs
}
