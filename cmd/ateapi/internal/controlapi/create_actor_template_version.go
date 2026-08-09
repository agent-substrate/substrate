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
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *Service) CreateActorTemplateVersion(ctx context.Context, req *ateapipb.CreateActorTemplateVersionRequest) (*ateapipb.ActorTemplateVersion, error) {
	if errs := validateCreateActorTemplateVersionRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	in := req.GetActorTemplateVersion()
	name := in.GetMetadata().GetName()
	parent := in.GetActorTemplate().GetName()

	// Default before validating: readyz fields are checked at their effective
	// values, and the defaulted spec is what gets stored.
	spec := proto.Clone(in.GetSpec()).(*ateapipb.ActorTemplateVersionSpec)
	defaultActorTemplateVersionSpec(spec)
	if errs := validateActorTemplateVersionSpec(spec, field.NewPath("actor_template_version", "spec")); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	// Freeze the sandbox resolution into status at creation time.
	assets, err := resolveTemplateVersionSandbox(s.sandboxConfigLister, spec.GetSandboxConfig())
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "while resolving sandbox for ActorTemplateVersion %s: %v", name, err)
	}

	// The parent-exists check must not race DeleteActorTemplate's
	// no-remaining-versions scan.
	lock, err := s.persistence.AcquireLock(ctx, actorTemplateLockKey(parent))
	if errors.Is(err, store.ErrLockConflict) {
		return nil, status.Error(codes.Aborted, "another operation is using this ActorTemplate")
	}
	if err != nil {
		return nil, fmt.Errorf("while locking ActorTemplate: %w", err)
	}
	defer lock.Close()

	exists, err := s.persistence.ActorTemplateExists(lock.Context(), parent)
	if err != nil {
		return nil, fmt.Errorf("while checking ActorTemplate existence: %w", err)
	}
	if !exists {
		return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplate %s not found", parent)
	}

	// The request's status is ignored: versions start INITIAL with the
	// sandbox resolution frozen at creation.
	version := &ateapipb.ActorTemplateVersion{
		Metadata: &ateapipb.ResourceMetadata{
			Name: name,
		},
		ActorTemplate: &ateapipb.ObjectRef{Name: parent},
		Spec:          spec,
		Status: &ateapipb.ActorTemplateVersionStatus{
			State:           ateapipb.ActorTemplateVersionStatus_STATE_INITIAL,
			ResolvedSandbox: assets,
		},
	}
	stored, err := s.persistence.CreateActorTemplateVersion(lock.Context(), version)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "ActorTemplateVersion %s already exists", name)
		}
		return nil, fmt.Errorf("while recording actor template version: %w", err)
	}

	return stored, nil
}

func validateCreateActorTemplateVersionRequest(req *ateapipb.CreateActorTemplateVersionRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	version := req.GetActorTemplateVersion()
	versionPath := fldPath.Child("actor_template_version")
	if version == nil {
		errs = append(errs, field.Required(versionPath, ""))
		return errs
	}

	// ActorTemplateVersion is global-scoped: metadata.atespace must be empty,
	// name required + valid.
	metaPath := versionPath.Child("metadata")
	if val, p := version.GetMetadata().GetAtespace(), metaPath.Child("atespace"); val != "" {
		errs = append(errs, field.Invalid(p, val, "must be empty for a global-scoped resource"))
	}
	if val, p := version.GetMetadata().GetName(), metaPath.Child("name"); val == "" {
		errs = append(errs, field.Required(p, ""))
	} else {
		errs = append(errs, resources.ValidateResourceName(val, p)...)
	}

	if val, p := version.GetActorTemplate(), versionPath.Child("actor_template"); val == nil {
		errs = append(errs, field.Required(p, ""))
	} else {
		errs = append(errs, resources.ValidateGlobalObjectRef(val, p)...)
	}

	// Spec content is validated by validateActorTemplateVersionSpec after
	// defaulting; only its presence is a request-shape concern.
	if version.GetSpec() == nil {
		errs = append(errs, field.Required(versionPath.Child("spec"), ""))
	}

	return errs
}
