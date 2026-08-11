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

// actorTemplateVersionLockKey names the distributed lock pinning one
// ActorTemplateVersion. Lock ordering: acquire the parent template's
// actorTemplateLockKey before this one.
func actorTemplateVersionLockKey(versionRef resources.ActorTemplateVersionRef) string {
	return "lock:actor-template-version:" + versionRef.String()
}

func (s *Service) CreateActorTemplateVersion(ctx context.Context, req *ateapipb.CreateActorTemplateVersionRequest) (*ateapipb.ActorTemplateVersion, error) {
	if errs := validateCreateActorTemplateVersionRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	in := req.GetActorTemplateVersion()
	versionRef := resources.ActorTemplateVersionRefFromActorTemplateVersion(in)
	parentRef := resources.ActorTemplateRefFromObjectRef(in.GetActorTemplate())

	// Default before validating: readyz fields are checked at their effective
	// values, and the defaulted workload definition is what gets stored.
	version := proto.Clone(in).(*ateapipb.ActorTemplateVersion)
	defaultActorTemplateVersion(version)
	if errs := validateActorTemplateVersion(version, field.NewPath("actor_template_version")); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	// Freeze the sandbox resolution at creation time.
	assets, err := resolveTemplateVersionSandbox(s.sandboxConfigLister, version.GetSandboxConfig())
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "while resolving sandbox for ActorTemplateVersion %s: %v", versionRef, err)
	}

	// Grab the parent lock to make sure actor template is not deleted while we create ATV.
	lock, err := s.persistence.AcquireLock(ctx, actorTemplateLockKey(parentRef))
	if errors.Is(err, store.ErrLockConflict) {
		return nil, status.Error(codes.Aborted, "another operation is using this ActorTemplate")
	}
	if err != nil {
		return nil, fmt.Errorf("while locking ActorTemplate: %w", err)
	}
	defer lock.Close()

	exists, err := s.persistence.ActorTemplateExists(lock.Context(), parentRef)
	if err != nil {
		return nil, fmt.Errorf("while checking ActorTemplate existence: %w", err)
	}
	if !exists {
		return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplate %s not found", parentRef)
	}

	// The request's server-owned fields are ignored: versions start INITIAL
	// with the sandbox resolution frozen into sandbox_config at creation.
	version.Metadata = &ateapipb.ResourceMetadata{Atespace: versionRef.Atespace, Name: versionRef.Name}
	version.ActorTemplate = parentRef.ToObjectRef()
	version.GoldenSnapshot = nil
	version.Phase = &ateapipb.ActorTemplateVersionPhase{Phase: ateapipb.ActorTemplateVersionPhase_PHASE_INITIAL}
	version.SandboxConfig.SandboxAssets = assets
	stored, err := s.persistence.CreateActorTemplateVersion(lock.Context(), version)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "ActorTemplateVersion %s already exists", versionRef)
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

	// ActorTemplateVersion is Atespaced: metadata.atespace and name are
	// required + valid.
	metaPath := versionPath.Child("metadata")
	if val, p := version.GetMetadata().GetAtespace(), metaPath.Child("atespace"); val == "" {
		errs = append(errs, field.Required(p, ""))
	} else {
		errs = append(errs, resources.ValidateResourceName(val, p)...)
	}
	if val, p := version.GetMetadata().GetName(), metaPath.Child("name"); val == "" {
		errs = append(errs, field.Required(p, ""))
	} else {
		errs = append(errs, resources.ValidateResourceName(val, p)...)
	}

	if val, p := version.GetActorTemplate(), versionPath.Child("actor_template"); val == nil {
		errs = append(errs, field.Required(p, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, p)...)
		// The parent lives in the same atespace as its versions.
		if ns := version.GetMetadata().GetAtespace(); ns != "" && val.GetAtespace() != "" && val.GetAtespace() != ns {
			errs = append(errs, field.Invalid(p.Child("atespace"), val.GetAtespace(), "must match the version's atespace"))
		}
	}

	// The workload definition is validated by validateActorTemplateVersion
	// after defaulting.

	return errs
}
