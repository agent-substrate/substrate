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
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *Service) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (created *ateapipb.Actor, err error) {
	if errs := validateCreateActorRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	start := time.Now()
	in := req.GetActor()
	templateNamespace := in.GetActorTemplateNamespace()
	templateName := in.GetActorTemplateName()
	if in.GetActorTemplate() != "" {
		// Native templates are global-scoped: the namespace attribute stays empty.
		templateName = in.GetActorTemplate()
	}
	// Recorded only after validation, so every operation uniformly measures a
	// validated request; malformed ones stay visible in rpc.server.call.duration.
	defer func() {
		s.instruments.recordLifecycleOp(ctx, ateattr.OperationCreate, start, err,
			ateattr.TemplateNameKey.String(templateName),
			ateattr.TemplateNamespaceKey.String(templateNamespace),
		)
	}()
	var sourceSnapshot *ateapipb.ActorSnapshot
	var sourceSnapshotRef *ateapipb.ObjectRef
	if ref := req.GetSourceSnapshot(); ref != nil {
		if _, ok := ref.GetReference().(*ateapipb.ActorSnapshotRef_Tag); !ok {
			return nil, status.Error(codes.FailedPrecondition, "source ActorSnapshot must be referenced by tag")
		}
		lock, snapshot, canonical, tag, err := s.lockActorSnapshot(ctx, ref)
		if err != nil {
			return nil, err
		}
		defer lock.Close()
		ctx = lock.Context()
		sourceSnapshot = snapshot
		sourceSnapshotRef = canonical
		target := in.GetMetadata()
		switch tag.GetScope() {
		case ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE:
			if tag.GetMetadata().GetAtespace() != target.GetAtespace() {
				return nil, status.Error(codes.FailedPrecondition, "ActorSnapshot tag is not published outside its Atespace")
			}
		case ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED:
		default:
			return nil, status.Error(codes.FailedPrecondition, "source ActorSnapshot tag has an invalid scope")
		}
	}
	setSpanActorRefAttributes(ctx, resources.ActorRefFromActor(in))

	var template *atev1alpha1.ActorTemplate
	var versionPin string
	if in.GetActorTemplate() != "" {
		at, err := s.persistence.GetActorTemplate(ctx, in.GetActorTemplate())
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplate %q not found", in.GetActorTemplate())
			}
			return nil, fmt.Errorf("while getting ActorTemplate: %w", err)
		}
		versionPin = in.GetActorTemplateVersion()
		if versionPin == "" {
			versionPin = at.GetSpec().GetDefaultVersionOnCreate().GetName()
		}
		if versionPin == "" {
			return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplate %q has no default_version_on_create; pin a version", in.GetActorTemplate())
		}
		atv, err := s.persistence.GetActorTemplateVersion(ctx, versionPin)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplateVersion %q not found", versionPin)
			}
			return nil, fmt.Errorf("while getting ActorTemplateVersion: %w", err)
		}
		if atv.GetActorTemplate().GetName() != at.GetMetadata().GetName() {
			return nil, status.Errorf(codes.FailedPrecondition,
				"ActorTemplateVersion %q belongs to ActorTemplate %q, not %q",
				versionPin, atv.GetActorTemplate().GetName(), at.GetMetadata().GetName())
		}
		// Only READY versions have a golden snapshot to run from. The golden
		// actor building that snapshot is the one exception; like
		// commitSnapshotScope, the reserved atespace is the discriminator.
		if state := atv.GetStatus().GetState(); state != ateapipb.ActorTemplateVersionStatus_STATE_READY &&
			in.GetMetadata().GetAtespace() != resources.GoldenActorAtespace {
			return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplateVersion %q is not READY (state %s)", versionPin, state)
		}
		if template, err = crdTemplateFromVersion(at, atv); err != nil {
			return nil, err
		}
	} else {
		var err error
		template, err = s.actorTemplateLister.ActorTemplates(templateNamespace).Get(templateName)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplate %s/%s not found", templateNamespace, templateName)
			}
			return nil, fmt.Errorf("while getting ActorTemplate: %w", err)
		}
	}
	// TODO: Permit compatible DATA snapshots when runtimes can extract portable data.
	if sourceSnapshot != nil && sourceSnapshot.GetActorTemplateUid() != string(template.GetUID()) {
		return nil, status.Error(codes.FailedPrecondition, "ActorSnapshot requires the source ActorTemplate")
	}
	if sourceSnapshot != nil {
		for _, volume := range template.Spec.Volumes {
			if volume.ExternalVolumeTemplate != nil {
				// TODO: Permit cloning after CSI volume snapshots are supported.
				return nil, status.Error(codes.FailedPrecondition, "ActorSnapshot cloning does not support external volumes")
			}
		}
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

	// Volume creation is completed asynchronously after the actor is recorded.
	initVols, err := initialActorVolumes(ctx, s.storageClassLister, template)
	if err != nil {
		return nil, err
	}

	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: atespace,
			Name:     name,
		},
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		ActorTemplateNamespace: in.GetActorTemplateNamespace(),
		ActorTemplateName:      in.GetActorTemplateName(),
		ActorTemplate:          in.GetActorTemplate(),
		ActorTemplateVersion:   versionPin,
		WorkerSelector:         in.GetWorkerSelector(),
		ActorVolumes:           initVols,
		LatestSnapshot:         sourceSnapshotRef,
	}
	stored, err := s.persistence.CreateActor(ctx, actor)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "Actor %s already exists", name)
		}
		return nil, fmt.Errorf("while recording actor: %w", err)
	}

	setSpanActorAttributes(ctx, stored)
	return stored, nil
}

func validateCreateActorRequest(req *ateapipb.CreateActorRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	actor := req.GetActor()
	actorPath := fldPath.Child("actor")
	if actor == nil {
		errs = append(errs, field.Required(actorPath, ""))
		return errs
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

	// The actor names its template either as a global-scoped control-plane
	// ActorTemplate or as a namespaced CRD ActorTemplate, never both.
	if val := actor.GetActorTemplate(); val != "" {
		errs = append(errs, resources.ValidateResourceName(val, actorPath.Child("actor_template"))...)
		if actor.GetActorTemplateNamespace() != "" {
			errs = append(errs, field.Forbidden(actorPath.Child("actor_template_namespace"), "must be empty when actor_template is set"))
		}
		if actor.GetActorTemplateName() != "" {
			errs = append(errs, field.Forbidden(actorPath.Child("actor_template_name"), "must be empty when actor_template is set"))
		}
		if pin := actor.GetActorTemplateVersion(); pin != "" {
			errs = append(errs, resources.ValidateResourceName(pin, actorPath.Child("actor_template_version"))...)
		}
	} else {
		if actor.GetActorTemplateVersion() != "" {
			errs = append(errs, field.Forbidden(actorPath.Child("actor_template_version"), "requires actor_template"))
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
	}

	if val := actor.GetWorkerSelector(); val != nil {
		errs = append(errs, validateSelector(val, actorPath.Child("worker_selector"))...)
	}
	if val := req.GetSourceSnapshot(); val != nil {
		if err := validateActorSnapshotRef(val, "source_snapshot"); err != nil {
			errs = append(errs, field.Invalid(fldPath.Child("source_snapshot"), val, err.Error()))
		}
	}

	return errs
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
