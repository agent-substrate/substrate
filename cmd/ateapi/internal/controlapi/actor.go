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
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (created *ateapipb.Actor, err error) {
	if errs := validateCreateActorRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	start := time.Now()
	in := req.GetActor()
	// Recorded only after validation, so every operation uniformly measures a
	// validated request; malformed ones stay visible in rpc.server.call.duration.
	defer func() {
		s.instruments.recordLifecycleOp(ctx, ateattr.OperationCreate, start, err,
			ateattr.TemplateNameKey.String(in.GetActorTemplateName()),
			ateattr.TemplateNamespaceKey.String(in.GetActorTemplateNamespace()),
		)
	}()
	templateNamespace := in.GetActorTemplateNamespace()
	templateName := in.GetActorTemplateName()

	setSpanActorRefAttributes(ctx, resources.ActorRefFromActor(in))

	template, err := s.actorTemplateLister.ActorTemplates(templateNamespace).Get(templateName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplate %s/%s not found", templateNamespace, templateName)
		}
		return nil, fmt.Errorf("while getting ActorTemplate: %w", err)
	}

	var sourceSnapshotStatus *ateapipb.ActorSourceSnapshotStatus
	if tag := in.GetSourceSnapshotTag(); tag != nil {
		sourceSnapshotStatus, err = s.resolveSnapshotSource(ctx, in.GetMetadata().GetAtespace(), tag, template)
		if err != nil {
			return nil, err
		}
	}

	atespace := in.GetMetadata().GetAtespace()
	name := in.GetMetadata().GetName()

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
		ActorTemplateNamespace: templateNamespace,
		ActorTemplateName:      templateName,
		WorkerSelector:         in.GetWorkerSelector(),
		SourceSnapshotTag:      in.GetSourceSnapshotTag(),
		Status: &ateapipb.ActorStatus{
			State:          ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			ActorVolumes:   initVols,
			LatestSnapshot: sourceSnapshotStatus.GetSnapshot(),
			SourceSnapshot: sourceSnapshotStatus,
		},
	}
	stored, err := s.persistence.CreateActor(ctx, actor)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "Actor %s already exists", name)
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s not found", atespace)
		}
		return nil, fmt.Errorf("while recording actor: %w", err)
	}

	setSpanActorAttributes(ctx, stored)
	return stored, nil
}

// resolveSnapshotSource resolves a CreateActor request's source snapshot tag
// and checks that its scope and ActorSnapshot are compatible with creating
// an Actor in actorAtespace from template.
func (s *RPCService) resolveSnapshotSource(ctx context.Context, actorAtespace string, tagRef *ateapipb.ObjectRef, template *atev1alpha1.ActorTemplate) (*ateapipb.ActorSourceSnapshotStatus, error) {
	tag, err := s.persistence.GetActorSnapshotTag(ctx, resources.ActorSnapshotTagRefFromObjectRef(tagRef))
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "ActorSnapshot not found")
	}
	if err != nil {
		return nil, fmt.Errorf("while getting actor snapshot tag: %w", err)
	}
	snapshot, err := s.persistence.GetActorSnapshot(ctx, resources.ActorSnapshotRefFromObjectRef(tag.GetSnapshot()))
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "ActorSnapshot not found")
	}
	if err != nil {
		return nil, fmt.Errorf("while getting actor snapshot: %w", err)
	}
	switch tag.GetScope() {
	case ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE:
		if tag.GetMetadata().GetAtespace() != actorAtespace {
			return nil, status.Error(codes.FailedPrecondition, "ActorSnapshot tag is not published outside its Atespace")
		}
	case ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED:
	default:
		return nil, status.Error(codes.FailedPrecondition, "source ActorSnapshot tag has an invalid scope")
	}
	// TODO: Permit compatible DATA snapshots when runtimes can extract portable data.
	if snapshot.GetStatus().GetActorTemplateUid() != string(template.GetUID()) {
		return nil, status.Error(codes.FailedPrecondition, "ActorSnapshot requires the source ActorTemplate")
	}
	for _, volume := range template.Spec.Volumes {
		if volume.ExternalVolumeTemplate != nil {
			// TODO: Permit cloning after CSI volume snapshots are supported.
			return nil, status.Error(codes.FailedPrecondition, "ActorSnapshot cloning does not support external volumes")
		}
	}
	return &ateapipb.ActorSourceSnapshotStatus{
		Snapshot: &ateapipb.ObjectRef{
			Atespace: snapshot.GetMetadata().GetAtespace(),
			Name:     snapshot.GetMetadata().GetName(),
		},
		SnapshotUid: snapshot.GetMetadata().GetUid(),
	}, nil
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

	errs = append(errs, validateNoUnknownFields(actor, actorPath)...)

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
	if tag := actor.GetSourceSnapshotTag(); tag != nil {
		errs = append(errs, resources.ValidateObjectRef(tag, actorPath.Child("source_snapshot_tag"))...)
	}

	return errs
}

func (s *RPCService) GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	if errs := validateGetActorRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	actor, err := s.persistence.GetActor(ctx, actorRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "Actor %s not found", actorRef)
	} else if err != nil {
		return nil, fmt.Errorf("while getting actor from DB: %w", err)
	}
	return actor, nil
}

func validateGetActorRequest(req *ateapipb.GetActorRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.Actor, fldPath.Child("actor"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	return errs
}

func (s *RPCService) ListActors(ctx context.Context, req *ateapipb.ListActorsRequest) (*ateapipb.ListActorsResponse, error) {
	if errs := validateListActorsRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	page, err := s.persistence.ListActors(ctx, req.GetAtespace(), store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing actors in db: %w", err))
	}
	return &ateapipb.ListActorsResponse{
		Actors:        page.Items,
		NextPageToken: page.NextPageToken,
	}, nil
}

func validateListActorsRequest(req *ateapipb.ListActorsRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	// An empty atespace is allowed here and means "all atespaces".
	if val, fldPath := req.Atespace, fldPath.Child("atespace"); val != "" {
		errs = append(errs, resources.ValidateResourceName(val, fldPath)...)
	}

	if val, fldPath := req.PageSize, fldPath.Child("page_size"); val < 0 {
		errs = append(errs, field.Invalid(fldPath, val, "must be greater than or equal to 0"))
	}

	return errs
}

func (s *RPCService) UpdateActor(ctx context.Context, req *ateapipb.UpdateActorRequest) (*ateapipb.Actor, error) {
	if errs := validateUpdateActorRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	in := req.GetActor()
	actorRef := resources.ActorRefFromActor(in)
	setSpanActorRefAttributes(ctx, actorRef)

	storedActor, err := s.persistence.UpdateActor(ctx, actorRef, store.PreconditionFrom(in), func(toUpdate *ateapipb.Actor) error {
		// Status and Metadata are server-owned fields.
		status, metadata := toUpdate.GetStatus(), toUpdate.GetMetadata()
		// Whole-object replace: clear first, so a field the client left unset is
		// cleared rather than kept from the stored actor.
		// Merge cannot smuggle in unknown fields because validation already rejected them.
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, in)
		// Restore status and metadata from the server.
		toUpdate.Status = status
		toUpdate.Metadata = metadata
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrImmutableField) {
			return nil, status.Errorf(codes.InvalidArgument, "while updating actor %s: %v", actorRef, err)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "actor %s/%s not found with uid %s", in.GetMetadata().GetAtespace(), in.GetMetadata().GetName(), in.GetMetadata().GetUid())
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "actor %s not found", actorRef)
		}
		if errors.Is(err, store.ErrPreconditionRequired) {
			return nil, status.Errorf(codes.InvalidArgument, "while updating actor %s: %v", actorRef, err)
		}
		return nil, fmt.Errorf("while updating actor: %w", err)
	}

	setSpanActorAttributes(ctx, storedActor)
	return storedActor, nil
}

func validateUpdateActorRequest(req *ateapipb.UpdateActorRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	actor := req.GetActor()
	actorPath := fldPath.Child("actor")
	if actor == nil {
		return field.ErrorList{field.Required(actorPath, "")}
	}

	errs = append(errs, validateNoUnknownFields(actor, actorPath)...)

	errs = append(errs, resources.ValidateUpdateMetadataRef(actor.GetMetadata(), actorPath.Child("metadata"))...)

	if selector := actor.GetWorkerSelector(); selector != nil {
		errs = append(errs, validateSelector(selector, actorPath.Child("worker_selector"))...)
	}

	return errs
}

func (s *RPCService) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (deleted *ateapipb.Actor, err error) {
	if errs := validateDeleteActorRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	start := time.Now()
	// Template dims only once the record resolved: the request names only the
	// actor, so failures before the load carry none. No pool pair: delete only
	// runs from SUSPENDED or CRASHED, which already released the worker.
	defer func() {
		var attrs []attribute.KeyValue
		if deleted != nil {
			attrs = append(attrs,
				ateattr.TemplateNameKey.String(deleted.GetActorTemplateName()),
				ateattr.TemplateNamespaceKey.String(deleted.GetActorTemplateNamespace()),
			)
		}
		s.instruments.recordLifecycleOp(ctx, ateattr.OperationDelete, start, err, attrs...)
	}()
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)

	deleted, err = s.actorWorkflow.DeleteActor(ctx, actorRef, req.GetAnyState())
	if err != nil {
		return nil, err
	}

	return deleted, nil
}

func validateDeleteActorRequest(req *ateapipb.DeleteActorRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.Actor, fldPath.Child("actor"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	return errs
}

func (s *RPCService) PauseActor(ctx context.Context, req *ateapipb.PauseActorRequest) (*ateapipb.PauseActorResponse, error) {
	if errs := validatePauseActorRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)

	actor, err := s.actorWorkflow.PauseActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Actor %s not found", actorRef)
		}
		return nil, err
	}

	setSpanActorAttributes(ctx, actor)
	return &ateapipb.PauseActorResponse{Actor: actor}, nil
}

func validatePauseActorRequest(req *ateapipb.PauseActorRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.Actor, fldPath.Child("actor"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	return errs
}

func (s *RPCService) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	if errs := validateResumeActorRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)

	actor, resumed, err := s.actorWorkflow.ResumeActor(ctx, actorRef, req.GetBoot())
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Actor %s not found", actorRef)
		}
		return nil, err
	}

	setSpanActorAttributes(ctx, actor)
	return &ateapipb.ResumeActorResponse{Actor: actor, Resumed: resumed}, nil
}

func validateResumeActorRequest(req *ateapipb.ResumeActorRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.Actor, fldPath.Child("actor"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	return errs
}

func (s *RPCService) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	if errs := validateSuspendActorRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)

	actor, err := s.actorWorkflow.SuspendActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Actor %s not found", actorRef)
		}
		return nil, err
	}
	setSpanActorAttributes(ctx, actor)
	return &ateapipb.SuspendActorResponse{Actor: actor}, nil
}

func validateSuspendActorRequest(req *ateapipb.SuspendActorRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.Actor, fldPath.Child("actor"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
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
