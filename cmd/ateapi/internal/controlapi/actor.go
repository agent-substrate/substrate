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
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (created *ateapipb.Actor, err error) {
	// First scrub any fields that users are not allowed to set.
	inActor := req.Actor
	if inActor != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(inActor.Metadata)
		inActor.Status = nil
	}

	// Validate the request, including the object within it.
	if errs := validateCreateActorRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	start := time.Now()
	// Recorded only after validation, so every operation uniformly measures a
	// validated request; malformed ones stay visible in rpc.server.call.duration.
	defer func() {
		s.instruments.recordLifecycleOp(ctx, ateattr.OperationCreate, start, err,
			ateattr.TemplateNameKey.String(inActor.GetActorTemplate().GetName()),
			ateattr.TemplateAtespaceKey.String(inActor.GetActorTemplate().GetAtespace()),
		)
	}()

	setSpanActorRefAttributes(ctx, resources.ActorRefFromActor(inActor))

	// Handle the creation, including validation of the final stored object.
	stored, err := s.impl.CreateActor(ctx, inActor)
	setSpanActorAttributes(ctx, stored)

	return stored, err
}

func (s *ServiceImpl) CreateActor(ctx context.Context, inActor *ateapipb.Actor) (*ateapipb.Actor, error) {
	// Check that the referenced ActorTemplate exists.
	// FIXME: This is not atomic and it is not a guarantee that the template
	// will still exist later.  Checking it here produces a nice error UX, but
	// we still have to handle the template not existing later, which makes the
	// UX inconsistent, at best.  Is it actually worth checking at all?
	template, err := resolveActorTemplate(ctx, s.store, inActor)
	if err != nil {
		return nil, err
	}

	// If a source snapshot tag is requested, resolve it to a concrete
	// snapshot.
	var sourceSnapshotStatus *ateapipb.ActorSourceSnapshotStatus
	if tag := inActor.GetSourceSnapshotTag(); tag != nil {
		sourceSnapshotStatus, err = s.resolveSnapshotSource(ctx, inActor.GetMetadata().GetAtespace(), tag, template)
		if err != nil {
			return nil, err
		}
	}

	atespace := inActor.GetMetadata().GetAtespace()
	name := inActor.GetMetadata().GetName()

	// Volume creation is completed asynchronously after the actor is recorded.
	initVols, err := initialActorVolumes(ctx, s.storageClassLister, template)
	if err != nil {
		return nil, err
	}

	// Verify that the result is properly valid before storing it.
	outActor := proto.CloneOf(inActor)
	outActor.Status = &ateapipb.ActorStatus{
		State:          ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		ActorVolumes:   initVols,
		LatestSnapshot: sourceSnapshotStatus.GetSnapshot(),
		SourceSnapshot: sourceSnapshotStatus,
	}
	if errs := validateActorUpdate(ctx, field.NewPath("actor"), outActor, inActor, true); len(errs) > 0 {
		return nil, toGRPCInternalError(errs)
	}

	// Save the data in the storage layer.
	stored, err := s.store.CreateActor(ctx, outActor)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "Actor %s already exists", name)
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s not found", atespace)
		}
		return nil, fmt.Errorf("while recording actor: %w", err)
	}

	return stored, nil
}

// resolveSnapshotSource resolves a CreateActor request's source snapshot tag
// and checks that its scope and ActorSnapshot are compatible with creating
// an Actor in actorAtespace from template.
func (s *ServiceImpl) resolveSnapshotSource(ctx context.Context, actorAtespace string, tagRef *ateapipb.ObjectRef, template *ateapipb.ActorTemplate) (*ateapipb.ActorSourceSnapshotStatus, error) {
	tag, err := s.store.GetActorSnapshotTag(ctx, resources.ActorSnapshotTagRefFromObjectRef(tagRef))
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "ActorSnapshot not found")
	}
	if err != nil {
		return nil, fmt.Errorf("while getting actor snapshot tag: %w", err)
	}
	snapshot, err := s.GetActorSnapshot(ctx, resources.ActorSnapshotRefFromObjectRef(tag.GetSnapshot()))
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
	if snapshot.GetStatus().GetActorTemplateUid() != template.GetMetadata().GetUid() {
		return nil, status.Error(codes.FailedPrecondition, "ActorSnapshot requires the source ActorTemplate")
	}
	for _, volume := range template.GetVolumes() {
		if volume.GetExternalVolumeTemplate() != nil {
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

func validateCreateActorRequest(ctx context.Context, req *ateapipb.CreateActorRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_CreateActorRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	if errs := validateGetActorRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	actor, err := s.impl.GetActor(ctx, actorRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "Actor %s not found", actorRef)
	} else if err != nil {
		return nil, fmt.Errorf("while getting actor from DB: %w", err)
	}
	return actor, nil
}

func (s *ServiceImpl) GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	return s.store.GetActor(ctx, actorRef)
}

func validateGetActorRequest(ctx context.Context, req *ateapipb.GetActorRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_GetActorRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) ListActors(ctx context.Context, req *ateapipb.ListActorsRequest) (*ateapipb.ListActorsResponse, error) {
	if errs := validateListActorsRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	page, err := s.impl.ListActors(ctx, req.GetAtespace(), store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing actors in db: %w", err))
	}
	return &ateapipb.ListActorsResponse{
		Actors:        page.Items,
		NextPageToken: page.NextPageToken,
	}, nil
}

func (s *ServiceImpl) ListActors(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.Actor], error) {
	return s.store.ListActors(ctx, atespace, opts)
}

func validateListActorsRequest(ctx context.Context, req *ateapipb.ListActorsRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_ListActorsRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) UpdateActor(ctx context.Context, req *ateapipb.UpdateActorRequest) (*ateapipb.Actor, error) {
	// First scrub any fields that users are not allowed to set.
	inActor := req.Actor
	if inActor != nil { // otherwise validation will flag it
		scrubResourceMetadataForUpdate(inActor.Metadata)
		inActor.Status = nil
	}

	// Validate the request.
	if errs := validateUpdateActorRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	actorRef := resources.ActorRefFromActor(inActor)
	setSpanActorRefAttributes(ctx, actorRef)

	storedActor, err := s.impl.UpdateActor(ctx, actorRef, store.PreconditionFrom(inActor), func(toUpdate *ateapipb.Actor) error {
		// Status and Metadata are server-owned fields.
		status, metadata := toUpdate.GetStatus(), toUpdate.GetMetadata()
		// Whole-object replace: clear first, so a field the client left unset is
		// cleared rather than kept from the stored actor.
		// Merge cannot smuggle in unknown fields because validation already rejected them.
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, inActor)
		// Restore status and metadata from the server.
		toUpdate.Status = status
		toUpdate.Metadata = metadata
		return nil
	})
	if err != nil {
		return nil, err
	}

	setSpanActorAttributes(ctx, storedActor)

	return storedActor, err
}

func (s *ServiceImpl) UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapipb.Actor) error) (*ateapipb.Actor, error) {
	storedActor, err := s.store.UpdateActor(ctx, actorRef, precondition, func(toUpdate *ateapipb.Actor) error {
		// Apply the mutation function to the stored value.
		oldVal := proto.CloneOf(toUpdate)
		if err := mutate(toUpdate); err != nil {
			return err
		}
		newVal := toUpdate

		// Validate the user's input before doing any further work.
		if errs := validateActorUpdate(ctx, field.NewPath("actor"), newVal, oldVal, false); len(errs) > 0 {
			return toGRPCStatusError(errs)
		}

		// Do any further work on the resource.

		// Validate the final value before storing it.
		if errs := validateActorUpdate(ctx, field.NewPath("actor"), newVal, oldVal, true); len(errs) > 0 {
			return toGRPCInternalError(errs)
		}

		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "actor %s not found", actorRef)
		}
		if errors.Is(err, store.ErrPreconditionRequired) {
			return nil, status.Errorf(codes.InvalidArgument, "while updating actor %s: %v", actorRef, err)
		}
		return nil, fmt.Errorf("while updating actor: %w", err)
	}
	return storedActor, nil
}

func validateUpdateActorRequest(ctx context.Context, req *ateapipb.UpdateActorRequest) field.ErrorList {
	// Call the generated validation.
	// We model this as a create rather than an update because updates assume
	// the existence of a "current" value, which we do not have yet.  This is
	// validating the request itself. The result will be validated later, after
	// we have a current value to compare against.
	op := operation.Operation{Type: operation.Create}
	return Validate_UpdateActorRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (deleted *ateapipb.Actor, err error) {
	if errs := validateDeleteActorRequest(ctx, req); len(errs) > 0 {
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
				ateattr.TemplateNameKey.String(deleted.GetActorTemplate().GetName()),
				ateattr.TemplateAtespaceKey.String(deleted.GetActorTemplate().GetAtespace()),
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

func (s *ServiceImpl) DeleteActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	return s.store.DeleteActor(ctx, actorRef)
}

func validateDeleteActorRequest(ctx context.Context, req *ateapipb.DeleteActorRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_DeleteActorRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) PauseActor(ctx context.Context, req *ateapipb.PauseActorRequest) (*ateapipb.PauseActorResponse, error) {
	if errs := validatePauseActorRequest(ctx, req); len(errs) > 0 {
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

func validatePauseActorRequest(ctx context.Context, req *ateapipb.PauseActorRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_PauseActorRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	if errs := validateResumeActorRequest(ctx, req); len(errs) > 0 {
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

func validateResumeActorRequest(ctx context.Context, req *ateapipb.ResumeActorRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_ResumeActorRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	if errs := validateSuspendActorRequest(ctx, req); len(errs) > 0 {
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

func validateSuspendActorRequest(ctx context.Context, req *ateapipb.SuspendActorRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_SuspendActorRequest(ctx, op, nil, req, nil)
}

func validateActorUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapipb.Actor, requireStatus bool) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Update}
	errs := Validate_Actor(ctx, op, fldPath, newVal, oldVal)
	if requireStatus {
		// Status is optional in the schema, but is actually required to be set
		// by the server.  If it was specified, it was already validated above,
		// but if it was not specified we need to flag that as an error.
		errs = append(errs, validate.RequiredPointer(ctx, op, fldPath.Child("status"), newVal.GetStatus(), nil)...)
	}
	return errs
}

// This exists only because nested subfield tags are not supported yet.
func ValidateCustom_UpdateActorRequest_Actor(ctx context.Context, op operation.Operation, fldPath *field.Path, actor, _ *ateapipb.Actor) field.ErrorList {
	if actor == nil || actor.Metadata == nil {
		return nil // handled by DV
	}

	// Updates are validated in 2 steps: first the update request and then the
	// resource itself. DV for the request doesn't descend into the resource
	// metadata.  Once DV supports nested subfield tags, this can be changed to
	// something like:
	//   +k8s:subfield(metadata)=+k8s:subfield(atespace)=+k8s:required
	errs := Validate_ResourceMetadata(ctx, op, fldPath.Child("metadata"), actor.Metadata, nil)
	errs = append(errs, validate.RequiredValue(ctx, op, fldPath.Child("metadata", "atespace"), &actor.Metadata.Atespace, nil)...)
	return errs
}
