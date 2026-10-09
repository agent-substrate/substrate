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
	"crypto/rand"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/actoridjwt"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (created *ateapipb.Actor, err error) {
	// First scrub any fields that users are not allowed to set, then fill the
	// defaults so validation sees the final resource state.
	inActor := req.Actor
	if inActor != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(inActor.Metadata)
		inActor.Status = nil
		defaults.Apply(inActor)
	}

	// Validate the request, including the object within it.
	if errs := apivalidation.ValidateCreateActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
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

	// Resolve the explicit tag, or freeze the template's current golden default.
	tagRef := inActor.GetSourceTag()
	if tagRef == nil {
		tagRef = template.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()
	} else {
		for _, volume := range template.GetVolumes() {
			if volume.GetExternal() != nil {
				// TODO: Permit cloning after CSI volume snapshots are supported.
				return nil, apierror.FailedPrecondition("Tag cloning does not support ActorTemplates with external volumes")
			}
		}
	}
	var sourceTag *ateapipb.Tag
	if tagRef != nil {
		sourceTag, err = s.resolveTagSource(ctx, inActor.GetMetadata().GetAtespace(), tagRef, template)
		if err != nil {
			return nil, err
		}
		if inActor.GetSourceTag() == nil {
			if err := validateGoldenSnapshotFidelity(sourceTag.GetStatus().GetSnapshot()); err != nil {
				return nil, err
			}
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
		State:           ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		ExternalVolumes: initVols,
	}
	if sourceTag != nil {
		// The Actor starts out borrowing the tag's snapshot rather than
		// copying it. The snapshot URI is under the tag's prefix, not the Actor's, which
		// is what keeps the Actor from collecting those objects. Its first
		// suspend writes a snapshot under its own prefix and takes over from
		// there.
		snap := proto.CloneOf(sourceTag.GetStatus().GetSnapshot())
		snap.Generation = 1
		snap.Owner = ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG
		// The Actor is born with guest state, so stamp the template that state
		// was built on now rather than at the first resume.
		snap.ActorTemplateUid = sourceTag.GetStatus().GetActorTemplateUid()
		outActor.Status.Snapshots = []*ateapipb.Snapshot{snap}
		outActor.Status.LastAssignedGeneration = 1
	}
	if errs := apivalidation.ValidateActorUpdate(ctx, field.NewPath("actor"), outActor, inActor, true); len(errs) > 0 {
		return nil, toGRPCInternalError(errs)
	}

	// Save the data in the storage layer.
	stored, err := s.store.CreateActor(ctx, outActor)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, apierror.AlreadyExists("Actor %s already exists", name)
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			return nil, apierror.FailedPrecondition("Atespace %s not found", atespace)
		}
		return nil, fmt.Errorf("while recording actor: %w", err)
	}

	// Without this an actor that is created and never resumed has no record at
	// all, at any retention.
	logActorStateChanged(ctx, stored, ateattr.OperationCreate)

	return stored, nil
}

// resolveTagSource resolves a CreateActor request's source tag
// and checks that the tag is usable for creating an Actor in actorAtespace
// from template.
func (s *ServiceImpl) resolveTagSource(ctx context.Context, actorAtespace string, tagRef *ateapipb.ObjectRef, template *ateapipb.ActorTemplate) (*ateapipb.Tag, error) {
	tag, err := s.store.GetTag(ctx, resources.TagRefFromObjectRef(tagRef))
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.FailedPrecondition("Tag not found")
	}
	if err != nil {
		return nil, fmt.Errorf("while getting tag: %w", err)
	}
	switch tag.GetScope() {
	case ateapipb.TagScope_TAG_SCOPE_ATESPACE:
		if tag.GetMetadata().GetAtespace() != actorAtespace {
			return nil, apierror.FailedPrecondition("Tag is not published outside its Atespace")
		}
	case ateapipb.TagScope_TAG_SCOPE_PUBLISHED:
	default:
		return nil, apierror.FailedPrecondition("source Tag has an invalid scope")
	}
	// A tag might have an empty Snapshot URI if the tag creation failed or is ongoing.
	if tagDurableSnapshotURI(tag, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED) == "" {
		return nil, apierror.FailedPrecondition("source Tag is still being created or failed creation")
	}
	// TODO: Permit compatible VOLUMES snapshots when runtimes can extract portable data.
	if tag.GetStatus().GetActorTemplateUid() != template.GetMetadata().GetUid() {
		return nil, apierror.FailedPrecondition("source Tag must be taken from an actor with ActorTemplate uid %q", tag.GetStatus().GetActorTemplateUid())
	}
	return tag, nil
}

func (s *RPCService) GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	if errs := apivalidation.ValidateGetActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	actor, err := s.impl.GetActor(ctx, actorRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("Actor %s not found", actorRef)
	} else if err != nil {
		return nil, fmt.Errorf("while getting actor from DB: %w", err)
	}
	return actor, nil
}

func (s *ServiceImpl) GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	return s.store.GetActor(ctx, actorRef)
}

func (s *RPCService) ListActors(ctx context.Context, req *ateapipb.ListActorsRequest) (*ateapipb.ListActorsResponse, error) {
	if errs := apivalidation.ValidateListActorsRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
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

func (s *RPCService) UpdateActor(ctx context.Context, req *ateapipb.UpdateActorRequest) (*ateapipb.Actor, error) {
	// First scrub any fields that users are not allowed to set.
	inActor := req.Actor
	if inActor != nil { // otherwise validation will flag it
		scrubResourceMetadataForUpdate(inActor.Metadata)
		inActor.Status = nil
	}

	// Validate the request.
	if errs := apivalidation.ValidateUpdateActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
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
		defaults.Apply(toUpdate)
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
		if errs := apivalidation.ValidateActorUpdate(ctx, field.NewPath("actor"), newVal, oldVal, false); len(errs) > 0 {
			return resources.ToAPIError(errs)
		}

		// Do any further work on the resource.

		// Update actor template is only allowed while the actor is suspended.
		// The repointed ref must also resolve, mirroring CreateActor's
		// check (same non-atomicity caveat; resume re-resolves and fails
		// cleanly), and the replacement's sandbox config, volumes, and
		// volume mounts must match the old template's. It must also store
		// snapshots under the location the actor's own already live in.
		if !proto.Equal(oldVal.GetActorTemplate(), newVal.GetActorTemplate()) {
			if state := oldVal.GetStatus().GetState(); state != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
				return apierror.FailedPrecondition(
					"actor must be %s to change its actor template (got: %s)", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, state)
			}
			newTemplate, err := resolveActorTemplate(ctx, s.store, newVal)
			if err != nil {
				return err
			}
			if err := validateSnapshotLocationUnchanged(oldVal, newTemplate); err != nil {
				return err
			}
			oldTemplate, err := resolveActorTemplate(ctx, s.store, oldVal)
			switch {
			case err == nil:
				// Snapshots are not portable across sandbox runtime
				// families, so the replacement template must name the same
				// SandboxConfig.
				if !proto.Equal(oldTemplate.GetSandboxConfig(), newTemplate.GetSandboxConfig()) {
					oldSC, newSC := oldTemplate.GetSandboxConfig(), newTemplate.GetSandboxConfig()
					return apierror.FailedPrecondition(
						"the current actor template names SandboxConfig %q (class %s) but the new one names %q (class %s); the sandbox config must be identical to repoint an actor",
						oldSC.GetConfigName(), oldSC.GetSandboxClass(), newSC.GetConfigName(), newSC.GetSandboxClass())
				}
				if err := validateTemplateVolumesUnchanged(oldTemplate, newTemplate); err != nil {
					return err
				}
			case errors.Is(err, errActorTemplateNotFound):
				// The old template is gone, so there is nothing left to
				// compare the sandbox config or volume layout against.
			default:
				return err
			}
		}

		// Validate the final value before storing it.
		if errs := apivalidation.ValidateActorUpdate(ctx, field.NewPath("actor"), newVal, oldVal, true); len(errs) > 0 {
			return toGRPCInternalError(errs)
		}

		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("actor %s not found", actorRef)
		}
		if errors.Is(err, store.ErrPreconditionRequired) {
			return nil, apierror.InvalidArgument("while updating actor %s: %v", actorRef, err)
		}
		return nil, fmt.Errorf("while updating actor: %w", err)
	}
	return storedActor, nil
}

// validateTemplateVolumesUnchanged rejects a template repoint that changes
// the template's volumes or any container's volume mounts: an actor's
// snapshot data is laid out per the volumes and mount paths it was captured
// with, so a different layout would restore it to the wrong places. The
// volumes list must be identical, and containers present in both templates
// must keep identical mounts, order included; containers added or removed by
// the new template are unconstrained.
func validateTemplateVolumesUnchanged(oldTemplate, newTemplate *ateapipb.ActorTemplate) error {
	if !slices.EqualFunc(oldTemplate.GetVolumes(), newTemplate.GetVolumes(), func(a, b *ateapipb.Volume) bool {
		return proto.Equal(a, b)
	}) {
		return apierror.FailedPrecondition(
			"volumes differ between the current and the new actor template; volumes must be identical to repoint an actor")
	}

	newContainers := make(map[string]*ateapipb.Container, len(newTemplate.GetContainers()))
	for _, c := range newTemplate.GetContainers() {
		newContainers[c.GetName()] = c
	}
	for _, oldC := range oldTemplate.GetContainers() {
		newC, ok := newContainers[oldC.GetName()]
		if !ok {
			continue
		}
		if !slices.EqualFunc(oldC.GetVolumeMounts(), newC.GetVolumeMounts(), func(a, b *ateapipb.VolumeMount) bool {
			return proto.Equal(a, b)
		}) {
			return apierror.FailedPrecondition(
				"volume mounts of container %q differ between the current and the new actor template; volume mounts must be identical to repoint an actor", oldC.GetName())
		}
	}
	return nil
}

// validateSnapshotLocationUnchanged rejects a template repoint that would
// store the actor's next snapshots under a different location than the one it
// already owns. This is needed to not leak snapshots when the actor is deleted:
// Deleting an actor collects everything under its external snapshot prefix. If
// the location prefix ever changes, we risk leaking the snapshots under the old prefix.
func validateSnapshotLocationUnchanged(actor *ateapipb.Actor, newTemplate *ateapipb.ActorTemplate) error {
	_, st := findLatestSnapshotStorage(actor.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	currentSnapshotURI := st.GetObject().GetSnapshotUri()
	if currentSnapshotURI == "" {
		return nil
	}
	currentURI, err := resources.ParseSnapshotURI(currentSnapshotURI)
	if err != nil {
		return fmt.Errorf("while parsing the external snapshot %q: %w", currentSnapshotURI, err)
	}
	// Tag-owned snapshot
	if !currentURI.OwnedBy(actorSnapshotOwner(actor)) {
		return nil
	}
	// Compared as prefixes, so a newLocation spelled with and without a
	// trailing slash counts as the same.
	newLocation := newTemplate.GetSnapshotConfig().GetStorageLocation()
	// Generate what the new snapshot prefix would look like for this actor.
	nextSnapshotLocationPrefix, err := currentURI.Owner().Prefix(newLocation)
	if err != nil {
		return fmt.Errorf("while resolving the new actor template's storage location %q: %w", newLocation, err)
	}
	if nextSnapshotLocationPrefix != currentURI.OwnerPrefix() {
		return apierror.FailedPrecondition(
			"the actor's snapshots are stored under %q but the new actor template stores them under %q: the storage location must be identical to repoint an actor that owns a snapshot",
			currentURI.Location(), newLocation)
	}
	return nil
}

func (s *RPCService) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (deleted *ateapipb.Actor, err error) {
	if errs := apivalidation.ValidateDeleteActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
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

	deleted, err = s.actorWorkflow.DeleteActor(ctx, actorRef, req.GetAnyState(), toDeletePreconditions(req.GetOptions()))
	if err != nil {
		return nil, err
	}

	return deleted, nil
}

func (s *ServiceImpl) DeleteActor(ctx context.Context, actorRef resources.ActorRef, precondition store.DeletePreconditions) (*ateapipb.Actor, error) {
	return s.store.DeleteActor(ctx, actorRef, precondition)
}

func (s *RPCService) PauseActor(ctx context.Context, req *ateapipb.PauseActorRequest) (*ateapipb.PauseActorResponse, error) {
	if errs := apivalidation.ValidatePauseActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)

	actor, err := s.actorWorkflow.PauseActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		return nil, err
	}

	setSpanActorAttributes(ctx, actor)
	return &ateapipb.PauseActorResponse{Actor: actor}, nil
}

func (s *RPCService) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	if errs := apivalidation.ValidateResumeActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)

	actor, resumed, err := s.actorWorkflow.ResumeActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		return nil, err
	}

	setSpanActorAttributes(ctx, actor)
	return &ateapipb.ResumeActorResponse{Actor: actor, Resumed: resumed}, nil
}

func (s *RPCService) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	if errs := apivalidation.ValidateSuspendActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)

	actor, err := s.actorWorkflow.SuspendActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		return nil, err
	}
	setSpanActorAttributes(ctx, actor)
	return &ateapipb.SuspendActorResponse{Actor: actor}, nil
}

func (s *RPCService) RevertActor(ctx context.Context, req *ateapipb.RevertActorRequest) (*ateapipb.RevertActorResponse, error) {
	if errs := apivalidation.ValidateRevertActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)

	actor, err := s.actorWorkflow.RevertActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		return nil, err
	}
	setSpanActorAttributes(ctx, actor)
	return &ateapipb.RevertActorResponse{Actor: actor}, nil
}

func (s *RPCService) MintActorJWT(ctx context.Context, req *ateapipb.MintActorJWTRequest) (*ateapipb.MintActorJWTResponse, error) {
	if errs := apivalidation.ValidateMintActorJWTRequest(ctx, req); len(errs) > 0 {
		return nil, apierror.InvalidArgument("%v", errs.ToAggregate())
	}

	// TODO(authz): Authorization layer needs to check whether the caller has
	// the mintActorJWT permission/relation with this actor.  This could be an
	// atelet (via the relationship of the atelet running the actor), or the
	// egress gateway (via a cluster-level grant?)

	// Verify that this actor exists in the store.  It doesn't need to be
	// running, since we may need to issue JWTs during actor boot / resume.
	dbActor, err := s.impl.GetActor(ctx, resources.ActorRefFromObjectRef(req.GetActor()))
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("actor not found")
	} else if err != nil {
		return nil, fmt.Errorf("while retrieving actor: %w", err)
	}

	// We only issue tokens with audience bindings.
	if len(req.GetAudiences()) == 0 {
		return nil, fmt.Errorf("at least one audience must be requested")
	}

	// JWT timestamps have one-second resolution; truncating keeps expires_at
	// equal to the exp claim.
	now := time.Now().Truncate(time.Second)
	expiresAt := now.Add(time.Duration(req.GetExpirationSeconds()) * time.Second)
	actorClaims := &actoridjwt.Claims{
		Issuer:     s.actorJWTIssuer,
		Subject:    fmt.Sprintf("actor/%s/%s", dbActor.GetMetadata().GetAtespace(), dbActor.GetMetadata().GetName()),
		Audiences:  req.GetAudiences(),
		Expiration: expiresAt,
		NotBefore:  now.Add(-5 * time.Minute),
		IssuedAt:   now,
		JTI:        rand.Text(),

		Substrate: actoridjwt.SubstrateClaims{
			Atespace:  dbActor.GetMetadata().GetAtespace(),
			ActorName: dbActor.GetMetadata().GetName(),
			ActorUID:  dbActor.GetMetadata().GetUid(),
		},
	}

	actorJWT, err := s.actorIDJWTPool.SignJWT(actorClaims)
	if err != nil {
		return nil, fmt.Errorf("while signing actor JWT: %w", err)
	}

	return &ateapipb.MintActorJWTResponse{
		ActorJwt:  actorJWT,
		ExpiresAt: timestamppb.New(expiresAt),
	}, nil
}

func (s *RPCService) MintActorCertificate(ctx context.Context, req *ateapipb.MintActorCertificateRequest) (*ateapipb.MintActorCertificateResponse, error) {
	if errs := apivalidation.ValidateMintActorCertificateRequest(ctx, req); len(errs) > 0 {
		return nil, apierror.InvalidArgument("%v", errs.ToAggregate())
	}

	// TODO(authz): Authorization layer needs to check whether the caller has
	// the mintActorCertificate permission/relation with this actor.    This
	// could be an atelet (via the relationship of the atelet running the
	// actor), or the egress gateway (via a cluster-level grant?)

	// Check that the caller authenticated with a client certificate --- we
	// should not allow bootstrapping a proof-of-possession credential from a
	// bearer credential.  Note, we don't care that it was a certificate issued
	// by Substrate, or something else.
	//
	// TODO(authz): Perhaps this can be handled with an OpenFGA condition.
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, apierror.Unauthenticated("no peer transport information found")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, apierror.Unauthenticated("unexpected peer transport credentials")
	}
	if len(tlsInfo.State.PeerCertificates) == 0 {
		return nil, apierror.Unauthenticated("could not verify peer certificate")
	}

	// Verify that this actor exists in the store.  It doesn't need to be
	// running, since we may need to issue certificates during actor boot / resume.
	dbActor, err := s.impl.GetActor(ctx, resources.ActorRefFromObjectRef(req.GetActor()))
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("actor not found")
	} else if err != nil {
		return nil, fmt.Errorf("while retrieving actor: %w", err)
	}
	if dbActor.GetMetadata().GetUid() != req.GetActorUid() {
		return nil, apierror.Aborted("conflict; actor has been deleted and recreated")
	}

	// Parse the CSR
	csr, err := x509.ParseCertificateRequest(req.GetCertificateSigningRequest())
	if err != nil {
		return nil, fmt.Errorf("while parsing CSR: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		slog.ErrorContext(ctx, "Failed to verify CSR signature", slog.Any("err", err))
		return nil, apierror.InvalidArgument("Failed to verify CSR signature")
	}

	template := &x509.Certificate{
		URIs: []*url.URL{resources.ActorSPIFFEID(resources.ActorRef{
			Atespace: dbActor.GetMetadata().GetAtespace(),
			Name:     dbActor.GetMetadata().GetName(),
		})},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	err = substratex509.AddActorIdentityToCertificate(
		&substratex509.ActorIdentity{
			Atespace:  dbActor.GetMetadata().GetAtespace(),
			ActorName: dbActor.GetMetadata().GetName(),
			ActorUid:  dbActor.GetMetadata().GetUid(),
		},
		template,
	)
	if err != nil {
		return nil, fmt.Errorf("while adding Substrate extension: %w", err)
	}

	// Sign and return the actor cert.
	chain, err := s.actorIDCAPool.CreateCertificate(template, csr.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("while signing certificate: %w", err)
	}

	return &ateapipb.MintActorCertificateResponse{
		ActorCertificates: chain,
	}, nil
}
