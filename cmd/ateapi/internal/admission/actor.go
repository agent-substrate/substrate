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

package admission

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

// CreateActor validates the input actor, resolves its template and optional
// source/golden tag, initializes its server-owned status, validates the final
// object, and stores it.
//
// Returns ErrInvalid if inActor fails declarative create validation,
// ErrFailedPrecondition if the referenced ActorTemplate, source or golden Tag,
// or a referenced StorageClass is missing or unusable, store.ErrAlreadyExists
// if the actor name is already taken in its atespace, or
// store.ErrFailedPrecondition if the actor's atespace does not exist.
func (a *Admission) CreateActor(ctx context.Context, inActor *ateapipb.Actor) (*ateapipb.Actor, error) {
	specActor := proto.CloneOf(inActor)
	specActor.Status = nil
	defaults.Apply(specActor)

	if errs := apivalidation.ValidateActorCreate(ctx, field.NewPath("actor"), specActor); len(errs) > 0 {
		return nil, invalidf("%v", errs.ToAggregate())
	}

	// Check that the referenced ActorTemplate exists.
	// FIXME: This is not atomic and it is not a guarantee that the template
	// will still exist later. Checking it here produces a nice error UX, but
	// we still have to handle the template not existing later, which makes the
	// UX inconsistent, at best. Is it actually worth checking at all?
	template, err := a.resolveActorTemplate(ctx, specActor)
	if err != nil {
		return nil, err
	}

	// Resolve the explicit tag, or freeze the template's current golden default.
	tagRef := specActor.GetSourceTag()
	if tagRef == nil {
		tagRef = template.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()
	} else {
		for _, volume := range template.GetVolumes() {
			if volume.GetExternalVolumeTemplate() != nil {
				// TODO: Permit cloning after CSI volume snapshots are supported.
				return nil, failedPreconditionf("Tag cloning does not support ActorTemplates with external volumes")
			}
		}
	}
	var sourceTag *ateapipb.Tag
	if tagRef != nil {
		sourceTag, err = a.resolveTagSource(ctx, specActor.GetMetadata().GetAtespace(), tagRef, template)
		if err != nil {
			return nil, err
		}
		if specActor.GetSourceTag() == nil {
			if err := validateGoldenSnapshotScope(sourceTag.GetStatus().GetSnapshot()); err != nil {
				return nil, err
			}
		}
	}

	// Volume creation is completed asynchronously after the actor is recorded.
	initVols, err := initialActorVolumes(ctx, a.storageClassLister, template)
	if err != nil {
		return nil, err
	}

	// Verify that the result is properly valid before storing it.
	outActor := proto.CloneOf(specActor)
	outActor.Status = &ateapipb.ActorStatus{
		State:        ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		ActorVolumes: initVols,
	}
	if sourceTag != nil {
		// The Actor starts out borrowing the tag's external snapshot rather than
		// copying it. The snapshot URI is under the tag's prefix, not the Actor's, which
		// is what keeps the Actor from collecting those objects. Its first
		// suspend writes a snapshot under its own prefix and takes over from
		// there.
		outActor.Status.ExternalSnapshot = proto.CloneOf(sourceTag.GetStatus().GetSnapshot())
		// The Actor is born with guest state, so stamp the template that state
		// was built on now rather than at the first resume. The Tag records it
		// beside its snapshot rather than on it, so the clone above does not
		// carry it. Left empty, a repoint before that first resume reads as "no
		// guest state" instead of "replaced template", and the resume restores
		// the old template's memory and rootfs in full instead of the volume
		// data alone.
		outActor.Status.ExternalSnapshot.ActorTemplateUid = sourceTag.GetStatus().GetActorTemplateUid()
	}
	if errs := apivalidation.ValidateActorUpdate(ctx, field.NewPath("actor"), outActor, specActor, true); len(errs) > 0 {
		return nil, fmt.Errorf("%v", errs.ToAggregate())
	}

	return a.store.CreateActor(ctx, outActor)
}

// resolveTagSource resolves a CreateActor request's source tag and checks that
// the tag is usable for creating an Actor in actorAtespace from template.
func (a *Admission) resolveTagSource(ctx context.Context, actorAtespace string, tagRef *ateapipb.ObjectRef, template *ateapipb.ActorTemplate) (*ateapipb.Tag, error) {
	tag, err := a.store.GetTag(ctx, resources.TagRefFromObjectRef(tagRef))
	if errors.Is(err, store.ErrNotFound) {
		return nil, failedPreconditionf("Tag not found")
	}
	if err != nil {
		return nil, fmt.Errorf("while getting tag: %w", err)
	}
	switch tag.GetScope() {
	case ateapipb.TagScope_TAG_SCOPE_ATESPACE:
		if tag.GetMetadata().GetAtespace() != actorAtespace {
			return nil, failedPreconditionf("Tag is not published outside its Atespace")
		}
	case ateapipb.TagScope_TAG_SCOPE_PUBLISHED:
	default:
		return nil, failedPreconditionf("source Tag has an invalid scope")
	}
	// A tag might have an empty Snapshot URI if the tag creation failed or is ongoing.
	if tag.GetStatus().GetSnapshot().GetSnapshotUri() == "" {
		return nil, failedPreconditionf("source Tag is still being created or failed creation")
	}
	// TODO: Permit compatible DATA snapshots when runtimes can extract portable data.
	if tag.GetStatus().GetActorTemplateUid() != template.GetMetadata().GetUid() {
		return nil, failedPreconditionf("source Tag must be taken from an actor with ActorTemplate uid %q", tag.GetStatus().GetActorTemplateUid())
	}
	return tag, nil
}

// validateGoldenSnapshotScope rejects a golden snapshot that does not carry
// the guest state (memory + fs delta) a restore needs. Golden actors always
// commit Full, so this only trips on golden snapshots taken before that rule
// existed.
func validateGoldenSnapshotScope(snapshot *ateapipb.ExternalSnapshot) error {
	scope := snapshot.GetContentScope()
	switch scope {
	case ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED,
		ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL:
		return nil
	default:
		return failedPreconditionf(
			"ActorTemplate golden snapshot %q was taken with scope %s, not Full; regenerate the golden snapshot",
			snapshot.GetSnapshotUri(), scope)
	}
}

// initialActorVolumes constructs initial volume objects in PENDING state before volume creation.
func initialActorVolumes(_ context.Context, scLister storagev1listers.StorageClassLister, template *ateapipb.ActorTemplate) ([]*ateapipb.ExternalVolume, error) {
	var volumes []*ateapipb.ExternalVolume
	for _, vol := range template.GetVolumes() {
		if vol.GetExternalVolumeTemplate() != nil {
			scName := vol.GetExternalVolumeTemplate().GetStorageClassName()
			sc, err := scLister.Get(scName)
			if err != nil {
				if k8serrors.IsNotFound(err) {
					return nil, failedPreconditionf("StorageClass %q not found", scName)
				}
				return nil, fmt.Errorf("failed to get StorageClass %q: %w", scName, err)
			}

			volumes = append(volumes, &ateapipb.ExternalVolume{
				VolumeName: vol.GetName(),
				VolumeType: sc.Provisioner,
				Status:     ateapipb.ExternalVolume_STATUS_PENDING,
			})
		}
	}
	return volumes, nil
}

// GetActor retrieves an Actor by reference.
//
// Returns store.ErrNotFound if the actor does not exist.
func (a *Admission) GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	return a.store.GetActor(ctx, actorRef)
}

// ListActors lists Actors in atespace (or across all atespaces if empty).
//
// Returns store.ErrInvalidPageSize or store.ErrInvalidPageToken if pagination
// options are invalid.
func (a *Admission) ListActors(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.Actor], error) {
	return a.store.ListActors(ctx, atespace, opts)
}

// UpdateActorSpec replaces the user-mutable specification fields of an Actor,
// preserving server-owned metadata and status, and enforcing template
// repointing preconditions and declarative validation.
//
// Returns store.ErrPreconditionRequired if metadata uid or version is unset,
// store.ErrNotFound if the actor does not exist, store.ErrUIDConflict or
// store.ErrVersionConflict if the precondition does not match the stored actor,
// ErrInvalid if the updated spec fails declarative validation, or
// ErrFailedPrecondition if repointing actor_template violates state, sandbox
// config, volume, or snapshot location preconditions.
func (a *Admission) UpdateActorSpec(ctx context.Context, inActor *ateapipb.Actor) (*ateapipb.Actor, error) {
	actorRef := resources.ActorRefFromActor(inActor)
	return a.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(inActor), func(toUpdate *ateapipb.Actor) error {
		oldVal := proto.CloneOf(toUpdate)

		// Status and Metadata are server-owned fields.
		status, metadata := toUpdate.GetStatus(), toUpdate.GetMetadata()
		// Whole-object replace: clear first, so a field the client left unset is
		// cleared rather than kept from the stored actor.
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, inActor)
		toUpdate.Status = status
		toUpdate.Metadata = metadata
		defaults.Apply(toUpdate)
		newVal := toUpdate

		// Validate the user's input before doing any further work.
		if errs := apivalidation.ValidateActorUpdate(ctx, field.NewPath("actor"), newVal, oldVal, false); len(errs) > 0 {
			return invalidf("%v", errs.ToAggregate())
		}

		// Updating actor_template is only allowed while the actor is suspended.
		// The repointed ref must also resolve, mirroring CreateActor's
		// check (same non-atomicity caveat; resume re-resolves and fails
		// cleanly), and the replacement's sandbox config, volumes, and
		// volume mounts must match the old template's. It must also store
		// snapshots under the location the actor's own already live in.
		if !proto.Equal(oldVal.GetActorTemplate(), newVal.GetActorTemplate()) {
			if state := oldVal.GetStatus().GetState(); state != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
				return failedPreconditionf(
					"actor must be %s to change its actor template (got: %s)",
					ateapipb.ActorState_ACTOR_STATE_SUSPENDED, state)
			}
			newTemplate, err := a.resolveActorTemplate(ctx, newVal)
			if err != nil {
				return err
			}
			if err := validateSnapshotLocationUnchanged(oldVal, newTemplate); err != nil {
				return err
			}
			oldTemplate, err := a.resolveActorTemplate(ctx, oldVal)
			switch {
			case err == nil:
				// Snapshots are not portable across sandbox runtime
				// families, so the replacement template must name the same
				// SandboxConfig.
				if !proto.Equal(oldTemplate.GetSandboxConfig(), newTemplate.GetSandboxConfig()) {
					oldSC, newSC := oldTemplate.GetSandboxConfig(), newTemplate.GetSandboxConfig()
					return failedPreconditionf(
						"the current actor template names SandboxConfig %q (class %s) but the new one names %q (class %s); the sandbox config must be identical to repoint an actor",
						oldSC.GetConfigName(), oldSC.GetSandboxClass(), newSC.GetConfigName(), newSC.GetSandboxClass())
				}
				if err := validateTemplateVolumesUnchanged(oldTemplate, newTemplate); err != nil {
					return err
				}
			case errors.Is(err, ErrFailedPrecondition):
				// The old template is gone, so there is nothing left to
				// compare the sandbox config or volume layout against.
			default:
				return err
			}
		}

		// Validate the final value before storing it.
		if errs := apivalidation.ValidateActorUpdate(ctx, field.NewPath("actor"), newVal, oldVal, true); len(errs) > 0 {
			return fmt.Errorf("%v", errs.ToAggregate())
		}

		return nil
	})
}

// UpdateActorStatus updates the server-owned status of an Actor and validates
// the resulting resource before storing it.
//
// Returns store.ErrPreconditionRequired if precondition omits uid or version,
// store.ErrNotFound if the actor does not exist, store.ErrUIDConflict or
// store.ErrVersionConflict if precondition does not match the stored actor, or
// the error returned by mutate verbatim.
func (a *Admission) UpdateActorStatus(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapipb.ActorStatus) error) (*ateapipb.Actor, error) {
	return a.store.UpdateActor(ctx, actorRef, precondition, func(toUpdate *ateapipb.Actor) error {
		oldVal := proto.CloneOf(toUpdate)
		if err := mutate(toUpdate.Status); err != nil {
			return err
		}
		if errs := apivalidation.ValidateActorUpdate(ctx, field.NewPath("actor"), toUpdate, oldVal, true); len(errs) > 0 {
			return fmt.Errorf("%v", errs.ToAggregate())
		}
		return nil
	})
}

// DeleteActor deletes an Actor by reference.
//
// Returns store.ErrNotFound if the actor does not exist, or
// store.ErrUIDConflict or store.ErrVersionConflict if precondition does not
// match the stored actor.
func (a *Admission) DeleteActor(ctx context.Context, actorRef resources.ActorRef, precondition store.DeletePreconditions) (*ateapipb.Actor, error) {
	return a.store.DeleteActor(ctx, actorRef, precondition)
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
		return failedPreconditionf(
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
			return failedPreconditionf(
				"volume mounts of container %q differ between the current and the new actor template; volume mounts must be identical to repoint an actor",
				oldC.GetName())
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
	currentSnapshotURI := actor.GetStatus().GetExternalSnapshot().GetSnapshotUri()
	if currentSnapshotURI == "" {
		return nil
	}
	currentURI, err := resources.ParseSnapshotURI(currentSnapshotURI)
	if err != nil {
		return fmt.Errorf("while parsing the external snapshot %q: %w", currentSnapshotURI, err)
	}
	owner := resources.ActorSnapshotOwner(actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetUid())
	if !currentURI.OwnedBy(owner) {
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
		return failedPreconditionf(
			"the actor's snapshots are stored under %q but the new actor template stores them under %q: the storage location must be identical to repoint an actor that owns a snapshot",
			currentURI.Location(), newLocation)
	}
	return nil
}
