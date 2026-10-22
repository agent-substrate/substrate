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
	"slices"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type externalVolumeRefStore interface {
	GetExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapi.ExternalVolume, error)
	UpdateExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.Precondition, mutate func(toUpdate *ateapi.ExternalVolume) error) (*ateapi.ExternalVolume, error)
}

type externalVolumeDeleter interface {
	DeleteExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.DeletePreconditions) (*ateapi.ExternalVolume, error)
}

func templateHasExternalVolumeRef(template *ateapi.ActorTemplate) bool {
	for _, vol := range template.GetVolumes() {
		if vol.GetExternal().GetRef() != nil {
			return true
		}
	}
	return false
}

// resolveExternalVolumeRefs checks the actor's bindings against the template
// and returns a mount for every ExternalVolume the actor references.
func resolveExternalVolumeRefs(ctx context.Context, st externalVolumeRefStore, actor *ateapi.Actor, template *ateapi.ActorTemplate) ([]*ateapi.ExternalVolumeStatus, error) {
	bindings := actor.GetExternalVolumeBindings()
	unnamed := make(map[string]bool)
	for _, vol := range template.GetVolumes() {
		if vol.GetExternal().GetRef() != nil && vol.GetExternal().GetRef().GetName() == "" {
			unnamed[vol.GetName()] = true
		}
	}
	for slot := range bindings {
		if !unnamed[slot] {
			return nil, apierror.InvalidArgument("external_volume_bindings[%q] does not name an unnamed external.ref in the template", slot)
		}
	}

	var mounts []*ateapi.ExternalVolumeStatus
	for _, vol := range template.GetVolumes() {
		ref := vol.GetExternal().GetRef()
		if ref == nil {
			continue
		}
		name := ref.GetName()
		if name == "" {
			if name = bindings[vol.GetName()]; name == "" {
				return nil, apierror.InvalidArgument("external_volume_bindings must bind volume %q", vol.GetName())
			}
		}
		volume, err := getExternalVolume(ctx, st, resources.ExternalVolumeRef{Atespace: actor.GetMetadata().GetAtespace(), Name: name})
		if err != nil {
			return nil, err
		}
		mounts = append(mounts, externalVolumeMount(vol.GetName(), volume))
	}
	return mounts, nil
}

func getExternalVolume(ctx context.Context, st externalVolumeRefStore, volumeRef resources.ExternalVolumeRef) (*ateapi.ExternalVolume, error) {
	volume, err := st.GetExternalVolume(ctx, volumeRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.FailedPrecondition("ExternalVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while getting external volume %s: %w", volumeRef, err)
	}
	return volume, nil
}

func externalVolumeMount(templateVolumeName string, volume *ateapi.ExternalVolume) *ateapi.ExternalVolumeStatus {
	return &ateapi.ExternalVolumeStatus{
		Name:               templateVolumeName,
		ExternalVolumeName: volume.GetMetadata().GetName(),
		StorageVolumeId:    volume.GetVolumeId(),
		VolumeType:         volume.GetVolumeType(),
		VolumeContext:      volume.GetVolumeContext(),
	}
}

func borrowedVolumeRefs(actor *ateapi.Actor) []resources.ExternalVolumeRef {
	var refs []resources.ExternalVolumeRef
	for _, vol := range actor.GetStatus().GetExternalVolumes().GetVolumes() {
		if vol.GetExternalVolumeName() != "" {
			refs = append(refs, resources.ExternalVolumeRef{Atespace: actor.GetMetadata().GetAtespace(), Name: vol.GetExternalVolumeName()})
		}
	}
	return refs
}

func actorRefOf(actor *ateapi.Actor, active bool) *ateapi.ActorRef {
	return &ateapi.ActorRef{
		ActorUid:  actor.GetMetadata().GetUid(),
		ActorName: actor.GetMetadata().GetName(),
		Active:    active,
	}
}

func findActorRef(volume *ateapi.ExternalVolume, actorUID string) *ateapi.ActorRef {
	for _, ref := range volume.GetRefs() {
		if ref.GetActorUid() == actorUID {
			return ref
		}
	}
	return nil
}

func setActorRef(volume *ateapi.ExternalVolume, ref *ateapi.ActorRef) {
	if existing := findActorRef(volume, ref.GetActorUid()); existing != nil {
		existing.ActorName, existing.Active = ref.GetActorName(), ref.GetActive()
		return
	}
	volume.Refs = append(volume.Refs, ref)
}

// updateExternalVolumeRefs applies mutate under the volume's version guard.
// TODO: move references to their own table so claims do not contend on one row.
func updateExternalVolumeRefs(ctx context.Context, st externalVolumeRefStore, volumeRef resources.ExternalVolumeRef, mutate func(*ateapi.ExternalVolume) error) (*ateapi.ExternalVolume, error) {
	volume, err := st.GetExternalVolume(ctx, volumeRef)
	if err != nil {
		return nil, err
	}
	return st.UpdateExternalVolume(ctx, volumeRef, store.PreconditionFrom(volume), mutate)
}

// bindExternalVolumes records an inactive reference for every ExternalVolume
// the actor mounts.
func bindExternalVolumes(ctx context.Context, st externalVolumeRefStore, actor *ateapi.Actor) error {
	var errs []error
	for _, volumeRef := range borrowedVolumeRefs(actor) {
		_, err := updateExternalVolumeRefs(ctx, st, volumeRef, func(toUpdate *ateapi.ExternalVolume) error {
			if findActorRef(toUpdate, actor.GetMetadata().GetUid()) == nil {
				setActorRef(toUpdate, actorRefOf(actor, false))
			}
			return nil
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("while binding shared volume %s: %w", volumeRef, err))
		}
	}
	return errors.Join(errs...)
}

var errExternalVolumeHeld = errors.New("shared volume is held by another actor")

// claimExternalVolumes marks the actor's references active and refreshes the
// recorded mounts. A volume another actor holds active is refused.
// TODO: let volumes whose access mode allows it carry several active references.
func claimExternalVolumes(ctx context.Context, st externalVolumeRefStore, actor *ateapi.Actor) ([]*ateapi.ExternalVolumeStatus, error) {
	volumes := slices.Clone(actor.GetStatus().GetExternalVolumes().GetVolumes())
	actorUID := actor.GetMetadata().GetUid()
	for i, mount := range volumes {
		if mount.GetExternalVolumeName() == "" {
			continue
		}
		volumeRef := resources.ExternalVolumeRef{Atespace: actor.GetMetadata().GetAtespace(), Name: mount.GetExternalVolumeName()}
		var holder string
		claimed, err := updateExternalVolumeRefs(ctx, st, volumeRef, func(toUpdate *ateapi.ExternalVolume) error {
			for _, ref := range toUpdate.GetRefs() {
				if ref.GetActive() && ref.GetActorUid() != actorUID {
					holder = ref.GetActorName()
					return errExternalVolumeHeld
				}
			}
			setActorRef(toUpdate, actorRefOf(actor, true))
			return nil
		})
		switch {
		case err == nil:
		case errors.Is(err, errExternalVolumeHeld):
			return nil, apierror.FailedPrecondition("ExternalVolume %s is already held by actor %q; pause or suspend it before resuming another actor", volumeRef, holder)
		case errors.Is(err, store.ErrNotFound):
			return nil, apierror.FailedPrecondition("ExternalVolume %s not found", volumeRef)
		case errors.Is(err, store.ErrVersionConflict), errors.Is(err, store.ErrUIDConflict):
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		default:
			return nil, apierror.FailedPrecondition("ExternalVolume %s: %v", volumeRef, err)
		}
		volumes[i] = externalVolumeMount(mount.GetName(), claimed)
	}
	return volumes, nil
}

// deactivateExternalVolumes marks the actor's references inactive.
func deactivateExternalVolumes(ctx context.Context, st externalVolumeRefStore, actor *ateapi.Actor) error {
	var errs []error
	for _, volumeRef := range borrowedVolumeRefs(actor) {
		_, err := updateExternalVolumeRefs(ctx, st, volumeRef, func(toUpdate *ateapi.ExternalVolume) error {
			if ref := findActorRef(toUpdate, actor.GetMetadata().GetUid()); ref != nil {
				ref.Active = false
			}
			return nil
		})
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			errs = append(errs, fmt.Errorf("while deactivating shared volume %s: %w", volumeRef, err))
		}
	}
	return errors.Join(errs...)
}

// releaseExternalVolumes drops the actor's references and deletes each volume
// left without references whose delete trigger is LAST_ACTOR.
func releaseExternalVolumes(ctx context.Context, st externalVolumeRefStore, deleter externalVolumeDeleter, actor *ateapi.Actor) error {
	var errs []error
	for _, volumeRef := range borrowedVolumeRefs(actor) {
		released, err := updateExternalVolumeRefs(ctx, st, volumeRef, func(toUpdate *ateapi.ExternalVolume) error {
			toUpdate.Refs = slices.DeleteFunc(toUpdate.GetRefs(), func(ref *ateapi.ActorRef) bool {
				return ref.GetActorUid() == actor.GetMetadata().GetUid()
			})
			return nil
		})
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("while releasing shared volume %s: %w", volumeRef, err))
			continue
		}
		if released.GetDeleteTrigger() != ateapi.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR || len(released.GetRefs()) > 0 {
			continue
		}
		_, err = deleter.DeleteExternalVolume(ctx, volumeRef, store.DeletePreconditions{UID: released.GetMetadata().GetUid()})
		if code := status.Code(err); err != nil && code != codes.NotFound && code != codes.FailedPrecondition {
			errs = append(errs, fmt.Errorf("while deleting shared volume %s: %w", volumeRef, err))
		}
	}
	return errors.Join(errs...)
}
