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

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type sharedVolumeRefStore interface {
	GetSharedVolume(ctx context.Context, volumeRef resources.SharedVolumeRef) (*ateapipb.SharedVolume, error)
	UpdateSharedVolume(ctx context.Context, volumeRef resources.SharedVolumeRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.SharedVolume) error) (*ateapipb.SharedVolume, error)
}

type sharedVolumeDeleter interface {
	DeleteSharedVolume(ctx context.Context, volumeRef resources.SharedVolumeRef, precondition store.DeletePreconditions) (*ateapipb.SharedVolume, error)
}

func templateHasSharedVolumeRef(template *ateapipb.ActorTemplate) bool {
	for _, vol := range template.GetVolumes() {
		if vol.GetExternal().GetSharedVolume() != nil {
			return true
		}
	}
	return false
}

// resolveSharedVolumeRefs checks the actor's bindings against the template
// and returns a mount for every SharedVolume the actor references.
func resolveSharedVolumeRefs(ctx context.Context, st sharedVolumeRefStore, actor *ateapipb.Actor, template *ateapipb.ActorTemplate) ([]*ateapipb.ExternalVolume, error) {
	bindings := actor.GetSharedVolumeBindings()
	unnamed := make(map[string]bool)
	for _, vol := range template.GetVolumes() {
		if vol.GetExternal().GetSharedVolume() != nil && vol.GetExternal().GetSharedVolume().GetName() == "" {
			unnamed[vol.GetName()] = true
		}
	}
	for slot := range bindings {
		if !unnamed[slot] {
			return nil, apierror.InvalidArgument("shared_volume_bindings[%q] does not name an unnamed external.shared_volume in the template", slot)
		}
	}

	var mounts []*ateapipb.ExternalVolume
	for _, vol := range template.GetVolumes() {
		ref := vol.GetExternal().GetSharedVolume()
		if ref == nil {
			continue
		}
		name := ref.GetName()
		if name == "" {
			if name = bindings[vol.GetName()]; name == "" {
				return nil, apierror.InvalidArgument("shared_volume_bindings must bind volume %q", vol.GetName())
			}
		}
		volume, err := getReadySharedVolume(ctx, st, resources.SharedVolumeRef{Atespace: actor.GetMetadata().GetAtespace(), Name: name})
		if err != nil {
			return nil, err
		}
		mounts = append(mounts, sharedVolumeMount(vol.GetName(), volume))
	}
	return mounts, nil
}

func getReadySharedVolume(ctx context.Context, st sharedVolumeRefStore, volumeRef resources.SharedVolumeRef) (*ateapipb.SharedVolume, error) {
	volume, err := st.GetSharedVolume(ctx, volumeRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.FailedPrecondition("SharedVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while getting shared volume %s: %w", volumeRef, err)
	}
	if state := volume.GetStatus().GetState(); state != ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_READY {
		return nil, apierror.FailedPrecondition("SharedVolume %s is not ready (state %s)", volumeRef, state)
	}
	return volume, nil
}

func sharedVolumeMount(templateVolumeName string, volume *ateapipb.SharedVolume) *ateapipb.ExternalVolume {
	return &ateapipb.ExternalVolume{
		Name:             templateVolumeName,
		SharedVolumeName: volume.GetMetadata().GetName(),
		StorageVolumeId:  volume.GetVolumeId(),
		VolumeType:       volume.GetVolumeType(),
		VolumeContext:    volume.GetVolumeContext(),
	}
}

func borrowedVolumeRefs(actor *ateapipb.Actor) []resources.SharedVolumeRef {
	var refs []resources.SharedVolumeRef
	for _, vol := range actor.GetStatus().GetExternalVolumes() {
		if vol.GetSharedVolumeName() != "" {
			refs = append(refs, resources.SharedVolumeRef{Atespace: actor.GetMetadata().GetAtespace(), Name: vol.GetSharedVolumeName()})
		}
	}
	return refs
}

func actorRefOf(actor *ateapipb.Actor, active bool) *ateapipb.ActorRef {
	return &ateapipb.ActorRef{
		ActorUid:  actor.GetMetadata().GetUid(),
		ActorName: actor.GetMetadata().GetName(),
		Active:    active,
	}
}

func findActorRef(volume *ateapipb.SharedVolume, actorUID string) *ateapipb.ActorRef {
	for _, ref := range volume.GetStatus().GetRefs() {
		if ref.GetActorUid() == actorUID {
			return ref
		}
	}
	return nil
}

func setActorRef(volume *ateapipb.SharedVolume, ref *ateapipb.ActorRef) {
	if existing := findActorRef(volume, ref.GetActorUid()); existing != nil {
		existing.ActorName, existing.Active = ref.GetActorName(), ref.GetActive()
		return
	}
	if volume.Status == nil {
		volume.Status = &ateapipb.SharedVolumeStatus{}
	}
	volume.Status.Refs = append(volume.Status.Refs, ref)
}

// updateSharedVolumeRefs applies mutate under the volume's version guard.
// TODO: move references to their own table so claims do not contend on one row.
func updateSharedVolumeRefs(ctx context.Context, st sharedVolumeRefStore, volumeRef resources.SharedVolumeRef, mutate func(*ateapipb.SharedVolume) error) (*ateapipb.SharedVolume, error) {
	volume, err := st.GetSharedVolume(ctx, volumeRef)
	if err != nil {
		return nil, err
	}
	return st.UpdateSharedVolume(ctx, volumeRef, store.PreconditionFrom(volume), mutate)
}

// bindSharedVolumes records an inactive reference for every SharedVolume
// the actor mounts.
func bindSharedVolumes(ctx context.Context, st sharedVolumeRefStore, actor *ateapipb.Actor) error {
	var errs []error
	for _, volumeRef := range borrowedVolumeRefs(actor) {
		_, err := updateSharedVolumeRefs(ctx, st, volumeRef, func(toUpdate *ateapipb.SharedVolume) error {
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

var errSharedVolumeHeld = errors.New("shared volume is held by another actor")

// claimSharedVolumes marks the actor's references active and refreshes the
// recorded mounts. A READ_WRITE_ONCE volume another actor holds is refused.
// TODO: let READ_WRITE_MANY volumes carry several active references.
func claimSharedVolumes(ctx context.Context, st sharedVolumeRefStore, actor *ateapipb.Actor) ([]*ateapipb.ExternalVolume, error) {
	volumes := slices.Clone(actor.GetStatus().GetExternalVolumes())
	actorUID := actor.GetMetadata().GetUid()
	for i, mount := range volumes {
		if mount.GetSharedVolumeName() == "" {
			continue
		}
		volumeRef := resources.SharedVolumeRef{Atespace: actor.GetMetadata().GetAtespace(), Name: mount.GetSharedVolumeName()}
		var holder string
		claimed, err := updateSharedVolumeRefs(ctx, st, volumeRef, func(toUpdate *ateapipb.SharedVolume) error {
			if toUpdate.GetStatus().GetState() != ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_READY {
				return fmt.Errorf("shared volume is not ready (state %s)", toUpdate.GetStatus().GetState())
			}
			for _, ref := range toUpdate.GetStatus().GetRefs() {
				if ref.GetActive() && ref.GetActorUid() != actorUID {
					holder = ref.GetActorName()
					return errSharedVolumeHeld
				}
			}
			setActorRef(toUpdate, actorRefOf(actor, true))
			return nil
		})
		switch {
		case err == nil:
		case errors.Is(err, errSharedVolumeHeld):
			return nil, apierror.FailedPrecondition("SharedVolume %s is already held by actor %q; pause or suspend it before resuming another actor", volumeRef, holder)
		case errors.Is(err, store.ErrNotFound):
			return nil, apierror.FailedPrecondition("SharedVolume %s not found", volumeRef)
		case errors.Is(err, store.ErrVersionConflict), errors.Is(err, store.ErrUIDConflict):
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		default:
			return nil, apierror.FailedPrecondition("SharedVolume %s: %v", volumeRef, err)
		}
		volumes[i] = sharedVolumeMount(mount.GetName(), claimed)
	}
	return volumes, nil
}

// deactivateSharedVolumes marks the actor's references inactive.
func deactivateSharedVolumes(ctx context.Context, st sharedVolumeRefStore, actor *ateapipb.Actor) error {
	var errs []error
	for _, volumeRef := range borrowedVolumeRefs(actor) {
		_, err := updateSharedVolumeRefs(ctx, st, volumeRef, func(toUpdate *ateapipb.SharedVolume) error {
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

// releaseSharedVolumes drops the actor's references and deletes each volume
// left without references whose delete trigger is LAST_ACTOR.
func releaseSharedVolumes(ctx context.Context, st sharedVolumeRefStore, deleter sharedVolumeDeleter, actor *ateapipb.Actor) error {
	var errs []error
	for _, volumeRef := range borrowedVolumeRefs(actor) {
		released, err := updateSharedVolumeRefs(ctx, st, volumeRef, func(toUpdate *ateapipb.SharedVolume) error {
			toUpdate.Status.Refs = slices.DeleteFunc(toUpdate.GetStatus().GetRefs(), func(ref *ateapipb.ActorRef) bool {
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
		if released.GetDeleteTrigger() != ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR || len(released.GetStatus().GetRefs()) > 0 {
			continue
		}
		_, err = deleter.DeleteSharedVolume(ctx, volumeRef, store.DeletePreconditions{UID: released.GetMetadata().GetUid()})
		if code := status.Code(err); err != nil && code != codes.NotFound && code != codes.FailedPrecondition {
			errs = append(errs, fmt.Errorf("while deleting shared volume %s: %w", volumeRef, err))
		}
	}
	return errors.Join(errs...)
}
