// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controlapi

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

// externalVolumesStatus wraps vols for ActorStatus, leaving the field unset
// when the actor has no external volumes.
func externalVolumesStatus(vols []*ateapi.ExternalVolumeStatus) *ateapi.ExternalVolumes {
	if len(vols) == 0 {
		return nil
	}
	return &ateapi.ExternalVolumes{Volumes: vols}
}

// initialExternalVolumes constructs the actor's volume objects before volume
// creation. A volume has no storage volume ID until it is provisioned.
func initialExternalVolumes(ctx context.Context, scLister storagev1listers.StorageClassLister, template *ateapi.ActorTemplate) ([]*ateapi.ExternalVolumeStatus, error) {
	if template == nil {
		return nil, apierror.InvalidArgument("template is required")
	}
	var volumes []*ateapi.ExternalVolumeStatus
	for _, vol := range template.GetVolumes() {
		if vol.GetExternal().GetTemplate() != nil {
			scName := vol.GetExternal().GetTemplate().GetStorageClassName()
			sc, err := scLister.Get(scName)
			if err != nil {
				if k8serrors.IsNotFound(err) {
					return nil, apierror.FailedPrecondition("StorageClass %q not found", scName)
				}
				return nil, apierror.Internal("failed to get StorageClass %q: %v", scName, err)
			}

			volumes = append(volumes, &ateapi.ExternalVolumeStatus{
				Name:       vol.GetName(),
				VolumeType: sc.Provisioner,
			})
		}
	}
	return volumes, nil
}

// createActorVolumes provisions the external volumes in volumesToCreate that have no storage volume ID, using the provided volume plugin.
// It returns the list of external volumes (with storage IDs), or an error if any creation fails.
// Any volumes processed before or during a failure are returned alongside the error so they can be persisted on the actor.
//
// sourceSnapshots are the volume snapshots of the tag the Actor was created
// from, matched to volumes by name. A volume with an entry is restored from its
// snapshot. A volume without one is provisioned empty.
func createActorVolumes(ctx context.Context, registry VolumePluginRegistry, scLister storagev1listers.StorageClassLister, actorUID string, template *ateapi.ActorTemplate, volumesToCreate []*ateapi.ExternalVolumeStatus, sourceSnapshots []*ateapi.ExternalVolumeSnapshot) (resultVolumes []*ateapi.ExternalVolumeStatus, err error) {
	resultVolumes = make([]*ateapi.ExternalVolumeStatus, 0, len(volumesToCreate))

	var currentIdx int
	defer func() {
		if err != nil {
			// If we encounter an error, append the rest of the volumes to the result with the last
			// known state. This allows the caller to persist the state of the volumes.
			resultVolumes = append(resultVolumes, volumesToCreate[currentIdx:]...)
		}
	}()

	for idx, vol := range volumesToCreate {
		currentIdx = idx

		var specVol *ateapi.Volume
		volName := vol.GetName()
		for _, tVol := range template.GetVolumes() {
			if tVol.GetName() == volName {
				specVol = tVol
				break
			}
		}
		if specVol == nil || specVol.GetExternal().GetTemplate() == nil {
			return resultVolumes, apierror.NotFound("volume %q not found in template", volName)
		}

		if vol.GetStorageVolumeId() != "" {
			resultVolumes = append(resultVolumes, vol)
			continue
		}

		actVolID := actorVolumeID(actorUID, volName)

		scName := specVol.GetExternal().GetTemplate().GetStorageClassName()
		sc, err := scLister.Get(scName)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return resultVolumes, apierror.FailedPrecondition("StorageClass %q not found", scName)
			}
			return resultVolumes, apierror.Internal("failed to get StorageClass %q: %v", scName, err)
		}

		if sc.Provisioner != vol.GetVolumeType() {
			return resultVolumes, apierror.FailedPrecondition("volume %q has mismatched type %q (expected %q from StorageClass %q)", volName, vol.GetVolumeType(), sc.Provisioner, scName)
		}

		plugin, err := registry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			return resultVolumes, apierror.FailedPrecondition("failed to get volume plugin for driver %q (StorageClass %q): %v", sc.Provisioner, scName, err)
		}

		// CreateActor checked that the snapshot is ready.
		sourceSnapshotID := findVolumeSnapshot(sourceSnapshots, volName).GetStorageSnapshotId()

		resp, volErr := plugin.CreateVolume(ctx, volume.CreateVolumeRequest{
			Name:             actVolID,
			CapacityBytes:    specVol.GetExternal().GetTemplate().GetCapacityBytes(),
			Parameters:       sc.Parameters,
			SourceSnapshotID: sourceSnapshotID,
		})
		if volErr != nil {
			// CSI answers NotFound when the content source is gone, as when
			// the tag was deleted before this Actor first resumed. Retrying
			// cannot help.
			if sourceSnapshotID != "" && status.Code(volErr) == codes.NotFound {
				return resultVolumes, apierror.FailedPrecondition("snapshot %q to restore volume %q from no longer exists in the storage system: %v", sourceSnapshotID, volName, volErr)
			}
			return resultVolumes, apierror.Internal("failed to create volume %q: %v", specVol.GetName(), volErr)
		}

		if resp.VolumeID == "" {
			// An empty ID would read as not yet provisioned and be created again.
			return resultVolumes, apierror.Internal("volume plugin returned no ID for volume %q", specVol.GetName())
		}

		resultVolumes = append(resultVolumes, &ateapi.ExternalVolumeStatus{
			Name:            volName,
			StorageVolumeId: resp.VolumeID,
			VolumeType:      sc.Provisioner,
			VolumeContext:   resp.VolumeContext,
		})
	}
	return resultVolumes, nil
}

// findVolumeSnapshot returns the entry in snapshots for the named volume, or
// nil if there is none.
func findVolumeSnapshot(snapshots []*ateapi.ExternalVolumeSnapshot, volName string) *ateapi.ExternalVolumeSnapshot {
	for _, snap := range snapshots {
		if snap.GetSourceVolumeName() == volName {
			return snap
		}
	}
	return nil
}

// tagVolumeSnapshots returns the volume snapshots of the tag an Actor was
// created from, held by the tag-owned snapshot it starts with. It returns nil
// once the Actor has replaced that snapshot with its own, by which time its
// volumes have been provisioned.
func tagVolumeSnapshots(status *ateapi.ActorStatus) []*ateapi.ExternalVolumeSnapshot {
	return latestSnapshot(status, func(snap *ateapi.Snapshot) bool {
		return snap.GetOwner() == ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG
	}).GetVolumeSnapshots()
}

// ensureTagVolumeSnapshotsUsable checks that the volume snapshots tag
// recorded can seed the external volumes vols an Actor would get, and returns
// the tag. A volume the tag has no snapshot for is provisioned empty, whether
// the tag left it out or captured no volumes.
//
// CreateTag does not wait for snapshots to become ready, so a snapshot the tag
// recorded as not ready is looked up in the storage system, and the call fails
// with FailedPrecondition until it is ready. One now ready is recorded on the
// tag, so later Actors skip the lookup.
func (s *ServiceImpl) ensureTagVolumeSnapshotsUsable(ctx context.Context, tag *ateapi.Tag, vols []*ateapi.ExternalVolumeStatus) (*ateapi.Tag, error) {
	tagRef := resources.TagRefFromTag(tag)
	var nowReady []string
	for _, vol := range vols {
		volName := vol.GetName()
		snap := findVolumeSnapshot(tag.GetStatus().GetSnapshot().GetVolumeSnapshots(), volName)
		if snap == nil {
			continue
		}
		snapshotID := snap.GetStorageSnapshotId()
		// A COMPLETED tag always has every handle: CreateTag finishes a tag
		// only once they are all recorded. A missing one means the stored tag
		// is wrong, so fail instead of provisioning the volume empty.
		if snapshotID == "" {
			return nil, apierror.Internal("Tag %s has no snapshot handle for volume %q", tagRef, volName)
		}
		// A handle means nothing to another driver. The StorageClass may have
		// been recreated with a different provisioner since the tag was taken.
		if snap.GetVolumeType() != vol.GetVolumeType() {
			return nil, apierror.FailedPrecondition("Tag %s snapshot of volume %q was taken by driver %q, but the volume is provisioned by %q", tagRef, volName, snap.GetVolumeType(), vol.GetVolumeType())
		}
		if snap.GetReadyToUse() {
			continue
		}
		observed, found, err := s.lookupVolumeSnapshot(ctx, snap)
		switch {
		case errors.Is(err, errors.ErrUnsupported):
			// The driver can never report readiness, so refusing here would
			// block restore for good. CreateVolume fails if the snapshot
			// cannot be restored.
			slog.WarnContext(ctx, "driver cannot report snapshot readiness; restoring anyway",
				slog.String("snapshot_id", snapshotID), slog.String("volume_name", volName))
			continue
		case err != nil:
			return nil, apierror.Internal("failed to read Tag %s snapshot %q of volume %q: %v", tagRef, snapshotID, volName, err)
		case !found:
			return nil, apierror.FailedPrecondition("Tag %s snapshot %q of volume %q no longer exists in the storage system", tagRef, snapshotID, volName)
		case !observed.ReadyToUse:
			return nil, apierror.FailedPrecondition("Tag %s snapshot %q of volume %q is not ready to use yet; retry later", tagRef, snapshotID, volName)
		}
		nowReady = append(nowReady, volName)
	}
	return s.recordTagVolumeSnapshotsReady(ctx, tag, nowReady), nil
}

// refreshTagVolumeSnapshots asks the storage system whether the volume
// snapshots a finished tag records as not ready have become ready, and records
// those that have. CreateTag does not wait for snapshots, so GetTag uses this
// to show when a tag can seed an Actor. A failed lookup is logged and leaves
// the snapshot not ready.
func (s *ServiceImpl) refreshTagVolumeSnapshots(ctx context.Context, tag *ateapi.Tag) *ateapi.Tag {
	// Only a finished tag is refreshed. A pending one may still lack
	// handles, and a deleting one is going away.
	completed := tagDurableSnapshotURI(tag, ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED) != ""
	deleting := tag.GetMetadata().GetDeleteTime() != nil
	if !completed || deleting {
		return tag
	}
	var nowReady []string
	for _, snap := range tag.GetStatus().GetSnapshot().GetVolumeSnapshots() {
		if snap.GetReadyToUse() {
			continue
		}
		observed, found, err := s.lookupVolumeSnapshot(ctx, snap)
		if err != nil {
			if !errors.Is(err, errors.ErrUnsupported) {
				slog.WarnContext(ctx, "failed to read tag volume snapshot",
					slog.String("tag", resources.TagRefFromTag(tag).String()),
					slog.String("snapshot_id", snap.GetStorageSnapshotId()), slog.Any("error", err))
			}
			continue
		}
		if found && observed.ReadyToUse {
			nowReady = append(nowReady, snap.GetSourceVolumeName())
		}
	}
	return s.recordTagVolumeSnapshotsReady(ctx, tag, nowReady)
}

// lookupVolumeSnapshot asks the driver that took snap about it.
func (s *ServiceImpl) lookupVolumeSnapshot(ctx context.Context, snap *ateapi.ExternalVolumeSnapshot) (volume.Snapshot, bool, error) {
	plugin, err := s.pluginRegistry.GetPlugin(ctx, snap.GetVolumeType())
	if err != nil {
		return volume.Snapshot{}, false, fmt.Errorf("failed to get volume plugin for driver %q: %w", snap.GetVolumeType(), err)
	}
	return plugin.GetSnapshot(ctx, snap.GetStorageSnapshotId())
}

// recordTagVolumeSnapshotsReady sets ready_to_use on the tag's snapshots of the
// named volumes and returns the stored tag. Recording readiness only saves
// later callers a lookup, so a failed write is logged and the tag is returned
// unchanged.
func (s *ServiceImpl) recordTagVolumeSnapshotsReady(ctx context.Context, tag *ateapi.Tag, volNames []string) *ateapi.Tag {
	if len(volNames) == 0 {
		return tag
	}
	tagRef := resources.TagRefFromTag(tag)
	updated, err := s.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapi.Tag) error {
		for _, volName := range volNames {
			if snap := findVolumeSnapshot(toUpdate.GetStatus().GetSnapshot().GetVolumeSnapshots(), volName); snap != nil {
				snap.ReadyToUse = true
			}
		}
		return nil
	})
	if err != nil {
		slog.WarnContext(ctx, "failed to record tag volume snapshots as ready",
			slog.String("tag", tagRef.String()), slog.Any("error", err))
		return tag
	}
	return updated
}

// deleteActorVolumes deletes all external volumes in the list.
func deleteActorVolumes(ctx context.Context, registry VolumePluginRegistry, actorUID string, volumes []*ateapi.ExternalVolumeStatus) error {
	if actorUID == "" {
		return errors.New("actorUID is required")
	}
	var errs []error
	for _, vol := range volumes {
		volID := vol.GetStorageVolumeId()
		if volID == "" {
			// If the volume hasn't been successfully created yet, it's possible
			// that it doesn't have a storage volume ID. In that case, fallback
			// to the original requested volID.
			volID = actorVolumeID(actorUID, vol.GetName())
		}
		// TODO: Standardize volume plugin lookup and error handling across control plane
		// and worker plane (e.g. via a shared helper).
		plugin, err := registry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to get volume plugin for %q: %w", vol.GetVolumeType(), err))
			continue
		}
		if err := plugin.DeleteVolume(ctx, volID); err != nil {
			if status.Code(err) == codes.NotFound {
				slog.WarnContext(ctx, "Volume not found during delete, assuming already deleted", slog.String("volume_id", volID))
				continue
			}
			errs = append(errs, fmt.Errorf("failed to delete volume %q: %w", volID, err))
		}
	}
	return errors.Join(errs...)
}

// getMountedActorVolumes filters the actor's volumes and returns only those that are declared and mounted in the ActorTemplate.
func getMountedActorVolumes(ctx context.Context, ref *ateapi.ObjectRef, volumes []*ateapi.ExternalVolumeStatus, template *ateapi.ActorTemplate) []*ateapi.ExternalVolumeStatus {
	var mounted []*ateapi.ExternalVolumeStatus
	for _, vol := range volumes {
		// Find the corresponding volume in the ActorTemplate to check if it's mounted
		var matchedTemplateVol *ateapi.Volume
		for _, tVol := range template.GetVolumes() {
			if vol.GetName() == tVol.GetName() {
				matchedTemplateVol = tVol
				break
			}
		}

		if matchedTemplateVol == nil {
			slog.WarnContext(ctx, "Volume not found in template, skipping", slog.String("volume_id", vol.GetStorageVolumeId()))
			continue
		}

		if !isVolumeMounted(matchedTemplateVol.GetName(), template) {
			slog.InfoContext(ctx, "Volume not mounted in template, skipping", slog.String("volume_id", vol.GetStorageVolumeId()))
			continue
		}
		mounted = append(mounted, vol)
	}
	return mounted
}

func actorVolumeID(actorUID string, volumeName string) string {
	return fmt.Sprintf("substrate-%s-%s", actorUID, volumeName)
}

// tagVolumeSnapshotID names the snapshot a tag takes of one volume. It uses the
// tag's UID so that a second create under a reused tag name gets new snapshot
// names. The volume name is hashed so the result stays at 62 characters for
// any volume name. Some drivers, like the Compute Engine PD driver, use it as
// a resource name limited to 63 characters.
func tagVolumeSnapshotID(tagUID string, volumeName string) string {
	sum := sha256.Sum256([]byte(volumeName))
	return fmt.Sprintf("substrate-snap-%s-%x", tagUID, sum[:5])
}

// detachActorVolumes detaches all mounted external volumes for an actor from its assigned node.
func detachActorVolumes(ctx context.Context, registry VolumePluginRegistry, actor *ateapi.Actor, template *ateapi.ActorTemplate, action string) error {
	node := actor.GetStatus().GetAssignedNode()
	if node == "" {
		slog.WarnContext(ctx, fmt.Sprintf("Actor has no assigned node during %s, skipping detach volumes", action), slog.String("actor_id", actor.GetMetadata().GetName()))
		return nil
	}

	ref := &ateapi.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()}
	// If the template is available, only detach volumes that are actively mounted
	// in the template's containers. If the template is missing/deleted, fall back to
	// attempting detachment for all external volumes recorded on the actor so we do
	// not orphan attached disks on the worker node.
	volumesToDetach := actor.GetStatus().GetExternalVolumes().GetVolumes()
	if template != nil {
		volumesToDetach = getMountedActorVolumes(ctx, ref, actor.GetStatus().GetExternalVolumes().GetVolumes(), template)
	}
	// Collect errors for all volumes to detach, but continue processing so we attempt to detach all volumes.
	var errs []error
	for _, vol := range volumesToDetach {
		// StorageVolumeId is only populated once the volume is provisioned.
		// Skip volumes that were never created (e.g. creation failed).
		if vol.GetStorageVolumeId() == "" {
			slog.WarnContext(ctx, "Volume has no storage volume ID, skipping detach", slog.String("volume_name", vol.GetName()), slog.String("actor_id", actor.GetMetadata().GetName()))
			continue
		}
		slog.InfoContext(ctx, "Detaching volume from node", slog.String("volume_id", vol.GetStorageVolumeId()), slog.String("node", node))
		plugin, err := registry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to get volume plugin for %q: %w", vol.GetVolumeType(), err))
			continue
		}
		if err := plugin.DetachVolume(ctx, vol.GetStorageVolumeId(), node); err != nil {
			if status.Code(err) == codes.NotFound {
				slog.WarnContext(ctx, "Volume not found during detach, assuming already detached", slog.String("volume_id", vol.GetStorageVolumeId()), slog.String("node", node))
				continue
			}
			errs = append(errs, fmt.Errorf("failed to detach volume %q from node %q: %w", vol.GetStorageVolumeId(), node, err))
		}
	}
	return errors.Join(errs...)
}
