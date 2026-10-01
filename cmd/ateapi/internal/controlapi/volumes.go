// Copyright 2026 Google LLC
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
	"errors"
	"fmt"
	"log/slog"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

// initialActorVolumes constructs initial volume objects in PENDING state before volume creation.
func initialActorVolumes(ctx context.Context, scLister storagev1listers.StorageClassLister, template *ateapipb.ActorTemplate) ([]*ateapipb.ExternalVolume, error) {
	if template == nil {
		return nil, status.Error(codes.InvalidArgument, "template is required")
	}
	var volumes []*ateapipb.ExternalVolume
	for _, vol := range template.GetVolumes() {
		if vol.GetExternalVolumeTemplate() != nil {
			scName := vol.GetExternalVolumeTemplate().GetStorageClassName()
			sc, err := scLister.Get(scName)
			if err != nil {
				if k8serrors.IsNotFound(err) {
					return nil, status.Errorf(codes.FailedPrecondition, "StorageClass %q not found", scName)
				}
				return nil, status.Errorf(codes.Internal, "failed to get StorageClass %q: %v", scName, err)
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

// createActorVolumes provisions external volumes specified in volumesToCreate using the provided volume plugin.
// It returns the list of external volumes (with updated status and storage IDs), or an error if any creation fails.
//
// sourceSnapshots are the volume snapshots the Actor's external snapshot
// carries, matched to volumes by name. A volume with a matching entry is
// restored from it; one without is provisioned empty. CreateActor has already
// checked that a tag-seeded Actor has an entry for every external volume, so a
// missing entry here means the Actor was not seeded from a tag at all.
//
// Any volumes processed before or during a failure are returned alongside the error so they can be persisted on the actor.
func createActorVolumes(ctx context.Context, registry VolumePluginRegistry, scLister storagev1listers.StorageClassLister, actorUID string, template *ateapipb.ActorTemplate, volumesToCreate []*ateapipb.ExternalVolume, sourceSnapshots []*ateapipb.ExternalVolumeSnapshot) (resultVolumes []*ateapipb.ExternalVolume, err error) {
	resultVolumes = make([]*ateapipb.ExternalVolume, 0, len(volumesToCreate))

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

		var specVol *ateapipb.Volume
		volName := vol.GetVolumeName()
		for _, tVol := range template.GetVolumes() {
			if tVol.GetName() == volName {
				specVol = tVol
				break
			}
		}
		if specVol == nil || specVol.GetExternalVolumeTemplate() == nil {
			return resultVolumes, status.Errorf(codes.NotFound, "volume %q not found in template", volName)
		}

		switch vol.GetStatus() {
		case ateapipb.ExternalVolume_STATUS_PENDING:
			// proceed with volume creation
		case ateapipb.ExternalVolume_STATUS_CREATED:
			resultVolumes = append(resultVolumes, vol)
			continue
		case ateapipb.ExternalVolume_STATUS_DELETING:
			return resultVolumes, status.Errorf(codes.FailedPrecondition, "cannot create volume %q in DELETING status", volName)
		default:
			return resultVolumes, status.Errorf(codes.Internal, "unexpected status %s for volume %q", vol.GetStatus(), volName)
		}

		actVolID := actorVolumeID(actorUID, volName)

		scName := specVol.GetExternalVolumeTemplate().GetStorageClassName()
		sc, err := scLister.Get(scName)
		if err != nil {
			return resultVolumes, status.Errorf(codes.Internal, "failed to get StorageClass %q: %v", scName, err)
		}

		if sc.Provisioner != vol.GetVolumeType() {
			return resultVolumes, status.Errorf(codes.FailedPrecondition, "volume %q has mismatched type %q (expected %q from StorageClass %q)", volName, vol.GetVolumeType(), sc.Provisioner, scName)
		}

		plugin, err := registry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			return resultVolumes, status.Errorf(codes.FailedPrecondition, "failed to get volume plugin for driver %q (StorageClass %q): %v", sc.Provisioner, scName, err)
		}

		sourceSnapshotID, err := resolveVolumeSource(ctx, plugin, sourceSnapshots, volName, sc.Provisioner)
		if err != nil {
			return resultVolumes, err
		}

		createResp, volErr := plugin.CreateVolume(ctx, volume.CreateVolumeRequest{
			Name:             actVolID,
			Capacity:         specVol.GetExternalVolumeTemplate().GetCapacity(),
			DriverName:       sc.Provisioner,
			Parameters:       sc.Parameters,
			SourceSnapshotID: sourceSnapshotID,
		})
		if volErr != nil {
			return resultVolumes, status.Errorf(codes.Internal, "failed to create volume %q: %v", specVol.GetName(), volErr)
		}

		resultVolumes = append(resultVolumes, &ateapipb.ExternalVolume{
			VolumeName:      volName,
			StorageVolumeId: createResp.VolumeID,
			VolumeType:      sc.Provisioner,
			Status:          ateapipb.ExternalVolume_STATUS_CREATED,
			VolumeContext:   createResp.VolumeContext,
		})
	}
	return resultVolumes, nil
}

// resolveVolumeSource finds the snapshot a volume should be restored from and
// confirms the storage system is ready to serve it, returning "" when the
// volume is to be provisioned empty.
//
// Readiness is checked here rather than at tag creation: CreateTag does not
// wait for a driver to finish copying, so this is the first point at which the
// data is actually needed.
func resolveVolumeSource(ctx context.Context, plugin volume.VolumePluginControlPlane, sourceSnapshots []*ateapipb.ExternalVolumeSnapshot, volName string, provisioner string) (string, error) {
	var source *ateapipb.ExternalVolumeSnapshot
	for _, snap := range sourceSnapshots {
		if snap.GetVolumeName() == volName {
			source = snap
			break
		}
	}
	if source == nil {
		return "", nil
	}

	// A snapshot handle is meaningful only to the driver that issued it, so a
	// template repointed at a different storage class cannot restore from it.
	if source.GetVolumeType() != provisioner {
		return "", status.Errorf(codes.FailedPrecondition, "volume %q snapshot was taken by driver %q but the volume would be provisioned by %q", volName, source.GetVolumeType(), provisioner)
	}

	snapshotID := source.GetStorageSnapshotId()
	// An entry without a handle is a volume whose snapshot creation did not
	// finish; provisioning it empty would be silent data loss.
	if snapshotID == "" {
		return "", status.Errorf(codes.FailedPrecondition, "volume %q has no snapshot handle: its snapshot creation did not finish", volName)
	}
	observed, found, err := plugin.GetSnapshot(ctx, snapshotID)
	if err != nil {
		return "", status.Errorf(codes.Internal, "failed to read snapshot %q for volume %q: %v", snapshotID, volName, err)
	}
	switch {
	case !found && source.GetReadyToUse():
		// The snapshot was ready when the tag recorded it and the driver no
		// longer has it, so it was deleted behind our back. Restoring is
		// impossible; an empty volume would be silent data loss.
		return "", status.Errorf(codes.FailedPrecondition, "snapshot %q for volume %q no longer exists in the storage system", snapshotID, volName)
	case !found:
		// Not found and never observed ready: the driver cannot report on its
		// own snapshots (no LIST_SNAPSHOTS). Proceed and let CreateVolume fail
		// if the handle is bad.
		slog.WarnContext(ctx, "Cannot confirm snapshot readiness; restoring anyway", slog.String("snapshot_id", snapshotID), slog.String("volume_name", volName))
	case !observed.ReadyToUse:
		return "", status.Errorf(codes.FailedPrecondition, "snapshot %q for volume %q is not yet ready to use", snapshotID, volName)
	}
	return snapshotID, nil
}

// deleteActorVolumes deletes all external volumes in the list.
func deleteActorVolumes(ctx context.Context, registry VolumePluginRegistry, actorUID string, volumes []*ateapipb.ExternalVolume) error {
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
			volID = actorVolumeID(actorUID, vol.GetVolumeName())
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
func getMountedActorVolumes(ctx context.Context, ref *ateapipb.ObjectRef, volumes []*ateapipb.ExternalVolume, template *ateapipb.ActorTemplate) []*ateapipb.ExternalVolume {
	var mounted []*ateapipb.ExternalVolume
	for _, vol := range volumes {
		// Find the corresponding volume in the ActorTemplate to check if it's mounted
		var matchedTemplateVol *ateapipb.Volume
		for _, tVol := range template.GetVolumes() {
			if vol.GetVolumeName() == tVol.GetName() {
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

// tagVolumeSnapshotID names the snapshot a tag takes of one volume. It is
// derived from the tag's UID rather than its name so that a second create under
// a reused name cannot land on the first one's snapshots.
func tagVolumeSnapshotID(tagUID string, volumeName string) string {
	return fmt.Sprintf("substrate-snap-%s-%s", tagUID, volumeName)
}

// detachActorVolumes detaches all mounted external volumes for an actor from its worker node.
func detachActorVolumes(ctx context.Context, st detachActorVolumesStore, registry VolumePluginRegistry, actor *ateapipb.Actor, template *ateapipb.ActorTemplate, action string) error {
	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment == nil {
		slog.WarnContext(ctx, fmt.Sprintf("Actor has no assigned worker pod during %s, skipping detach volumes", action), slog.String("actor_id", actor.GetMetadata().GetName()))
		return nil
	}

	worker, err := st.GetWorker(ctx, assignment.GetWorker().GetName())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			slog.WarnContext(ctx, fmt.Sprintf("Worker not found in store during %s, skipping detach volumes", action), slog.String("actor_id", actor.GetMetadata().GetName()))
			return nil
		}
		return fmt.Errorf("failed to get worker: %w", err)
	}

	node := worker.GetNodeName()
	if node == "" {
		slog.WarnContext(ctx, fmt.Sprintf("Worker has no assigned node name during %s, skipping detach volumes", action), slog.String("actor_id", actor.GetMetadata().GetName()))
		return nil
	}

	ref := &ateapipb.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()}
	// If the template is available, only detach volumes that are actively mounted
	// in the template's containers. If the template is missing/deleted, fall back to
	// attempting detachment for all external volumes recorded on the actor so we do
	// not orphan attached disks on the worker node.
	volumesToDetach := actor.GetStatus().GetActorVolumes()
	if template != nil {
		volumesToDetach = getMountedActorVolumes(ctx, ref, actor.GetStatus().GetActorVolumes(), template)
	}
	// Collect errors for all volumes to detach, but continue processing so we attempt to detach all volumes.
	var errs []error
	for _, vol := range volumesToDetach {
		// StorageVolumeId is only populated once the volume is provisioned.
		// Skip volumes that were never created (e.g. failed during PENDING state).
		if vol.GetStorageVolumeId() == "" {
			slog.WarnContext(ctx, "Volume has no storage volume ID, skipping detach", slog.String("volume_name", vol.GetVolumeName()), slog.String("actor_id", actor.GetMetadata().GetName()))
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

// detachActorVolumesStore enumerates the subset of store methods needed to
// detach actor volumes.
type detachActorVolumesStore interface {
	GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error)
}
