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

	"github.com/agent-substrate/substrate/internal/apierror"
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
		return nil, apierror.InvalidArgument("template is required")
	}
	var volumes []*ateapipb.ExternalVolume
	for _, vol := range template.GetVolumes() {
		if vol.GetExternalVolumeTemplate() != nil {
			scName := vol.GetExternalVolumeTemplate().GetStorageClassName()
			sc, err := scLister.Get(scName)
			if err != nil {
				if k8serrors.IsNotFound(err) {
					return nil, apierror.FailedPrecondition("StorageClass %q not found", scName)
				}
				return nil, apierror.Internal("failed to get StorageClass %q: %v", scName, err)
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
// Any volumes processed before or during a failure are returned alongside the error so they can be persisted on the actor.
func createActorVolumes(ctx context.Context, registry VolumePluginRegistry, scLister storagev1listers.StorageClassLister, actorUID string, template *ateapipb.ActorTemplate, volumesToCreate []*ateapipb.ExternalVolume) (resultVolumes []*ateapipb.ExternalVolume, err error) {
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
			return resultVolumes, apierror.NotFound("volume %q not found in template", volName)
		}

		switch vol.GetStatus() {
		case ateapipb.ExternalVolume_STATUS_PENDING:
			// proceed with volume creation
		case ateapipb.ExternalVolume_STATUS_CREATED:
			resultVolumes = append(resultVolumes, vol)
			continue
		case ateapipb.ExternalVolume_STATUS_DELETING:
			return resultVolumes, apierror.FailedPrecondition("cannot create volume %q in DELETING status", volName)
		default:
			return resultVolumes, apierror.Internal("unexpected status %s for volume %q", vol.GetStatus(), volName)
		}

		actVolID := actorVolumeID(actorUID, volName)

		scName := specVol.GetExternalVolumeTemplate().GetStorageClassName()
		sc, err := scLister.Get(scName)
		if err != nil {
			return resultVolumes, apierror.Internal("failed to get StorageClass %q: %v", scName, err)
		}

		if sc.Provisioner != vol.GetVolumeType() {
			return resultVolumes, apierror.FailedPrecondition("volume %q has mismatched type %q (expected %q from StorageClass %q)", volName, vol.GetVolumeType(), sc.Provisioner, scName)
		}

		plugin, err := registry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			return resultVolumes, apierror.FailedPrecondition("failed to get volume plugin for driver %q (StorageClass %q): %v", sc.Provisioner, scName, err)
		}

		resp, volErr := plugin.CreateVolume(ctx, volume.CreateVolumeRequest{
			Name:       actVolID,
			Capacity:   specVol.GetExternalVolumeTemplate().GetCapacity(),
			Parameters: sc.Parameters,
		})
		if volErr != nil {
			return resultVolumes, apierror.Internal("failed to create volume %q: %v", specVol.GetName(), volErr)
		}

		resultVolumes = append(resultVolumes, &ateapipb.ExternalVolume{
			VolumeName:      volName,
			StorageVolumeId: resp.VolumeID,
			VolumeType:      sc.Provisioner,
			Status:          ateapipb.ExternalVolume_STATUS_CREATED,
			VolumeContext:   resp.VolumeContext,
		})
	}
	return resultVolumes, nil
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

// detachActorVolumes unpublishes each of the actor's external volumes from the
// node its attached_node records. The record, not the worker assignment, names
// the node: a crash clears the assignment but keeps the record, and the worker
// may already be gone. A volume with no recorded node, or with no storage
// volume ID (never provisioned, so never published), is skipped. NotFound
// counts as already detached. Every volume is attempted and the errors are
// joined.
func detachActorVolumes(ctx context.Context, registry VolumePluginRegistry, actor *ateapipb.Actor, action string) error {
	var errs []error
	for _, vol := range actor.GetStatus().GetActorVolumes() {
		node := vol.GetAttachedNode()
		if node == "" {
			continue
		}
		if vol.GetStorageVolumeId() == "" {
			slog.WarnContext(ctx, "Volume has no storage volume ID, skipping detach", slog.String("volume_name", vol.GetVolumeName()), slog.String("node", node), slog.String("action", action))
			continue
		}
		slog.InfoContext(ctx, "Detaching volume from node", slog.String("volume_id", vol.GetStorageVolumeId()), slog.String("node", node), slog.String("action", action))
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

// clearVolumeAttachments clears every volume's attached_node. Call it only in
// a write that follows a successful detachActorVolumes, so that a failed
// detach leaves the record for the next attempt.
func clearVolumeAttachments(actorStatus *ateapipb.ActorStatus) {
	for _, vol := range actorStatus.GetActorVolumes() {
		vol.AttachedNode = ""
	}
}
