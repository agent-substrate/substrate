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
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	globalVolumePlugin volume.VolumePluginControlPlane = volume.NewMockVolumePlugin()
)

// TODO: Replace with actual volume plugin search
func getVolumePlugin() volume.VolumePluginControlPlane {
	return globalVolumePlugin
}

// initialActorVolumes constructs initial volume objects in PENDING state before volume creation.
func initialActorVolumes(template *atev1alpha1.ActorTemplate) []*ateapipb.ExternalVolume {
	var volumes []*ateapipb.ExternalVolume
	for _, vol := range template.Spec.Volumes {
		if vol.ExternalVolumeTemplate != nil {
			volumes = append(volumes, &ateapipb.ExternalVolume{
				VolumeName: vol.Name,
				VolumeType: "mock", // TODO fix when we support multiple plugins
				Status:     ateapipb.ExternalVolume_STATUS_PENDING,
			})
		}
	}
	return volumes
}

// createActorVolumes provisions external volumes specified in volumesToCreate using the provided volume plugin.
// It returns the list of external volumes (with updated status and storage IDs), or an error if any creation fails.
// Any volumes processed before or during a failure are returned alongside the error so they can be persisted on the actor.
func createActorVolumes(ctx context.Context, actorUID string, template *atev1alpha1.ActorTemplate, volumesToCreate []*ateapipb.ExternalVolume) (resultVolumes []*ateapipb.ExternalVolume, err error) {
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

		var specVol *atev1alpha1.Volume
		volName := vol.GetVolumeName()
		for i := range template.Spec.Volumes {
			if template.Spec.Volumes[i].Name == volName {
				specVol = &template.Spec.Volumes[i]
				break
			}
		}
		if specVol == nil || specVol.ExternalVolumeTemplate == nil {
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

		plugin := getVolumePlugin()
		if plugin == nil {
			return resultVolumes, status.Errorf(codes.Internal, "volume plugin is not configured for creating volume %q", volName)
		}
		storageVolumeID, volErr := plugin.CreateVolume(ctx, actVolID, specVol.ExternalVolumeTemplate.Capacity.String(), specVol.ExternalVolumeTemplate.StorageClassName)
		if volErr != nil {
			return resultVolumes, status.Errorf(codes.Internal, "failed to create volume %q: %v", specVol.Name, volErr)
		}

		resultVolumes = append(resultVolumes, &ateapipb.ExternalVolume{
			VolumeName:      volName,
			StorageVolumeId: storageVolumeID,
			VolumeType:      vol.GetVolumeType(),
			Status:          ateapipb.ExternalVolume_STATUS_CREATED,
		})
	}
	return resultVolumes, nil
}

// deleteActorVolumes deletes all external volumes in the list.
func deleteActorVolumes(ctx context.Context, actorUID string, volumes []*ateapipb.ExternalVolume) error {
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
		if err := getVolumePlugin().DeleteVolume(ctx, volID); err != nil {
			errs = append(errs, fmt.Errorf("failed to delete volume %q: %w", volID, err))
		}
	}
	return errors.Join(errs...)
}

// getMountedActorVolumes filters the actor's volumes and returns only those that are declared and mounted in the ActorTemplate.
func getMountedActorVolumes(ctx context.Context, ref *ateapipb.ObjectRef, volumes []*ateapipb.ExternalVolume, template *atev1alpha1.ActorTemplate) []*ateapipb.ExternalVolume {
	var mounted []*ateapipb.ExternalVolume
	for _, vol := range volumes {
		// Find the corresponding volume in the ActorTemplate to check if it's mounted
		var matchedTemplateVol *atev1alpha1.Volume
		for _, tVol := range template.Spec.Volumes {
			if vol.GetVolumeName() == tVol.Name {
				matchedTemplateVol = &tVol
				break
			}
		}

		if matchedTemplateVol == nil {
			slog.WarnContext(ctx, "Volume not found in template, skipping", slog.String("volume_id", vol.GetStorageVolumeId()))
			continue
		}

		if !isVolumeMounted(matchedTemplateVol.Name, template) {
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

// detachActorVolumes detaches all mounted external volumes for an actor from its worker node.
func detachActorVolumes(ctx context.Context, st store.Interface, actor *ateapipb.Actor, template *atev1alpha1.ActorTemplate, action string) error {
	assignment := actor.GetWorkerAssignment()
	if assignment == nil {
		slog.WarnContext(ctx, fmt.Sprintf("Actor has no assigned worker pod during %s, skipping detach volumes", action), slog.String("actor_id", actor.GetMetadata().GetName()))
		return nil
	}

	worker, err := st.GetWorker(ctx, assignment.GetWorkerNamespace(), assignment.GetWorkerPool(), assignment.GetWorkerPod())
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
	for _, vol := range getMountedActorVolumes(ctx, ref, actor.GetActorVolumes(), template) {
		slog.InfoContext(ctx, "Detaching volume from node", slog.String("volume_id", vol.GetStorageVolumeId()), slog.String("node", node))
		err := getVolumePlugin().DetachVolume(ctx, vol.GetStorageVolumeId(), node)
		if err != nil {
			return fmt.Errorf("failed to detach volume %q from node %q: %w", vol.GetStorageVolumeId(), node, err)
		}
	}
	return nil
}
