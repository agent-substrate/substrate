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
	globalVolumePlugin = volume.NewMockVolumePlugin()
)

// TODO: Replace with actual volume plugin search
func getVolumePlugin() volume.VolumePluginControlPlane {
	return globalVolumePlugin
}

// TODO: we should persist creation first so that we can handle background cleanup.
// this probably requires us to add a PROVISIONING actor state.

// createActorVolumes provisions external volumes specified in the actor template.
// It returns the list of created external volumes, or an error if any creation fails.
// If any volume creation fails, it cleans up any volumes created in this call on a best-effort basis.
func (s *Service) createActorVolumes(ctx context.Context, ref *ateapipb.ObjectRef, template *atev1alpha1.ActorTemplate) ([]*ateapipb.ExternalVolume, error) {
	var volumes []*ateapipb.ExternalVolume
	for _, vol := range template.Spec.Volumes {
		if vol.ExternalVolumeTemplate != nil {
			// Use a unique name for the volume to ensure idempotency
			uniqueVolName := actorVolumeID(ref, vol.Name)
			storageVolumeID, err := getVolumePlugin().CreateVolume(ctx, uniqueVolName, vol.ExternalVolumeTemplate.Capacity.String(), vol.ExternalVolumeTemplate.StorageClassName)
			if err != nil {
				// TODO: need better system - best effort cleanup of already created volumes
				s.deleteActorVolumes(ctx, ref, volumes)
				return nil, status.Errorf(codes.Internal, "failed to create volume %q: %v", vol.Name, err)
			}
			volumes = append(volumes, &ateapipb.ExternalVolume{
				ActorVolumeId:   uniqueVolName,
				StorageVolumeId: storageVolumeID,
				VolumeType:      "mock", // TODO fix when we support multiple plugins
				Status:          ateapipb.ExternalVolume_CREATED,
			})
		}
	}
	return volumes, nil
}

// deleteActorVolumes deletes all external volumes in the list on a best-effort basis.
func (s *Service) deleteActorVolumes(ctx context.Context, ref *ateapipb.ObjectRef, volumes []*ateapipb.ExternalVolume) {
	for _, vol := range volumes {
		if err := getVolumePlugin().DeleteVolume(ctx, vol.GetStorageVolumeId()); err != nil {
			slog.ErrorContext(ctx, "failed to delete volume",
				slog.String("atespace", ref.GetAtespace()),
				slog.String("actor_id", ref.GetName()),
				slog.String("volume_id", vol.GetStorageVolumeId()),
				slog.Any("error", err))
		}
	}
}

// getMountedActorVolumes filters the actor's volumes and returns only those that are declared and mounted in the ActorTemplate.
func getMountedActorVolumes(ctx context.Context, ref *ateapipb.ObjectRef, volumes []*ateapipb.ExternalVolume, template *atev1alpha1.ActorTemplate) []*ateapipb.ExternalVolume {
	var mounted []*ateapipb.ExternalVolume
	for _, vol := range volumes {
		// Find the corresponding volume in the ActorTemplate to check if it's mounted
		var matchedTemplateVol *atev1alpha1.Volume
		for _, tVol := range template.Spec.Volumes {
			expectedID := actorVolumeID(ref, tVol.Name)
			if vol.GetActorVolumeId() == expectedID {
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

func actorVolumeID(ref *ateapipb.ObjectRef, volumeName string) string {
	// TODO consider if this should be actor UUID
	return fmt.Sprintf("%s-%s-%s", ref.GetAtespace(), ref.GetName(), volumeName)
}

// detachActorVolumes detaches all mounted external volumes for an actor from its worker node.
func detachActorVolumes(ctx context.Context, st store.Interface, actor *ateapipb.Actor, template *atev1alpha1.ActorTemplate, action string) error {
	if actor.GetAteomPodNamespace() == "" {
		slog.WarnContext(ctx, fmt.Sprintf("Actor has no assigned worker pod during %s, skipping detach volumes", action), slog.String("actor_id", actor.GetMetadata().GetName()))
		return nil
	}

	worker, err := st.GetWorker(ctx, actor.GetAteomPodNamespace(), actor.GetWorkerPoolName(), actor.GetAteomPodName())
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
