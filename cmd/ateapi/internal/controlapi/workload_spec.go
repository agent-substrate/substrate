//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package controlapi

import (
	"fmt"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// workloadSpecFromActorTemplate builds a WorkloadSpec from the template;
// container env is copied verbatim.
func workloadSpecFromActorTemplate(actorTemplate *atev1alpha1.ActorTemplate, actor *ateapipb.Actor) (*ateletpb.WorkloadSpec, error) {
	workloadSpec := &ateletpb.WorkloadSpec{}

	// add volumes
	for _, vol := range actorTemplate.Spec.Volumes {
		// volume is durable-dir type
		if vol.VolumeSource.DurableDir != nil {
			workloadSpec.Volumes = append(workloadSpec.Volumes, &ateletpb.Volume{
				Name: vol.Name,
				Type: ateletpb.VolumeType_VOLUME_TYPE_DURABLE_DIR,
				Source: &ateletpb.Volume_DurableDir{
					DurableDir: &ateletpb.DurableDirVolume{},
				},
			})
		}
	}

	// TODO: order may be important for nested mounts. Also need to think about
	// nested mount support in general.
	if err := appendExternalVolumes(workloadSpec, actorTemplate, actor); err != nil {
		return nil, err
	}

	for _, ctr := range actorTemplate.Spec.Containers {
		ateletCtr := &ateletpb.Container{
			Name:            ctr.Name,
			Image:           ctr.Image,
			Command:         ctr.Command,
			Args:            ctr.Args,
			Readyz:          toAteletReadyz(ctr.Readyz),
			SecurityContext: toAteletSecurityContext(ctr.SecurityContext),
		}
		for _, env := range ctr.Env {
			ateletCtr.Env = append(ateletCtr.Env, &ateletpb.EnvEntry{
				Name:  env.Name,
				Value: env.Value,
			})
		}
		for _, mount := range ctr.VolumeMounts {
			ateletCtr.VolumeMounts = append(ateletCtr.VolumeMounts, &ateletpb.VolumeMount{
				Name:      mount.Name,
				MountPath: mount.MountPath,
			})
		}
		workloadSpec.Containers = append(workloadSpec.Containers, ateletCtr)
	}

	return workloadSpec, nil
}

// appendExternalVolumes maps template external volumes to resolved actor volumes and appends them to workloadSpec
// if they are referenced in container volumeMounts.
func appendExternalVolumes(workloadSpec *ateletpb.WorkloadSpec, template *atev1alpha1.ActorTemplate, actor *ateapipb.Actor) error {
	if template == nil {
		return nil
	}
	for _, vol := range template.Spec.Volumes {
		if vol.ExternalVolumeTemplate != nil {
			if !isVolumeMounted(vol.Name, template) {
				continue
			}
			if actor == nil {
				return fmt.Errorf("actor is required when externalVolumeTemplate is present")
			}

			var storageVolID string
			var volType string
			var volCtx map[string]string
			for _, dbVol := range actor.GetStatus().GetActorVolumes() {
				if dbVol.GetVolumeName() == vol.Name {
					storageVolID = dbVol.GetStorageVolumeId()
					volType = dbVol.GetVolumeType()
					volCtx = dbVol.GetVolumeContext()
					break
				}
			}
			if storageVolID == "" {
				return fmt.Errorf("volume %s not found for actor %s", vol.Name, actor.GetMetadata().GetName())
			}
			workloadSpec.Volumes = append(workloadSpec.Volumes, &ateletpb.Volume{
				Name: vol.Name,
				Type: ateletpb.VolumeType_VOLUME_TYPE_EXTERNAL,
				Source: &ateletpb.Volume_External{
					External: &ateletpb.ExternalVolumeSource{
						StorageVolumeId: storageVolID,
						VolumeType:      volType,
						VolumeContext:   volCtx,
					},
				},
			})
		}
	}
	return nil
}

func isVolumeMounted(volumeName string, template *atev1alpha1.ActorTemplate) bool {
	for _, ctr := range template.Spec.Containers {
		for _, mount := range ctr.VolumeMounts {
			if mount.Name == volumeName {
				return true
			}
		}
	}
	return false
}

// toAteletReadyz projects the CRD readyz field onto the ateletpb wire type.
// Returns nil when the source is nil so containers without a probe stay
// unchanged on the wire.
func toAteletReadyz(in *atev1alpha1.ContainerReadyz) *ateletpb.Readyz {
	if in == nil {
		return nil
	}
	out := &ateletpb.Readyz{TimeoutSeconds: in.TimeoutSeconds}
	if in.HTTPGet != nil {
		out.HttpGet = &ateletpb.HTTPGetAction{
			Path: in.HTTPGet.Path,
			Port: in.HTTPGet.Port,
		}
	}
	return out
}

// toAteletSecurityContext projects the CRD securityContext onto the ateletpb
// wire type. Returns nil when the source is nil or carries nothing, so
// containers that set no security settings stay unchanged on the wire.
func toAteletSecurityContext(in *atev1alpha1.SecurityContext) *ateletpb.SecurityContext {
	if in == nil || in.Capabilities == nil {
		return nil
	}
	caps := in.Capabilities
	if len(caps.Add) == 0 && len(caps.Drop) == 0 {
		return nil
	}
	return &ateletpb.SecurityContext{
		Capabilities: &ateletpb.Capabilities{
			Add:  capabilityNames(caps.Add),
			Drop: capabilityNames(caps.Drop),
		},
	}
}

func capabilityNames(in []atev1alpha1.Capability) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, c := range in {
		out = append(out, string(c))
	}
	return out
}
