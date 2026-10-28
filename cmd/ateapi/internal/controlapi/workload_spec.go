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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// toAteletResources copies declared limits into atelet's form. Returns nil
// when no limits are declared, so the OCI spec stays untouched for templates
// that do not use them.
func toAteletResources(r *ateapi.ResourceRequirements) *ateletpb.ResourceRequirements {
	l := r.GetLimits()
	if l.GetMemoryBytes() == 0 && l.GetCpuMillicores() == 0 {
		return nil
	}
	return &ateletpb.ResourceRequirements{Limits: &ateletpb.Resources{CpuMillicores: l.GetCpuMillicores(), MemoryBytes: l.GetMemoryBytes()}}
}

// workloadSpecFromActorTemplate builds a WorkloadSpec from the template;
// container env is copied verbatim.
func workloadSpecFromActorTemplate(actorTemplate *ateapi.ActorTemplate, actor *ateapi.Actor, volumePublishContexts map[string]map[string]string) (*ateletpb.WorkloadSpec, error) {
	workloadSpec := &ateletpb.WorkloadSpec{}

	// Convert volumes to atelet's representation.  ActorTemplate validation has
	// already ensured that only one source is set.
	for _, vol := range actorTemplate.GetVolumes() {
		switch {
		case vol.GetDurableDir() != nil:
			workloadSpec.Volumes = append(workloadSpec.Volumes, &ateletpb.Volume{
				Name:       vol.GetName(),
				DurableDir: &ateletpb.DurableDirVolumeSource{},
			})

		case vol.GetSystemInfo() != nil:
			ateletSystemInfo := &ateletpb.SystemInfoVolumeSource{}
			for _, dataSource := range vol.GetSystemInfo().GetDataSources() {
				switch {
				case dataSource.GetActorMetadata() != nil:
					actorMetadata := &ateletpb.ActorMetadataDataSource{}
					for _, item := range dataSource.GetActorMetadata().GetItems() {
						actorMetadata.Items = append(actorMetadata.Items, &ateletpb.ActorMetadataItem{
							Field: toAteletActorMetadataField(item.GetField()),
							Path:  item.GetPath(),
						})
					}
					ateletSystemInfo.DataSources = append(ateletSystemInfo.DataSources, &ateletpb.SystemInfoDataSource{
						ActorMetadata: actorMetadata,
					})
				case dataSource.GetTrustBundle() != nil:
					// atelet resolves named trustBundles against its allowlist
					// and ClusterTrustBundle informer at write time
					ateletSystemInfo.DataSources = append(ateletSystemInfo.DataSources, &ateletpb.SystemInfoDataSource{
						TrustBundle: &ateletpb.TrustBundleDataSource{
							Names: dataSource.GetTrustBundle().GetNames(),
							Path:  dataSource.GetTrustBundle().GetPath(),
						},
					})
				default:
					continue // Drop unrecognized data sources
				}
			}
			workloadSpec.Volumes = append(workloadSpec.Volumes, &ateletpb.Volume{
				Name:       vol.GetName(),
				SystemInfo: ateletSystemInfo,
			})

		case vol.GetImage() != nil:
			workloadSpec.Volumes = append(workloadSpec.Volumes, &ateletpb.Volume{
				Name: vol.GetName(),
				Image: &ateletpb.ImageVolumeSource{
					Reference: vol.GetImage().GetReference(),
				},
			})

		default:
			continue // Drop unrecognized volumes.
		}
	}

	// TODO: order may be important for nested mounts. Also need to think about
	// nested mount support in general.
	// This function is called from several places, so we do NOT check the
	// ExternalVolumes gate here.  Callers must check it, if appropriate.
	if err := appendExternalVolumes(workloadSpec, actorTemplate, actor, volumePublishContexts); err != nil {
		return nil, err
	}

	for _, ctr := range actorTemplate.GetContainers() {
		ctrResources := toAteletResources(ctr.GetResources())
		ateletCtr := &ateletpb.Container{
			Name:            ctr.GetName(),
			Image:           ctr.GetImage(),
			Command:         ctr.GetCommand(),
			Args:            ctr.GetArgs(),
			WakeupProbe:     toAteletWakeupProbe(ctr.GetWakeupProbe()),
			SecurityContext: toAteletSecurityContext(ctr.GetSecurityContext()),
			Resources:       ctrResources,
		}
		for _, env := range ctr.GetEnv() {
			ateletCtr.Env = append(ateletCtr.Env, &ateletpb.EnvVar{
				Name:  env.GetName(),
				Value: env.GetValue(),
			})
		}
		for _, mount := range ctr.GetVolumeMounts() {
			ateletCtr.VolumeMounts = append(ateletCtr.VolumeMounts, &ateletpb.VolumeMount{
				Name:      mount.GetName(),
				MountPath: mount.GetMountPath(),
			})
		}
		workloadSpec.Containers = append(workloadSpec.Containers, ateletCtr)
	}

	return workloadSpec, nil
}

// appendExternalVolumes maps template external volumes to resolved actor volumes
// and appends them to workloadSpec if they are referenced in container
// volumeMounts. volumePublishContexts carries the attachment metadata of the current
// resume by volume name; it is nil on paths that do not mount.
func appendExternalVolumes(workloadSpec *ateletpb.WorkloadSpec, template *ateapi.ActorTemplate, actor *ateapi.Actor, volumePublishContexts map[string]map[string]string) error {
	if template == nil {
		return nil
	}
	for _, vol := range template.GetVolumes() {
		if vol.GetExternal().GetTemplate() != nil {
			if !isVolumeMounted(vol.GetName(), template) {
				continue
			}
			if actor == nil {
				return fmt.Errorf("actor is required when an external volume is present")
			}

			var storageVolID string
			var volType string
			var volCtx map[string]string
			for _, dbVol := range actor.GetStatus().GetExternalVolumes().GetVolumes() {
				if dbVol.GetName() == vol.GetName() {
					storageVolID = dbVol.GetStorageVolumeId()
					volType = dbVol.GetVolumeType()
					volCtx = dbVol.GetVolumeContext()
					break
				}
			}
			if storageVolID == "" {
				return fmt.Errorf("volume %s not found for actor %s", vol.GetName(), actor.GetMetadata().GetName())
			}
			workloadSpec.Volumes = append(workloadSpec.Volumes, &ateletpb.Volume{
				Name: vol.GetName(),
				External: &ateletpb.ExternalVolumeSource{
					StorageVolumeId: storageVolID,
					VolumeType:      volType,
					VolumeContext:   volCtx,
					PublishContext:  volumePublishContexts[vol.GetName()],
				},
			})
		}
	}
	return nil
}

func isVolumeMounted(volumeName string, template *ateapi.ActorTemplate) bool {
	for _, ctr := range template.GetContainers() {
		for _, mount := range ctr.GetVolumeMounts() {
			if mount.GetName() == volumeName {
				return true
			}
		}
	}
	return false
}

// toAteletActorMetadataField projects the template field selector onto the
// atelet wire enum. Unknown values map to UNSPECIFIED, which atelet skips;
// template validation makes that unreachable for stored templates.
func toAteletActorMetadataField(in ateapi.ActorMetadataField) ateletpb.ActorMetadataField {
	switch in {
	case ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_NAME:
		return ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME
	case ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE:
		return ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE
	case ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_UID:
		return ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_UID
	default:
		return ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_UNSPECIFIED
	}
}

// toAteletWakeupProbe projects the template wakeup probe field onto the ateletpb wire
// type. Returns nil when the source is nil so containers without a probe
// stay unchanged on the wire.
func toAteletWakeupProbe(in *ateapi.WakeupProbe) *ateletpb.WakeupProbe {
	if in == nil {
		return nil
	}
	out := &ateletpb.WakeupProbe{TimeoutSeconds: in.GetTimeoutSeconds()}
	if in.GetHttpGet() != nil {
		out.HttpGet = &ateletpb.HttpGetAction{
			Path: in.GetHttpGet().GetPath(),
			Port: in.GetHttpGet().GetPort(),
		}
	}
	return out
}

// toAteletSecurityContext projects the template securityContext onto the
// ateletpb wire type. Returns nil when the source is nil or carries nothing,
// so containers that set no security settings stay unchanged on the wire.
func toAteletSecurityContext(in *ateapi.SecurityContext) *ateletpb.SecurityContext {
	caps := in.GetCapabilities()
	if caps == nil || (len(caps.GetAdd()) == 0 && len(caps.GetDrop()) == 0) {
		return nil
	}
	return &ateletpb.SecurityContext{
		Capabilities: &ateletpb.Capabilities{
			Add:  caps.GetAdd(),
			Drop: caps.GetDrop(),
		},
	}
}
