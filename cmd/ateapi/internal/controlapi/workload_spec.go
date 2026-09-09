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
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/resource"
)

// toAteletResources resolves a container's declared limits into the scalars
// atelet and the ateoms consume; everything downstream compares numbers.
// Returns nil when the container declares no limits, so the OCI spec stays
// untouched for templates that do not use them.
func toAteletResources(r *ateapipb.Resources) (*ateletpb.ResourceLimits, error) {
	out := &ateletpb.ResourceLimits{}
	for _, limit := range r.GetLimits() {
		q, err := resource.ParseQuantity(limit.GetQuantity())
		if err != nil {
			return nil, fmt.Errorf("invalid container resource limit %s=%q: %w", limit.GetName(), limit.GetQuantity(), err)
		}
		switch limit.GetName() {
		case "cpu":
			out.CpuMillis = q.MilliValue()
		case "memory":
			out.MemoryBytes = q.Value()
		}
	}
	if out.MemoryBytes == 0 && out.CpuMillis == 0 {
		return nil, nil
	}
	return out, nil
}

// workloadSpecFromActorTemplate builds a WorkloadSpec from the template;
// container env is copied verbatim.
func workloadSpecFromActorTemplate(actorTemplate *ateapipb.ActorTemplate, actor *ateapipb.Actor) (*ateletpb.WorkloadSpec, error) {
	workloadSpec := &ateletpb.WorkloadSpec{}

	// Convert volumes to atelet's representation.  ActorTemplate validation has
	// already ensured that only one source is set.
	for _, vol := range actorTemplate.GetVolumes() {
		switch {
		case vol.GetDurableDir() != nil:
			workloadSpec.Volumes = append(workloadSpec.Volumes, &ateletpb.Volume{
				Name: vol.GetName(),
				Source: &ateletpb.Volume_DurableDir{
					DurableDir: &ateletpb.DurableDirVolume{},
				},
			})

		case vol.GetSystemInfo() != nil:
			ateletSystemInfo := &ateletpb.SystemInfoVolume{}
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
						DataSource: &ateletpb.SystemInfoDataSource_ActorMetadata{
							ActorMetadata: actorMetadata,
						},
					})
				case dataSource.GetTrustBundle() != nil:
					// atelet resolves named trustBundles against its allowlist
					// and ClusterTrustBundle informer at write time
					ateletSystemInfo.DataSources = append(ateletSystemInfo.DataSources, &ateletpb.SystemInfoDataSource{
						DataSource: &ateletpb.SystemInfoDataSource_TrustBundle{
							TrustBundle: &ateletpb.TrustBundleDataSource{
								Name: dataSource.GetTrustBundle().GetName(),
								Path: dataSource.GetTrustBundle().GetPath(),
							},
						},
					})
				default:
					continue // Drop unrecognized data sources
				}
			}
			workloadSpec.Volumes = append(workloadSpec.Volumes, &ateletpb.Volume{
				Name: vol.GetName(),
				Source: &ateletpb.Volume_SystemInfo{
					SystemInfo: ateletSystemInfo,
				},
			})

		case vol.GetImage() != nil:
			workloadSpec.Volumes = append(workloadSpec.Volumes, &ateletpb.Volume{
				Name: vol.GetName(),
				Source: &ateletpb.Volume_Image{
					Image: &ateletpb.ImageVolumeSource{
						Reference: vol.GetImage().GetReference(),
					},
				},
			})

		default:
			continue // Drop unrecognized volumes.
		}
	}

	// TODO: order may be important for nested mounts. Also need to think about
	// nested mount support in general.
	if err := appendExternalVolumes(workloadSpec, actorTemplate, actor); err != nil {
		return nil, err
	}

	for _, ctr := range actorTemplate.GetContainers() {
		ctrResources, err := toAteletResources(ctr.GetResources())
		if err != nil {
			return nil, err
		}
		ateletCtr := &ateletpb.Container{
			Name:            ctr.GetName(),
			Image:           ctr.GetImage(),
			Command:         ctr.GetCommand(),
			Args:            ctr.GetArgs(),
			Readyz:          toAteletReadyz(ctr.GetReadyz()),
			SecurityContext: toAteletSecurityContext(ctr.GetSecurityContext()),
			Resources:       ctrResources,
		}
		for _, env := range ctr.GetEnv() {
			ateletCtr.Env = append(ateletCtr.Env, &ateletpb.EnvEntry{
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

// appendExternalVolumes maps template external volumes to resolved actor volumes and appends them to workloadSpec
// if they are referenced in container volumeMounts.
func appendExternalVolumes(workloadSpec *ateletpb.WorkloadSpec, template *ateapipb.ActorTemplate, actor *ateapipb.Actor) error {
	if template == nil {
		return nil
	}
	for _, vol := range template.GetVolumes() {
		if vol.GetExternalVolumeTemplate() != nil {
			if !isVolumeMounted(vol.GetName(), template) {
				continue
			}
			if actor == nil {
				return fmt.Errorf("actor is required when externalVolumeTemplate is present")
			}

			var storageVolID string
			var volType string
			var volCtx map[string]string
			for _, dbVol := range actor.GetStatus().GetActorVolumes() {
				if dbVol.GetVolumeName() == vol.GetName() {
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

func isVolumeMounted(volumeName string, template *ateapipb.ActorTemplate) bool {
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
func toAteletActorMetadataField(in ateapipb.ActorMetadataField) ateletpb.ActorMetadataField {
	switch in {
	case ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME:
		return ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME
	case ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE:
		return ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE
	case ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_UID:
		return ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_UID
	default:
		return ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_UNSPECIFIED
	}
}

// toAteletReadyz projects the template readyz field onto the ateletpb wire
// type. Returns nil when the source is nil so containers without a probe
// stay unchanged on the wire.
func toAteletReadyz(in *ateapipb.ContainerReadyz) *ateletpb.Readyz {
	if in == nil {
		return nil
	}
	out := &ateletpb.Readyz{TimeoutSeconds: in.GetTimeoutSeconds()}
	if in.GetHttpGet() != nil {
		out.HttpGet = &ateletpb.HTTPGetAction{
			Path: in.GetHttpGet().GetPath(),
			Port: in.GetHttpGet().GetPort(),
		}
	}
	return out
}

// toAteletSecurityContext projects the template securityContext onto the
// ateletpb wire type. Returns nil when the source is nil or carries nothing,
// so containers that set no security settings stay unchanged on the wire.
func toAteletSecurityContext(in *ateapipb.SecurityContext) *ateletpb.SecurityContext {
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
