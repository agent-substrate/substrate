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
	"fmt"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// validTemplateVersionSpec returns a fully-populated spec that passes
// validation, optionally mutated. It is already in defaulted form.
func validTemplateVersionSpec(mutations ...func(*ateapipb.ActorTemplateVersionSpec)) *ateapipb.ActorTemplateVersionSpec {
	spec := &ateapipb.ActorTemplateVersionSpec{
		PauseImage: "pause@sha256:abc",
		Containers: []*ateapipb.Container{{
			Name:  "main",
			Image: "app@sha256:def",
			Env:   []*ateapipb.EnvVar{{Name: "MODE", Source: &ateapipb.EnvVar_Value{Value: "prod"}}},
			Readyz: &ateapipb.ContainerReadyz{
				HttpGet:        &ateapipb.HTTPGetAction{Path: "/readyz", Port: 8080},
				TimeoutSeconds: 30,
			},
			VolumeMounts: []*ateapipb.VolumeMount{{Name: "data", MountPath: "/data"}},
		}},
		Volumes: []*ateapipb.Volume{{
			Name:   "data",
			Source: &ateapipb.Volume_DurableDir{DurableDir: &ateapipb.DurableDirVolumeSource{}},
		}},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{
			StorageLocation: "gs://bucket/snapshots",
		},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
			ConfigName:   "gvisor-default",
		},
	}
	for _, m := range mutations {
		m(spec)
	}
	return spec
}

func TestValidateActorTemplateVersionSpec(t *testing.T) {
	specPath := field.NewPath("spec")
	tests := []struct {
		name string
		spec *ateapipb.ActorTemplateVersionSpec
		want field.ErrorList
	}{{
		"valid full spec",
		validTemplateVersionSpec(),
		nil,
	}, {
		"valid minimal spec",
		&ateapipb.ActorTemplateVersionSpec{
			PauseImage:      "pause@sha256:abc",
			Containers:      []*ateapipb.Container{{Name: "main", Image: "app@sha256:def"}},
			SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://bucket/snapshots"},
			SandboxConfig: &ateapipb.SandboxConfig{
				SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
				ConfigName:   "gvisor-default",
			},
		},
		nil,
	}, {
		"nil spec",
		nil,
		field.ErrorList{field.Required(specPath, "")},
	}, {
		"missing pause_image",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.PauseImage = "" }),
		field.ErrorList{field.Required(specPath.Child("pause_image"), "")},
	}, {
		"unpinned pause_image",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.PauseImage = "pause:latest" }),
		field.ErrorList{field.Invalid(specPath.Child("pause_image"), "pause:latest", "")},
	}, {
		"missing container name",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].Name = "" }),
		field.ErrorList{field.Required(specPath.Child("containers").Index(0).Child("name"), "")},
	}, {
		"invalid container name",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].Name = "MAIN" }),
		field.ErrorList{field.Invalid(specPath.Child("containers").Index(0).Child("name"), "MAIN", "")},
	}, {
		"reserved container name",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].Name = "pause" }),
		field.ErrorList{field.Invalid(specPath.Child("containers").Index(0).Child("name"), "pause", "")},
	}, {
		"duplicate container names",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Containers = append(s.Containers, &ateapipb.Container{Name: "main", Image: "app@sha256:def"})
		}),
		field.ErrorList{field.Duplicate(specPath.Child("containers").Index(1).Child("name"), "main")},
	}, {
		"too many containers",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Containers = nil
			for i := 0; i < maxContainers+1; i++ {
				s.Containers = append(s.Containers, &ateapipb.Container{
					Name: fmt.Sprintf("c%d", i), Image: "app@sha256:def",
				})
			}
			s.Volumes = nil
		}),
		field.ErrorList{field.TooMany(specPath.Child("containers"), maxContainers+1, maxContainers)},
	}, {
		"no containers",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Containers = nil
			s.Volumes = nil
		}),
		field.ErrorList{field.Required(specPath.Child("containers"), "")},
	}, {
		"missing container image",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].Image = "" }),
		field.ErrorList{field.Required(specPath.Child("containers").Index(0).Child("image"), "")},
	}, {
		"unpinned container image",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].Image = "app:v1" }),
		field.ErrorList{field.Invalid(specPath.Child("containers").Index(0).Child("image"), "app:v1", "")},
	}, {
		"too many command items",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Containers[0].Command = make([]string, maxCommandItems+1)
		}),
		field.ErrorList{field.TooMany(specPath.Child("containers").Index(0).Child("command"), maxCommandItems+1, maxCommandItems)},
	}, {
		"missing env name",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].Env[0].Name = "" }),
		field.ErrorList{field.Required(specPath.Child("containers").Index(0).Child("env").Index(0).Child("name"), "")},
	}, {
		"env name with equals sign",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].Env[0].Name = "A=B" }),
		field.ErrorList{field.Invalid(specPath.Child("containers").Index(0).Child("env").Index(0).Child("name"), "A=B", "")},
	}, {
		"env without source",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].Env[0].Source = nil }),
		field.ErrorList{field.Required(specPath.Child("containers").Index(0).Child("env").Index(0).Child("value"), "")},
	}, {
		"readyz without http_get",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].Readyz.HttpGet = nil }),
		field.ErrorList{field.Required(specPath.Child("containers").Index(0).Child("readyz", "http_get"), "")},
	}, {
		"readyz port out of range",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].Readyz.HttpGet.Port = 0 }),
		field.ErrorList{field.Invalid(specPath.Child("containers").Index(0).Child("readyz", "http_get", "port"), int32(0), "")},
	}, {
		"readyz path with query string",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].Readyz.HttpGet.Path = "/readyz?x=1" }),
		field.ErrorList{field.Invalid(specPath.Child("containers").Index(0).Child("readyz", "http_get", "path"), "/readyz?x=1", "")},
	}, {
		"readyz timeout above maximum",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Containers[0].Readyz.TimeoutSeconds = maxReadyzTimeout + 1
		}),
		field.ErrorList{field.Invalid(specPath.Child("containers").Index(0).Child("readyz", "timeout_seconds"), int32(maxReadyzTimeout+1), "")},
	}, {
		"volume mount references unknown volume",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Containers[0].VolumeMounts = append(s.Containers[0].VolumeMounts, &ateapipb.VolumeMount{Name: "ghost", MountPath: "/ghost"})
		}),
		field.ErrorList{field.Invalid(specPath.Child("containers").Index(0).Child("volume_mounts").Index(1).Child("name"), "ghost", "")},
	}, {
		"duplicate mount path",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Containers[0].VolumeMounts = append(s.Containers[0].VolumeMounts, &ateapipb.VolumeMount{Name: "data", MountPath: "/data"})
		}),
		field.ErrorList{field.Invalid(specPath.Child("containers").Index(0).Child("volume_mounts").Index(1).Child("mount_path"), "/data", "")},
	}, {
		"missing mount path",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].VolumeMounts[0].MountPath = "" }),
		field.ErrorList{field.Required(specPath.Child("containers").Index(0).Child("volume_mounts").Index(0).Child("mount_path"), "")},
	}, {
		"relative mount path",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].VolumeMounts[0].MountPath = "data" }),
		field.ErrorList{field.Invalid(specPath.Child("containers").Index(0).Child("volume_mounts").Index(0).Child("mount_path"), "data", "")},
	}, {
		"mount path with dotdot segment",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].VolumeMounts[0].MountPath = "/data/../etc" }),
		field.ErrorList{field.Invalid(specPath.Child("containers").Index(0).Child("volume_mounts").Index(0).Child("mount_path"), "/data/../etc", "")},
	}, {
		"mount path with trailing slash",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].VolumeMounts[0].MountPath = "/data/" }),
		field.ErrorList{field.Invalid(specPath.Child("containers").Index(0).Child("volume_mounts").Index(0).Child("mount_path"), "/data/", "")},
	}, {
		"mount path with colon",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Containers[0].VolumeMounts[0].MountPath = "/da:ta" }),
		field.ErrorList{field.Invalid(specPath.Child("containers").Index(0).Child("volume_mounts").Index(0).Child("mount_path"), "/da:ta", "")},
	}, {
		"mount path too long",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Containers[0].VolumeMounts[0].MountPath = "/" + strings.Repeat("a", maxMountPathLen)
		}),
		field.ErrorList{field.TooLong(specPath.Child("containers").Index(0).Child("volume_mounts").Index(0).Child("mount_path"), "/"+strings.Repeat("a", maxMountPathLen), maxMountPathLen)},
	}, {
		"missing volume name",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Volumes[0].Name = "" }),
		field.ErrorList{
			field.Required(specPath.Child("volumes").Index(0).Child("name"), ""),
			// The mount referencing "data" no longer matches a declared volume.
			field.Invalid(specPath.Child("containers").Index(0).Child("volume_mounts").Index(0).Child("name"), "data", ""),
		},
	}, {
		"duplicate volume names",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Volumes = append(s.Volumes, &ateapipb.Volume{
				Name:   "data",
				Source: &ateapipb.Volume_DurableDir{DurableDir: &ateapipb.DurableDirVolumeSource{}},
			})
		}),
		field.ErrorList{field.Duplicate(specPath.Child("volumes").Index(1).Child("name"), "data")},
	}, {
		"volume without source",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.Volumes[0].Source = nil }),
		field.ErrorList{field.Required(specPath.Child("volumes").Index(0).Child("source"), "")},
	}, {
		"unmounted volume",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Volumes = append(s.Volumes, &ateapipb.Volume{
				Name:   "scratch",
				Source: &ateapipb.Volume_DurableDir{DurableDir: &ateapipb.DurableDirVolumeSource{}},
			})
		}),
		field.ErrorList{field.Invalid(specPath.Child("volumes").Index(1).Child("name"), "scratch", "")},
	}, {
		"external volume missing capacity",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Volumes[0].Source = &ateapipb.Volume_ExternalVolumeTemplate{
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{StorageClassName: "standard"},
			}
		}),
		field.ErrorList{field.Required(specPath.Child("volumes").Index(0).Child("external_volume_template", "capacity"), "")},
	}, {
		"external volume bad capacity",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Volumes[0].Source = &ateapipb.Volume_ExternalVolumeTemplate{
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{Capacity: "lots", StorageClassName: "standard"},
			}
		}),
		field.ErrorList{field.Invalid(specPath.Child("volumes").Index(0).Child("external_volume_template", "capacity"), "lots", "")},
	}, {
		"external volume missing storage class",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Volumes[0].Source = &ateapipb.Volume_ExternalVolumeTemplate{
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{Capacity: "10Gi"},
			}
		}),
		field.ErrorList{field.Required(specPath.Child("volumes").Index(0).Child("external_volume_template", "storage_class_name"), "")},
	}, {
		"external volume forbidden on microvm",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.SandboxConfig = &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM, ConfigName: "microvm"}
			s.Volumes[0].Source = &ateapipb.Volume_ExternalVolumeTemplate{
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{Capacity: "10Gi", StorageClassName: "standard"},
			}
		}),
		field.ErrorList{field.Forbidden(specPath.Child("volumes").Index(0).Child("external_volume_template"), "")},
	}, {
		"missing snapshots_config",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.SnapshotsConfig = nil }),
		field.ErrorList{field.Required(specPath.Child("snapshots_config"), "")},
	}, {
		"missing storage_location",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.SnapshotsConfig.StorageLocation = "" }),
		field.ErrorList{field.Required(specPath.Child("snapshots_config", "storage_location"), "")},
	}, {
		"storage_location without bucket",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) { s.SnapshotsConfig.StorageLocation = "/just/a/path" }),
		field.ErrorList{field.Invalid(specPath.Child("snapshots_config", "storage_location"), "/just/a/path", "")},
	}, {
		"on_commit FULL not a subset of on_pause DATA",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.SnapshotsConfig.OnPause = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
			s.SnapshotsConfig.OnCommit = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
		}),
		field.ErrorList{field.Invalid(specPath.Child("snapshots_config", "on_commit"), ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, "")},
	}, {
		"on_commit UNSPECIFIED means FULL, not a subset of on_pause DATA",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.SnapshotsConfig.OnPause = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
		}),
		field.ErrorList{field.Invalid(specPath.Child("snapshots_config", "on_commit"), ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED, "")},
	}, {
		"on_pause DATA with on_commit DATA is valid",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.SnapshotsConfig.OnPause = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
			s.SnapshotsConfig.OnCommit = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
		}),
		nil,
	}, {
		"undefined on_pause enum value",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.SnapshotsConfig.OnPause = ateapipb.SnapshotContentScope(99)
		}),
		field.ErrorList{field.NotSupported[string](specPath.Child("snapshots_config", "on_pause"), ateapipb.SnapshotContentScope(99), nil)},
	}, {
		"golden resume requires microvm",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.SnapshotsConfig.OnResume = &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN}
		}),
		field.ErrorList{field.Invalid(specPath.Child("snapshots_config", "on_resume", "from_data"), ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN, "")},
	}, {
		"golden resume valid on microvm",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.SandboxConfig = &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM, ConfigName: "microvm"}
			s.SnapshotsConfig.OnResume = &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN}
		}),
		nil,
	}, {
		"missing sandbox_config",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.SandboxConfig = nil
		}),
		field.ErrorList{field.Required(specPath.Child("sandbox_config"), "")},
	}, {
		"unspecified sandbox class",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.SandboxConfig.SandboxClass = ateapipb.SandboxClass_SANDBOX_CLASS_UNSPECIFIED
		}),
		field.ErrorList{field.Required(specPath.Child("sandbox_config", "sandbox_class"), "")},
	}, {
		"undefined sandbox class",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.SandboxConfig.SandboxClass = ateapipb.SandboxClass(99)
		}),
		field.ErrorList{field.NotSupported[string](specPath.Child("sandbox_config", "sandbox_class"), ateapipb.SandboxClass(99), nil)},
	}, {
		"missing sandbox config_name",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.SandboxConfig.ConfigName = ""
		}),
		field.ErrorList{field.Required(specPath.Child("sandbox_config", "config_name"), "")},
	}, {
		"invalid sandbox config_name",
		validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.SandboxConfig.ConfigName = "Bad_Name"
		}),
		field.ErrorList{field.Invalid(specPath.Child("sandbox_config", "config_name"), "Bad_Name", "")},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateActorTemplateVersionSpec(tt.spec, specPath)
			assertValidateErr(t, got, tt.want)
		})
	}
}

func TestDefaultActorTemplateVersionSpec(t *testing.T) {
	spec := &ateapipb.ActorTemplateVersionSpec{
		Containers: []*ateapipb.Container{
			{Name: "no-readyz"},
			{Name: "defaulted", Readyz: &ateapipb.ContainerReadyz{HttpGet: &ateapipb.HTTPGetAction{Port: 8080}}},
			{Name: "explicit", Readyz: &ateapipb.ContainerReadyz{
				HttpGet:        &ateapipb.HTTPGetAction{Path: "/healthz", Port: 9090},
				TimeoutSeconds: 120,
			}},
		},
	}

	defaultActorTemplateVersionSpec(spec)

	if got := spec.Containers[0].GetReadyz(); got != nil {
		t.Errorf("container without readyz gained one: %v", got)
	}
	if got := spec.Containers[1].GetReadyz().GetTimeoutSeconds(); got != defaultReadyzTimeout {
		t.Errorf("defaulted timeout = %d, want %d", got, defaultReadyzTimeout)
	}
	if got := spec.Containers[1].GetReadyz().GetHttpGet().GetPath(); got != defaultReadyzPath {
		t.Errorf("defaulted path = %q, want %q", got, defaultReadyzPath)
	}
	if got := spec.Containers[2].GetReadyz().GetTimeoutSeconds(); got != 120 {
		t.Errorf("explicit timeout changed to %d, want 120", got)
	}
	if got := spec.Containers[2].GetReadyz().GetHttpGet().GetPath(); got != "/healthz" {
		t.Errorf("explicit path changed to %q, want /healthz", got)
	}
	if got := spec.GetSandboxConfig(); got != nil {
		t.Errorf("defaulting materialized a sandbox config: %v", got)
	}
}
