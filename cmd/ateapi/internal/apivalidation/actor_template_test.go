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

package apivalidation

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateCreateActorTemplateRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.CreateActorTemplateRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate()},
		nil,
	}, {
		"missing actor_template",
		&ateapipb.CreateActorTemplateRequest{},
		field.ErrorList{field.Required(field.NewPath("actor_template"), "")},
	}, {
		"missing metadata.atespace",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Metadata.Atespace = ""
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "metadata", "atespace"), "")},
	}, {
		"invalid metadata.atespace",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Metadata.Atespace = "NS_1"
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "metadata", "atespace"), "NS_1", "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing metadata.name",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Metadata.Name = ""
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "metadata", "name"), "")},
	}, {
		"invalid metadata.name",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Metadata.Name = "Tmpl_A"
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "metadata", "name"), "Tmpl_A", "").WithOrigin("format=k8s-short-name")},
	}, {
		"valid data-scoped snapshots",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.OnPause = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
			tmpl.SnapshotConfig.OnCommit = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
		})},
		nil,
	}, {
		"invalid worker_selector label key",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"bad key": "v"}}
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "worker_selector", "match_labels"), "bad key", "").WithOrigin("format=k8s-label-key")},
	}, {
		"no containers",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers = nil
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "containers"), "")},
	}, {
		"container missing name",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Name = ""
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "containers").Index(0).Child("name"), "")},
	}, {
		"container invalid name",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Name = "Main_1"
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "containers").Index(0).Child("name"), "Main_1", "").WithOrigin("format=k8s-short-name")},
	}, {
		"container missing image",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Image = ""
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "containers").Index(0).Child("image"), "")},
	}, {
		"volume mount referencing a declared volume",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "data", DurableDir: &ateapipb.DurableDirVolumeSource{}}}
			tmpl.Containers[0].VolumeMounts = []*ateapipb.VolumeMount{{Name: "data", MountPath: "/var/data"}}
		})},
		nil,
	}, {
		"volume mount referencing an undeclared volume",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapipb.VolumeMount{{Name: "ghost-vol", MountPath: "/var/data"}}
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "containers").Index(0).Child("volume_mounts").Index(0).Child("name"), "ghost-vol", "")},
	}, {
		"missing snapshot_config",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig = nil
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "snapshot_config"), "")},
	}, {
		"missing snapshot_config.storage_location",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.StorageLocation = ""
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "snapshot_config", "storage_location"), "")},
	}, {
		"storage_location without a bucket",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.StorageLocation = "my-bucket/snapshots"
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "snapshot_config", "storage_location"), "my-bucket/snapshots", "")},
	}, {
		"storage_location with a query",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.StorageLocation = "gs://my-bucket/snapshots?versions=true"
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "snapshot_config", "storage_location"), "gs://my-bucket/snapshots?versions=true", "")},
	}, {
		"on_commit broader than on_pause",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.OnPause = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
			tmpl.SnapshotConfig.OnCommit = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "snapshot_config", "on_commit"), "SNAPSHOT_CONTENT_SCOPE_FULL", "")},
	}, {
		// Leaving on_commit unset over a DATA on_pause is both a required
		// violation (on_commit has no default of its own) and a subset
		// violation (UNSPECIFIED is not DATA).
		"on_commit unset with data on_pause",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.OnPause = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
			tmpl.SnapshotConfig.OnCommit = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED
		})},
		field.ErrorList{
			field.Required(field.NewPath("actor_template", "snapshot_config", "on_commit"), ""),
			field.Invalid(field.NewPath("actor_template", "snapshot_config", "on_commit"), "SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED", ""),
		},
	}, {
		"missing sandbox_config",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SandboxConfig = nil
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "sandbox_config"), "")},
	}, {
		"unspecified sandbox_config.sandbox_class",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SandboxConfig.SandboxClass = ateapipb.SandboxClass_SANDBOX_CLASS_UNSPECIFIED
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "sandbox_config", "sandbox_class"), "")},
	}, {
		"missing sandbox_config.config_name",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SandboxConfig.ConfigName = ""
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "sandbox_config", "config_name"), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateCreateActorTemplateRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateGetActorTemplateRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.GetActorTemplateRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.GetActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl-a"}},
		nil,
	}, {
		"missing actor_template",
		&ateapipb.GetActorTemplateRequest{},
		field.ErrorList{field.Required(field.NewPath("actor_template"), "")},
	}, {
		"missing atespace",
		&ateapipb.GetActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-a"}},
		field.ErrorList{field.Required(field.NewPath("actor_template", "atespace"), "")},
	}, {
		"missing name",
		&ateapipb.GetActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor_template", "name"), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateGetActorTemplateRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateListActorTemplatesRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.ListActorTemplatesRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.ListActorTemplatesRequest{PageSize: 10},
		nil,
	}, {
		"zero page size",
		&ateapipb.ListActorTemplatesRequest{},
		nil,
	}, {
		"valid atespace filter",
		&ateapipb.ListActorTemplatesRequest{Atespace: "ns1"},
		nil,
	}, {
		"invalid atespace filter",
		&ateapipb.ListActorTemplatesRequest{Atespace: "NS_1"},
		field.ErrorList{field.Invalid(field.NewPath("atespace"), "NS_1", "").WithOrigin("format=k8s-short-name")},
	}, {
		"negative page size",
		&ateapipb.ListActorTemplatesRequest{PageSize: -1},
		field.ErrorList{field.Invalid(field.NewPath("page_size"), int32(-1), "").WithOrigin("minimum")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateListActorTemplatesRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateDeleteActorTemplateRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.DeleteActorTemplateRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.DeleteActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl-a"}},
		nil,
	}, {
		"missing actor_template",
		&ateapipb.DeleteActorTemplateRequest{},
		field.ErrorList{field.Required(field.NewPath("actor_template"), "")},
	}, {
		"missing atespace",
		&ateapipb.DeleteActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-a"}},
		field.ErrorList{field.Required(field.NewPath("actor_template", "atespace"), "")},
	}, {
		"missing name",
		&ateapipb.DeleteActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor_template", "name"), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateDeleteActorTemplateRequest(context.Background(), tt.req), tt.want)
		})
	}
}

// TestValidateActorTemplate exercises the generated resource validation
// directly. The request handler still runs the hand-written validator; this
// pins each declarative rule as it is added, ahead of the conversion.
func TestValidateActorTemplate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ateapipb.ActorTemplate) // nil leaves the template valid
		want   field.ErrorList
	}{{
		name: "valid",
	}, {
		name:   "missing metadata",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.Metadata = nil },
		want:   field.ErrorList{field.Required(field.NewPath("metadata"), "")},
	}, {
		name:   "missing metadata.atespace",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.Metadata.Atespace = "" },
		want:   field.ErrorList{field.Required(field.NewPath("metadata", "atespace"), "")},
	}, {
		name:   "invalid metadata.atespace",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.Metadata.Atespace = "NS1" },
		want:   field.ErrorList{field.Invalid(field.NewPath("metadata", "atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name:   "invalid metadata.name",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.Metadata.Name = "TMPL A" },
		want:   field.ErrorList{field.Invalid(field.NewPath("metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "worker_selector with an invalid label value",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "Not Valid"}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("worker_selector", "match_labels").Key("tier"), nil, "").WithOrigin("format=k8s-label-value")},
	}, {
		name:   "missing sandbox_config",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.SandboxConfig = nil },
		want:   field.ErrorList{field.Required(field.NewPath("sandbox_config"), "")},
	}, {
		name: "unspecified sandbox_class",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SandboxConfig.SandboxClass = ateapipb.SandboxClass_SANDBOX_CLASS_UNSPECIFIED
		},
		want: field.ErrorList{field.Required(field.NewPath("sandbox_config", "sandbox_class"), "")},
	}, {
		name:   "sandbox_class outside the enum",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.SandboxConfig.SandboxClass = ateapipb.SandboxClass(99) },
		want:   field.ErrorList{field.Invalid(field.NewPath("sandbox_config", "sandbox_class"), nil, "").WithOrigin("maximum")},
	}, {
		name:   "negative sandbox_class",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.SandboxConfig.SandboxClass = ateapipb.SandboxClass(-1) },
		want:   field.ErrorList{field.Invalid(field.NewPath("sandbox_config", "sandbox_class"), nil, "").WithOrigin("minimum")},
	}, {
		name:   "missing config_name",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.SandboxConfig.ConfigName = "" },
		want:   field.ErrorList{field.Required(field.NewPath("sandbox_config", "config_name"), "")},
	}, {
		name:   "invalid config_name",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.SandboxConfig.ConfigName = "NOT_A_NAME" },
		want:   field.ErrorList{field.Invalid(field.NewPath("sandbox_config", "config_name"), nil, "").WithOrigin("format=k8s-long-name")},
	}, {
		name:   "missing snapshot_config",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.SnapshotConfig = nil },
		want:   field.ErrorList{field.Required(field.NewPath("snapshot_config"), "")},
	}, {
		name: "storage_location too long",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.StorageLocation = "gs://" + strings.Repeat("x", 1020)
		},
		want: field.ErrorList{field.TooLong(field.NewPath("snapshot_config", "storage_location"), nil, 1024).WithOrigin("maxLength")},
	}, {
		name:   "missing storage_location",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.SnapshotConfig.StorageLocation = "" },
		want:   field.ErrorList{field.Required(field.NewPath("snapshot_config", "storage_location"), "")},
	}, {
		name: "unspecified snapshot scopes",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.OnPause = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED
			tmpl.SnapshotConfig.OnCommit = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED
		},
		want: field.ErrorList{
			field.Required(field.NewPath("snapshot_config", "on_pause"), ""),
			field.Required(field.NewPath("snapshot_config", "on_commit"), ""),
		},
	}, {
		name: "on_commit outside the enum",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.OnCommit = ateapipb.SnapshotContentScope(99)
		},
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot_config", "on_commit"), nil, "").WithOrigin("maximum")},
	}, {
		name: "negative on_pause",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.OnPause = ateapipb.SnapshotContentScope(-1)
		},
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot_config", "on_pause"), nil, "").WithOrigin("minimum")},
	}, {
		name:   "missing on_resume",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.SnapshotConfig.OnResume = nil },
		want:   field.ErrorList{field.Required(field.NewPath("snapshot_config", "on_resume"), "")},
	}, {
		name: "unspecified on_resume from_data",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.OnResume = &ateapipb.OnResumeConfig{}
		},
		want: field.ErrorList{field.Required(field.NewPath("snapshot_config", "on_resume", "from_data"), "")},
	}, {
		name: "valid on_resume",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.OnResume = &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT}
		},
	}, {
		name: "valid on_resume with golden",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.OnResume = &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN}
		},
	}, {
		name: "negative on_resume from_data",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.OnResume = &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource(-1)}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot_config", "on_resume", "from_data"), nil, "").WithOrigin("minimum")},
	}, {
		name: "on_resume from_data outside the enum",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotConfig.OnResume = &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource(99)}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot_config", "on_resume", "from_data"), nil, "").WithOrigin("maximum")},
	}, {
		name:   "no containers",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.Containers = nil },
		want:   field.ErrorList{field.Required(field.NewPath("containers"), "")},
	}, {
		name: "too many containers",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			for i := 0; i < 10; i++ {
				tmpl.Containers = append(tmpl.Containers, &ateapipb.Container{Name: fmt.Sprintf("c-%d", i), Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"})
			}
		},
		want: field.ErrorList{field.TooMany(field.NewPath("containers"), 11, 10).WithOrigin("maxItems")},
	}, {
		name: "duplicate container name",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers = append(tmpl.Containers, &ateapipb.Container{Name: "main", Image: "example.com/other:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"})
		},
		want: field.ErrorList{field.Duplicate(field.NewPath("containers").Index(1), nil)},
	}, {
		name: "duplicate env name",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Env = []*ateapipb.EnvVar{{Name: "PORT", Value: "1"}, {Name: "PORT", Value: "2"}}
		},
		want: field.ErrorList{field.Duplicate(field.NewPath("containers").Index(0).Child("env").Index(1), nil)},
	}, {
		name: "the same volume mounted at two paths is allowed",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapipb.VolumeMount{
				{Name: "data", MountPath: "/var/data"},
				{Name: "data", MountPath: "/mnt/data"},
			}
		},
	}, {
		name: "two volumes at the same path are rejected",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapipb.VolumeMount{
				{Name: "data", MountPath: "/var/data"},
				{Name: "other", MountPath: "/var/data"},
			}
		},
		want: field.ErrorList{field.Duplicate(field.NewPath("containers").Index(0).Child("volume_mounts").Index(1), nil)},
	}, {
		name: "nested mount paths are rejected",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapipb.VolumeMount{
				{Name: "data", MountPath: "/data"},
				{Name: "config", MountPath: "/data/config"},
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("volume_mounts").Index(1).Child("mount_path"), nil, "")},
	}, {
		name: "the same path in different containers is allowed",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers = append(tmpl.Containers, &ateapipb.Container{Name: "sidecar", Image: "example.com/side:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"})
			tmpl.Containers[0].VolumeMounts = []*ateapipb.VolumeMount{{Name: "data", MountPath: "/var/data"}}
			tmpl.Containers[1].VolumeMounts = []*ateapipb.VolumeMount{{Name: "data", MountPath: "/var/data"}}
		},
	}, {
		name: "duplicate volume name",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{
				{Name: "scratch", DurableDir: &ateapipb.DurableDirVolumeSource{}},
				{Name: "scratch", DurableDir: &ateapipb.DurableDirVolumeSource{}},
			}
		},
		want: field.ErrorList{field.Duplicate(field.NewPath("volumes").Index(1), nil)},
	}, {
		name: "too many command entries",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Command = make([]string, 65)
		},
		want: field.ErrorList{field.TooMany(field.NewPath("containers").Index(0).Child("command"), 65, 64).WithOrigin("maxItems")},
	}, {
		name: "command entry too long",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Command = []string{strings.Repeat("x", 4097)}
		},
		want: field.ErrorList{field.TooLong(field.NewPath("containers").Index(0).Child("command").Index(0), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "valid command and args",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Command = []string{"/bin/app"}
			// Repeated argv values are legitimate; these lists are atomic,
			// not sets.
			tmpl.Containers[0].Args = []string{"--serve", "-v", "-v"}
		},
	}, {
		name: "too many args entries",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Args = make([]string, 65)
		},
		want: field.ErrorList{field.TooMany(field.NewPath("containers").Index(0).Child("args"), 65, 64).WithOrigin("maxItems")},
	}, {
		name: "args entry too long",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Args = []string{strings.Repeat("x", 4097)}
		},
		want: field.ErrorList{field.TooLong(field.NewPath("containers").Index(0).Child("args").Index(0), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "too many volume_mounts",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			for i := 0; i < 33; i++ {
				tmpl.Containers[0].VolumeMounts = append(tmpl.Containers[0].VolumeMounts,
					&ateapipb.VolumeMount{Name: "data", MountPath: fmt.Sprintf("/mnt/p%d", i)})
			}
		},
		want: field.ErrorList{field.TooMany(field.NewPath("containers").Index(0).Child("volume_mounts"), 33, 32).WithOrigin("maxItems")},
	}, {
		name: "too many env entries",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			for i := 0; i < 33; i++ {
				tmpl.Containers[0].Env = append(tmpl.Containers[0].Env, &ateapipb.EnvVar{Name: fmt.Sprintf("VAR_%d", i)})
			}
		},
		want: field.ErrorList{field.TooMany(field.NewPath("containers").Index(0).Child("env"), 33, 32).WithOrigin("maxItems")},
	}, {
		name: "image too long",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Image = strings.Repeat("x", 513) + "@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		},
		want: field.ErrorList{
			field.Invalid(field.NewPath("containers").Index(0).Child("image"), nil, ""),
			field.TooLong(field.NewPath("containers").Index(0).Child("image"), nil, 512).WithOrigin("maxLength"),
		},
	}, {
		name: "valid image: pinned by digest",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Image = "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		},
	}, {
		name: "container image missing digest",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Image = "example.com/app:v1"
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("image"), nil, "")},
	}, {
		name:   "container missing name",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.Containers[0].Name = "" },
		want:   field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("name"), "")},
	}, {
		name:   "container invalid name",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.Containers[0].Name = "Main_1" },
		want:   field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name:   "container missing image",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.Containers[0].Image = "" },
		want:   field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("image"), "")},
	}, {
		name: "valid env",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Env = []*ateapipb.EnvVar{{Name: "PORT", Value: "8080"}, {Name: "DEBUG"}}
		},
	}, {
		name: "env name with an equals sign",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Env = []*ateapipb.EnvVar{{Name: "FOO=BAR"}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("env").Index(0).Child("name"), nil, "")},
	}, {
		name: "env missing name",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Env = []*ateapipb.EnvVar{{Value: "8080"}}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("env").Index(0).Child("name"), "")},
	}, {
		name: "valid security_context capabilities",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].SecurityContext = &ateapipb.SecurityContext{Capabilities: &ateapipb.Capabilities{
				Add:  []string{"NET_BIND_SERVICE"},
				Drop: []string{"ALL"},
			}}
		},
	}, {
		name: "capabilities add rejects ALL",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].SecurityContext = &ateapipb.SecurityContext{Capabilities: &ateapipb.Capabilities{Add: []string{"ALL"}}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("security_context", "capabilities", "add").Index(0), nil, "")},
	}, {
		name: "lowercase capability",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].SecurityContext = &ateapipb.SecurityContext{Capabilities: &ateapipb.Capabilities{Drop: []string{"net_raw"}}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("security_context", "capabilities", "drop").Index(0), nil, "")},
	}, {
		name: "duplicate capability",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].SecurityContext = &ateapipb.SecurityContext{Capabilities: &ateapipb.Capabilities{Add: []string{"NET_BIND_SERVICE", "NET_BIND_SERVICE"}}}
		},
		want: field.ErrorList{field.Duplicate(field.NewPath("containers").Index(0).Child("security_context", "capabilities", "add").Index(1), nil)},
	}, {
		name: "too many capabilities",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			caps := make([]string, 65)
			for i := range caps {
				caps[i] = fmt.Sprintf("CAP%d", i)
			}
			tmpl.Containers[0].SecurityContext = &ateapipb.SecurityContext{Capabilities: &ateapipb.Capabilities{Add: caps}}
		},
		want: field.ErrorList{field.TooMany(field.NewPath("containers").Index(0).Child("security_context", "capabilities", "add"), 65, 64).WithOrigin("maxItems")},
	}, {
		name: "valid wakeup probe",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapipb.ContainerWakeupProbe{
				HttpGet:        &ateapipb.HTTPGetAction{Path: "/healthz", Port: 8080},
				TimeoutSeconds: 60,
			}
		},
	}, {
		name: "wakeup probe missing http_get",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapipb.ContainerWakeupProbe{TimeoutSeconds: 60}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("wakeup_probe", "http_get"), "")},
	}, {
		name: "missing wakeup probe timeout_seconds",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapipb.ContainerWakeupProbe{HttpGet: &ateapipb.HTTPGetAction{Path: "/healthz", Port: 8080}}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("wakeup_probe", "timeout_seconds"), "")},
	}, {
		name: "missing wakeup probe http_get.path",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapipb.ContainerWakeupProbe{HttpGet: &ateapipb.HTTPGetAction{Port: 8080}, TimeoutSeconds: 60}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("wakeup_probe", "http_get", "path"), "")},
	}, {
		name: "wakeup probe timeout_seconds out of range",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapipb.ContainerWakeupProbe{
				HttpGet:        &ateapipb.HTTPGetAction{Path: "/healthz", Port: 8080},
				TimeoutSeconds: 3601,
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("wakeup_probe", "timeout_seconds"), nil, "").WithOrigin("maximum")},
	}, {
		name: "negative wakeup probe timeout_seconds",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapipb.ContainerWakeupProbe{
				HttpGet:        &ateapipb.HTTPGetAction{Path: "/healthz", Port: 8080},
				TimeoutSeconds: -1,
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("wakeup_probe", "timeout_seconds"), nil, "").WithOrigin("minimum")},
	}, {
		name: "wakeup probe missing port",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapipb.ContainerWakeupProbe{
				HttpGet:        &ateapipb.HTTPGetAction{Path: "/healthz"},
				TimeoutSeconds: 60,
			}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("wakeup_probe", "http_get", "port"), "")},
	}, {
		name: "negative wakeup probe port",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapipb.ContainerWakeupProbe{
				HttpGet:        &ateapipb.HTTPGetAction{Path: "/healthz", Port: -1},
				TimeoutSeconds: 60,
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("wakeup_probe", "http_get", "port"), nil, "").WithOrigin("minimum")},
	}, {
		name: "wakeup probe port out of range",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapipb.ContainerWakeupProbe{
				HttpGet:        &ateapipb.HTTPGetAction{Path: "/healthz", Port: 65536},
				TimeoutSeconds: 60,
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("wakeup_probe", "http_get", "port"), nil, "").WithOrigin("maximum")},
	}, {
		name: "wakeup probe path not starting with slash",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapipb.ContainerWakeupProbe{
				HttpGet:        &ateapipb.HTTPGetAction{Path: "readyz", Port: 8080},
				TimeoutSeconds: 60,
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("wakeup_probe", "http_get", "path"), nil, "")},
	}, {
		name: "valid volume_mount",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapipb.VolumeMount{{Name: "data", MountPath: "/var/data"}}
		},
	}, {
		name: "volume_mount missing name",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapipb.VolumeMount{{MountPath: "/var/data"}}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("volume_mounts").Index(0).Child("name"), "")},
	}, {
		name: "volume_mount invalid name",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapipb.VolumeMount{{Name: "Data_1", MountPath: "/var/data"}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("volume_mounts").Index(0).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "volume_mount missing mount_path",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapipb.VolumeMount{{Name: "data"}}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("volume_mounts").Index(0).Child("mount_path"), "")},
	}, {
		name: "relative mount_path",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapipb.VolumeMount{{Name: "data", MountPath: "var/data"}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("volume_mounts").Index(0).Child("mount_path"), nil, "")},
	}, {
		name: "valid durable_dir volume",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "scratch", DurableDir: &ateapipb.DurableDirVolumeSource{}}}
		},
	}, {
		name: "too many volumes",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			for i := 0; i < 33; i++ {
				tmpl.Volumes = append(tmpl.Volumes, &ateapipb.Volume{Name: fmt.Sprintf("vol-%d", i), DurableDir: &ateapipb.DurableDirVolumeSource{}})
			}
		},
		want: field.ErrorList{field.TooMany(field.NewPath("volumes"), 33, 32).WithOrigin("maxItems")},
	}, {
		name: "volume missing name",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{DurableDir: &ateapipb.DurableDirVolumeSource{}}}
		},
		want: field.ErrorList{field.Required(field.NewPath("volumes").Index(0).Child("name"), "")},
	}, {
		name: "volume invalid name",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "Scratch_1", DurableDir: &ateapipb.DurableDirVolumeSource{}}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("volumes").Index(0).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "volume with no source",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "scratch"}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("volumes").Index(0), nil, "one of").WithOrigin("union")},
	}, {
		name: "volume with two sources",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{
				Name:       "scratch",
				DurableDir: &ateapipb.DurableDirVolumeSource{},
				Image:      &ateapipb.ImageVolumeSource{Reference: "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
			}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("volumes").Index(0), nil, "one of").WithOrigin("union")},
	}, {
		name: "valid image volume",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "tools", Image: &ateapipb.ImageVolumeSource{Reference: "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}}}
		},
	}, {
		name: "image volume missing reference",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "tools", Image: &ateapipb.ImageVolumeSource{}}}
		},
		want: field.ErrorList{field.Required(field.NewPath("volumes").Index(0).Child("image", "reference"), "")},
	}, {
		name: "image volume reference missing digest",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "tools", Image: &ateapipb.ImageVolumeSource{Reference: "example.com/app:v1"}}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("volumes").Index(0).Child("image", "reference"), nil, "")},
	}, {
		name: "valid external volume template",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "data", ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{Capacity: "10Gi", StorageClassName: "fast-ssd"}}}
		},
	}, {
		name: "external volume template missing capacity",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "data", ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{StorageClassName: "fast-ssd"}}}
		},
		want: field.ErrorList{field.Required(field.NewPath("volumes").Index(0).Child("external_volume_template", "capacity"), "")},
	}, {
		name: "external volume template malformed capacity",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "data", ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{Capacity: "ten gigs", StorageClassName: "fast-ssd"}}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("volumes").Index(0).Child("external_volume_template", "capacity"), nil, "")},
	}, {
		name: "external volume template capacity at the length bound",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "data", ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{Capacity: strings.Repeat("1", 30) + "Gi", StorageClassName: "fast-ssd"}}}
		},
	}, {
		name: "external volume template capacity too long",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "data", ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{Capacity: strings.Repeat("1", 31) + "Gi", StorageClassName: "fast-ssd"}}}
		},
		want: field.ErrorList{field.TooLong(field.NewPath("volumes").Index(0).Child("external_volume_template", "capacity"), nil, 32).WithOrigin("maxLength")},
	}, {
		name: "external volume template missing storage_class_name",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "data", ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{Capacity: "10Gi"}}}
		},
		want: field.ErrorList{field.Required(field.NewPath("volumes").Index(0).Child("external_volume_template", "storage_class_name"), "")},
	}, {
		name: "external volume template invalid storage_class_name",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "data", ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{Capacity: "10Gi", StorageClassName: "Fast SSD"}}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("volumes").Index(0).Child("external_volume_template", "storage_class_name"), nil, "").WithOrigin("format=k8s-long-name")},
	}, {
		name: "valid resources",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{
				{Name: "cpu", Quantity: "500m"}, {Name: "memory", Quantity: "2Gi"},
			}}
		},
	}, {
		name: "limit missing name",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{{Quantity: "2Gi"}}}
		},
		want: field.ErrorList{
			field.Required(field.NewPath("containers").Index(0).Child("resources", "limits").Index(0).Child("name"), ""),
			field.NotSupported[string](field.NewPath("containers").Index(0).Child("resources", "limits").Index(0).Child("name"), nil, nil),
		},
	}, {
		name: "limit missing quantity",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "cpu"}}}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("resources", "limits").Index(0).Child("quantity"), "")},
	}, {
		name: "unsupported limit name",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "gpu", Quantity: "1"}}}
		},
		want: field.ErrorList{field.NotSupported[string](field.NewPath("containers").Index(0).Child("resources", "limits").Index(0).Child("name"), nil, nil)},
	}, {
		name: "duplicate limit name",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{
				{Name: "cpu", Quantity: "1"}, {Name: "cpu", Quantity: "2"},
			}}
		},
		want: field.ErrorList{field.Duplicate(field.NewPath("containers").Index(0).Child("resources", "limits").Index(1), nil)},
	}, {
		name: "malformed limit quantity",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "memory", Quantity: "two gigs"}}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("resources", "limits").Index(0).Child("quantity"), nil, "")},
	}, {
		name: "too many limits",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{
				{Name: "cpu", Quantity: "1"}, {Name: "memory", Quantity: "1Gi"}, {Name: "cpu", Quantity: "2"},
			}}
		},
		// maxItems short-circuits the per-item and uniqueness checks.
		want: field.ErrorList{field.TooMany(field.NewPath("containers").Index(0).Child("resources", "limits"), 3, 2).WithOrigin("maxItems")},
	}, {
		name: "template-level resources validated too",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "gpu", Quantity: "1"}}}
		},
		want: field.ErrorList{field.NotSupported[string](field.NewPath("resources", "limits").Index(0).Child("name"), nil, nil)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpl := validActorTemplate()
			if tt.mutate != nil {
				tt.mutate(tmpl)
			}
			op := operation.Operation{Type: operation.Create}
			assertValidateErr(t, Validate_ActorTemplate(context.Background(), op, nil, tmpl, nil), tt.want)
		})
	}
}

// validActorTemplate returns the smallest template that passes create
// validation; mutations tweak it per test case. The snapshot scopes and
// resume policy are set explicitly because TestValidateActorTemplate
// exercises validation directly, without defaulting.
func validActorTemplate(mutations ...func(*ateapipb.ActorTemplate)) *ateapipb.ActorTemplate {
	template := &ateapipb.ActorTemplate{
		Metadata:   &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tmpl-a"},
		Containers: []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation: "gs://my-bucket/snapshots",
			OnPause:         ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			OnCommit:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			OnResume:        &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT},
		},
		SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"},
	}
	for _, m := range mutations {
		m(template)
	}
	return template
}
