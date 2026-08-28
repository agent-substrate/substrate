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
	"context"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// validActorTemplate returns the smallest template that passes create
// validation; mutations tweak it per test case.
func validActorTemplate(mutations ...func(*ateapipb.ActorTemplate)) *ateapipb.ActorTemplate {
	template := &ateapipb.ActorTemplate{
		Metadata:        &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tmpl-a"},
		Containers:      []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1"}},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://my-bucket/snapshots"},
		SandboxConfig:   &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"},
	}
	for _, m := range mutations {
		m(template)
	}
	return template
}

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
			tmpl.SnapshotsConfig.OnPause = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
			tmpl.SnapshotsConfig.OnCommit = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
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
		"missing snapshots_config",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotsConfig = nil
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "snapshots_config"), "")},
	}, {
		"missing snapshots_config.storage_location",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotsConfig.StorageLocation = ""
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "snapshots_config", "storage_location"), "")},
	}, {
		"on_commit broader than on_pause",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotsConfig.OnPause = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
			tmpl.SnapshotsConfig.OnCommit = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "snapshots_config", "on_commit"), "SNAPSHOT_CONTENT_SCOPE_FULL", "")},
	}, {
		// UNSPECIFIED defaults to FULL, so leaving on_commit unset over a DATA
		// on_pause is also a subset violation.
		"on_commit unset with data on_pause",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotsConfig.OnPause = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "snapshots_config", "on_commit"), "SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED", "")},
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
			assertValidateErr(t, validateCreateActorTemplateRequest(context.Background(), tt.req), tt.want)
		})
	}
}

// TestCreateActorTemplate covers the atespace precondition: creation fails
// while the atespace is missing, and succeeds once the atespace exists.
func TestCreateActorTemplate(t *testing.T) {
	persistence := newTestPersistence(t)
	s := &RPCService{impl: persistence}
	ctx := context.Background()
	req := func(atespace, name string) *ateapipb.CreateActorTemplateRequest {
		return &ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Metadata = &ateapipb.ResourceMetadata{Atespace: atespace, Name: name}
		})}
	}

	if _, err := s.CreateActorTemplate(ctx, req("ns-missing", "tmpl-a")); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActorTemplate in missing atespace = %v, want FailedPrecondition", err)
	}

	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ns1"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	created, err := s.CreateActorTemplate(ctx, req("ns1", "tmpl-a"))
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	if created.GetMetadata().GetName() != "tmpl-a" {
		t.Errorf("created name = %q, want tmpl-a", created.GetMetadata().GetName())
	}
}

// TestCreateActorTemplateIgnoresServerOwnedFields pins the create contract:
// status on the request is dropped and new templates start with an empty
// status. The store persists whatever the handler hands it, so the handler is
// the only guard.
func TestCreateActorTemplateIgnoresServerOwnedFields(t *testing.T) {
	persistence := newTestPersistence(t)
	s := &RPCService{impl: persistence}
	ctx := context.Background()

	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ns1"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}

	in := validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
		tmpl.Metadata.Uid = "11111111-1111-1111-1111-111111111111"
		tmpl.Metadata.Version = 42
		tmpl.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"pool": "default"}}
		tmpl.Containers = []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1"}}
		tmpl.SnapshotsConfig = &ateapipb.SnapshotsConfig{StorageLocation: "gs://my-bucket/snapshots"}
		tmpl.Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "memory", Quantity: "1Gi"}}}
		// Server-owned status a client must not be able to set.
		tmpl.Status = &ateapipb.ActorTemplateStatus{
			GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
				GoldenSnapshot: &ateapipb.ObjectRef{Atespace: "ate-golden", Name: "sneaky"},
			},
			SandboxAssets: &ateapipb.SandboxAssets{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR},
		}
	})
	created, err := s.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: in})
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}

	want := validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
		tmpl.Metadata.Version = 1
		tmpl.WorkerSelector = in.GetWorkerSelector()
		tmpl.Containers = in.GetContainers()
		tmpl.SnapshotsConfig = in.GetSnapshotsConfig()
		tmpl.Resources = in.GetResources()
		tmpl.Status = &ateapipb.ActorTemplateStatus{}
	})
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("CreateActorTemplate response mismatch (-want +got):\n%s", diff)
	}
	if got := created.GetMetadata().GetUid(); got == "" || got == in.GetMetadata().GetUid() {
		t.Errorf("created uid = %q, want a fresh server-assigned uid", got)
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
			assertValidateErr(t, validateGetActorTemplateRequest(tt.req), tt.want)
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
		field.ErrorList{field.Invalid(field.NewPath("atespace"), "NS_1", "")},
	}, {
		"negative page size",
		&ateapipb.ListActorTemplatesRequest{PageSize: -1},
		field.ErrorList{field.Invalid(field.NewPath("page_size"), int32(-1), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateListActorTemplatesRequest(tt.req), tt.want)
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
			assertValidateErr(t, validateDeleteActorTemplateRequest(tt.req), tt.want)
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
		name:   "missing snapshots_config",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.SnapshotsConfig = nil },
		want:   field.ErrorList{field.Required(field.NewPath("snapshots_config"), "")},
	}, {
		name:   "missing storage_location",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.SnapshotsConfig.StorageLocation = "" },
		want:   field.ErrorList{field.Required(field.NewPath("snapshots_config", "storage_location"), "")},
	}, {
		name: "unspecified snapshot scopes are allowed",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotsConfig.OnPause = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED
			tmpl.SnapshotsConfig.OnCommit = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED
		},
	}, {
		name: "on_commit outside the enum",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotsConfig.OnCommit = ateapipb.SnapshotContentScope(99)
		},
		want: field.ErrorList{field.Invalid(field.NewPath("snapshots_config", "on_commit"), nil, "").WithOrigin("maximum")},
	}, {
		name: "negative on_pause",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotsConfig.OnPause = ateapipb.SnapshotContentScope(-1)
		},
		want: field.ErrorList{field.Invalid(field.NewPath("snapshots_config", "on_pause"), nil, "").WithOrigin("minimum")},
	}, {
		name: "on_resume from_data outside the enum",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SnapshotsConfig.OnResume = &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource(99)}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("snapshots_config", "on_resume", "from_data"), nil, "").WithOrigin("maximum")},
	}, {
		name:   "no containers",
		mutate: func(tmpl *ateapipb.ActorTemplate) { tmpl.Containers = nil },
		want:   field.ErrorList{field.Required(field.NewPath("containers"), "")},
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
		name: "env missing name",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Env = []*ateapipb.EnvVar{{Value: "8080"}}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("env").Index(0).Child("name"), "")},
	}, {
		name: "valid readyz",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Readyz = &ateapipb.ContainerReadyz{
				HttpGet:        &ateapipb.HTTPGetAction{Path: "/healthz", Port: 8080},
				TimeoutSeconds: 60,
			}
		},
	}, {
		name: "readyz missing http_get",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Readyz = &ateapipb.ContainerReadyz{TimeoutSeconds: 60}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("readyz", "http_get"), "")},
	}, {
		name: "readyz timeout_seconds out of range",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Readyz = &ateapipb.ContainerReadyz{
				HttpGet:        &ateapipb.HTTPGetAction{Port: 8080},
				TimeoutSeconds: 3601,
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("readyz", "timeout_seconds"), nil, "").WithOrigin("maximum")},
	}, {
		name: "negative readyz timeout_seconds",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Readyz = &ateapipb.ContainerReadyz{
				HttpGet:        &ateapipb.HTTPGetAction{Port: 8080},
				TimeoutSeconds: -1,
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("readyz", "timeout_seconds"), nil, "").WithOrigin("minimum")},
	}, {
		name: "readyz missing port",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Readyz = &ateapipb.ContainerReadyz{HttpGet: &ateapipb.HTTPGetAction{}}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("readyz", "http_get", "port"), "")},
	}, {
		name: "readyz port out of range",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Readyz = &ateapipb.ContainerReadyz{HttpGet: &ateapipb.HTTPGetAction{Port: 65536}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("readyz", "http_get", "port"), nil, "").WithOrigin("maximum")},
	}, {
		name: "readyz path with query string",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Readyz = &ateapipb.ContainerReadyz{HttpGet: &ateapipb.HTTPGetAction{Path: "/readyz?verbose=1", Port: 8080}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("readyz", "http_get", "path"), nil, "")},
	}, {
		name: "readyz path not starting with slash",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Readyz = &ateapipb.ContainerReadyz{HttpGet: &ateapipb.HTTPGetAction{Path: "readyz", Port: 8080}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("readyz", "http_get", "path"), nil, "")},
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
		name: "root mount_path",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapipb.VolumeMount{{Name: "data", MountPath: "/"}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("volume_mounts").Index(0).Child("mount_path"), nil, "")},
	}, {
		name: "mount_path with dot-dot segment",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapipb.VolumeMount{{Name: "data", MountPath: "/var/../etc"}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("volume_mounts").Index(0).Child("mount_path"), nil, "")},
	}, {
		name: "mount_path with trailing slash",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapipb.VolumeMount{{Name: "data", MountPath: "/var/data/"}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("volume_mounts").Index(0).Child("mount_path"), nil, "")},
	}, {
		name: "valid durable_dir volume",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "scratch", DurableDir: &ateapipb.DurableDirVolumeSource{}}}
		},
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
				Image:      &ateapipb.ImageVolumeSource{Reference: "example.com/app@sha256:abc"},
			}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("volumes").Index(0), nil, "one of").WithOrigin("union")},
	}, {
		name: "valid image volume",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "tools", Image: &ateapipb.ImageVolumeSource{Reference: "example.com/app@sha256:abc"}}}
		},
	}, {
		name: "image volume missing reference",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Volumes = []*ateapipb.Volume{{Name: "tools", Image: &ateapipb.ImageVolumeSource{}}}
		},
		want: field.ErrorList{field.Required(field.NewPath("volumes").Index(0).Child("image", "reference"), "")},
	}, {
		name: "image volume reference not pinned by digest",
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
		name: "zero limit quantity",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "memory", Quantity: "0"}}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("resources", "limits").Index(0).Child("quantity"), nil, "")},
	}, {
		name: "cpu limit of 1000 cores",
		mutate: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Containers[0].Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "cpu", Quantity: "1k"}}}
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
