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
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "metadata", "atespace"), "NS_1", "")},
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
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "metadata", "name"), "Tmpl_A", "")},
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
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "worker_selector", "match_labels").Key("bad key"), "bad key", "")},
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
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "containers").Index(0).Child("name"), "Main_1", "")},
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
			assertValidateErr(t, validateCreateActorTemplateRequest(tt.req), tt.want)
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
