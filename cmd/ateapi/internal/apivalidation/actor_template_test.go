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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateCreateActorTemplateRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapi.CreateActorTemplateRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate()},
		nil,
	}, {
		"valid golden egress rules",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.GoldenEgressPolicy = &ateapi.EgressPolicyTemplate{Rules: []*ateapi.EgressRule{{Http: &ateapi.HttpRule{Hostnames: []string{"api.example.com"}, Ports: &ateapi.Ports{Numbers: []int32{80}}}}}}
		})},
		nil,
	}, {
		"missing golden egress rules",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.GoldenEgressPolicy = &ateapi.EgressPolicyTemplate{}
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "golden_egress_policy", "rules"), "")},
	}, {
		"empty golden egress rules",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.GoldenEgressPolicy = &ateapi.EgressPolicyTemplate{Rules: []*ateapi.EgressRule{}}
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "golden_egress_policy", "rules"), "")},
	}, {
		"invalid golden egress hostname",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.GoldenEgressPolicy = &ateapi.EgressPolicyTemplate{Rules: []*ateapi.EgressRule{{Http: &ateapi.HttpRule{Hostnames: []string{"bad/host"}, Ports: &ateapi.Ports{Numbers: []int32{80}}}}}}
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "golden_egress_policy", "rules").Index(0).Child("http", "hostnames").Index(0), nil, "")},
	}, {
		"golden egress rules tie across protocols",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.GoldenEgressPolicy = &ateapi.EgressPolicyTemplate{Rules: []*ateapi.EgressRule{
				{Https: &ateapi.HttpsRule{Hostnames: []string{"api.example.com"}, Ports: &ateapi.Ports{Numbers: []int32{443}}}},
				{TlsPassthrough: &ateapi.TlsPassthroughRule{Hostnames: []string{"api.example.com"}, Ports: &ateapi.Ports{Numbers: []int32{443}}}},
			}}
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "golden_egress_policy", "rules").Index(1).Child("tls_passthrough", "hostnames").Index(0), nil, "")},
	}, {
		"too many golden egress rules",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.GoldenEgressPolicy = &ateapi.EgressPolicyTemplate{}
			for i := range 257 {
				tmpl.GoldenEgressPolicy.Rules = append(tmpl.GoldenEgressPolicy.Rules, &ateapi.EgressRule{Http: &ateapi.HttpRule{Hostnames: []string{fmt.Sprintf("host-%d.example.com", i)}, Ports: &ateapi.Ports{Numbers: []int32{80}}}})
			}
		})},
		field.ErrorList{field.TooMany(field.NewPath("actor_template", "golden_egress_policy", "rules"), 257, 256).WithOrigin("maxItems")},
	}, {
		"missing actor_template",
		&ateapi.CreateActorTemplateRequest{},
		field.ErrorList{field.Required(field.NewPath("actor_template"), "")},
	}, {
		"missing metadata.atespace",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Metadata.Atespace = ""
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "metadata", "atespace"), "")},
	}, {
		"invalid metadata.atespace",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Metadata.Atespace = "NS_1"
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "metadata", "atespace"), "NS_1", "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing metadata.name",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Metadata.Name = ""
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "metadata", "name"), "")},
	}, {
		"invalid metadata.name",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Metadata.Name = "Tmpl_A"
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "metadata", "name"), "Tmpl_A", "").WithOrigin("format=k8s-short-name")},
	}, {
		"valid volumes fidelity",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.PreferredFidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
		})},
		nil,
	}, {
		"valid rootfs fidelity",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.PreferredFidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
		})},
		nil,
	}, {
		"invalid worker_selector label key",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"bad key": "v"}}
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "worker_selector", "match_labels"), "bad key", "").WithOrigin("format=k8s-label-key")},
	}, {
		"no containers",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers = nil
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "containers"), "")},
	}, {
		"container missing name",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Name = ""
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "containers").Index(0).Child("name"), "")},
	}, {
		"container invalid name",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Name = "Main_1"
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "containers").Index(0).Child("name"), "Main_1", "").WithOrigin("format=k8s-short-name")},
	}, {
		"container missing image",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Image = ""
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "containers").Index(0).Child("image"), "")},
	}, {
		"volume mount referencing a declared volume",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{Name: "data", DurableDir: &ateapi.DurableDirVolumeSource{}}}
			tmpl.Containers[0].VolumeMounts = []*ateapi.VolumeMount{{Name: "data", MountPath: "/var/data"}}
		})},
		nil,
	}, {
		"volume mount referencing an undeclared volume",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapi.VolumeMount{{Name: "ghost-vol", MountPath: "/var/data"}}
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "containers").Index(0).Child("volume_mounts").Index(0).Child("name"), "ghost-vol", "")},
	}, {
		"missing snapshot_config is allowed",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig = nil
		})},
		nil,
	}, {
		"valid storage_location",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.StorageLocation = "gs://my-bucket/snapshots"
		})},
		nil,
	}, {
		"storage_location without a bucket",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.StorageLocation = "my-bucket/snapshots"
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "snapshot_config", "storage_location"), "my-bucket/snapshots", "")},
	}, {
		"storage_location with a query",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.StorageLocation = "gs://my-bucket/snapshots?versions=true"
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "snapshot_config", "storage_location"), "gs://my-bucket/snapshots?versions=true", "")},
	}, {
		// preferred_fidelity has no default of its own at this layer, so leaving it
		// unset is a required violation.
		"preferred_fidelity unset",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.PreferredFidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
		})},
		field.ErrorList{
			field.Required(field.NewPath("actor_template", "snapshot_config", "preferred_fidelity"), ""),
		},
	}, {
		"missing resources",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Resources = nil
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "resources"), "")},
	}, {
		"missing cpu",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Resources = &ateapi.ResourceRequirements{Limits: resources.CPUMemory(0, 512<<20)}
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "resources", "limits", "cpu_millicores"), "")},
	}, {
		"missing memory",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Resources = &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 0)}
		})},
		field.ErrorList{field.Required(field.NewPath("actor_template", "resources", "limits", "memory_bytes"), "")},
	}, {
		"empty resources",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Resources = &ateapi.ResourceRequirements{}
		})},
		field.ErrorList{
			field.Required(field.NewPath("actor_template", "resources", "limits", "cpu_millicores"), ""),
			field.Required(field.NewPath("actor_template", "resources", "limits", "memory_bytes"), ""),
		},
	}, {
		"valid actor_labels",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.ActorLabels = map[string]string{"example.com/team": "payments"}
		})},
		nil,
	}, {
		"invalid actor_labels key",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.ActorLabels = map[string]string{"bad key!": "1"}
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "actor_labels"), "bad key!", "").WithOrigin("format=k8s-label-key")},
	}, {
		"too many actor_labels",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.ActorLabels = selectorLabelsOfSize(65)
		})},
		field.ErrorList{field.TooMany(field.NewPath("actor_template", "actor_labels"), 65, 64).WithOrigin("maxProperties")},
	}, {
		"reserved actor_labels key",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.ActorLabels = map[string]string{"ate.dev/agent": "a"}
		})},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "actor_labels"), "ate.dev/agent", "")},
	}, {
		"actor_labels over the size cap",
		&ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.ActorLabels = labelsOfBytes(1)
		})},
		field.ErrorList{field.TooLong(field.NewPath("actor_template", "actor_labels"), 4097, 4096)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := tt.want
			var wantWithPreview map[string]field.ErrorList
			if tt.req.GetActorTemplate().GetGoldenEgressPolicy() != nil {
				want = field.ErrorList{field.Forbidden(field.NewPath("actor_template", "golden_egress_policy"), "")}
				wantWithPreview = map[string]field.ErrorList{"GoldenEgressPolicy": tt.want, "ALL": tt.want}
			}
			if tt.req.GetActorTemplate().GetSnapshotConfig().GetStorageLocation() != "" {
				want = field.ErrorList{field.Forbidden(field.NewPath("actor_template", "snapshot_config", "storage_location"), "")}
				wantWithPreview = map[string]field.ErrorList{"PerTemplateSnapshotLocation": tt.want, "ALL": tt.want}
			}
			assertValidate(t, func() field.ErrorList {
				return ValidateCreateActorTemplateRequest(context.Background(), tt.req)
			}, want, wantWithPreview)
		})
	}
}

func TestValidateCreateActorTemplateRequest_PreviewRestricted(t *testing.T) {
	setPreviewForTest(t, "ALL")
	req := &ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
		tmpl.Volumes = []*ateapi.Volume{{Name: "data", External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{CapacityBytes: 10 << 30, StorageClassName: "fast-ssd"}}}}
		tmpl.SnapshotConfig.StorageLocation = "gs://my-bucket/snapshots"
	})}

	t.Run("allowed", func(t *testing.T) {
		assertValidateErr(t, ValidateCreateActorTemplateRequest(context.Background(), req), nil)
	})
	t.Run("restricted", func(t *testing.T) {
		ctx := preview.ContextWithOptOut(context.Background())
		want := field.ErrorList{
			field.Forbidden(field.NewPath("actor_template", "volumes").Index(0).Child("external"), ""),
			field.Forbidden(field.NewPath("actor_template", "snapshot_config", "storage_location"), ""),
		}
		assertValidateErr(t, ValidateCreateActorTemplateRequest(ctx, req), want)
	})
}

func TestValidateCreateActorTemplateRequest_PerTemplateSnapshotLocation(t *testing.T) {
	storageLocation := field.NewPath("actor_template", "snapshot_config", "storage_location")
	withLocation := func(tmpl *ateapi.ActorTemplate) { tmpl.SnapshotConfig.StorageLocation = "gs://my-bucket/snapshots" }
	tests := []struct {
		name   string
		gates  string
		optOut bool
		mutate func(*ateapi.ActorTemplate)
		want   field.ErrorList
	}{{
		name:   "gate off, opted in, location set",
		mutate: withLocation,
		want:   field.ErrorList{field.Forbidden(storageLocation, "")},
	}, {
		name:   "gate off, opted out, location set",
		optOut: true,
		mutate: withLocation,
		want:   field.ErrorList{field.Forbidden(storageLocation, "")},
	}, {
		name:   "gate off, opted out, no location",
		optOut: true,
	}, {
		name:   "gate on, opted out, location set",
		gates:  "PerTemplateSnapshotLocation",
		optOut: true,
		mutate: withLocation,
		want:   field.ErrorList{field.Forbidden(storageLocation, "")},
	}, {
		name:   "gate on, opted out, no location",
		gates:  "PerTemplateSnapshotLocation",
		optOut: true,
	}, {
		name:   "gate on, opted in, location set",
		gates:  "PerTemplateSnapshotLocation",
		mutate: withLocation,
	}, {
		name:  "gate on, opted in, no location",
		gates: "PerTemplateSnapshotLocation",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setPreviewForTest(t, tt.gates)
			ctx := context.Background()
			if tt.optOut {
				ctx = preview.ContextWithOptOut(ctx)
			}
			var mutations []func(*ateapi.ActorTemplate)
			if tt.mutate != nil {
				mutations = append(mutations, tt.mutate)
			}
			req := &ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(mutations...)}
			assertValidateErr(t, ValidateCreateActorTemplateRequest(ctx, req), tt.want)
		})
	}
}

func TestValidateGetActorTemplateRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapi.GetActorTemplateRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapi.GetActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: "ns1", Name: "tmpl-a"}},
		nil,
	}, {
		"missing actor_template",
		&ateapi.GetActorTemplateRequest{},
		field.ErrorList{field.Required(field.NewPath("actor_template"), "")},
	}, {
		"missing atespace",
		&ateapi.GetActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Name: "tmpl-a"}},
		field.ErrorList{field.Required(field.NewPath("actor_template", "atespace"), "")},
	}, {
		"missing name",
		&ateapi.GetActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: "ns1"}},
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
		req  *ateapi.ListActorTemplatesRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapi.ListActorTemplatesRequest{Options: &ateapi.ListOptions{PageSize: 10}},
		nil,
	}, {
		"zero page size",
		&ateapi.ListActorTemplatesRequest{},
		nil,
	}, {
		"valid atespace filter",
		&ateapi.ListActorTemplatesRequest{Atespace: "ns1"},
		nil,
	}, {
		"invalid atespace filter",
		&ateapi.ListActorTemplatesRequest{Atespace: "NS_1"},
		field.ErrorList{field.Invalid(field.NewPath("atespace"), "NS_1", "").WithOrigin("format=k8s-short-name")},
	}, {
		"negative page size",
		&ateapi.ListActorTemplatesRequest{Options: &ateapi.ListOptions{PageSize: -1}},
		field.ErrorList{field.Invalid(field.NewPath("options", "page_size"), int32(-1), "").WithOrigin("minimum")},
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
		req  *ateapi.DeleteActorTemplateRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapi.DeleteActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: "ns1", Name: "tmpl-a"}},
		nil,
	}, {
		"missing actor_template",
		&ateapi.DeleteActorTemplateRequest{},
		field.ErrorList{field.Required(field.NewPath("actor_template"), "")},
	}, {
		"missing atespace",
		&ateapi.DeleteActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Name: "tmpl-a"}},
		field.ErrorList{field.Required(field.NewPath("actor_template", "atespace"), "")},
	}, {
		"missing name",
		&ateapi.DeleteActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: "ns1"}},
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
		mutate func(*ateapi.ActorTemplate) // nil leaves the template valid
		// want is the result with no preview gates enabled.
		want field.ErrorList
		// wantWithPreview maps a --preview value to the result with those
		// gates enabled.
		wantWithPreview map[string]field.ErrorList
	}{{
		name: "valid",
	}, {
		name: "golden snapshot enabled without wakeup probe",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.GoldenSnapshotConfig = &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED}
		},
	}, {
		name: "golden snapshot disabled",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.GoldenSnapshotConfig = &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED}
		},
	}, {
		name: "golden snapshot mode unspecified",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.GoldenSnapshotConfig = &ateapi.GoldenSnapshotConfig{}
		},
		want: field.ErrorList{field.Required(field.NewPath("snapshot_config", "golden_snapshot_config", "mode"), "")},
	}, {
		name: "golden snapshot mode negative",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.GoldenSnapshotConfig = &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode(-1)}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot_config", "golden_snapshot_config", "mode"), nil, "").WithOrigin("minimum")},
	}, {
		name: "golden snapshot mode unknown",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.GoldenSnapshotConfig = &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode(3)}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot_config", "golden_snapshot_config", "mode"), nil, "").WithOrigin("maximum")},
	}, {
		name:   "missing metadata",
		mutate: func(tmpl *ateapi.ActorTemplate) { tmpl.Metadata = nil },
		want:   field.ErrorList{field.Required(field.NewPath("metadata"), "")},
	}, {
		name:   "missing metadata.atespace",
		mutate: func(tmpl *ateapi.ActorTemplate) { tmpl.Metadata.Atespace = "" },
		want:   field.ErrorList{field.Required(field.NewPath("metadata", "atespace"), "")},
	}, {
		name:   "invalid metadata.atespace",
		mutate: func(tmpl *ateapi.ActorTemplate) { tmpl.Metadata.Atespace = "NS1" },
		want:   field.ErrorList{field.Invalid(field.NewPath("metadata", "atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name:   "invalid metadata.name",
		mutate: func(tmpl *ateapi.ActorTemplate) { tmpl.Metadata.Name = "TMPL A" },
		want:   field.ErrorList{field.Invalid(field.NewPath("metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "worker_selector with an invalid label value",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"tier": "Not Valid"}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("worker_selector", "match_labels").Key("tier"), nil, "").WithOrigin("format=k8s-label-value")},
	}, {
		name:   "missing snapshot_config is allowed",
		mutate: func(tmpl *ateapi.ActorTemplate) { tmpl.SnapshotConfig = nil },
		want:   nil,
	}, {
		name: "storage_location too long",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.StorageLocation = "gs://" + strings.Repeat("x", 1020)
		},
		want:            field.ErrorList{field.Forbidden(field.NewPath("snapshot_config", "storage_location"), "")},
		wantWithPreview: map[string]field.ErrorList{"PerTemplateSnapshotLocation": {field.TooLong(field.NewPath("snapshot_config", "storage_location"), nil, 1024).WithOrigin("maxLength")}},
	}, {
		name:   "missing storage_location",
		mutate: func(tmpl *ateapi.ActorTemplate) { tmpl.SnapshotConfig.StorageLocation = "" },
		// Optional with or without the gate: an empty field falls back to
		// --snapshot-location.
		want:            nil,
		wantWithPreview: map[string]field.ErrorList{"PerTemplateSnapshotLocation": nil},
	}, {
		name: "unspecified snapshot scope",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.PreferredFidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
		},
		want: field.ErrorList{
			field.Required(field.NewPath("snapshot_config", "preferred_fidelity"), ""),
		},
	}, {
		name: "preferred_fidelity outside the enum",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.PreferredFidelity = ateapi.SnapshotFidelity(99)
		},
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot_config", "preferred_fidelity"), nil, "").WithOrigin("maximum")},
	}, {
		name: "negative preferred_fidelity",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.SnapshotConfig.PreferredFidelity = ateapi.SnapshotFidelity(-1)
		},
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot_config", "preferred_fidelity"), nil, "").WithOrigin("minimum")},
	}, {
		name:   "no containers",
		mutate: func(tmpl *ateapi.ActorTemplate) { tmpl.Containers = nil },
		want:   field.ErrorList{field.Required(field.NewPath("containers"), "")},
	}, {
		name: "too many containers",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			for i := 0; i < 10; i++ {
				tmpl.Containers = append(tmpl.Containers, &ateapi.Container{Name: fmt.Sprintf("c-%d", i), Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"})
			}
		},
		want: field.ErrorList{field.TooMany(field.NewPath("containers"), 11, 10).WithOrigin("maxItems")},
	}, {
		name: "duplicate container name",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers = append(tmpl.Containers, &ateapi.Container{Name: "main", Image: "example.com/other:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"})
		},
		want: field.ErrorList{field.Duplicate(field.NewPath("containers").Index(1), nil)},
	}, {
		name: "duplicate env name",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Env = []*ateapi.EnvVar{{Name: "PORT", Value: "1"}, {Name: "PORT", Value: "2"}}
		},
		want: field.ErrorList{field.Duplicate(field.NewPath("containers").Index(0).Child("env").Index(1), nil)},
	}, {
		name: "the same volume mounted at two paths is allowed",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapi.VolumeMount{
				{Name: "data", MountPath: "/var/data"},
				{Name: "data", MountPath: "/mnt/data"},
			}
		},
	}, {
		name: "two volumes at the same path are rejected",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapi.VolumeMount{
				{Name: "data", MountPath: "/var/data"},
				{Name: "other", MountPath: "/var/data"},
			}
		},
		want: field.ErrorList{field.Duplicate(field.NewPath("containers").Index(0).Child("volume_mounts").Index(1), nil)},
	}, {
		name: "nested mount paths are rejected",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapi.VolumeMount{
				{Name: "data", MountPath: "/data"},
				{Name: "config", MountPath: "/data/config"},
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("volume_mounts").Index(1).Child("mount_path"), nil, "")},
	}, {
		name: "the same path in different containers is allowed",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers = append(tmpl.Containers, &ateapi.Container{Name: "sidecar", Image: "example.com/side:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"})
			tmpl.Containers[0].VolumeMounts = []*ateapi.VolumeMount{{Name: "data", MountPath: "/var/data"}}
			tmpl.Containers[1].VolumeMounts = []*ateapi.VolumeMount{{Name: "data", MountPath: "/var/data"}}
		},
	}, {
		name: "duplicate volume name",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{
				{Name: "scratch", DurableDir: &ateapi.DurableDirVolumeSource{}},
				{Name: "scratch", DurableDir: &ateapi.DurableDirVolumeSource{}},
			}
		},
		want: field.ErrorList{field.Duplicate(field.NewPath("volumes").Index(1), nil)},
	}, {
		name: "too many command entries",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Command = make([]string, 65)
		},
		want: field.ErrorList{field.TooMany(field.NewPath("containers").Index(0).Child("command"), 65, 64).WithOrigin("maxItems")},
	}, {
		name: "command entry too long",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Command = []string{strings.Repeat("x", 4097)}
		},
		want: field.ErrorList{field.TooLong(field.NewPath("containers").Index(0).Child("command").Index(0), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "valid command and args",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Command = []string{"/bin/app"}
			// Repeated argv values are legitimate; these lists are atomic,
			// not sets.
			tmpl.Containers[0].Args = []string{"--serve", "-v", "-v"}
		},
	}, {
		name: "too many args entries",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Args = make([]string, 65)
		},
		want: field.ErrorList{field.TooMany(field.NewPath("containers").Index(0).Child("args"), 65, 64).WithOrigin("maxItems")},
	}, {
		name: "args entry too long",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Args = []string{strings.Repeat("x", 4097)}
		},
		want: field.ErrorList{field.TooLong(field.NewPath("containers").Index(0).Child("args").Index(0), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "too many volume_mounts",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			for i := 0; i < 33; i++ {
				tmpl.Containers[0].VolumeMounts = append(tmpl.Containers[0].VolumeMounts,
					&ateapi.VolumeMount{Name: "data", MountPath: fmt.Sprintf("/mnt/p%d", i)})
			}
		},
		want: field.ErrorList{field.TooMany(field.NewPath("containers").Index(0).Child("volume_mounts"), 33, 32).WithOrigin("maxItems")},
	}, {
		name: "too many env entries",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			for i := 0; i < 33; i++ {
				tmpl.Containers[0].Env = append(tmpl.Containers[0].Env, &ateapi.EnvVar{Name: fmt.Sprintf("VAR_%d", i)})
			}
		},
		want: field.ErrorList{field.TooMany(field.NewPath("containers").Index(0).Child("env"), 33, 32).WithOrigin("maxItems")},
	}, {
		name: "image too long",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Image = strings.Repeat("x", 513) + "@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		},
		want: field.ErrorList{
			field.Invalid(field.NewPath("containers").Index(0).Child("image"), nil, ""),
			field.TooLong(field.NewPath("containers").Index(0).Child("image"), nil, 512).WithOrigin("maxLength"),
		},
	}, {
		name: "valid image: pinned by digest",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Image = "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		},
	}, {
		name: "container image missing digest",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Image = "example.com/app:v1"
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("image"), nil, "")},
	}, {
		name:   "container missing name",
		mutate: func(tmpl *ateapi.ActorTemplate) { tmpl.Containers[0].Name = "" },
		want:   field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("name"), "")},
	}, {
		name:   "container invalid name",
		mutate: func(tmpl *ateapi.ActorTemplate) { tmpl.Containers[0].Name = "Main_1" },
		want:   field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name:   "container missing image",
		mutate: func(tmpl *ateapi.ActorTemplate) { tmpl.Containers[0].Image = "" },
		want:   field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("image"), "")},
	}, {
		name: "valid env",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Env = []*ateapi.EnvVar{{Name: "PORT", Value: "8080"}, {Name: "DEBUG"}}
		},
	}, {
		name: "env name with an equals sign",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Env = []*ateapi.EnvVar{{Name: "FOO=BAR"}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("env").Index(0).Child("name"), nil, "")},
	}, {
		name: "env missing name",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Env = []*ateapi.EnvVar{{Value: "8080"}}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("env").Index(0).Child("name"), "")},
	}, {
		name: "valid security_context capabilities",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].SecurityContext = &ateapi.SecurityContext{Capabilities: &ateapi.Capabilities{
				Add:  []string{"NET_BIND_SERVICE"},
				Drop: []string{"ALL"},
			}}
		},
	}, {
		name: "capabilities add rejects ALL",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].SecurityContext = &ateapi.SecurityContext{Capabilities: &ateapi.Capabilities{Add: []string{"ALL"}}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("security_context", "capabilities", "add").Index(0), nil, "")},
	}, {
		name: "lowercase capability",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].SecurityContext = &ateapi.SecurityContext{Capabilities: &ateapi.Capabilities{Drop: []string{"net_raw"}}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("security_context", "capabilities", "drop").Index(0), nil, "")},
	}, {
		name: "duplicate capability",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].SecurityContext = &ateapi.SecurityContext{Capabilities: &ateapi.Capabilities{Add: []string{"NET_BIND_SERVICE", "NET_BIND_SERVICE"}}}
		},
		want: field.ErrorList{field.Duplicate(field.NewPath("containers").Index(0).Child("security_context", "capabilities", "add").Index(1), nil)},
	}, {
		name: "too many capabilities",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			caps := make([]string, 65)
			for i := range caps {
				caps[i] = fmt.Sprintf("CAP%d", i)
			}
			tmpl.Containers[0].SecurityContext = &ateapi.SecurityContext{Capabilities: &ateapi.Capabilities{Add: caps}}
		},
		want: field.ErrorList{field.TooMany(field.NewPath("containers").Index(0).Child("security_context", "capabilities", "add"), 65, 64).WithOrigin("maxItems")},
	}, {
		name: "valid wakeup probe",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapi.WakeupProbe{
				HttpGet:        &ateapi.HttpGetAction{Path: "/healthz", Port: 8080},
				TimeoutSeconds: 60,
			}
		},
	}, {
		name: "wakeup probe missing http_get",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapi.WakeupProbe{TimeoutSeconds: 60}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("wakeup_probe", "http_get"), "")},
	}, {
		name: "missing wakeup probe timeout_seconds",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapi.WakeupProbe{HttpGet: &ateapi.HttpGetAction{Path: "/healthz", Port: 8080}}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("wakeup_probe", "timeout_seconds"), "")},
	}, {
		name: "missing wakeup probe http_get.path",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapi.WakeupProbe{HttpGet: &ateapi.HttpGetAction{Port: 8080}, TimeoutSeconds: 60}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("wakeup_probe", "http_get", "path"), "")},
	}, {
		name: "wakeup probe timeout_seconds out of range",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapi.WakeupProbe{
				HttpGet:        &ateapi.HttpGetAction{Path: "/healthz", Port: 8080},
				TimeoutSeconds: 3601,
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("wakeup_probe", "timeout_seconds"), nil, "").WithOrigin("maximum")},
	}, {
		name: "negative wakeup probe timeout_seconds",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapi.WakeupProbe{
				HttpGet:        &ateapi.HttpGetAction{Path: "/healthz", Port: 8080},
				TimeoutSeconds: -1,
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("wakeup_probe", "timeout_seconds"), nil, "").WithOrigin("minimum")},
	}, {
		name: "wakeup probe missing port",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapi.WakeupProbe{
				HttpGet:        &ateapi.HttpGetAction{Path: "/healthz"},
				TimeoutSeconds: 60,
			}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("wakeup_probe", "http_get", "port"), "")},
	}, {
		name: "negative wakeup probe port",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapi.WakeupProbe{
				HttpGet:        &ateapi.HttpGetAction{Path: "/healthz", Port: -1},
				TimeoutSeconds: 60,
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("wakeup_probe", "http_get", "port"), nil, "").WithOrigin("minimum")},
	}, {
		name: "wakeup probe port out of range",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapi.WakeupProbe{
				HttpGet:        &ateapi.HttpGetAction{Path: "/healthz", Port: 65536},
				TimeoutSeconds: 60,
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("wakeup_probe", "http_get", "port"), nil, "").WithOrigin("maximum")},
	}, {
		name: "wakeup probe path with query string",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapi.WakeupProbe{
				HttpGet:        &ateapi.HttpGetAction{Path: "/readyz?verbose=1", Port: 8080},
				TimeoutSeconds: 60,
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("wakeup_probe", "http_get", "path"), nil, "")},
	}, {
		name: "wakeup probe path not starting with slash",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].WakeupProbe = &ateapi.WakeupProbe{
				HttpGet:        &ateapi.HttpGetAction{Path: "readyz", Port: 8080},
				TimeoutSeconds: 60,
			}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("wakeup_probe", "http_get", "path"), nil, "")},
	}, {
		name: "valid volume_mount",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapi.VolumeMount{{Name: "data", MountPath: "/var/data"}}
		},
	}, {
		name: "volume_mount missing name",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapi.VolumeMount{{MountPath: "/var/data"}}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("volume_mounts").Index(0).Child("name"), "")},
	}, {
		name: "volume_mount invalid name",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapi.VolumeMount{{Name: "Data_1", MountPath: "/var/data"}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("volume_mounts").Index(0).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "volume_mount missing mount_path",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapi.VolumeMount{{Name: "data"}}
		},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("volume_mounts").Index(0).Child("mount_path"), "")},
	}, {
		name: "relative mount_path",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].VolumeMounts = []*ateapi.VolumeMount{{Name: "data", MountPath: "var/data"}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("volume_mounts").Index(0).Child("mount_path"), nil, "")},
	}, {
		name: "valid durable_dir volume",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{Name: "scratch", DurableDir: &ateapi.DurableDirVolumeSource{}}}
		},
	}, {
		name: "too many volumes",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			for i := 0; i < 33; i++ {
				tmpl.Volumes = append(tmpl.Volumes, &ateapi.Volume{Name: fmt.Sprintf("vol-%d", i), DurableDir: &ateapi.DurableDirVolumeSource{}})
			}
		},
		want: field.ErrorList{field.TooMany(field.NewPath("volumes"), 33, 32).WithOrigin("maxItems")},
	}, {
		name: "volume missing name",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{DurableDir: &ateapi.DurableDirVolumeSource{}}}
		},
		want: field.ErrorList{field.Required(field.NewPath("volumes").Index(0).Child("name"), "")},
	}, {
		name: "volume invalid name",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{Name: "Scratch_1", DurableDir: &ateapi.DurableDirVolumeSource{}}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("volumes").Index(0).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "volume with no source",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{Name: "scratch"}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("volumes").Index(0), nil, "one of").WithOrigin("union")},
	}, {
		name: "volume with two sources",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{
				Name:       "scratch",
				DurableDir: &ateapi.DurableDirVolumeSource{},
				Image:      &ateapi.ImageVolumeSource{Reference: "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
			}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("volumes").Index(0), nil, "one of").WithOrigin("union")},
	}, {
		name: "valid image volume",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{Name: "tools", Image: &ateapi.ImageVolumeSource{Reference: "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}}}
		},
	}, {
		name: "image volume missing reference",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{Name: "tools", Image: &ateapi.ImageVolumeSource{}}}
		},
		want: field.ErrorList{field.Required(field.NewPath("volumes").Index(0).Child("image", "reference"), "")},
	}, {
		name: "image volume reference missing digest",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{Name: "tools", Image: &ateapi.ImageVolumeSource{Reference: "example.com/app:v1"}}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("volumes").Index(0).Child("image", "reference"), nil, "")},
	}, {
		name: "valid external volume template",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{Name: "data", External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{CapacityBytes: 10 << 30, StorageClassName: "fast-ssd"}}}}
		},
		want:            field.ErrorList{field.Forbidden(field.NewPath("volumes").Index(0).Child("external"), "")},
		wantWithPreview: map[string]field.ErrorList{"ALL": nil},
	}, {
		name: "external volume template missing capacity",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{Name: "data", External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{StorageClassName: "fast-ssd"}}}}
		},
		want:            field.ErrorList{field.Forbidden(field.NewPath("volumes").Index(0).Child("external"), "")},
		wantWithPreview: map[string]field.ErrorList{"ALL": {field.Required(field.NewPath("volumes").Index(0).Child("external", "template", "capacity_bytes"), "")}},
	}, {
		name: "external volume template negative capacity",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{Name: "data", External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{CapacityBytes: -1, StorageClassName: "fast-ssd"}}}}
		},
		want:            field.ErrorList{field.Forbidden(field.NewPath("volumes").Index(0).Child("external"), "")},
		wantWithPreview: map[string]field.ErrorList{"ALL": {field.Invalid(field.NewPath("volumes").Index(0).Child("external", "template", "capacity_bytes"), nil, "").WithOrigin("minimum")}},
	}, {
		name: "external volume template missing storage_class_name",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{Name: "data", External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{CapacityBytes: 10 << 30}}}}
		},
		want:            field.ErrorList{field.Forbidden(field.NewPath("volumes").Index(0).Child("external"), "")},
		wantWithPreview: map[string]field.ErrorList{"ALL": {field.Required(field.NewPath("volumes").Index(0).Child("external", "template", "storage_class_name"), "")}},
	}, {
		name: "external volume template invalid storage_class_name",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Volumes = []*ateapi.Volume{{Name: "data", External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{CapacityBytes: 10 << 30, StorageClassName: "Fast SSD"}}}}
		},
		want:            field.ErrorList{field.Forbidden(field.NewPath("volumes").Index(0).Child("external"), "")},
		wantWithPreview: map[string]field.ErrorList{"ALL": {field.Invalid(field.NewPath("volumes").Index(0).Child("external", "template", "storage_class_name"), nil, "").WithOrigin("format=k8s-long-name")}},
	}, {
		name: "valid resources",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Resources = &ateapi.ResourceRequirements{Limits: resources.CPUMemory(500, 2<<30)}
		},
	}, {
		name: "negative cpu",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Resources = &ateapi.ResourceRequirements{Limits: &ateapi.Resources{CpuMillicores: -1}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("resources", "limits", "cpu_millicores"), nil, "").WithOrigin("minimum")},
	}, {
		name: "negative memory",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Resources = &ateapi.ResourceRequirements{Limits: &ateapi.Resources{MemoryBytes: -1}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("resources", "limits", "memory_bytes"), nil, "").WithOrigin("minimum")},
	}, {
		name: "cpu above maximum",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Containers[0].Resources = &ateapi.ResourceRequirements{Limits: &ateapi.Resources{CpuMillicores: 1000000}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("resources", "limits", "cpu_millicores"), nil, "").WithOrigin("maximum")},
	}, {
		name: "template-level resources validated too",
		mutate: func(tmpl *ateapi.ActorTemplate) {
			tmpl.Resources = &ateapi.ResourceRequirements{Limits: &ateapi.Resources{CpuMillicores: 1000, MemoryBytes: -1}}
		},
		want: field.ErrorList{field.Invalid(field.NewPath("resources", "limits", "memory_bytes"), nil, "").WithOrigin("minimum")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpl := validActorTemplate()
			if tt.mutate != nil {
				tt.mutate(tmpl)
			}
			assertValidate(t, func() field.ErrorList {
				op := MakeCreateOp(context.Background())
				return Validate_ActorTemplate(context.Background(), op, nil, tmpl, nil)
			}, tt.want, tt.wantWithPreview)
		})
	}
}

// validActorTemplate returns the smallest template that passes create
// validation; mutations tweak it per test case. The snapshot scopes and
// resume policy are set explicitly because TestValidateActorTemplate
// exercises validation directly, without defaulting.
func validActorTemplate(mutations ...func(*ateapi.ActorTemplate)) *ateapi.ActorTemplate {
	template := &ateapi.ActorTemplate{
		Metadata:   &ateapi.ResourceMetadata{Atespace: "ns1", Name: "tmpl-a"},
		Containers: []*ateapi.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
		SnapshotConfig: &ateapi.SnapshotConfig{
			PreferredFidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
		Resources: &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 512<<20)},
	}
	for _, m := range mutations {
		m(template)
	}
	return template
}
