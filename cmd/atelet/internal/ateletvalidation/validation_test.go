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

package ateletvalidation

import (
	"context"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func validRequestActorSuspendRequest(mutate ...func(*ateletpb.RequestActorSuspendRequest)) *ateletpb.RequestActorSuspendRequest {
	r := &ateletpb.RequestActorSuspendRequest{
		ActorAtespace: "team-a",
		ActorName:     "actor-1",
		ActorUid:      "01234567-89ab-cdef-0123-456789abcdef",
	}
	for _, m := range mutate {
		m(r)
	}
	return r
}

func TestValidateRequestActorSuspendRequest(t *testing.T) {
	valid := validRequestActorSuspendRequest

	tests := []struct {
		name string
		obj  *ateletpb.RequestActorSuspendRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing actor_atespace",
		obj:  valid(func(r *ateletpb.RequestActorSuspendRequest) { r.ActorAtespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_atespace"), "")},
	}, {
		name: "invalid actor_atespace: uppercase",
		obj:  valid(func(r *ateletpb.RequestActorSuspendRequest) { r.ActorAtespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_name",
		obj:  valid(func(r *ateletpb.RequestActorSuspendRequest) { r.ActorName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		name: "invalid actor_name: trailing dash",
		obj:  valid(func(r *ateletpb.RequestActorSuspendRequest) { r.ActorName = "actor-" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateletpb.RequestActorSuspendRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: not a uuid",
		obj:  valid(func(r *ateletpb.RequestActorSuspendRequest) { r.ActorUid = "not-a-uuid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_RequestActorSuspendRequest(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

// TestValidateRequestActorSuspendRequestEdge covers the handler-facing
// wrapper: valid passes, invalid comes back as InvalidArgument.
func TestValidateRequestActorSuspendRequestEdge(t *testing.T) {
	if err := ValidateRequestActorSuspendRequest(context.Background(), validRequestActorSuspendRequest()); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	err := ValidateRequestActorSuspendRequest(context.Background(), &ateletpb.RequestActorSuspendRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request error = %v, want InvalidArgument", err)
	}
}

func validMintActorCertificateRequest(mutate ...func(*ateletpb.MintActorCertificateRequest)) *ateletpb.MintActorCertificateRequest {
	r := &ateletpb.MintActorCertificateRequest{
		ActorAtespace:             "team-a",
		ActorName:                 "actor-1",
		ActorUid:                  "01234567-89ab-cdef-0123-456789abcdef",
		CertificateSigningRequest: []byte("der-bytes"),
	}
	for _, m := range mutate {
		m(r)
	}
	return r
}

func TestValidateMintActorCertificateRequest(t *testing.T) {
	valid := validMintActorCertificateRequest

	tests := []struct {
		name string
		obj  *ateletpb.MintActorCertificateRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing actor_atespace",
		obj:  valid(func(r *ateletpb.MintActorCertificateRequest) { r.ActorAtespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_atespace"), "")},
	}, {
		name: "invalid actor_atespace: uppercase",
		obj:  valid(func(r *ateletpb.MintActorCertificateRequest) { r.ActorAtespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_name",
		obj:  valid(func(r *ateletpb.MintActorCertificateRequest) { r.ActorName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		name: "invalid actor_name: trailing dash",
		obj:  valid(func(r *ateletpb.MintActorCertificateRequest) { r.ActorName = "actor-" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateletpb.MintActorCertificateRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: not a uuid",
		obj:  valid(func(r *ateletpb.MintActorCertificateRequest) { r.ActorUid = "not-a-uuid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "missing certificate_signing_request",
		obj:  valid(func(r *ateletpb.MintActorCertificateRequest) { r.CertificateSigningRequest = nil }),
		want: field.ErrorList{field.Required(field.NewPath("certificate_signing_request"), "")},
	}, {
		name: "certificate_signing_request at the bound",
		obj: valid(func(r *ateletpb.MintActorCertificateRequest) {
			r.CertificateSigningRequest = make([]byte, 16384)
		}),
	}, {
		name: "certificate_signing_request too large",
		obj: valid(func(r *ateletpb.MintActorCertificateRequest) {
			r.CertificateSigningRequest = make([]byte, 16385)
		}),
		want: field.ErrorList{field.TooLong(field.NewPath("certificate_signing_request"), nil, 16384).WithOrigin("maxBytes")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_MintActorCertificateRequest(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

// TestValidateMintActorCertificateRequestEdge covers the handler-facing
// wrapper: valid passes, invalid comes back as InvalidArgument.
func TestValidateMintActorCertificateRequestEdge(t *testing.T) {
	if err := ValidateMintActorCertificateRequest(context.Background(), validMintActorCertificateRequest()); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	err := ValidateMintActorCertificateRequest(context.Background(), &ateletpb.MintActorCertificateRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request error = %v, want InvalidArgument", err)
	}
}

func validTerminateRequest(mutate ...func(*ateletpb.TerminateRequest)) *ateletpb.TerminateRequest {
	r := &ateletpb.TerminateRequest{
		TargetAteomUid:        "0f9a3b1c-2d4e-5f60-7182-93a4b5c6d7e8",
		Atespace:              "team-a",
		ActorName:             "actor-1",
		ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
		ActorTemplateAtespace: "team-a",
		ActorTemplateName:     "tmpl-1",
		Spec:                  &ateletpb.WorkloadSpec{},
	}
	for _, m := range mutate {
		m(r)
	}
	return r
}

// TestValidateTerminateRequest exercises the identity header shared by every
// AteomHerder request; the other herder requests reuse the same generated
// checks for those fields.
func TestValidateTerminateRequest(t *testing.T) {
	valid := validTerminateRequest

	tests := []struct {
		name string
		obj  *ateletpb.TerminateRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing target_ateom_uid",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.TargetAteomUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("target_ateom_uid"), "")},
	}, {
		name: "missing atespace",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.Atespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		name: "missing actor_name",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.ActorName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "unset template identity is allowed",
		obj: valid(func(r *ateletpb.TerminateRequest) {
			r.ActorTemplateAtespace = ""
			r.ActorTemplateName = ""
		}),
	}, {
		name: "unset spec is allowed",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.Spec = nil }),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_TerminateRequest(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func TestValidateSetWorkerCapacityRequest(t *testing.T) {
	tests := []struct {
		name string
		obj  *ateletpb.SetWorkerCapacityRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj: &ateletpb.SetWorkerCapacityRequest{
			Capacity: &ateapipb.WorkerResources{},
		},
	}, {
		name: "missing capacity",
		obj:  &ateletpb.SetWorkerCapacityRequest{},
		want: field.ErrorList{field.Required(field.NewPath("capacity"), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_SetWorkerCapacityRequest(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func TestValidateVolume(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.Volume)) *ateletpb.Volume {
		v := &ateletpb.Volume{Name: "data", DurableDir: &ateletpb.DurableDirVolume{}}
		for _, m := range mutate {
			m(v)
		}
		return v
	}

	tests := []struct {
		name string
		obj  *ateletpb.Volume
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing name",
		obj:  valid(func(v *ateletpb.Volume) { v.Name = "" }),
		want: field.ErrorList{field.Required(field.NewPath("name"), "")},
	}, {
		name: "invalid name: uppercase",
		obj:  valid(func(v *ateletpb.Volume) { v.Name = "Data" }),
		want: field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "no source set",
		obj:  valid(func(v *ateletpb.Volume) { v.DurableDir = nil }),
		want: field.ErrorList{field.Invalid(nil, nil, "").WithOrigin("union")},
	}, {
		name: "two sources set",
		obj: valid(func(v *ateletpb.Volume) {
			v.External = &ateletpb.ExternalVolumeSource{StorageVolumeId: "vol-1"}
		}),
		want: field.ErrorList{field.Invalid(nil, nil, "").WithOrigin("union")},
	}, {
		name: "system-info source alone",
		obj: valid(func(v *ateletpb.Volume) {
			v.DurableDir = nil
			v.SystemInfo = &ateletpb.SystemInfoVolume{DataSources: []*ateletpb.SystemInfoDataSource{
				{TrustBundle: &ateletpb.TrustBundleDataSource{Name: "podcert", Path: "trust/bundle.pem"}},
			}}
		}),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_Volume(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func TestValidateSystemInfoDataSource(t *testing.T) {
	tests := []struct {
		name string
		obj  *ateletpb.SystemInfoDataSource
		want field.ErrorList
	}{{
		name: "trust bundle alone",
		obj:  &ateletpb.SystemInfoDataSource{TrustBundle: &ateletpb.TrustBundleDataSource{Name: "podcert", Path: "p"}},
	}, {
		name: "actor metadata alone",
		obj:  &ateletpb.SystemInfoDataSource{ActorMetadata: &ateletpb.ActorMetadataDataSource{}},
	}, {
		name: "neither set",
		obj:  &ateletpb.SystemInfoDataSource{},
		want: field.ErrorList{field.Invalid(nil, nil, "").WithOrigin("union")},
	}, {
		name: "both set",
		obj: &ateletpb.SystemInfoDataSource{
			ActorMetadata: &ateletpb.ActorMetadataDataSource{},
			TrustBundle:   &ateletpb.TrustBundleDataSource{Name: "podcert", Path: "p"},
		},
		want: field.ErrorList{field.Invalid(nil, nil, "").WithOrigin("union")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_SystemInfoDataSource(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func TestValidateSystemInfoVolume(t *testing.T) {
	ds := func(path string) *ateletpb.SystemInfoDataSource {
		return &ateletpb.SystemInfoDataSource{TrustBundle: &ateletpb.TrustBundleDataSource{Name: "podcert", Path: path}}
	}
	tests := []struct {
		name string
		obj  *ateletpb.SystemInfoVolume
		want field.ErrorList
	}{{
		name: "valid",
		obj: &ateletpb.SystemInfoVolume{DataSources: []*ateletpb.SystemInfoDataSource{
			ds("a.pem"), ds("b.pem"),
			{ActorMetadata: &ateletpb.ActorMetadataDataSource{Items: []*ateletpb.ActorMetadataItem{
				{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "name"},
			}}},
		}},
	}, {
		name: "duplicate path across trust bundles",
		obj:  &ateletpb.SystemInfoVolume{DataSources: []*ateletpb.SystemInfoDataSource{ds("a.pem"), ds("a.pem")}},
		want: field.ErrorList{field.Duplicate(field.NewPath("data_sources").Index(1).Child("trust_bundle", "path"), nil)},
	}, {
		name: "duplicate path between bundle and metadata item",
		obj: &ateletpb.SystemInfoVolume{DataSources: []*ateletpb.SystemInfoDataSource{
			ds("name"),
			{ActorMetadata: &ateletpb.ActorMetadataDataSource{Items: []*ateletpb.ActorMetadataItem{
				{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "name"},
			}}},
		}},
		want: field.ErrorList{field.Duplicate(field.NewPath("data_sources").Index(1).Child("actor_metadata", "items").Index(0).Child("path"), nil)},
	}, {
		name: "too many data sources",
		obj: &ateletpb.SystemInfoVolume{DataSources: []*ateletpb.SystemInfoDataSource{
			ds("a"), ds("b"), ds("c"), ds("d"), ds("e"), ds("f"), ds("g"), ds("h"), ds("i"),
		}},
		want: field.ErrorList{field.TooMany(field.NewPath("data_sources"), 9, 8).WithOrigin("maxItems")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_SystemInfoVolume(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

// TestValidateWorkloadSpecVolumes exercises the volumes list through the
// parent, as the control plane's template tests do, so the union and
// uniqueness errors are asserted at their real field paths.
func TestValidateWorkloadSpecVolumes(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.WorkloadSpec)) *ateletpb.WorkloadSpec {
		s := &ateletpb.WorkloadSpec{Volumes: []*ateletpb.Volume{
			{Name: "data", DurableDir: &ateletpb.DurableDirVolume{}},
		}}
		for _, m := range mutate {
			m(s)
		}
		return s
	}

	tests := []struct {
		name string
		obj  *ateletpb.WorkloadSpec
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "no volumes: the delete flow may send containers only",
		obj:  &ateletpb.WorkloadSpec{},
	}, {
		name: "volume with no source",
		obj:  valid(func(s *ateletpb.WorkloadSpec) { s.Volumes[0].DurableDir = nil }),
		want: field.ErrorList{field.Invalid(field.NewPath("volumes").Index(0), nil, "one of").WithOrigin("union")},
	}, {
		name: "volume with two sources",
		obj: valid(func(s *ateletpb.WorkloadSpec) {
			s.Volumes[0].SystemInfo = &ateletpb.SystemInfoVolume{}
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("volumes").Index(0), nil, "one of").WithOrigin("union")},
	}, {
		name: "duplicate volume names",
		obj: valid(func(s *ateletpb.WorkloadSpec) {
			s.Volumes = append(s.Volumes, &ateletpb.Volume{Name: "data", SystemInfo: &ateletpb.SystemInfoVolume{}})
		}),
		want: field.ErrorList{field.Duplicate(field.NewPath("volumes").Index(1), nil)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_WorkloadSpec(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func TestValidateExternalVolumeSource(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.ExternalVolumeSource)) *ateletpb.ExternalVolumeSource {
		v := &ateletpb.ExternalVolumeSource{
			StorageVolumeId: "projects/p/zones/z/disks/vol-1",
			VolumeType:      "substrate.io/mock",
			VolumeContext:   map[string]string{"fsType": "ext4"},
		}
		for _, m := range mutate {
			m(v)
		}
		return v
	}

	tests := []struct {
		name string
		obj  *ateletpb.ExternalVolumeSource
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing storage_volume_id",
		obj:  valid(func(v *ateletpb.ExternalVolumeSource) { v.StorageVolumeId = "" }),
		want: field.ErrorList{field.Required(field.NewPath("storage_volume_id"), "")},
	}, {
		name: "storage_volume_id with a control character",
		obj:  valid(func(v *ateletpb.ExternalVolumeSource) { v.StorageVolumeId = "vol\x01" }),
		want: field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "")},
	}, {
		name: "storage_volume_id too long",
		obj:  valid(func(v *ateletpb.ExternalVolumeSource) { v.StorageVolumeId = strings.Repeat("x", 257) }),
		want: field.ErrorList{field.TooLong(field.NewPath("storage_volume_id"), nil, 256).WithOrigin("maxLength")},
	}, {
		name: "unset volume_type is allowed",
		obj:  valid(func(v *ateletpb.ExternalVolumeSource) { v.VolumeType = "" }),
	}, {
		name: "volume_type without the prefix",
		obj:  valid(func(v *ateletpb.ExternalVolumeSource) { v.VolumeType = "pd.csi.storage.gke.io" }),
	}, {
		name: "invalid volume_type: uppercase",
		obj:  valid(func(v *ateletpb.ExternalVolumeSource) { v.VolumeType = "substrate.io/Mock" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "")},
	}, {
		name: "volume_context key too long",
		obj: valid(func(v *ateletpb.ExternalVolumeSource) {
			v.VolumeContext = map[string]string{strings.Repeat("k", 129): "v"}
		}),
		want: field.ErrorList{field.TooLong(field.NewPath("volume_context"), nil, 128).WithOrigin("maxLength")},
	}, {
		name: "volume_context value too long",
		obj: valid(func(v *ateletpb.ExternalVolumeSource) {
			v.VolumeContext = map[string]string{"k": strings.Repeat("v", 257)}
		}),
		want: field.ErrorList{field.TooLong(field.NewPath("volume_context").Key("k"), nil, 256).WithOrigin("maxLength")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_ExternalVolumeSource(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func TestValidateImageVolumeSource(t *testing.T) {
	tests := []struct {
		name string
		obj  *ateletpb.ImageVolumeSource
		want field.ErrorList
	}{{
		name: "valid",
		obj:  &ateletpb.ImageVolumeSource{Reference: "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
	}, {
		name: "missing reference",
		obj:  &ateletpb.ImageVolumeSource{},
		want: field.ErrorList{field.Required(field.NewPath("reference"), "")},
	}, {
		name: "reference not pinned by digest",
		obj:  &ateletpb.ImageVolumeSource{Reference: "example.com/app:v1"},
		want: field.ErrorList{field.Invalid(field.NewPath("reference"), nil, "")},
	}, {
		name: "reference with a malformed digest",
		obj:  &ateletpb.ImageVolumeSource{Reference: "example.com/app@sha256:abc"},
		want: field.ErrorList{field.Invalid(field.NewPath("reference"), nil, "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_ImageVolumeSource(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func TestValidateResourceLimits(t *testing.T) {
	tests := []struct {
		name string
		obj  *ateletpb.ResourceLimits
		want field.ErrorList
	}{{
		name: "valid",
		obj:  &ateletpb.ResourceLimits{MemoryBytes: 1 << 30, CpuMillis: 500},
	}, {
		name: "zero means unset",
		obj:  &ateletpb.ResourceLimits{},
	}, {
		name: "negative memory_bytes",
		obj:  &ateletpb.ResourceLimits{MemoryBytes: -1},
		want: field.ErrorList{field.Invalid(field.NewPath("memory_bytes"), nil, "").WithOrigin("minimum")},
	}, {
		name: "negative cpu_millis",
		obj:  &ateletpb.ResourceLimits{CpuMillis: -1},
		want: field.ErrorList{field.Invalid(field.NewPath("cpu_millis"), nil, "").WithOrigin("minimum")},
	}, {
		name: "cpu_millis at the cap",
		obj:  &ateletpb.ResourceLimits{CpuMillis: 999999},
	}, {
		name: "cpu_millis above the cap",
		obj:  &ateletpb.ResourceLimits{CpuMillis: 1000000},
		want: field.ErrorList{field.Invalid(field.NewPath("cpu_millis"), nil, "").WithOrigin("maximum")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_ResourceLimits(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

// TestValidateSecurityContext exercises the capability rules through the
// parent, so errors carry their real paths.
func TestValidateSecurityContext(t *testing.T) {
	sc := func(add, drop []string) *ateletpb.SecurityContext {
		return &ateletpb.SecurityContext{Capabilities: &ateletpb.Capabilities{Add: add, Drop: drop}}
	}
	capsPath := func(kind string) *field.Path { return field.NewPath("capabilities", kind).Index(0) }

	tests := []struct {
		name string
		obj  *ateletpb.SecurityContext
		want field.ErrorList
	}{{
		name: "valid",
		obj:  sc([]string{"NET_BIND_SERVICE"}, []string{"ALL"}),
	}, {
		name: "empty",
		obj:  &ateletpb.SecurityContext{},
	}, {
		name: "add does not accept ALL",
		obj:  sc([]string{"ALL"}, nil),
		want: field.ErrorList{field.Invalid(capsPath("add"), nil, "")},
	}, {
		name: "drop accepts ALL",
		obj:  sc(nil, []string{"ALL"}),
	}, {
		name: "CAP_ prefix rejected",
		obj:  sc([]string{"CAP_NET_BIND_SERVICE"}, nil),
		want: field.ErrorList{field.Invalid(capsPath("add"), nil, "")},
	}, {
		name: "lowercase rejected",
		obj:  sc(nil, []string{"net_bind_service"}),
		want: field.ErrorList{field.Invalid(capsPath("drop"), nil, "")},
	}, {
		name: "duplicate capability rejected by the set",
		obj:  sc([]string{"SYS_TIME", "SYS_TIME"}, nil),
		want: field.ErrorList{field.Duplicate(field.NewPath("capabilities", "add").Index(1), nil)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_SecurityContext(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

// TestValidateWorkloadSpecContainers exercises the newly keyed containers
// list through the parent, as with volumes.
func TestValidateWorkloadSpecContainers(t *testing.T) {
	ctr := func(name string) *ateletpb.Container { return &ateletpb.Container{Name: name} }
	tests := []struct {
		name string
		obj  *ateletpb.WorkloadSpec
		want field.ErrorList
	}{{
		name: "valid",
		obj:  &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{ctr("main"), ctr("sidecar")}},
	}, {
		name: "missing container name",
		obj:  &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{ctr("")}},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0).Child("name"), "")},
	}, {
		name: "invalid container name: uppercase",
		obj:  &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{ctr("Main")}},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "duplicate container names",
		obj:  &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{ctr("main"), ctr("main")}},
		want: field.ErrorList{field.Duplicate(field.NewPath("containers").Index(1), nil)},
	}, {
		name: "negative limits surface through the container",
		obj: &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{
			Name:      "main",
			Resources: &ateletpb.ResourceLimits{CpuMillis: -1},
		}}},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("resources", "cpu_millis"), nil, "").WithOrigin("minimum")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_WorkloadSpec(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func TestValidateEnvEntry(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.EnvEntry)) *ateletpb.EnvEntry {
		e := &ateletpb.EnvEntry{Name: "PORT", Value: "8080"}
		for _, m := range mutate {
			m(e)
		}
		return e
	}
	tests := []struct {
		name string
		obj  *ateletpb.EnvEntry
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing name",
		obj:  valid(func(e *ateletpb.EnvEntry) { e.Name = "" }),
		want: field.ErrorList{field.Required(field.NewPath("name"), "")},
	}, {
		name: "name with equals sign",
		obj:  valid(func(e *ateletpb.EnvEntry) { e.Name = "A=B" }),
		want: field.ErrorList{field.Invalid(field.NewPath("name"), nil, "")},
	}, {
		name: "name with spaces and punctuation is allowed",
		obj:  valid(func(e *ateletpb.EnvEntry) { e.Name = "weird name!" }),
	}, {
		name: "name with a non-ASCII rune",
		obj:  valid(func(e *ateletpb.EnvEntry) { e.Name = "café" }),
		want: field.ErrorList{field.Invalid(field.NewPath("name"), nil, "")},
	}, {
		name: "name too long",
		obj:  valid(func(e *ateletpb.EnvEntry) { e.Name = strings.Repeat("N", 257) }),
		want: field.ErrorList{field.TooLong(field.NewPath("name"), nil, 256).WithOrigin("maxLength")},
	}, {
		name: "empty value is allowed",
		obj:  valid(func(e *ateletpb.EnvEntry) { e.Value = "" }),
	}, {
		name: "value too long",
		obj:  valid(func(e *ateletpb.EnvEntry) { e.Value = strings.Repeat("v", 32769) }),
		want: field.ErrorList{field.TooLong(field.NewPath("value"), nil, 32768).WithOrigin("maxLength")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_EnvEntry(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func TestValidateVolumeMount(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.VolumeMount)) *ateletpb.VolumeMount {
		m := &ateletpb.VolumeMount{Name: "data", MountPath: "/var/data"}
		for _, mu := range mutate {
			mu(m)
		}
		return m
	}
	badPath := func(p string) *ateletpb.VolumeMount {
		return valid(func(m *ateletpb.VolumeMount) { m.MountPath = p })
	}
	invalidPath := field.ErrorList{field.Invalid(field.NewPath("mount_path"), nil, "")}

	tests := []struct {
		name string
		obj  *ateletpb.VolumeMount
		want field.ErrorList
	}{
		{name: "valid", obj: valid()},
		{name: "missing name", obj: valid(func(m *ateletpb.VolumeMount) { m.Name = "" }),
			want: field.ErrorList{field.Required(field.NewPath("name"), "")}},
		{name: "invalid name: uppercase", obj: valid(func(m *ateletpb.VolumeMount) { m.Name = "Data" }),
			want: field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")}},
		{name: "missing mount_path", obj: badPath(""),
			want: field.ErrorList{field.Required(field.NewPath("mount_path"), "")}},
		{name: "relative mount_path", obj: badPath("var/data"), want: invalidPath},
		{name: "root mount_path", obj: badPath("/"), want: invalidPath},
		{name: "trailing slash", obj: badPath("/var/data/"), want: invalidPath},
		{name: "double slash", obj: badPath("/var//data"), want: invalidPath},
		{name: "colon", obj: badPath("/var/da:ta"), want: invalidPath},
		{name: "dot-dot segment", obj: badPath("/var/../etc"), want: invalidPath},
		{name: "control character", obj: badPath("/var/da\x01ta"), want: invalidPath},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_VolumeMount(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

// TestValidateContainerMountsAndEnv exercises the newly keyed env and
// volume_mounts lists through the parent, so errors carry real paths.
func TestValidateContainerMountsAndEnv(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.Container)) *ateletpb.Container {
		c := &ateletpb.Container{
			Name: "main",
			Env:  []*ateletpb.EnvEntry{{Name: "PORT", Value: "8080"}},
			VolumeMounts: []*ateletpb.VolumeMount{
				{Name: "data", MountPath: "/data"},
				{Name: "data", MountPath: "/mnt/data"},
			},
		}
		for _, m := range mutate {
			m(c)
		}
		return c
	}

	tests := []struct {
		name string
		obj  *ateletpb.Container
		want field.ErrorList
	}{{
		name: "valid: the same volume mounted at two paths",
		obj:  valid(),
	}, {
		name: "duplicate env names",
		obj: valid(func(c *ateletpb.Container) {
			c.Env = append(c.Env, &ateletpb.EnvEntry{Name: "PORT", Value: "9"})
		}),
		want: field.ErrorList{field.Duplicate(field.NewPath("env").Index(1), nil)},
	}, {
		name: "duplicate mount paths",
		obj: valid(func(c *ateletpb.Container) {
			c.VolumeMounts[1].MountPath = "/data"
		}),
		want: field.ErrorList{field.Duplicate(field.NewPath("volume_mounts").Index(1), nil)},
	}, {
		name: "nested mount paths",
		obj: valid(func(c *ateletpb.Container) {
			c.VolumeMounts[1].MountPath = "/data/nested"
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_mounts").Index(1).Child("mount_path"), nil, "")},
	}, {
		name: "nesting rejected regardless of order",
		obj: valid(func(c *ateletpb.Container) {
			c.VolumeMounts[0].MountPath = "/mnt/data/nested"
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_mounts").Index(1).Child("mount_path"), nil, "")},
	}, {
		name: "sibling paths with a shared segment prefix are allowed",
		obj: valid(func(c *ateletpb.Container) {
			c.VolumeMounts[0].MountPath = "/data/a"
			c.VolumeMounts[1].MountPath = "/data/ab"
		}),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_Container(context.Background(), op, nil, tt.obj, nil))
		})
	}
}
