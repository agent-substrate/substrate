// Copyright 2026 The Agent Substrate Authors
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
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func validExternalVolumeResource(mutate ...func(*ateapi.ExternalVolume)) *ateapi.ExternalVolume {
	v := &ateapi.ExternalVolume{
		Metadata:      &ateapi.ResourceMetadata{Atespace: "team-a", Name: "ctx"},
		VolumeId:      "nfs-server#exports#ctx",
		VolumeType:    "nfs.csi.k8s.io",
		VolumeContext: map[string]string{"server": "nfs-server", "share": "/exports/ctx"},
	}
	for _, m := range mutate {
		m(v)
	}
	return v
}

func TestValidateExternalVolumeResource(t *testing.T) {
	valid := validExternalVolumeResource

	tests := []struct {
		name string
		obj  *ateapi.ExternalVolume
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid without volume_context",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeContext = nil }),
	}, {
		name: "missing metadata",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.Metadata = nil }),
		want: field.ErrorList{field.Required(field.NewPath("metadata"), "")},
	}, {
		// An ExternalVolume is atespace-scoped.
		name: "missing metadata.atespace",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.Metadata.Atespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("metadata", "atespace"), "")},
	}, {
		name: "missing metadata.name",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.Metadata.Name = "" }),
		want: field.ErrorList{field.Required(field.NewPath("metadata", "name"), "")},
	}, {
		name: "invalid metadata.name",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.Metadata.Name = "Not A Name" }),
		want: field.ErrorList{field.Invalid(field.NewPath("metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing volume_id",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeId = "" }),
		want: field.ErrorList{field.Required(field.NewPath("volume_id"), "")},
	}, {
		name: "volume_id with a control character",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeId = "vol\x00id" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_id"), nil, "")},
	}, {
		name: "volume_id at the bound",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeId = strings.Repeat("x", 256) }),
	}, {
		name: "volume_id too long",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeId = strings.Repeat("x", 257) }),
		want: field.ErrorList{field.TooLong(field.NewPath("volume_id"), nil, 256).WithOrigin("maxLength")},
	}, {
		name: "missing volume_type",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeType = "" }),
		want: field.ErrorList{field.Required(field.NewPath("volume_type"), "")},
	}, {
		name: "volume_type that is not a driver name",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeType = "MockPlugin" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "")},
	}, {
		name: "volume_type with the substrate.io prefix",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeType = "substrate.io/mock" }),
	}, {
		name: "volume_context with many entries, as a CSI driver may return",
		obj: valid(func(v *ateapi.ExternalVolume) {
			ctxMap := make(map[string]string, 100)
			for i := range 100 {
				ctxMap[fmt.Sprintf("key-%03d", i)] = "v"
			}
			v.VolumeContext = ctxMap
		}),
	}, {
		name: "volume_context over the size bound",
		obj: valid(func(v *ateapi.ExternalVolume) {
			v.VolumeContext = map[string]string{"attachment": strings.Repeat("v", 40*1024)}
		}),
		want: field.ErrorList{field.TooLong(field.NewPath("volume_context"), nil, 40*1024)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeCreateOp(context.Background())
			assertValidateErr(t, Validate_ExternalVolume(context.Background(), op, nil, tt.obj, nil), tt.want)
		})
	}
}

// TestValidateExternalVolumeRequestPayloads covers the request validators
// the RPCs call.
func TestValidateExternalVolumeRequestPayloads(t *testing.T) {
	validRef := func() *ateapi.ObjectRef { return &ateapi.ObjectRef{Atespace: "as", Name: "nm"} }
	volumePath := field.NewPath("external_volume")

	tests := []struct {
		name     string
		gates    string
		validate func(ctx context.Context) field.ErrorList
		want     field.ErrorList
	}{
		{
			name:  "create: valid",
			gates: "ExternalVolumes",
			validate: func(ctx context.Context) field.ErrorList {
				return ValidateCreateExternalVolumeRequest(ctx, &ateapi.CreateExternalVolumeRequest{ExternalVolume: validExternalVolumeResource()})
			},
		},
		{
			name: "create: gate off",
			validate: func(ctx context.Context) field.ErrorList {
				return ValidateCreateExternalVolumeRequest(ctx, &ateapi.CreateExternalVolumeRequest{ExternalVolume: validExternalVolumeResource()})
			},
			want: field.ErrorList{field.Forbidden(volumePath, "")},
		},
		{
			name:  "create: missing external_volume",
			gates: "ExternalVolumes",
			validate: func(ctx context.Context) field.ErrorList {
				return ValidateCreateExternalVolumeRequest(ctx, &ateapi.CreateExternalVolumeRequest{})
			},
			want: field.ErrorList{field.Required(volumePath, "")},
		},
		{
			name:  "create: invalid nested volume",
			gates: "ExternalVolumes",
			validate: func(ctx context.Context) field.ErrorList {
				volume := validExternalVolumeResource(func(v *ateapi.ExternalVolume) { v.VolumeId = "" })
				return ValidateCreateExternalVolumeRequest(ctx, &ateapi.CreateExternalVolumeRequest{ExternalVolume: volume})
			},
			want: field.ErrorList{field.Required(volumePath.Child("volume_id"), "")},
		},
		{
			name: "get: valid",
			validate: func(ctx context.Context) field.ErrorList {
				return ValidateGetExternalVolumeRequest(ctx, &ateapi.GetExternalVolumeRequest{ExternalVolume: validRef()})
			},
		},
		{
			name: "get: missing external_volume",
			validate: func(ctx context.Context) field.ErrorList {
				return ValidateGetExternalVolumeRequest(ctx, &ateapi.GetExternalVolumeRequest{})
			},
			want: field.ErrorList{field.Required(volumePath, "")},
		},
		{
			name: "get: missing external_volume.atespace",
			validate: func(ctx context.Context) field.ErrorList {
				return ValidateGetExternalVolumeRequest(ctx, &ateapi.GetExternalVolumeRequest{ExternalVolume: &ateapi.ObjectRef{Name: "nm"}})
			},
			want: field.ErrorList{field.Required(volumePath.Child("atespace"), "")},
		},
		{
			name: "get: missing external_volume.name",
			validate: func(ctx context.Context) field.ErrorList {
				return ValidateGetExternalVolumeRequest(ctx, &ateapi.GetExternalVolumeRequest{ExternalVolume: &ateapi.ObjectRef{Atespace: "as"}})
			},
			want: field.ErrorList{field.Required(volumePath.Child("name"), "")},
		},
		{
			name: "delete: valid",
			validate: func(ctx context.Context) field.ErrorList {
				return ValidateDeleteExternalVolumeRequest(ctx, &ateapi.DeleteExternalVolumeRequest{ExternalVolume: validRef()})
			},
		},
		{
			name: "delete: missing external_volume",
			validate: func(ctx context.Context) field.ErrorList {
				return ValidateDeleteExternalVolumeRequest(ctx, &ateapi.DeleteExternalVolumeRequest{})
			},
			want: field.ErrorList{field.Required(volumePath, "")},
		},
		{
			name: "delete: missing external_volume.atespace",
			validate: func(ctx context.Context) field.ErrorList {
				return ValidateDeleteExternalVolumeRequest(ctx, &ateapi.DeleteExternalVolumeRequest{ExternalVolume: &ateapi.ObjectRef{Name: "nm"}})
			},
			want: field.ErrorList{field.Required(volumePath.Child("atespace"), "")},
		},
		{
			name: "delete: missing external_volume.name",
			validate: func(ctx context.Context) field.ErrorList {
				return ValidateDeleteExternalVolumeRequest(ctx, &ateapi.DeleteExternalVolumeRequest{ExternalVolume: &ateapi.ObjectRef{Atespace: "as"}})
			},
			want: field.ErrorList{field.Required(volumePath.Child("name"), "")},
		},
		{
			name: "list: valid across atespaces",
			validate: func(ctx context.Context) field.ErrorList {
				return ValidateListExternalVolumesRequest(ctx, &ateapi.ListExternalVolumesRequest{})
			},
		},
		{
			name: "list: invalid atespace",
			validate: func(ctx context.Context) field.ErrorList {
				return ValidateListExternalVolumesRequest(ctx, &ateapi.ListExternalVolumesRequest{Atespace: "AS"})
			},
			want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name: "list: negative page size",
			validate: func(ctx context.Context) field.ErrorList {
				return ValidateListExternalVolumesRequest(ctx, &ateapi.ListExternalVolumesRequest{Options: &ateapi.ListOptions{PageSize: -1}})
			},
			want: field.ErrorList{field.Invalid(field.NewPath("options", "page_size"), nil, "").WithOrigin("minimum")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setPreviewForTest(t, tt.gates)
			assertValidateErr(t, tt.validate(context.Background()), tt.want)
		})
	}
}
