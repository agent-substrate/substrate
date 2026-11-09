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
		DriverName:    "nfs.csi.k8s.io",
		VolumeContext: map[string]string{"server": "nfs-server", "share": "/exports/ctx"},
	}
	for _, m := range mutate {
		m(v)
	}
	return v
}

// volumeContextOfSize returns n distinct one-byte-value entries.
func volumeContextOfSize(n int) map[string]string {
	ctxMap := make(map[string]string, n)
	for i := range n {
		ctxMap[fmt.Sprintf("key-%04d", i)] = "v"
	}
	return ctxMap
}

func TestValidateExternalVolumeResource(t *testing.T) {
	valid := validExternalVolumeResource
	contextPath := field.NewPath("volume_context")

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
		name: "volume_id with an interior space",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeId = "vol 1" }),
	}, {
		name: "volume_id with a leading space",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeId = " vol-1" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_id"), nil, "")},
	}, {
		name: "volume_id with a trailing space",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeId = "vol-1 " }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_id"), nil, "")},
	}, {
		name: "volume_id with a tab",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeId = "vol\t1" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_id"), nil, "")},
	}, {
		name: "volume_id with a line break",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeId = "vol-1\n" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_id"), nil, "")},
	}, {
		name: "volume_id at the bound",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeId = strings.Repeat("x", 256) }),
	}, {
		name: "volume_id too long",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeId = strings.Repeat("x", 257) }),
		want: field.ErrorList{field.TooLong(field.NewPath("volume_id"), nil, 256).WithOrigin("maxLength")},
	}, {
		name: "missing driver_name",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.DriverName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("driver_name"), "")},
	}, {
		name: "driver_name that is not a driver name",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.DriverName = "MockPlugin" }),
		want: field.ErrorList{field.Invalid(field.NewPath("driver_name"), nil, "")},
	}, {
		name: "driver_name with the substrate.io prefix",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.DriverName = "substrate.io/mock" }),
	}, {
		name: "driver_name at the bound",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.DriverName = strings.Repeat("a", 63) }),
	}, {
		name: "driver_name too long",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.DriverName = strings.Repeat("a", 64) }),
		want: field.ErrorList{field.TooLong(field.NewPath("driver_name"), nil, 63).WithOrigin("maxLength")},
	}, {
		name: "volume_context with many entries, as a CSI driver may return",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeContext = volumeContextOfSize(1024) }),
	}, {
		name: "volume_context with too many entries",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeContext = volumeContextOfSize(1025) }),
		want: field.ErrorList{field.TooMany(contextPath, 1025, 1024).WithOrigin("maxProperties")},
	}, {
		name: "volume_context with an empty key",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeContext = map[string]string{"": "v"} }),
		want: field.ErrorList{field.TooShort(contextPath, "", 1).WithOrigin("minLength")},
	}, {
		name: "volume_context key at the bound",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeContext = map[string]string{strings.Repeat("k", 4096): "v"} }),
	}, {
		name: "volume_context key too long",
		obj:  valid(func(v *ateapi.ExternalVolume) { v.VolumeContext = map[string]string{strings.Repeat("k", 4097): "v"} }),
		want: field.ErrorList{field.TooLong(contextPath, nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "volume_context over the size bound",
		obj: valid(func(v *ateapi.ExternalVolume) {
			v.VolumeContext = map[string]string{"attachment": strings.Repeat("v", 40*1024)}
		}),
		want: field.ErrorList{field.TooLong(contextPath, nil, 40*1024)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeCreateOp(context.Background())
			assertValidateErr(t, Validate_ExternalVolume(context.Background(), op, nil, tt.obj, nil), tt.want)
		})
	}
}

// TestValidateExternalVolumeResource_Update covers the immutable fields. No
// RPC updates an ExternalVolume yet, so this pins what one would be allowed to
// change.
func TestValidateExternalVolumeResource_Update(t *testing.T) {
	valid := validExternalVolumeResource
	immutable := func(path ...string) field.ErrorList {
		return field.ErrorList{field.Invalid(field.NewPath(path[0], path[1:]...), nil, "").WithOrigin("immutable")}
	}

	tests := []struct {
		name   string
		oldObj *ateapi.ExternalVolume
		newObj *ateapi.ExternalVolume
		want   field.ErrorList
	}{{
		name:   "unchanged",
		oldObj: valid(),
		newObj: valid(),
	}, {
		name:   "change metadata.name",
		oldObj: valid(),
		newObj: valid(func(v *ateapi.ExternalVolume) { v.Metadata.Name = "other" }),
		want:   immutable("metadata", "name"),
	}, {
		name:   "change metadata.atespace",
		oldObj: valid(),
		newObj: valid(func(v *ateapi.ExternalVolume) { v.Metadata.Atespace = "team-b" }),
		want:   immutable("metadata", "atespace"),
	}, {
		name:   "change volume_id",
		oldObj: valid(),
		newObj: valid(func(v *ateapi.ExternalVolume) { v.VolumeId = "nfs-server#exports#other" }),
		want:   immutable("volume_id"),
	}, {
		name:   "change driver_name",
		oldObj: valid(),
		newObj: valid(func(v *ateapi.ExternalVolume) { v.DriverName = "pd.csi.storage.gke.io" }),
		want:   immutable("driver_name"),
	}, {
		name:   "change volume_context",
		oldObj: valid(),
		newObj: valid(func(v *ateapi.ExternalVolume) { v.VolumeContext["share"] = "/exports/other" }),
		want:   immutable("volume_context"),
	}, {
		name:   "clear volume_context",
		oldObj: valid(),
		newObj: valid(func(v *ateapi.ExternalVolume) { v.VolumeContext = nil }),
		want:   immutable("volume_context"),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeUpdateOp(context.Background())
			assertValidateErr(t, Validate_ExternalVolume(context.Background(), op, nil, tt.newObj, tt.oldObj), tt.want)
		})
	}
}

func TestValidateCreateExternalVolumeRequest(t *testing.T) {
	volumePath := field.NewPath("external_volume")

	tests := []struct {
		name  string
		gates string
		req   *ateapi.CreateExternalVolumeRequest
		want  field.ErrorList
	}{{
		name:  "valid",
		gates: "ExternalVolumes",
		req:   &ateapi.CreateExternalVolumeRequest{ExternalVolume: validExternalVolumeResource()},
	}, {
		name: "gate off",
		req:  &ateapi.CreateExternalVolumeRequest{ExternalVolume: validExternalVolumeResource()},
		want: field.ErrorList{field.Forbidden(volumePath, "")},
	}, {
		name:  "missing external_volume",
		gates: "ExternalVolumes",
		req:   &ateapi.CreateExternalVolumeRequest{},
		want:  field.ErrorList{field.Required(volumePath, "")},
	}, {
		name:  "invalid nested volume",
		gates: "ExternalVolumes",
		req:   &ateapi.CreateExternalVolumeRequest{ExternalVolume: validExternalVolumeResource(func(v *ateapi.ExternalVolume) { v.VolumeId = "" })},
		want:  field.ErrorList{field.Required(volumePath.Child("volume_id"), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setPreviewForTest(t, tt.gates)
			assertValidateErr(t, ValidateCreateExternalVolumeRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateGetExternalVolumeRequest(t *testing.T) {
	volumePath := field.NewPath("external_volume")

	tests := []struct {
		name string
		ref  *ateapi.ObjectRef
		want field.ErrorList
	}{{
		name: "valid",
		ref:  &ateapi.ObjectRef{Atespace: "as", Name: "nm"},
	}, {
		name: "missing external_volume",
		want: field.ErrorList{field.Required(volumePath, "")},
	}, {
		name: "missing external_volume.atespace",
		ref:  &ateapi.ObjectRef{Name: "nm"},
		want: field.ErrorList{field.Required(volumePath.Child("atespace"), "")},
	}, {
		name: "invalid external_volume.atespace",
		ref:  &ateapi.ObjectRef{Atespace: "AS", Name: "nm"},
		want: field.ErrorList{field.Invalid(volumePath.Child("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing external_volume.name",
		ref:  &ateapi.ObjectRef{Atespace: "as"},
		want: field.ErrorList{field.Required(volumePath.Child("name"), "")},
	}, {
		name: "invalid external_volume.name",
		ref:  &ateapi.ObjectRef{Atespace: "as", Name: "NM"},
		want: field.ErrorList{field.Invalid(volumePath.Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &ateapi.GetExternalVolumeRequest{ExternalVolume: tt.ref}
			assertValidateErr(t, ValidateGetExternalVolumeRequest(context.Background(), req), tt.want)
		})
	}
}

func TestValidateDeleteExternalVolumeRequest(t *testing.T) {
	volumePath := field.NewPath("external_volume")
	validRef := func() *ateapi.ObjectRef { return &ateapi.ObjectRef{Atespace: "as", Name: "nm"} }

	tests := []struct {
		name string
		req  *ateapi.DeleteExternalVolumeRequest
		want field.ErrorList
	}{{
		name: "valid",
		req:  &ateapi.DeleteExternalVolumeRequest{ExternalVolume: validRef()},
	}, {
		name: "missing external_volume",
		req:  &ateapi.DeleteExternalVolumeRequest{},
		want: field.ErrorList{field.Required(volumePath, "")},
	}, {
		name: "missing external_volume.atespace",
		req:  &ateapi.DeleteExternalVolumeRequest{ExternalVolume: &ateapi.ObjectRef{Name: "nm"}},
		want: field.ErrorList{field.Required(volumePath.Child("atespace"), "")},
	}, {
		name: "invalid external_volume.atespace",
		req:  &ateapi.DeleteExternalVolumeRequest{ExternalVolume: &ateapi.ObjectRef{Atespace: "AS", Name: "nm"}},
		want: field.ErrorList{field.Invalid(volumePath.Child("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing external_volume.name",
		req:  &ateapi.DeleteExternalVolumeRequest{ExternalVolume: &ateapi.ObjectRef{Atespace: "as"}},
		want: field.ErrorList{field.Required(volumePath.Child("name"), "")},
	}, {
		name: "invalid external_volume.name",
		req:  &ateapi.DeleteExternalVolumeRequest{ExternalVolume: &ateapi.ObjectRef{Atespace: "as", Name: "NM"}},
		want: field.ErrorList{field.Invalid(volumePath.Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "valid options",
		req: &ateapi.DeleteExternalVolumeRequest{
			ExternalVolume: validRef(),
			Options:        &ateapi.DeleteOptions{Uid: someActorUID, Version: 3},
		},
	}, {
		name: "invalid options.uid",
		req: &ateapi.DeleteExternalVolumeRequest{
			ExternalVolume: validRef(),
			Options:        &ateapi.DeleteOptions{Uid: "not-a-uuid"},
		},
		want: field.ErrorList{field.Invalid(field.NewPath("options", "uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "negative options.version",
		req: &ateapi.DeleteExternalVolumeRequest{
			ExternalVolume: validRef(),
			Options:        &ateapi.DeleteOptions{Version: -1},
		},
		want: field.ErrorList{field.Invalid(field.NewPath("options", "version"), nil, "").WithOrigin("minimum")},
	}, {
		// Zero values waive the guards, so they are never validated for shape.
		name: "zero options are waived, not validated",
		req: &ateapi.DeleteExternalVolumeRequest{
			ExternalVolume: validRef(),
			Options:        &ateapi.DeleteOptions{},
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateDeleteExternalVolumeRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateListExternalVolumesRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapi.ListExternalVolumesRequest
		want field.ErrorList
	}{{
		name: "valid across atespaces",
		req:  &ateapi.ListExternalVolumesRequest{},
	}, {
		name: "valid in one atespace",
		req:  &ateapi.ListExternalVolumesRequest{Atespace: "as", Options: &ateapi.ListOptions{PageSize: 10, PageToken: "tok"}},
	}, {
		name: "invalid atespace",
		req:  &ateapi.ListExternalVolumesRequest{Atespace: "AS"},
		want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "negative page size",
		req:  &ateapi.ListExternalVolumesRequest{Options: &ateapi.ListOptions{PageSize: -1}},
		want: field.ErrorList{field.Invalid(field.NewPath("options", "page_size"), nil, "").WithOrigin("minimum")},
	}, {
		name: "page token too long",
		req:  &ateapi.ListExternalVolumesRequest{Options: &ateapi.ListOptions{PageToken: strings.Repeat("t", 257)}},
		want: field.ErrorList{field.TooLongCharacters(field.NewPath("options", "page_token"), "", 256).WithOrigin("maxLength")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateListExternalVolumesRequest(context.Background(), tt.req), tt.want)
		})
	}
}
