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
	"slices"
	"strings"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateCreateExternalVolumeClassRequest(t *testing.T) {
	evc := field.NewPath("external_volume_class")
	tests := []struct {
		name            string
		mutate          func(*ateapi.ExternalVolumeClass)
		want            field.ErrorList
		wantWithPreview field.ErrorList
	}{{
		name: "valid",
		want: field.ErrorList{field.Forbidden(evc, "")},
	}, {
		name:            "driver_name with substrate.io/ prefix",
		mutate:          func(c *ateapi.ExternalVolumeClass) { c.DriverName = "substrate.io/mock" },
		want:            field.ErrorList{field.Forbidden(evc, "")},
		wantWithPreview: nil,
	}, {
		name:            "metadata.atespace must be empty",
		mutate:          func(c *ateapi.ExternalVolumeClass) { c.Metadata.Atespace = "as" },
		want:            field.ErrorList{field.Forbidden(evc, "")},
		wantWithPreview: field.ErrorList{field.Forbidden(evc.Child("metadata", "atespace"), "")},
	}, {
		name:            "missing driver_name",
		mutate:          func(c *ateapi.ExternalVolumeClass) { c.DriverName = "" },
		want:            field.ErrorList{field.Forbidden(evc, "")},
		wantWithPreview: field.ErrorList{field.Required(evc.Child("driver_name"), "")},
	}, {
		name:            "invalid driver_name",
		mutate:          func(c *ateapi.ExternalVolumeClass) { c.DriverName = "Bad_Driver" },
		want:            field.ErrorList{field.Forbidden(evc, "")},
		wantWithPreview: field.ErrorList{field.Invalid(evc.Child("driver_name"), nil, "")},
	}, {
		name:            "too-long driver_name",
		mutate:          func(c *ateapi.ExternalVolumeClass) { c.DriverName = "nfs.csi." + strings.Repeat("a", 56) },
		want:            field.ErrorList{field.Forbidden(evc, "")},
		wantWithPreview: field.ErrorList{field.TooLong(evc.Child("driver_name"), "", 63).WithOrigin("maxLength")},
	}, {
		name:            "too many parameters",
		mutate:          func(c *ateapi.ExternalVolumeClass) { c.Parameters = selectorLabelsOfSize(33) },
		want:            field.ErrorList{field.Forbidden(evc, "")},
		wantWithPreview: field.ErrorList{field.TooMany(evc.Child("parameters"), 33, 32).WithOrigin("maxProperties")},
	}, {
		name:            "too-long parameter value",
		mutate:          func(c *ateapi.ExternalVolumeClass) { c.Parameters = map[string]string{"k": strings.Repeat("v", 257)} },
		want:            field.ErrorList{field.Forbidden(evc, "")},
		wantWithPreview: field.ErrorList{field.TooLong(evc.Child("parameters").Key("k"), "", 256).WithOrigin("maxLength")},
	}, {
		name:            "valid mount_options",
		mutate:          func(c *ateapi.ExternalVolumeClass) { c.MountOptions = []string{"nfsvers=4.1", "noatime"} },
		want:            field.ErrorList{field.Forbidden(evc, "")},
		wantWithPreview: nil,
	}, {
		name:            "empty mount_options item",
		mutate:          func(c *ateapi.ExternalVolumeClass) { c.MountOptions = []string{""} },
		want:            field.ErrorList{field.Forbidden(evc, "")},
		wantWithPreview: field.ErrorList{field.TooShort(evc.Child("mount_options").Index(0), "", 1).WithOrigin("minLength")},
	}, {
		name:            "too-long mount_options item",
		mutate:          func(c *ateapi.ExternalVolumeClass) { c.MountOptions = []string{strings.Repeat("a", 257)} },
		want:            field.ErrorList{field.Forbidden(evc, "")},
		wantWithPreview: field.ErrorList{field.TooLong(evc.Child("mount_options").Index(0), "", 256).WithOrigin("maxLength")},
	}, {
		name:            "too many mount_options",
		mutate:          func(c *ateapi.ExternalVolumeClass) { c.MountOptions = slices.Repeat([]string{"ro"}, 33) },
		want:            field.ErrorList{field.Forbidden(evc, "")},
		wantWithPreview: field.ErrorList{field.TooMany(evc.Child("mount_options"), 33, 32).WithOrigin("maxItems")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ateapi.ExternalVolumeClass{
				Metadata:   &ateapi.ResourceMetadata{Name: "nfs"},
				DriverName: "nfs.csi.k8s.io",
			}
			if tt.mutate != nil {
				tt.mutate(c)
			}
			req := &ateapi.CreateExternalVolumeClassRequest{ExternalVolumeClass: c}
			assertValidate(t, func() field.ErrorList { return ValidateCreateExternalVolumeClassRequest(context.Background(), req) },
				tt.want, map[string]field.ErrorList{"ExternalVolumes": tt.wantWithPreview})
		})
	}
	t.Run("missing external_volume_class", func(t *testing.T) {
		assertValidateErr(t, ValidateCreateExternalVolumeClassRequest(context.Background(), &ateapi.CreateExternalVolumeClassRequest{}),
			field.ErrorList{field.Required(evc, "")})
	})
}

var externalVolumeClassRefCases = []struct {
	name string
	ref  *ateapi.ObjectRef
	want field.ErrorList
}{
	{"valid", &ateapi.ObjectRef{Name: "nfs"}, nil},
	{"missing ref", nil, field.ErrorList{field.Required(field.NewPath("external_volume_class"), "")}},
	{"atespace must be empty", &ateapi.ObjectRef{Atespace: "as", Name: "nfs"}, field.ErrorList{field.Forbidden(field.NewPath("external_volume_class", "atespace"), "")}},
	{"missing name", &ateapi.ObjectRef{}, field.ErrorList{field.Required(field.NewPath("external_volume_class", "name"), "")}},
}

func TestValidateGetExternalVolumeClassRequest(t *testing.T) {
	for _, tt := range externalVolumeClassRefCases {
		t.Run(tt.name, func(t *testing.T) {
			req := &ateapi.GetExternalVolumeClassRequest{ExternalVolumeClass: tt.ref}
			assertValidateErr(t, ValidateGetExternalVolumeClassRequest(context.Background(), req), tt.want)
		})
	}
}

func TestValidateDeleteExternalVolumeClassRequest(t *testing.T) {
	for _, tt := range externalVolumeClassRefCases {
		t.Run(tt.name, func(t *testing.T) {
			req := &ateapi.DeleteExternalVolumeClassRequest{ExternalVolumeClass: tt.ref}
			assertValidateErr(t, ValidateDeleteExternalVolumeClassRequest(context.Background(), req), tt.want)
		})
	}
}

func TestValidateListExternalVolumeClassesRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapi.ListExternalVolumeClassesRequest
		want field.ErrorList
	}{
		{"valid", &ateapi.ListExternalVolumeClassesRequest{}, nil},
		{"negative page_size", &ateapi.ListExternalVolumeClassesRequest{Options: &ateapi.ListOptions{PageSize: -1}}, field.ErrorList{field.Invalid(field.NewPath("options", "page_size"), int32(-1), "").WithOrigin("minimum")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateListExternalVolumeClassesRequest(context.Background(), tt.req), tt.want)
		})
	}
}
