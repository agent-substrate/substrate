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
	"strings"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateGetExternalVolumeSnapshotRequest(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		req       *ateapi.GetExternalVolumeSnapshotRequest
		wantError field.ErrorList
	}{
		{
			name: "valid",
			req:  &ateapi.GetExternalVolumeSnapshotRequest{ExternalVolumeSnapshot: &ateapi.ObjectRef{Atespace: "ns1", Name: "snap1"}},
		},
		{
			name:      "missing external_volume_snapshot",
			req:       &ateapi.GetExternalVolumeSnapshotRequest{},
			wantError: field.ErrorList{field.Required(field.NewPath("external_volume_snapshot"), "")},
		},
		{
			name:      "missing external_volume_snapshot.atespace",
			req:       &ateapi.GetExternalVolumeSnapshotRequest{ExternalVolumeSnapshot: &ateapi.ObjectRef{Name: "snap1"}},
			wantError: field.ErrorList{field.Required(field.NewPath("external_volume_snapshot", "atespace"), "")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateGetExternalVolumeSnapshotRequest(ctx, tt.req), tt.wantError)
		})
	}
}

func TestValidateListExternalVolumeSnapshotsRequest(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		req       *ateapi.ListExternalVolumeSnapshotsRequest
		wantError field.ErrorList
	}{
		{
			name: "all atespaces",
			req:  &ateapi.ListExternalVolumeSnapshotsRequest{},
		},
		{
			name: "one atespace",
			req:  &ateapi.ListExternalVolumeSnapshotsRequest{Atespace: "ns1"},
		},
		{
			name:      "invalid atespace",
			req:       &ateapi.ListExternalVolumeSnapshotsRequest{Atespace: "NS1"},
			wantError: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateListExternalVolumeSnapshotsRequest(ctx, tt.req), tt.wantError)
		})
	}
}

func TestValidateExternalVolumeSnapshot(t *testing.T) {
	ctx := context.Background()
	valid := func(opts ...func(*ateapi.ExternalVolumeSnapshot)) *ateapi.ExternalVolumeSnapshot {
		s := &ateapi.ExternalVolumeSnapshot{
			Metadata:   &ateapi.ResourceMetadata{Atespace: "ns1", Name: "substrate-snap-1"},
			VolumeType: "pd.csi.storage.gke.io",
			Origin: &ateapi.ExternalVolumeSnapshotOrigin{Tag: &ateapi.TagVolumeSnapshotOrigin{
				Tag:              &ateapi.ObjectRef{Atespace: "ns1", Name: "tag1"},
				TagUid:           someActorUID,
				SourceVolumeName: "data",
				SourceVolumeId:   "projects/p/zones/z/disks/d",
			}},
		}
		for _, opt := range opts {
			opt(s)
		}
		return s
	}
	origin := field.NewPath("origin", "tag")
	status := field.NewPath("status")
	tests := []struct {
		name      string
		snapshot  *ateapi.ExternalVolumeSnapshot
		wantError field.ErrorList
	}{
		{
			name:     "valid",
			snapshot: valid(),
		},
		{
			name: "valid with a handle",
			snapshot: valid(func(s *ateapi.ExternalVolumeSnapshot) {
				s.Status = &ateapi.ExternalVolumeSnapshotStatus{StorageSnapshotId: "snap-handle", ReadyToUse: true, SizeBytes: 1}
			}),
		},
		{
			name: "valid with an error",
			snapshot: valid(func(s *ateapi.ExternalVolumeSnapshot) {
				s.Status = &ateapi.ExternalVolumeSnapshotStatus{ErrorMessage: "quota exceeded"}
			}),
		},
		{
			name:      "missing metadata",
			snapshot:  valid(func(s *ateapi.ExternalVolumeSnapshot) { s.Metadata = nil }),
			wantError: field.ErrorList{field.Required(field.NewPath("metadata"), "")},
		},
		{
			name:     "missing metadata.atespace",
			snapshot: valid(func(s *ateapi.ExternalVolumeSnapshot) { s.Metadata.Atespace = "" }),
			wantError: field.ErrorList{
				field.Required(field.NewPath("metadata", "atespace"), ""),
				field.Invalid(origin.Child("tag", "atespace"), nil, ""),
			},
		},
		{
			name:      "missing volume_type",
			snapshot:  valid(func(s *ateapi.ExternalVolumeSnapshot) { s.VolumeType = "" }),
			wantError: field.ErrorList{field.Required(field.NewPath("volume_type"), "")},
		},
		{
			name:      "invalid volume_type",
			snapshot:  valid(func(s *ateapi.ExternalVolumeSnapshot) { s.VolumeType = "Not A Driver" }),
			wantError: field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "")},
		},
		{
			name:      "missing origin",
			snapshot:  valid(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin = nil }),
			wantError: field.ErrorList{field.Required(origin, "")},
		},
		{
			name:      "missing origin.tag.tag",
			snapshot:  valid(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.Tag.Tag = nil }),
			wantError: field.ErrorList{field.Required(origin.Child("tag"), "")},
		},
		{
			name:      "tag in another atespace",
			snapshot:  valid(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.Tag.Tag.Atespace = "ns2" }),
			wantError: field.ErrorList{field.Invalid(origin.Child("tag", "atespace"), nil, "")},
		},
		{
			name:      "invalid tag_uid",
			snapshot:  valid(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.Tag.TagUid = "not-a-uuid" }),
			wantError: field.ErrorList{field.Invalid(origin.Child("tag_uid"), nil, "").WithOrigin("format=k8s-uuid")},
		},
		{
			name:      "missing source_volume_name",
			snapshot:  valid(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.Tag.SourceVolumeName = "" }),
			wantError: field.ErrorList{field.Required(origin.Child("source_volume_name"), "")},
		},
		{
			name:      "invalid source_volume_name",
			snapshot:  valid(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.Tag.SourceVolumeName = "Data" }),
			wantError: field.ErrorList{field.Invalid(origin.Child("source_volume_name"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name:      "source_volume_id too long",
			snapshot:  valid(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.Tag.SourceVolumeId = strings.Repeat("x", 257) }),
			wantError: field.ErrorList{field.TooLong(origin.Child("source_volume_id"), nil, 256)},
		},
		{
			name:      "control character in source_volume_id",
			snapshot:  valid(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.Tag.SourceVolumeId = "disk\x00" }),
			wantError: field.ErrorList{field.Invalid(origin.Child("source_volume_id"), nil, "")},
		},
		{
			name: "storage_snapshot_id too long",
			snapshot: valid(func(s *ateapi.ExternalVolumeSnapshot) {
				s.Status = &ateapi.ExternalVolumeSnapshotStatus{StorageSnapshotId: strings.Repeat("x", 257)}
			}),
			wantError: field.ErrorList{field.TooLong(status.Child("storage_snapshot_id"), nil, 256)},
		},
		{
			name: "control character in storage_snapshot_id",
			snapshot: valid(func(s *ateapi.ExternalVolumeSnapshot) {
				s.Status = &ateapi.ExternalVolumeSnapshotStatus{StorageSnapshotId: "snap\x7f"}
			}),
			wantError: field.ErrorList{field.Invalid(status.Child("storage_snapshot_id"), nil, "")},
		},
		{
			name: "negative size_bytes",
			snapshot: valid(func(s *ateapi.ExternalVolumeSnapshot) {
				s.Status = &ateapi.ExternalVolumeSnapshotStatus{StorageSnapshotId: "snap-handle", SizeBytes: -1}
			}),
			wantError: field.ErrorList{field.Invalid(status.Child("size_bytes"), nil, "")},
		},
		{
			name: "ready_to_use without a handle",
			snapshot: valid(func(s *ateapi.ExternalVolumeSnapshot) {
				s.Status = &ateapi.ExternalVolumeSnapshotStatus{ReadyToUse: true}
			}),
			wantError: field.ErrorList{field.Invalid(status.Child("ready_to_use"), nil, "")},
		},
		{
			name: "size_bytes without a handle",
			snapshot: valid(func(s *ateapi.ExternalVolumeSnapshot) {
				s.Status = &ateapi.ExternalVolumeSnapshotStatus{SizeBytes: 1}
			}),
			wantError: field.ErrorList{field.Invalid(status.Child("size_bytes"), nil, "")},
		},
		{
			name: "error_message too long",
			snapshot: valid(func(s *ateapi.ExternalVolumeSnapshot) {
				s.Status = &ateapi.ExternalVolumeSnapshotStatus{ErrorMessage: strings.Repeat("x", 4097)}
			}),
			wantError: field.ErrorList{field.TooLong(status.Child("error_message"), nil, 4096)},
		},
		{
			name: "handle and error together",
			snapshot: valid(func(s *ateapi.ExternalVolumeSnapshot) {
				s.Status = &ateapi.ExternalVolumeSnapshotStatus{StorageSnapshotId: "snap-handle", ErrorMessage: "quota exceeded"}
			}),
			wantError: field.ErrorList{field.Invalid(status.Child("error_message"), nil, "")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateExternalVolumeSnapshot(ctx, tt.snapshot), tt.wantError)
		})
	}
}
