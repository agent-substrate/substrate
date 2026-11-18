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
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateGetExternalVolumeSnapshotRequest(t *testing.T) {
	ctx := context.Background()
	ref := field.NewPath("external_volume_snapshot")
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
			wantError: field.ErrorList{field.Required(ref, "")},
		},
		{
			name:      "missing external_volume_snapshot.atespace",
			req:       &ateapi.GetExternalVolumeSnapshotRequest{ExternalVolumeSnapshot: &ateapi.ObjectRef{Name: "snap1"}},
			wantError: field.ErrorList{field.Required(ref.Child("atespace"), "")},
		},
		{
			name:      "invalid external_volume_snapshot.atespace",
			req:       &ateapi.GetExternalVolumeSnapshotRequest{ExternalVolumeSnapshot: &ateapi.ObjectRef{Atespace: "NS1", Name: "snap1"}},
			wantError: field.ErrorList{field.Invalid(ref.Child("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name:      "missing external_volume_snapshot.name",
			req:       &ateapi.GetExternalVolumeSnapshotRequest{ExternalVolumeSnapshot: &ateapi.ObjectRef{Atespace: "ns1"}},
			wantError: field.ErrorList{field.Required(ref.Child("name"), "")},
		},
		{
			name:      "invalid external_volume_snapshot.name",
			req:       &ateapi.GetExternalVolumeSnapshotRequest{ExternalVolumeSnapshot: &ateapi.ObjectRef{Atespace: "ns1", Name: "SNAP1"}},
			wantError: field.ErrorList{field.Invalid(ref.Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
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
		{
			name: "positive page_size",
			req:  &ateapi.ListExternalVolumeSnapshotsRequest{Atespace: "ns1", Options: &ateapi.ListOptions{PageSize: 10}},
		},
		{
			name:      "negative page_size",
			req:       &ateapi.ListExternalVolumeSnapshotsRequest{Atespace: "ns1", Options: &ateapi.ListOptions{PageSize: -1}},
			wantError: field.ErrorList{field.Invalid(field.NewPath("options", "page_size"), nil, "").WithOrigin("minimum")},
		},
		{
			name: "page_token at the limit",
			req:  &ateapi.ListExternalVolumeSnapshotsRequest{Atespace: "ns1", Options: &ateapi.ListOptions{PageToken: strings.Repeat("x", 256)}},
		},
		{
			name:      "page_token too long",
			req:       &ateapi.ListExternalVolumeSnapshotsRequest{Atespace: "ns1", Options: &ateapi.ListOptions{PageToken: strings.Repeat("x", 257)}},
			wantError: field.ErrorList{field.TooLongCharacters(field.NewPath("options", "page_token"), "", 256).WithOrigin("maxLength")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateListExternalVolumeSnapshotsRequest(ctx, tt.req), tt.wantError)
		})
	}
}

// validExternalVolumeSnapshot returns a snapshot that passes validation, with
// opts applied.
func validExternalVolumeSnapshot(opts ...func(*ateapi.ExternalVolumeSnapshot)) *ateapi.ExternalVolumeSnapshot {
	s := &ateapi.ExternalVolumeSnapshot{
		Metadata:   &ateapi.ResourceMetadata{Atespace: "ns1", Name: "substrate-snap-1"},
		DriverName: "pd.csi.storage.gke.io",
		Origin: &ateapi.ExternalVolumeSnapshotOrigin{TagCapture: &ateapi.TagVolumeSnapshotOrigin{
			Tag:              &ateapi.ObjectRef{Atespace: "ns1", Name: "tag1"},
			TagUid:           someTagUID,
			SourceVolumeName: "data",
			SourceVolumeId:   "projects/p/zones/z/disks/d",
		}},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func withStatus(status *ateapi.ExternalVolumeSnapshotStatus) func(*ateapi.ExternalVolumeSnapshot) {
	return func(s *ateapi.ExternalVolumeSnapshot) { s.Status = status }
}

func TestValidateExternalVolumeSnapshot(t *testing.T) {
	ctx := context.Background()
	origin := field.NewPath("origin", "tag_capture")
	status := field.NewPath("status")
	tests := []struct {
		name      string
		snapshot  *ateapi.ExternalVolumeSnapshot
		wantError field.ErrorList
	}{
		{
			name:     "valid",
			snapshot: validExternalVolumeSnapshot(),
		},
		{
			name:     "valid with a handle",
			snapshot: validExternalVolumeSnapshot(withStatus(&ateapi.ExternalVolumeSnapshotStatus{SnapshotId: "snap-handle", ReadyToUse: true, RestoreSizeBytes: 1})),
		},
		{
			name:     "valid with an error",
			snapshot: validExternalVolumeSnapshot(withStatus(&ateapi.ExternalVolumeSnapshotStatus{ErrorMessage: "quota exceeded"})),
		},
		{
			name:      "missing metadata",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Metadata = nil }),
			wantError: field.ErrorList{field.Required(field.NewPath("metadata"), "")},
		},
		{
			name:      "missing metadata.atespace",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Metadata.Atespace = "" }),
			wantError: field.ErrorList{field.Required(field.NewPath("metadata", "atespace"), "")},
		},
		{
			name:      "invalid metadata.name",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Metadata.Name = "Snap" }),
			wantError: field.ErrorList{field.Invalid(field.NewPath("metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name:      "missing driver_name",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.DriverName = "" }),
			wantError: field.ErrorList{field.Required(field.NewPath("driver_name"), "")},
		},
		{
			name:     "driver_name at the limit",
			snapshot: validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.DriverName = strings.Repeat("a", 63) }),
		},
		{
			name:      "driver_name too long",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.DriverName = strings.Repeat("a", 64) }),
			wantError: field.ErrorList{field.TooLongCharacters(field.NewPath("driver_name"), "", 63).WithOrigin("maxLength")},
		},
		{
			name:      "invalid driver_name",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.DriverName = "Not A Driver" }),
			wantError: field.ErrorList{field.Invalid(field.NewPath("driver_name"), nil, "")},
		},
		{
			name:      "missing origin",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin = nil }),
			wantError: field.ErrorList{field.Required(field.NewPath("origin"), "")},
		},
		{
			name:      "no origin member",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture = nil }),
			wantError: field.ErrorList{field.Invalid(field.NewPath("origin"), nil, "").WithOrigin("union")},
		},
		{
			name:      "missing origin.tag_capture.tag",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture.Tag = nil }),
			wantError: field.ErrorList{field.Required(origin.Child("tag"), "")},
		},
		{
			name:      "missing origin.tag_capture.tag.name",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture.Tag.Name = "" }),
			wantError: field.ErrorList{field.Required(origin.Child("tag", "name"), "")},
		},
		{
			name:      "missing origin.tag_capture.tag.atespace",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture.Tag.Atespace = "" }),
			wantError: field.ErrorList{field.Required(origin.Child("tag", "atespace"), "")},
		},
		{
			name:      "tag in another atespace",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture.Tag.Atespace = "ns2" }),
			wantError: field.ErrorList{field.Invalid(origin.Child("tag", "atespace"), nil, "")},
		},
		{
			name:      "missing tag_uid",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture.TagUid = "" }),
			wantError: field.ErrorList{field.Required(origin.Child("tag_uid"), "")},
		},
		{
			name:      "invalid tag_uid",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture.TagUid = "not-a-uuid" }),
			wantError: field.ErrorList{field.Invalid(origin.Child("tag_uid"), nil, "").WithOrigin("format=k8s-uuid")},
		},
		{
			name:      "missing source_volume_name",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture.SourceVolumeName = "" }),
			wantError: field.ErrorList{field.Required(origin.Child("source_volume_name"), "")},
		},
		{
			name:      "invalid source_volume_name",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture.SourceVolumeName = "Data" }),
			wantError: field.ErrorList{field.Invalid(origin.Child("source_volume_name"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name:      "missing source_volume_id",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture.SourceVolumeId = "" }),
			wantError: field.ErrorList{field.Required(origin.Child("source_volume_id"), "")},
		},
		{
			name:     "source_volume_id at the limit",
			snapshot: validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture.SourceVolumeId = strings.Repeat("é", 256) }),
		},
		{
			name:      "source_volume_id too long",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture.SourceVolumeId = strings.Repeat("x", 257) }),
			wantError: field.ErrorList{field.TooLongCharacters(origin.Child("source_volume_id"), "", 256).WithOrigin("maxLength")},
		},
		{
			name:      "control character in source_volume_id",
			snapshot:  validExternalVolumeSnapshot(func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture.SourceVolumeId = "disk\x00" }),
			wantError: field.ErrorList{field.Invalid(origin.Child("source_volume_id"), nil, "")},
		},
		{
			name:     "snapshot_id at the limit",
			snapshot: validExternalVolumeSnapshot(withStatus(&ateapi.ExternalVolumeSnapshotStatus{SnapshotId: strings.Repeat("é", 256)})),
		},
		{
			name:      "snapshot_id too long",
			snapshot:  validExternalVolumeSnapshot(withStatus(&ateapi.ExternalVolumeSnapshotStatus{SnapshotId: strings.Repeat("x", 257)})),
			wantError: field.ErrorList{field.TooLongCharacters(status.Child("snapshot_id"), "", 256).WithOrigin("maxLength")},
		},
		{
			name:      "control character in snapshot_id",
			snapshot:  validExternalVolumeSnapshot(withStatus(&ateapi.ExternalVolumeSnapshotStatus{SnapshotId: "snap\x7f"})),
			wantError: field.ErrorList{field.Invalid(status.Child("snapshot_id"), nil, "")},
		},
		{
			name:      "negative restore_size_bytes",
			snapshot:  validExternalVolumeSnapshot(withStatus(&ateapi.ExternalVolumeSnapshotStatus{SnapshotId: "snap-handle", RestoreSizeBytes: -1})),
			wantError: field.ErrorList{field.Invalid(status.Child("restore_size_bytes"), nil, "").WithOrigin("minimum")},
		},
		{
			name:     "error_message at the limit",
			snapshot: validExternalVolumeSnapshot(withStatus(&ateapi.ExternalVolumeSnapshotStatus{ErrorMessage: strings.Repeat("é", 4096)})),
		},
		{
			name:      "error_message too long",
			snapshot:  validExternalVolumeSnapshot(withStatus(&ateapi.ExternalVolumeSnapshotStatus{ErrorMessage: strings.Repeat("x", 4097)})),
			wantError: field.ErrorList{field.TooLongCharacters(status.Child("error_message"), "", 4096).WithOrigin("maxLength")},
		},
		{
			name:      "handle and error together",
			snapshot:  validExternalVolumeSnapshot(withStatus(&ateapi.ExternalVolumeSnapshotStatus{SnapshotId: "snap-handle", ErrorMessage: "quota exceeded"})),
			wantError: field.ErrorList{field.Forbidden(status.Child("error_message"), "")},
		},
		{
			name:      "ready_to_use without a handle",
			snapshot:  validExternalVolumeSnapshot(withStatus(&ateapi.ExternalVolumeSnapshotStatus{ReadyToUse: true})),
			wantError: field.ErrorList{field.Forbidden(status.Child("ready_to_use"), "")},
		},
		{
			name:      "restore_size_bytes without a handle",
			snapshot:  validExternalVolumeSnapshot(withStatus(&ateapi.ExternalVolumeSnapshotStatus{RestoreSizeBytes: 1})),
			wantError: field.ErrorList{field.Forbidden(status.Child("restore_size_bytes"), "")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateExternalVolumeSnapshot(ctx, tt.snapshot), tt.wantError)
		})
	}
}

func TestValidateExternalVolumeSnapshotUpdate(t *testing.T) {
	ctx := context.Background()
	status := field.NewPath("status")
	pending := &ateapi.ExternalVolumeSnapshotStatus{}
	failed := &ateapi.ExternalVolumeSnapshotStatus{ErrorMessage: "quota exceeded"}
	captured := &ateapi.ExternalVolumeSnapshotStatus{SnapshotId: "snap-handle"}
	ready := &ateapi.ExternalVolumeSnapshotStatus{SnapshotId: "snap-handle", ReadyToUse: true, RestoreSizeBytes: 10 << 30}
	tests := []struct {
		name string
		// old changes the valid snapshot the update starts from.
		old       func(*ateapi.ExternalVolumeSnapshot)
		mutate    func(*ateapi.ExternalVolumeSnapshot)
		wantError field.ErrorList
	}{
		{
			name:      "driver_name",
			old:       withStatus(pending),
			mutate:    func(s *ateapi.ExternalVolumeSnapshot) { s.DriverName = "other.csi.example.com" },
			wantError: field.ErrorList{field.Invalid(field.NewPath("driver_name"), nil, "").WithOrigin("immutable")},
		},
		{
			name:      "origin",
			old:       withStatus(pending),
			mutate:    func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture.SourceVolumeName = "other" },
			wantError: field.ErrorList{field.Invalid(field.NewPath("origin"), nil, "").WithOrigin("immutable")},
		},
		{
			name:   "setting a status",
			old:    withStatus(nil),
			mutate: withStatus(captured),
		},
		{
			name:      "dropping the status",
			old:       withStatus(ready),
			mutate:    withStatus(nil),
			wantError: field.ErrorList{field.Invalid(status, nil, "").WithOrigin("update")},
		},
		{
			name:      "readiness without a handle",
			old:       withStatus(pending),
			mutate:    func(s *ateapi.ExternalVolumeSnapshot) { s.Status.ReadyToUse = true },
			wantError: field.ErrorList{field.Forbidden(status.Child("ready_to_use"), "")},
		},
		{
			name:      "size without a handle",
			old:       withStatus(pending),
			mutate:    func(s *ateapi.ExternalVolumeSnapshot) { s.Status.RestoreSizeBytes = 1 },
			wantError: field.ErrorList{field.Forbidden(status.Child("restore_size_bytes"), "")},
		},
		{
			name:   "a later error replaces the error",
			old:    withStatus(failed),
			mutate: func(s *ateapi.ExternalVolumeSnapshot) { s.Status.ErrorMessage = "other" },
		},
		{
			name:      "clearing the error without a handle",
			old:       withStatus(failed),
			mutate:    func(s *ateapi.ExternalVolumeSnapshot) { s.Status.ErrorMessage = "" },
			wantError: field.ErrorList{field.Forbidden(status.Child("error_message"), "")},
		},
		{
			name:   "recording the handle clears the error",
			old:    withStatus(failed),
			mutate: withStatus(captured),
		},
		{
			name:      "a handle that keeps the error",
			old:       withStatus(failed),
			mutate:    func(s *ateapi.ExternalVolumeSnapshot) { s.Status.SnapshotId = "snap-handle" },
			wantError: field.ErrorList{field.Forbidden(status.Child("error_message"), "")},
		},
		{
			name:      "an error while the handle is set",
			old:       withStatus(captured),
			mutate:    func(s *ateapi.ExternalVolumeSnapshot) { s.Status.ErrorMessage = "quota exceeded" },
			wantError: field.ErrorList{field.Forbidden(status.Child("error_message"), "")},
		},
		{
			name:      "clearing the error after the handle",
			old:       withStatus(&ateapi.ExternalVolumeSnapshotStatus{SnapshotId: "snap-handle", ErrorMessage: "quota exceeded"}),
			mutate:    func(s *ateapi.ExternalVolumeSnapshot) { s.Status.ErrorMessage = "" },
			wantError: field.ErrorList{field.Forbidden(status.Child("error_message"), "")},
		},
		{
			name:   "readiness and size after the handle",
			old:    withStatus(captured),
			mutate: withStatus(ready),
		},
		{
			name:      "changing the handle",
			old:       withStatus(captured),
			mutate:    func(s *ateapi.ExternalVolumeSnapshot) { s.Status.SnapshotId = "other-handle" },
			wantError: field.ErrorList{field.Invalid(status.Child("snapshot_id"), nil, "").WithOrigin("update")},
		},
		{
			name:      "clearing the handle",
			old:       withStatus(captured),
			mutate:    func(s *ateapi.ExternalVolumeSnapshot) { s.Status.SnapshotId = "" },
			wantError: field.ErrorList{field.Invalid(status.Child("snapshot_id"), nil, "").WithOrigin("update")},
		},
		{
			name:      "ready back to false",
			old:       withStatus(ready),
			mutate:    func(s *ateapi.ExternalVolumeSnapshot) { s.Status.ReadyToUse = false },
			wantError: field.ErrorList{field.Invalid(status.Child("ready_to_use"), nil, "").WithOrigin("update")},
		},
		{
			name:      "changing a known size",
			old:       withStatus(ready),
			mutate:    func(s *ateapi.ExternalVolumeSnapshot) { s.Status.RestoreSizeBytes = 1 },
			wantError: field.ErrorList{field.Invalid(status.Child("restore_size_bytes"), nil, "").WithOrigin("update")},
		},
		{
			name:      "clearing a known size",
			old:       withStatus(ready),
			mutate:    func(s *ateapi.ExternalVolumeSnapshot) { s.Status.RestoreSizeBytes = 0 },
			wantError: field.ErrorList{field.Invalid(status.Child("restore_size_bytes"), nil, "").WithOrigin("update")},
		},
		{
			name:   "metadata.atespace moved away from the tag's",
			old:    withStatus(pending),
			mutate: func(s *ateapi.ExternalVolumeSnapshot) { s.Metadata.Atespace = "ns2" },
			wantError: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "atespace"), nil, "").WithOrigin("immutable"),
				field.Invalid(field.NewPath("origin", "tag_capture", "tag", "atespace"), nil, ""),
			},
		},
		{
			name:   "origin tag atespace moved away from metadata.atespace",
			old:    withStatus(pending),
			mutate: func(s *ateapi.ExternalVolumeSnapshot) { s.Origin.TagCapture.Tag.Atespace = "ns2" },
			wantError: field.ErrorList{
				field.Invalid(field.NewPath("origin"), nil, "").WithOrigin("immutable"),
				field.Invalid(field.NewPath("origin", "tag_capture", "tag", "atespace"), nil, ""),
			},
		},
		// Unchanged values are not re-validated, so a stored snapshot that a
		// later release considers invalid can still be updated.
		{
			name: "unchanged invalid values",
			old: func(s *ateapi.ExternalVolumeSnapshot) {
				s.DriverName = "Not A Driver"
				s.Origin.TagCapture.SourceVolumeId = "disk\x00"
				s.Origin.TagCapture.Tag.Atespace = "ns2"
			},
			mutate: withStatus(failed),
		},
		// An update does not re-check a status rule whose fields it left
		// unchanged.
		{
			name:   "unchanged handle and error",
			old:    withStatus(&ateapi.ExternalVolumeSnapshotStatus{SnapshotId: "snap-handle", ErrorMessage: "quota exceeded"}),
			mutate: func(s *ateapi.ExternalVolumeSnapshot) { s.Status.ReadyToUse = true },
		},
		{
			name:   "unchanged readiness without a handle",
			old:    withStatus(&ateapi.ExternalVolumeSnapshotStatus{ReadyToUse: true}),
			mutate: func(s *ateapi.ExternalVolumeSnapshot) { s.Status.ErrorMessage = "quota exceeded" },
		},
		{
			name:   "unchanged size without a handle",
			old:    withStatus(&ateapi.ExternalVolumeSnapshotStatus{RestoreSizeBytes: 1}),
			mutate: func(s *ateapi.ExternalVolumeSnapshot) { s.Status.ErrorMessage = "quota exceeded" },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldVal := validExternalVolumeSnapshot(tt.old)
			newVal := proto.CloneOf(oldVal)
			tt.mutate(newVal)
			assertValidateErr(t, ValidateExternalVolumeSnapshotUpdate(ctx, newVal, oldVal), tt.wantError)
		})
	}
}
