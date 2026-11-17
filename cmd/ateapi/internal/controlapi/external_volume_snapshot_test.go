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

package controlapi

import (
	"errors"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// TestServiceImplExternalVolumeSnapshotValidation checks that ServiceImpl
// validates what it stores: the whole snapshot on create, and the change on
// update.
func TestServiceImplExternalVolumeSnapshotValidation(t *testing.T) {
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	ctx := t.Context()
	svc := &ServiceImpl{store: persistence}
	tag := storetest.MustCreateTag(t, ctx, persistence, &ateapi.Tag{
		Metadata: &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "v1"},
		Scope:    ateapi.TagScope_TAG_SCOPE_ATESPACE,
		Status: &ateapi.TagStatus{Snapshot: newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			"", "v1", "gs://bucket/atespaces/"+testAtespace+"/tags/v1", ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING)},
	})
	snapshot := func(volume string) *ateapi.ExternalVolumeSnapshot {
		return &ateapi.ExternalVolumeSnapshot{
			Metadata:   &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "snap-" + volume},
			DriverName: "pd.csi.storage.gke.io",
			Origin: &ateapi.ExternalVolumeSnapshotOrigin{TagCapture: &ateapi.TagVolumeSnapshotOrigin{
				Tag:              resources.TagRefFromTag(tag).ToObjectRef(),
				TagUid:           tag.GetMetadata().GetUid(),
				SourceVolumeName: volume,
				SourceVolumeId:   "projects/p/zones/z/disks/" + volume,
			}},
		}
	}

	invalid := snapshot("invalid")
	invalid.Origin.TagCapture.SourceVolumeId = "disk\x00"
	if _, err := svc.CreateExternalVolumeSnapshot(ctx, invalid); apierror.Code(err) != codes.Internal {
		t.Errorf("CreateExternalVolumeSnapshot(invalid) = %v, want Internal", err)
	}
	if _, err := svc.GetExternalVolumeSnapshot(ctx, resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(invalid)); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetExternalVolumeSnapshot after an invalid create = %v, want ErrNotFound", err)
	}

	created, err := svc.CreateExternalVolumeSnapshot(ctx, snapshot("data"))
	if err != nil {
		t.Fatalf("CreateExternalVolumeSnapshot failed: %v", err)
	}
	ref := resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(created)
	if _, err := svc.UpdateExternalVolumeSnapshot(ctx, ref, store.PreconditionFrom(created), func(toUpdate *ateapi.ExternalVolumeSnapshot) error {
		toUpdate.Status = &ateapi.ExternalVolumeSnapshotStatus{SnapshotId: "snap\x7f"}
		return nil
	}); apierror.Code(err) != codes.Internal {
		t.Errorf("UpdateExternalVolumeSnapshot with a control character in snapshot_id = %v, want Internal", err)
	}
	if got, err := svc.GetExternalVolumeSnapshot(ctx, ref); err != nil || !proto.Equal(got, created) {
		t.Errorf("a rejected update wrote: GetExternalVolumeSnapshot = %v, %v; want %v", got, err, created)
	}
	ready, err := svc.UpdateExternalVolumeSnapshot(ctx, ref, store.PreconditionFrom(created), func(toUpdate *ateapi.ExternalVolumeSnapshot) error {
		toUpdate.Status = &ateapi.ExternalVolumeSnapshotStatus{SnapshotId: "snap-handle", ReadyToUse: true}
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateExternalVolumeSnapshot to ready failed: %v", err)
	}
	if ready.GetStatus().GetSnapshotId() != "snap-handle" {
		t.Errorf("updated snapshot = %v, want the handle recorded", ready)
	}

	// Each of these results would pass create validation.
	for _, tc := range []struct {
		name          string
		stored        *ateapi.ExternalVolumeSnapshot
		mutate        func(*ateapi.ExternalVolumeSnapshot)
		wantImmutable bool
	}{
		{
			name:   "ready back to false",
			stored: ready,
			mutate: func(s *ateapi.ExternalVolumeSnapshot) { s.Status.ReadyToUse = false },
		},
		{
			name:   "changing the handle",
			stored: ready,
			mutate: func(s *ateapi.ExternalVolumeSnapshot) { s.Status.SnapshotId = "other-handle" },
		},
		{
			name:          "changing driver_name",
			stored:        ready,
			mutate:        func(s *ateapi.ExternalVolumeSnapshot) { s.DriverName = "other.csi.example.com" },
			wantImmutable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref := resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(tc.stored)
			_, err := svc.UpdateExternalVolumeSnapshot(ctx, ref, store.PreconditionFrom(tc.stored), func(toUpdate *ateapi.ExternalVolumeSnapshot) error {
				tc.mutate(toUpdate)
				return nil
			})
			if err == nil || errors.Is(err, store.ErrImmutableField) != tc.wantImmutable {
				t.Errorf("UpdateExternalVolumeSnapshot = %v, want an error; matching ErrImmutableField = %t", err, tc.wantImmutable)
			}
			if got, err := svc.GetExternalVolumeSnapshot(ctx, ref); err != nil || !proto.Equal(got, tc.stored) {
				t.Errorf("a rejected update wrote: GetExternalVolumeSnapshot = %v, %v; want %v", got, err, tc.stored)
			}
		})
	}

	// A value stored before validation tightened stays updatable while it is
	// unchanged. The store does not check source_volume_id.
	legacy := snapshot("legacy")
	legacy.Origin.TagCapture.SourceVolumeId = "disk\x00"
	legacy, err = persistence.CreateExternalVolumeSnapshot(ctx, legacy)
	if err != nil {
		t.Fatalf("storing a legacy snapshot: %v", err)
	}
	if _, err := svc.UpdateExternalVolumeSnapshot(ctx, resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(legacy), store.PreconditionFrom(legacy), func(toUpdate *ateapi.ExternalVolumeSnapshot) error {
		toUpdate.Status = &ateapi.ExternalVolumeSnapshotStatus{ErrorMessage: "quota exceeded"}
		return nil
	}); err != nil {
		t.Errorf("UpdateExternalVolumeSnapshot of a legacy snapshot's status failed: %v", err)
	}
}
