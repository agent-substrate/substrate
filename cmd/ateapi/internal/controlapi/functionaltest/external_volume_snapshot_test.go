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

package functionaltest

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

// pendingTag leaves a seeded tag still being created, the only state in which
// a tag takes new snapshots.
func pendingTag(tag *ateapi.Tag) {
	tag.Status.Snapshot.DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
}

// seedExternalVolumeSnapshot stores a snapshot of volume "data" owned by tag.
func seedExternalVolumeSnapshot(t *testing.T, tc *testContext, tag *ateapi.Tag) *ateapi.ExternalVolumeSnapshot {
	t.Helper()
	md := tag.GetMetadata()
	created, err := tc.persistence.CreateExternalVolumeSnapshot(context.Background(), &ateapi.ExternalVolumeSnapshot{
		Metadata:   &ateapi.ResourceMetadata{Atespace: md.GetAtespace(), Name: "substrate-snap-" + md.GetUid() + "-data"},
		DriverName: "pd.csi.storage.gke.io",
		Origin: &ateapi.ExternalVolumeSnapshotOrigin{TagCapture: &ateapi.TagVolumeSnapshotOrigin{
			Tag:              &ateapi.ObjectRef{Atespace: md.GetAtespace(), Name: md.GetName()},
			TagUid:           md.GetUid(),
			SourceVolumeName: "data",
			SourceVolumeId:   "projects/p/zones/z/disks/data",
		}},
	})
	if err != nil {
		t.Fatalf("CreateExternalVolumeSnapshot failed: %v", err)
	}
	return created
}

// TestExternalVolumeSnapshot_GetAndList reads snapshots with no preview gate
// enabled, from a client that does not opt in: reads are not gated.
func TestExternalVolumeSnapshot_GetAndList(t *testing.T) {
	tc := setupTest(t, namespaceForTest("ns-external-volume-snapshot"))
	defer tc.cleanup()
	ctx := context.Background()
	tag := seedTag(t, tc, "actor-1", "v1", pendingTag)
	created := seedExternalVolumeSnapshot(t, tc, tag)
	ref := &ateapi.ObjectRef{Atespace: testAtespace, Name: created.GetMetadata().GetName()}

	got, err := tc.client.GetExternalVolumeSnapshot(ctx, &ateapi.GetExternalVolumeSnapshotRequest{ExternalVolumeSnapshot: ref})
	if err != nil {
		t.Fatalf("GetExternalVolumeSnapshot failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetExternalVolumeSnapshot mismatch (-created +got):\n%s", diff)
	}
	for _, atespace := range []string{testAtespace, ""} {
		list, err := tc.client.ListExternalVolumeSnapshots(ctx, &ateapi.ListExternalVolumeSnapshotsRequest{Atespace: atespace})
		if err != nil {
			t.Fatalf("ListExternalVolumeSnapshots(%q) failed: %v", atespace, err)
		}
		if diff := cmp.Diff([]*ateapi.ExternalVolumeSnapshot{created}, list.GetExternalVolumeSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("ListExternalVolumeSnapshots(%q) mismatch (-want +got):\n%s", atespace, diff)
		}
	}
	other, err := tc.client.ListExternalVolumeSnapshots(ctx, &ateapi.ListExternalVolumeSnapshotsRequest{Atespace: "other-atespace"})
	if err != nil || len(other.GetExternalVolumeSnapshots()) != 0 {
		t.Errorf("ListExternalVolumeSnapshots(other-atespace) = %v, %v; want none", other.GetExternalVolumeSnapshots(), err)
	}

	// Pages of one follow next_page_token through the RPC.
	second := seedExternalVolumeSnapshot(t, tc, seedTag(t, tc, "actor-2", "v2", pendingTag))
	want := []*ateapi.ExternalVolumeSnapshot{created, second}
	if second.GetMetadata().GetName() < created.GetMetadata().GetName() {
		want = []*ateapi.ExternalVolumeSnapshot{second, created}
	}
	for _, atespace := range []string{testAtespace, ""} {
		var paged []*ateapi.ExternalVolumeSnapshot
		var pages int
		for token := ""; ; {
			list, err := tc.client.ListExternalVolumeSnapshots(ctx, &ateapi.ListExternalVolumeSnapshotsRequest{
				Atespace: atespace,
				Options:  &ateapi.ListOptions{PageSize: 1, PageToken: token},
			})
			if err != nil {
				t.Fatalf("ListExternalVolumeSnapshots(%q) page %d failed: %v", atespace, pages, err)
			}
			pages++
			paged = append(paged, list.GetExternalVolumeSnapshots()...)
			if token = list.GetNextPageToken(); token == "" {
				break
			}
		}
		if diff := cmp.Diff(want, paged, protocmp.Transform()); diff != "" {
			t.Errorf("paged ListExternalVolumeSnapshots(%q) mismatch (-want +got):\n%s", atespace, diff)
		}
		if pages < 2 {
			t.Errorf("ListExternalVolumeSnapshots(%q) returned %d pages of 1, want at least 2", atespace, pages)
		}
	}
}

func TestExternalVolumeSnapshot_GetAndListErrors(t *testing.T) {
	tc := setupTest(t, namespaceForTest("ns-external-volume-snapshot-errors"))
	defer tc.cleanup()
	ctx := context.Background()
	tests := []struct {
		name     string
		call     func() error
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name: "get missing",
			call: func() error {
				_, err := tc.client.GetExternalVolumeSnapshot(ctx, &ateapi.GetExternalVolumeSnapshotRequest{ExternalVolumeSnapshot: &ateapi.ObjectRef{Atespace: testAtespace, Name: "missing"}})
				return err
			},
			wantCode: codes.NotFound,
			wantMsg:  "ExternalVolumeSnapshot test-atespace/missing not found",
		},
		{
			name: "get with an invalid name",
			call: func() error {
				_, err := tc.client.GetExternalVolumeSnapshot(ctx, &ateapi.GetExternalVolumeSnapshotRequest{ExternalVolumeSnapshot: &ateapi.ObjectRef{Atespace: testAtespace, Name: "SNAP1"}})
				return err
			},
			wantCode: codes.InvalidArgument,
			wantMsg:  `external_volume_snapshot.name: Invalid value: "SNAP1": a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')`,
		},
		{
			name: "list with an invalid atespace",
			call: func() error {
				_, err := tc.client.ListExternalVolumeSnapshots(ctx, &ateapi.ListExternalVolumeSnapshotsRequest{Atespace: "NS1"})
				return err
			},
			wantCode: codes.InvalidArgument,
			wantMsg:  `atespace: Invalid value: "NS1": a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')`,
		},
		{
			name: "list with an invalid page token",
			call: func() error {
				_, err := tc.client.ListExternalVolumeSnapshots(ctx, &ateapi.ListExternalVolumeSnapshotsRequest{Atespace: testAtespace, Options: &ateapi.ListOptions{PageToken: "%%%"}})
				return err
			},
			wantCode: codes.InvalidArgument,
			wantMsg:  "invalid page_token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertGrpcError(t, tt.call(), tt.wantCode, tt.wantMsg)
		})
	}
}
