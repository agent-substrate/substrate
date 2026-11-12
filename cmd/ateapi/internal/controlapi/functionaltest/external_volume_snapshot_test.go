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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/preview"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		VolumeType: "pd.csi.storage.gke.io",
		Origin: &ateapi.ExternalVolumeSnapshotOrigin{Tag: &ateapi.TagVolumeSnapshotOrigin{
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

func TestExternalVolumeSnapshot_GetAndList(t *testing.T) {
	tests := []struct {
		name  string
		gates []string
		optIn bool
		// wantCode is the code of every call, or OK to check the results.
		wantCode codes.Code
	}{
		{name: "gate enabled, client opts in", gates: []string{"ExternalVolumes"}, optIn: true, wantCode: codes.OK},
		{name: "gate enabled, client does not opt in", gates: []string{"ExternalVolumes"}, wantCode: codes.InvalidArgument},
		{name: "gate disabled", optIn: true, wantCode: codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preview.InitForTest(t, tt.gates...)
			tc := setupPreviewTest(t, namespaceForTest("ns-external-volume-snapshot"))
			defer tc.cleanup()
			ctx := context.Background()
			client := tc.client
			if !tt.optIn {
				client = tc.noOptInClient
			}

			tag := seedTag(t, tc, "actor-1", "v1", pendingTag)
			created := seedExternalVolumeSnapshot(t, tc, tag)
			ref := &ateapi.ObjectRef{Atespace: testAtespace, Name: created.GetMetadata().GetName()}

			got, getErr := client.GetExternalVolumeSnapshot(ctx, &ateapi.GetExternalVolumeSnapshotRequest{ExternalVolumeSnapshot: ref})
			list, listErr := client.ListExternalVolumeSnapshots(ctx, &ateapi.ListExternalVolumeSnapshotsRequest{Atespace: testAtespace})
			listAll, listAllErr := client.ListExternalVolumeSnapshots(ctx, &ateapi.ListExternalVolumeSnapshotsRequest{})
			if tt.wantCode != codes.OK {
				for _, err := range []error{getErr, listErr, listAllErr} {
					if status.Code(err) != tt.wantCode || !strings.Contains(err.Error(), preview.Header) {
						t.Errorf("error = %v, want %v mentioning %s", err, tt.wantCode, preview.Header)
					}
				}
				return
			}
			if getErr != nil || listErr != nil || listAllErr != nil {
				t.Fatalf("GetExternalVolumeSnapshot = %v, ListExternalVolumeSnapshots = %v, %v; want all to succeed", getErr, listErr, listAllErr)
			}
			if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
				t.Errorf("GetExternalVolumeSnapshot mismatch (-created +got):\n%s", diff)
			}
			if diff := cmp.Diff([]*ateapi.ExternalVolumeSnapshot{created}, list.GetExternalVolumeSnapshots(), protocmp.Transform()); diff != "" {
				t.Errorf("ListExternalVolumeSnapshots mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff([]*ateapi.ExternalVolumeSnapshot{created}, listAll.GetExternalVolumeSnapshots(), protocmp.Transform()); diff != "" {
				t.Errorf("ListExternalVolumeSnapshots across all atespaces mismatch (-want +got):\n%s", diff)
			}
			_, err := client.GetExternalVolumeSnapshot(ctx, &ateapi.GetExternalVolumeSnapshotRequest{ExternalVolumeSnapshot: &ateapi.ObjectRef{Atespace: testAtespace, Name: "missing"}})
			assertGrpcError(t, err, codes.NotFound, "ExternalVolumeSnapshot test-atespace/missing not found")
			_, err = client.ListExternalVolumeSnapshots(ctx, &ateapi.ListExternalVolumeSnapshotsRequest{Atespace: testAtespace, Options: &ateapi.ListOptions{PageToken: "%%%"}})
			assertGrpcError(t, err, codes.InvalidArgument, "invalid page_token")
		})
	}
}
