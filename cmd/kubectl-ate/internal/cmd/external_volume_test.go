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

package cmd

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

type fakeExternalVolumeClient struct {
	volume *ateapi.ExternalVolume

	getCalls  int
	updateReq *ateapi.UpdateExternalVolumeRequest
}

func (f *fakeExternalVolumeClient) GetExternalVolume(_ context.Context, _ *ateapi.GetExternalVolumeRequest, _ ...grpc.CallOption) (*ateapi.ExternalVolume, error) {
	f.getCalls++
	return proto.Clone(f.volume).(*ateapi.ExternalVolume), nil
}

func (f *fakeExternalVolumeClient) UpdateExternalVolume(_ context.Context, in *ateapi.UpdateExternalVolumeRequest, _ ...grpc.CallOption) (*ateapi.ExternalVolume, error) {
	f.updateReq = in
	updated := proto.Clone(in.GetExternalVolume()).(*ateapi.ExternalVolume)
	updated.Metadata.Version++
	return updated, nil
}

func testExternalVolume() *ateapi.ExternalVolume {
	return &ateapi.ExternalVolume{
		Metadata: &ateapi.ResourceMetadata{
			Atespace: "space-1",
			Name:     "vol-1",
			Uid:      "8ba9b6ee-2e1a-4c0f-9f4e-2f0a0f1c3d55",
			Version:  1,
		},
		DeleteTrigger: ateapi.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR,
		VolumeId:      "projects/p/zones/z/disks/substrate-8ba9b6ee",
		VolumeType:    "pd.csi.storage.gke.io",
	}
}

func TestUpdateExternalVolumeDeleteTrigger(t *testing.T) {
	client := &fakeExternalVolumeClient{volume: testExternalVolume()}
	ref := &ateapi.ObjectRef{Atespace: "space-1", Name: "vol-1"}
	want := ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL

	resp, err := updateExternalVolumeDeleteTrigger(context.Background(), client, ref, want)
	if err != nil {
		t.Fatalf("updateExternalVolumeDeleteTrigger() error = %v", err)
	}

	if client.getCalls != 1 {
		t.Errorf("GetExternalVolume called %d times, want 1", client.getCalls)
	}

	sent := client.updateReq.GetExternalVolume().GetMetadata()
	if sent.GetUid() != "8ba9b6ee-2e1a-4c0f-9f4e-2f0a0f1c3d55" || sent.GetVersion() != 1 {
		t.Errorf("update sent uid %q version %d, want the values just read", sent.GetUid(), sent.GetVersion())
	}

	wantResp := testExternalVolume()
	wantResp.DeleteTrigger = want
	wantResp.Metadata.Version = 2
	if diff := cmp.Diff(wantResp, resp, protocmp.Transform()); diff != "" {
		t.Errorf("returned shared volume mismatch (-want +got):\n%s", diff)
	}
}

func TestParseDeleteTrigger(t *testing.T) {
	tests := []struct {
		value   string
		want    ateapi.DeleteTrigger
		wantErr bool
	}{
		{value: "", want: ateapi.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED},
		{value: "manual", want: ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL},
		{value: "Last-Actor", want: ateapi.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR},
		{value: "atespace", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			got, err := parseDeleteTrigger(tc.value)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseDeleteTrigger(%q) error = %v, wantErr %v", tc.value, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("parseDeleteTrigger(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}
