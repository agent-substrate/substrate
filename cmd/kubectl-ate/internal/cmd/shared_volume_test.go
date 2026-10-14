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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

type fakeSharedVolumeClient struct {
	volume *ateapipb.SharedVolume

	getCalls  int
	updateReq *ateapipb.UpdateSharedVolumeRequest
}

func (f *fakeSharedVolumeClient) GetSharedVolume(_ context.Context, _ *ateapipb.GetSharedVolumeRequest, _ ...grpc.CallOption) (*ateapipb.SharedVolume, error) {
	f.getCalls++
	return proto.Clone(f.volume).(*ateapipb.SharedVolume), nil
}

func (f *fakeSharedVolumeClient) UpdateSharedVolume(_ context.Context, in *ateapipb.UpdateSharedVolumeRequest, _ ...grpc.CallOption) (*ateapipb.SharedVolume, error) {
	f.updateReq = in
	updated := proto.Clone(in.GetSharedVolume()).(*ateapipb.SharedVolume)
	updated.Metadata.Version++
	return updated, nil
}

func testSharedVolume() *ateapipb.SharedVolume {
	return &ateapipb.SharedVolume{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "space-1",
			Name:     "vol-1",
			Uid:      "8ba9b6ee-2e1a-4c0f-9f4e-2f0a0f1c3d55",
			Version:  1,
		},
		DeleteTrigger: ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR,
		AccessMode:    ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_ONCE,
		VolumeId:      "projects/p/zones/z/disks/substrate-8ba9b6ee",
		VolumeType:    "pd.csi.storage.gke.io",
		Status: &ateapipb.SharedVolumeStatus{
			State: ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_READY,
		},
	}
}

func TestUpdateSharedVolumeDeleteTrigger(t *testing.T) {
	client := &fakeSharedVolumeClient{volume: testSharedVolume()}
	ref := &ateapipb.ObjectRef{Atespace: "space-1", Name: "vol-1"}
	want := ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL

	resp, err := updateSharedVolumeDeleteTrigger(context.Background(), client, ref, want)
	if err != nil {
		t.Fatalf("updateSharedVolumeDeleteTrigger() error = %v", err)
	}

	if client.getCalls != 1 {
		t.Errorf("GetSharedVolume called %d times, want 1", client.getCalls)
	}

	sent := client.updateReq.GetSharedVolume().GetMetadata()
	if sent.GetUid() != "8ba9b6ee-2e1a-4c0f-9f4e-2f0a0f1c3d55" || sent.GetVersion() != 1 {
		t.Errorf("update sent uid %q version %d, want the values just read", sent.GetUid(), sent.GetVersion())
	}

	wantResp := testSharedVolume()
	wantResp.DeleteTrigger = want
	wantResp.Metadata.Version = 2
	if diff := cmp.Diff(wantResp, resp, protocmp.Transform()); diff != "" {
		t.Errorf("returned shared volume mismatch (-want +got):\n%s", diff)
	}
}

func TestParseDeleteTrigger(t *testing.T) {
	tests := []struct {
		value   string
		want    ateapipb.DeleteTrigger
		wantErr bool
	}{
		{value: "", want: ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED},
		{value: "manual", want: ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL},
		{value: "Last-Actor", want: ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR},
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

func TestParseAccessMode(t *testing.T) {
	tests := []struct {
		value   string
		want    ateapipb.AccessMode
		wantErr bool
	}{
		{value: "", want: ateapipb.AccessMode_ACCESS_MODE_UNSPECIFIED},
		{value: "rwo", want: ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_ONCE},
		{value: "ReadWriteMany", want: ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_MANY},
		{value: "rox", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			got, err := parseAccessMode(tc.value)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseAccessMode(%q) error = %v, wantErr %v", tc.value, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("parseAccessMode(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}
