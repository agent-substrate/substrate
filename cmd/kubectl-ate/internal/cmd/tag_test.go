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

package cmd

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/cobra"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// fakeTagClient serves GetTag from a stored tag and records the
// update it is asked to make.
type fakeTagClient struct {
	tag *ateapi.Tag

	getCalls  int
	updateReq *ateapi.UpdateTagRequest
}

func (f *fakeTagClient) GetTag(_ context.Context, _ *ateapi.GetTagRequest, _ ...grpc.CallOption) (*ateapi.Tag, error) {
	f.getCalls++
	return proto.Clone(f.tag).(*ateapi.Tag), nil
}

func (f *fakeTagClient) UpdateTag(_ context.Context, in *ateapi.UpdateTagRequest, _ ...grpc.CallOption) (*ateapi.Tag, error) {
	f.updateReq = in
	// Bump the version the way a real server does, so the returned tag is
	// distinguishable from the one the caller sent.
	updated := proto.Clone(in.GetTag()).(*ateapi.Tag)
	updated.Metadata.Version++
	return updated, nil
}

// TestUpdateTagScope checks the read-modify-write wiring: the scope
// change is written against the uid and version that were just read.
func TestUpdateTagScope(t *testing.T) {
	testTag := &ateapi.Tag{
		Metadata: &ateapi.ResourceMetadata{
			Atespace: "space-1",
			Name:     "tag-1",
			Uid:      "8ba9b6ee-2e1a-4c0f-9f4e-2f0a0f1c3d55",
			Version:  1,
		},
		Scope: ateapi.TagScope_TAG_SCOPE_ATESPACE,
		Status: &ateapi.TagStatus{
			Snapshot: &ateapi.Snapshot{
				Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				DurableSnapshot: &ateapi.SnapshotStorage{
					State:  ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED,
					Object: &ateapi.ObjectSnapshot{SnapshotUri: "gs://private/atespaces/space-1/tags/tag-1"},
				},
			},
		},
	}

	client := &fakeTagClient{tag: testTag}
	ref := &ateapi.ObjectRef{Atespace: "space-1", Name: "tag-1"}
	want := ateapi.TagScope_TAG_SCOPE_PUBLISHED

	resp, err := updateTagScope(context.Background(), client, ref, want)
	if err != nil {
		t.Fatalf("updateTagScope() error = %v", err)
	}

	if client.getCalls != 1 {
		t.Errorf("GetTag called %d times, want 1", client.getCalls)
	}

	wantResp := testTag
	wantResp.Scope = want
	wantResp.Metadata.Version = 2
	if diff := cmp.Diff(wantResp, resp, protocmp.Transform()); diff != "" {
		t.Errorf("returned tag mismatch (-want +got):\n%s", diff)
	}
}

func TestParseTagScope(t *testing.T) {
	if got, err := parseTagScope("published"); err != nil || got != ateapi.TagScope_TAG_SCOPE_PUBLISHED {
		t.Fatalf("parseTagScope(published) = (%v, %v)", got, err)
	}
	if _, err := parseTagScope("global"); err == nil {
		t.Fatal("parseTagScope(global) succeeded")
	}
}

// TestWithVolumesFlag checks how --with-volumes maps onto
// CreateTagRequest.volume_selection.
func TestWithVolumesFlag(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		want     *ateapi.ExternalVolumeSelection
		wantArgs []string
		wantErr  bool
	}{
		{name: "unset captures none", args: []string{"t1"}, want: nil, wantArgs: []string{"t1"}},
		{name: "bare captures all", args: []string{"t1", "--with-volumes"}, want: &ateapi.ExternalVolumeSelection{}, wantArgs: []string{"t1"}},
		{name: "bare before another flag", args: []string{"--with-volumes", "--other", "x", "t1"}, want: &ateapi.ExternalVolumeSelection{}, wantArgs: []string{"t1"}},
		{name: "names", args: []string{"t1", "--with-volumes=data,cache"}, want: &ateapi.ExternalVolumeSelection{Names: []string{"data", "cache"}}, wantArgs: []string{"t1"}},
		{name: "repeated names", args: []string{"t1", "--with-volumes=data", "--with-volumes=cache"}, want: &ateapi.ExternalVolumeSelection{Names: []string{"data", "cache"}}, wantArgs: []string{"t1"}},
		{name: "explicit all", args: []string{"t1", "--with-volumes=*"}, want: &ateapi.ExternalVolumeSelection{}, wantArgs: []string{"t1"}},
		// pflag cannot tell a value from an argument once a space separates
		// them, so the names land in args. createTagArgs explains this.
		{name: "space-separated names become an argument", args: []string{"t1", "--with-volumes", "data"}, want: &ateapi.ExternalVolumeSelection{}, wantArgs: []string{"t1", "data"}},
		{name: "empty value", args: []string{"t1", "--with-volumes="}, wantErr: true},
		{name: "all then names", args: []string{"t1", "--with-volumes", "--with-volumes=data"}, wantErr: true},
		{name: "names then all", args: []string{"t1", "--with-volumes=data", "--with-volumes"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var v volumeSelectionValue
			cmd := &cobra.Command{Use: "tag"}
			cmd.Flags().String("other", "", "")
			addWithVolumesFlag(cmd, &v)
			err := cmd.Flags().Parse(tt.args)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse(%q) error = %v, wantErr %v", tt.args, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if diff := cmp.Diff(tt.want, v.sel, protocmp.Transform()); diff != "" {
				t.Errorf("selection mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantArgs, cmd.Flags().Args()); diff != "" {
				t.Errorf("args mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
