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

// fakeTagClient serves GetTag from a stored tag and records the
// update it is asked to make.
type fakeTagClient struct {
	tag *ateapipb.Tag

	getCalls  int
	updateReq *ateapipb.UpdateTagRequest
}

func (f *fakeTagClient) GetTag(_ context.Context, _ *ateapipb.GetTagRequest, _ ...grpc.CallOption) (*ateapipb.Tag, error) {
	f.getCalls++
	return proto.Clone(f.tag).(*ateapipb.Tag), nil
}

func (f *fakeTagClient) UpdateTag(_ context.Context, in *ateapipb.UpdateTagRequest, _ ...grpc.CallOption) (*ateapipb.Tag, error) {
	f.updateReq = in
	// Bump the version the way a real server does, so the returned tag is
	// distinguishable from the one the caller sent.
	updated := proto.Clone(in.GetTag()).(*ateapipb.Tag)
	updated.Metadata.Version++
	return updated, nil
}

// TestUpdateTagScope checks the read-modify-write wiring: the scope
// change is written against the uid and version that were just read.
func TestUpdateTagScope(t *testing.T) {
	testTag := &ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "space-1",
			Name:     "tag-1",
			Uid:      "8ba9b6ee-2e1a-4c0f-9f4e-2f0a0f1c3d55",
			Version:  1,
		},
		Scope: ateapipb.TagScope_TAG_SCOPE_ATESPACE,
		Status: &ateapipb.TagStatus{
			Snapshot: &ateapipb.ExternalSnapshot{SnapshotUri: "gs://private/atespaces/space-1/tags/tag-1"},
		},
	}

	client := &fakeTagClient{tag: testTag}
	ref := &ateapipb.ObjectRef{Atespace: "space-1", Name: "tag-1"}
	want := ateapipb.TagScope_TAG_SCOPE_PUBLISHED

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
