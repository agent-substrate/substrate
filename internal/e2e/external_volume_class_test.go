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

package e2e

import (
	"context"
	"slices"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/resources"
)

type fakeEVCAPClient struct {
	policy        *ateapi.AccessPolicy
	createErrOnce error
	updateErrOnce error
	createCalls   int
	updateCalls   int
}

func (f *fakeEVCAPClient) GetExternalVolumeClassAccessPolicy(_ context.Context, _ *ateapi.GetExternalVolumeClassAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	if f.policy == nil {
		return nil, status.Error(codes.NotFound, "not found")
	}
	return proto.CloneOf(f.policy), nil
}

func (f *fakeEVCAPClient) CreateExternalVolumeClassAccessPolicy(_ context.Context, in *ateapi.CreateExternalVolumeClassAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	f.createCalls++
	if f.createErrOnce != nil {
		err := f.createErrOnce
		f.createErrOnce = nil
		return nil, err
	}
	f.policy = proto.CloneOf(in.GetAccessPolicy())
	f.policy.Metadata = &ateapi.ResourceMetadata{Name: resources.SingletonName, Uid: "uid-1", Version: 1}
	return proto.CloneOf(f.policy), nil
}

func (f *fakeEVCAPClient) UpdateExternalVolumeClassAccessPolicy(_ context.Context, in *ateapi.UpdateExternalVolumeClassAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	f.updateCalls++
	if f.updateErrOnce != nil {
		err := f.updateErrOnce
		f.updateErrOnce = nil
		return nil, err
	}
	f.policy = proto.CloneOf(in.GetAccessPolicy())
	f.policy.Metadata.Version++
	return proto.CloneOf(f.policy), nil
}

func TestAllowAndRevokeAtespaceOnExternalVolumeClass(t *testing.T) {
	t.Run("creates initial policy when absent", func(t *testing.T) {
		c := &fakeEVCAPClient{}
		added, err := allowAtespaceOnExternalVolumeClass(context.Background(), c, "csi-nfs", "ate-a")
		if err != nil {
			t.Fatalf("allow: %v", err)
		}
		if !added || c.createCalls != 1 {
			t.Fatalf("added=%v createCalls=%d, want true, 1", added, c.createCalls)
		}
		got := c.policy.GetBindings()[0].GetMembers()
		if !slices.Equal(got, []string{"atespace:ate-a"}) {
			t.Fatalf("members = %v, want [atespace:ate-a]", got)
		}
	})

	t.Run("idempotent when already granted", func(t *testing.T) {
		c := &fakeEVCAPClient{
			policy: &ateapi.AccessPolicy{
				Metadata: &ateapi.ResourceMetadata{Name: resources.SingletonName, Uid: "uid-1", Version: 1},
				Bindings: []*ateapi.Binding{{Role: externalVolumeClassAllowedRole, Members: []string{"atespace:ate-a"}}},
			},
		}
		added, err := allowAtespaceOnExternalVolumeClass(context.Background(), c, "csi-nfs", "ate-a")
		if err != nil {
			t.Fatalf("allow: %v", err)
		}
		if added || c.updateCalls != 0 {
			t.Fatalf("added=%v updateCalls=%d, want false, 0", added, c.updateCalls)
		}
	})

	t.Run("retries on Aborted and appends member", func(t *testing.T) {
		c := &fakeEVCAPClient{
			policy: &ateapi.AccessPolicy{
				Metadata: &ateapi.ResourceMetadata{Name: resources.SingletonName, Uid: "uid-1", Version: 1},
				Bindings: []*ateapi.Binding{{Role: externalVolumeClassAllowedRole, Members: []string{"atespace:ate-a"}}},
			},
			updateErrOnce: status.Error(codes.Aborted, "version conflict"),
		}
		added, err := allowAtespaceOnExternalVolumeClass(context.Background(), c, "csi-nfs", "ate-b")
		if err != nil {
			t.Fatalf("allow: %v", err)
		}
		if !added || c.updateCalls != 2 {
			t.Fatalf("added=%v updateCalls=%d, want true, 2", added, c.updateCalls)
		}
		got := c.policy.GetBindings()[0].GetMembers()
		if !slices.Equal(got, []string{"atespace:ate-a", "atespace:ate-b"}) {
			t.Fatalf("members = %v, want [atespace:ate-a atespace:ate-b]", got)
		}
	})

	t.Run("revokes member and drops empty binding", func(t *testing.T) {
		c := &fakeEVCAPClient{
			policy: &ateapi.AccessPolicy{
				Metadata: &ateapi.ResourceMetadata{Name: resources.SingletonName, Uid: "uid-1", Version: 1},
				Bindings: []*ateapi.Binding{{Role: externalVolumeClassAllowedRole, Members: []string{"atespace:ate-a"}}},
			},
			updateErrOnce: status.Error(codes.Aborted, "version conflict"),
		}
		if err := revokeAtespaceOnExternalVolumeClass(context.Background(), c, "csi-nfs", "ate-a"); err != nil {
			t.Fatalf("revoke: %v", err)
		}
		if c.updateCalls != 2 {
			t.Fatalf("updateCalls = %d, want 2", c.updateCalls)
		}
		if len(c.policy.GetBindings()) != 0 {
			t.Fatalf("bindings = %v, want empty", c.policy.GetBindings())
		}
	})

	t.Run("revoke is no-op when policy is absent", func(t *testing.T) {
		c := &fakeEVCAPClient{}
		if err := revokeAtespaceOnExternalVolumeClass(context.Background(), c, "csi-nfs", "ate-a"); err != nil {
			t.Fatalf("revoke: %v", err)
		}
	})
}
