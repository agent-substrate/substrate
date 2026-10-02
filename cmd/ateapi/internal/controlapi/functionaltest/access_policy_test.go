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

package functionaltest

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func wantCode(t *testing.T, op string, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("%s: code = %v, want %v (err: %v)", op, got, want, err)
	}
}

func TestGlobalAccessPolicy_CRUD(t *testing.T) {
	tc := setupTest(t, namespaceForTest("ns-global-access-policy"))
	defer tc.cleanup()
	ctx := context.Background()

	_, err := tc.client.GetGlobalAccessPolicy(ctx, &ateapipb.GetGlobalAccessPolicyRequest{})
	wantCode(t, "Get before create", err, codes.NotFound)

	created, err := tc.client.CreateGlobalAccessPolicy(ctx, &ateapipb.CreateGlobalAccessPolicyRequest{
		AccessPolicy: &ateapipb.AccessPolicy{
			Metadata: &ateapipb.ResourceMetadata{Name: "default"},
			Bindings: []*ateapipb.Binding{{Role: "owner", Members: []string{"user:alice"}}},
		},
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if md := created.GetMetadata(); md.GetName() != "default" || md.GetUid() == "" || md.GetVersion() != 1 {
		t.Errorf("created metadata = %v, want name default, a uid and version 1", md)
	}

	_, err = tc.client.CreateGlobalAccessPolicy(ctx, &ateapipb.CreateGlobalAccessPolicyRequest{
		AccessPolicy: &ateapipb.AccessPolicy{
			Metadata: &ateapipb.ResourceMetadata{Name: "default"},
			Bindings: []*ateapipb.Binding{{Role: "owner", Members: []string{"user:alice"}}},
		},
	})
	wantCode(t, "second Create", err, codes.AlreadyExists)

	got, err := tc.client.GetGlobalAccessPolicy(ctx, &ateapipb.GetGlobalAccessPolicyRequest{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("Get mismatch (-created +got):\n%s", diff)
	}

	update := proto.Clone(created).(*ateapipb.AccessPolicy)
	update.Bindings = []*ateapipb.Binding{
		{Role: "owner", Members: []string{"user:alice"}},
		{Role: "viewer", Members: []string{"user:bob"}},
	}
	updated, err := tc.client.UpdateGlobalAccessPolicy(ctx, &ateapipb.UpdateGlobalAccessPolicyRequest{AccessPolicy: update})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if updated.GetMetadata().GetVersion() != created.GetMetadata().GetVersion()+1 {
		t.Errorf("updated version = %d, want %d", updated.GetMetadata().GetVersion(), created.GetMetadata().GetVersion()+1)
	}
	if diff := cmp.Diff(update.GetBindings(), updated.GetBindings(), protocmp.Transform()); diff != "" {
		t.Errorf("updated bindings mismatch (-want +got):\n%s", diff)
	}

	// The update above consumed created's version.
	_, err = tc.client.UpdateGlobalAccessPolicy(ctx, &ateapipb.UpdateGlobalAccessPolicyRequest{AccessPolicy: update})
	wantCode(t, "stale Update", err, codes.Aborted)
}

func TestAtespaceAccessPolicy_CRUD(t *testing.T) {
	tc := setupTest(t, namespaceForTest("ns-atespace-access-policy"))
	defer tc.cleanup()
	ctx := context.Background()
	createAtespace(t, tc, "team-a")
	ref := &ateapipb.ObjectRef{Name: "team-a"}
	policy := &ateapipb.AccessPolicy{
		Metadata: &ateapipb.ResourceMetadata{Name: "default"},
		Bindings: []*ateapipb.Binding{{Role: "editor", Members: []string{"user:alice"}}},
	}

	_, err := tc.client.CreateAtespaceAccessPolicy(ctx, &ateapipb.CreateAtespaceAccessPolicyRequest{
		Atespace:     &ateapipb.ObjectRef{Name: "missing"},
		AccessPolicy: policy,
	})
	wantCode(t, "Create on missing atespace", err, codes.FailedPrecondition)

	_, err = tc.client.GetAtespaceAccessPolicy(ctx, &ateapipb.GetAtespaceAccessPolicyRequest{Atespace: ref})
	wantCode(t, "Get before create", err, codes.NotFound)

	created, err := tc.client.CreateAtespaceAccessPolicy(ctx, &ateapipb.CreateAtespaceAccessPolicyRequest{Atespace: ref, AccessPolicy: policy})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	_, err = tc.client.CreateAtespaceAccessPolicy(ctx, &ateapipb.CreateAtespaceAccessPolicyRequest{Atespace: ref, AccessPolicy: policy})
	wantCode(t, "second Create", err, codes.AlreadyExists)

	got, err := tc.client.GetAtespaceAccessPolicy(ctx, &ateapipb.GetAtespaceAccessPolicyRequest{Atespace: ref})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("Get mismatch (-created +got):\n%s", diff)
	}

	update := proto.Clone(created).(*ateapipb.AccessPolicy)
	update.Bindings = []*ateapipb.Binding{{Role: "viewer", Members: []string{"user:bob"}}}
	updated, err := tc.client.UpdateAtespaceAccessPolicy(ctx, &ateapipb.UpdateAtespaceAccessPolicyRequest{Atespace: ref, AccessPolicy: update})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if diff := cmp.Diff(update.GetBindings(), updated.GetBindings(), protocmp.Transform()); diff != "" {
		t.Errorf("updated bindings mismatch (-want +got):\n%s", diff)
	}

	_, err = tc.client.DeleteAtespaceAccessPolicy(ctx, &ateapipb.DeleteAtespaceAccessPolicyRequest{
		Atespace: ref,
		Options:  &ateapipb.DeleteOptions{Version: created.GetMetadata().GetVersion()},
	})
	wantCode(t, "Delete with stale version", err, codes.Aborted)

	if _, err := tc.client.DeleteAtespaceAccessPolicy(ctx, &ateapipb.DeleteAtespaceAccessPolicyRequest{Atespace: ref}); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err = tc.client.GetAtespaceAccessPolicy(ctx, &ateapipb.GetAtespaceAccessPolicyRequest{Atespace: ref})
	wantCode(t, "Get after delete", err, codes.NotFound)
	_, err = tc.client.DeleteAtespaceAccessPolicy(ctx, &ateapipb.DeleteAtespaceAccessPolicyRequest{Atespace: ref})
	wantCode(t, "second Delete", err, codes.NotFound)

	// Deleting the atespace removes its policy with it.
	if _, err := tc.client.CreateAtespaceAccessPolicy(ctx, &ateapipb.CreateAtespaceAccessPolicyRequest{Atespace: ref, AccessPolicy: policy}); err != nil {
		t.Fatalf("re-Create failed: %v", err)
	}
	if _, err := tc.client.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{Atespace: ref}); err != nil {
		t.Fatalf("DeleteAtespace failed: %v", err)
	}
	_, err = tc.client.GetAtespaceAccessPolicy(ctx, &ateapipb.GetAtespaceAccessPolicyRequest{Atespace: ref})
	wantCode(t, "Get after DeleteAtespace", err, codes.NotFound)
}
