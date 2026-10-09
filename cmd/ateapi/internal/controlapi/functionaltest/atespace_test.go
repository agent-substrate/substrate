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
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCreateAtespace_Success(t *testing.T) {
	ns := namespaceForTest("ns-create-atespace")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	resp, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{
				Name:       teamA,
				Uid:        "caller-supplied-uid",
				Version:    999,
				CreateTime: timestamppb.New(time.Unix(1, 0)),
				UpdateTime: timestamppb.New(time.Unix(1, 0)),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	md := resp.GetMetadata()
	if md.GetName() != teamA {
		t.Errorf("Name = %q, want %q", md.GetName(), teamA)
	}
	if md.GetAtespace() != "" {
		t.Errorf("Atespace = %q, want empty (global-scoped)", md.GetAtespace())
	}
	if md.GetVersion() != 1 {
		t.Errorf("Version = %d, want 1 (caller-set 999 must be ignored)", md.GetVersion())
	}
	if md.GetUid() == "" || md.GetUid() == "caller-supplied-uid" {
		t.Errorf("uid = %q, want a server-generated value", md.GetUid())
	}

	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: teamA,
				Name:     testActorID},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		}}); err != nil {
		t.Errorf("CreateActor into freshly created atespace failed: %v", err)
	}
}

func TestCreateAtespace_AlreadyExists(t *testing.T) {
	ns := namespaceForTest("ns-create-atespace-dup")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: teamA}}}); err != nil {
		t.Fatalf("first CreateAtespace failed: %v", err)
	}
	_, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: teamA}}})
	assertGrpcError(t, err, codes.AlreadyExists, "Atespace "+teamA+" already exists")
}

func TestGetAtespace_Found(t *testing.T) {
	ns := namespaceForTest("ns-get-atespace")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	created, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: teamA}}})
	if err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	resp, err := tc.client.GetAtespace(context.Background(), &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: teamA}})
	if err != nil {
		t.Fatalf("GetAtespace failed: %v", err)
	}
	if diff := cmp.Diff(created, resp, protocmp.Transform()); diff != "" {
		t.Errorf("GetAtespace mismatch (-created +got):\n%s", diff)
	}
}

func TestGetAtespace_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-get-atespace-missing")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.GetAtespace(context.Background(), &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "nope"}})
	assertGrpcError(t, err, codes.NotFound, "Atespace nope not found")
}

func TestListAtespaces(t *testing.T) {
	ns := namespaceForTest("ns-list-atespaces")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	for _, n := range []string{teamA, teamB} {
		if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: n}}}); err != nil {
			t.Fatalf("CreateAtespace(%s) failed: %v", n, err)
		}
	}
	resp, err := tc.client.ListAtespaces(context.Background(), &ateapipb.ListAtespacesRequest{})
	if err != nil {
		t.Fatalf("ListAtespaces failed: %v", err)
	}
	got := map[string]bool{}
	for _, a := range resp.GetAtespaces() {
		got[a.GetMetadata().GetName()] = true
	}
	// setupTest seeds testAtespace; team-a and team-b were created above.
	for _, n := range []string{testAtespace, teamA, teamB} {
		if !got[n] {
			t.Errorf("ListAtespaces missing %q; got %v", n, got)
		}
	}
}

func TestDeleteAtespace_Empty_Success(t *testing.T) {
	ns := namespaceForTest("ns-delete-atespace-empty")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: teamA}}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	deleted, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: teamA}})
	if err != nil {
		t.Fatalf("DeleteAtespace failed: %v", err)
	}
	// DeleteAtespace returns the deleted resource.
	if got := deleted.GetMetadata().GetName(); got != teamA {
		t.Errorf("deleted atespace name = %q, want %q", got, teamA)
	}

	_, err = tc.client.GetAtespace(context.Background(), &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: teamA}})
	assertGrpcError(t, err, codes.NotFound, "Atespace "+teamA+" not found")
}

func TestDeleteAtespace_NonEmpty_Rejected(t *testing.T) {
	ns := namespaceForTest("ns-delete-atespace-nonempty")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: teamA}}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: teamA, Name: testActorID},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	_, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: teamA}})
	assertGrpcError(t, err, codes.FailedPrecondition, "Atespace "+teamA+" is not empty")
	// The atespace must survive a rejected delete.
	if _, err := tc.client.GetAtespace(context.Background(), &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: teamA}}); err != nil {
		t.Errorf("atespace should survive a rejected delete, got %v", err)
	}
}

// TestDeleteAtespace_ScopedToTargetAtespace pins (at the RPC layer) that the
// emptiness check is scoped to the target atespace: deleting an empty atespace
// succeeds even when a different atespace holds actors.
func TestDeleteAtespace_ScopedToTargetAtespace(t *testing.T) {
	ns := namespaceForTest("ns-delete-atespace-scoped")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)
	createAtespace(t, tc, teamA)
	createAtespace(t, tc, teamB)

	// Actor only in team-b.
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: teamB, Name: testActorID},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Empty team-a deletes fine despite team-b holding an actor.
	if _, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: teamA}}); err != nil {
		t.Errorf("DeleteAtespace(%s, empty) failed: %v", teamA, err)
	}
	// team-b is still non-empty → rejected.
	_, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: teamB}})
	assertGrpcError(t, err, codes.FailedPrecondition, "Atespace "+teamB+" is not empty")
}

func TestDeleteAtespace_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-delete-atespace-missing")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "nope"}})
	assertGrpcError(t, err, codes.NotFound, "Atespace nope not found")
}

func TestValidation_Atespace(t *testing.T) {
	ns := namespaceForTest("ns-validation-atespace")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	t.Run("ListAtespaces", func(t *testing.T) {
		_, err := tc.client.ListAtespaces(context.Background(), &ateapipb.ListAtespacesRequest{PageSize: -1})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "page_size: Invalid value")
	})

	t.Run("ListAtespaces invalid token", func(t *testing.T) {
		_, err := tc.client.ListAtespaces(context.Background(), &ateapipb.ListAtespacesRequest{PageToken: "%%%"})
		assertGrpcError(t, err, codes.InvalidArgument, "invalid page_token")
	})

	t.Run("CreateAtespace", func(t *testing.T) {
		_, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "atespace: Required value")
	})

	t.Run("GetAtespace", func(t *testing.T) {
		_, err := tc.client.GetAtespace(context.Background(), &ateapipb.GetAtespaceRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "atespace: Required value")
	})

	t.Run("DeleteAtespace", func(t *testing.T) {
		_, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "atespace: Required value")
	})
}

func TestDeleteAtespace_Preconditions(t *testing.T) {
	ns := namespaceForTest("ns-delete-atespace-preconditions")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	ref := &ateapipb.ObjectRef{Name: teamA}
	create := func() *ateapipb.Atespace {
		created, err := tc.client.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: ref.GetName()}}})
		if err != nil {
			t.Fatalf("CreateAtespace failed: %v", err)
		}
		return created
	}
	del := func(opts *ateapipb.DeleteOptions) error {
		_, err := tc.client.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{Atespace: ref, Options: opts})
		return err
	}

	atespace := create()
	uid, version := atespace.GetMetadata().GetUid(), atespace.GetMetadata().GetVersion()

	assertGrpcError(t, del(&ateapipb.DeleteOptions{Version: version + 1}), codes.Aborted, "concurrent update conflict, please retry")
	assertGrpcError(t, del(&ateapipb.DeleteOptions{Uid: uid, Version: version + 1}), codes.Aborted, "concurrent update conflict, please retry")
	assertGrpcError(t, del(&ateapipb.DeleteOptions{Uid: foreignUID}), codes.Aborted, "Atespace "+teamA+" does not have uid "+foreignUID)
	assertGrpcError(t, del(&ateapipb.DeleteOptions{Uid: foreignUID, Version: version}), codes.Aborted, "Atespace "+teamA+" does not have uid "+foreignUID)
	if _, err := tc.client.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{Atespace: ref}); err != nil {
		t.Fatalf("a refused delete removed the atespace: %v", err)
	}

	if err := del(&ateapipb.DeleteOptions{Version: version}); err != nil {
		t.Fatalf("DeleteAtespace with the matching version: %v", err)
	}
	atespace = create()
	if err := del(&ateapipb.DeleteOptions{Uid: atespace.GetMetadata().GetUid()}); err != nil {
		t.Fatalf("DeleteAtespace with the matching uid: %v", err)
	}
	atespace = create()
	if err := del(&ateapipb.DeleteOptions{Uid: atespace.GetMetadata().GetUid(), Version: atespace.GetMetadata().GetVersion()}); err != nil {
		t.Fatalf("DeleteAtespace with both guards: %v", err)
	}
	_, err := tc.client.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{Atespace: ref})
	assertGrpcError(t, err, codes.NotFound, "Atespace "+teamA+" not found")
}
