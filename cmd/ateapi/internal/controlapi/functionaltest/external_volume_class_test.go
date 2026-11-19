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
	"go.ate.dev/substrate/internal/preview"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

func newExternalVolumeClass(name string) *ateapi.ExternalVolumeClass {
	return &ateapi.ExternalVolumeClass{
		Metadata:   &ateapi.ResourceMetadata{Name: name},
		DriverName: "nfs.csi.k8s.io",
		Parameters: map[string]string{"server": "nfs.example.com"},
	}
}

func createExternalVolumeClass(t *testing.T, tc *testContext, name string) *ateapi.ExternalVolumeClass {
	t.Helper()
	created, err := tc.client.CreateExternalVolumeClass(context.Background(), &ateapi.CreateExternalVolumeClassRequest{ExternalVolumeClass: newExternalVolumeClass(name)})
	if err != nil {
		t.Fatalf("CreateExternalVolumeClass(%s) failed: %v", name, err)
	}
	return created
}

func seedMockExternalVolumeClass(t *testing.T, tc *testContext, name string) {
	t.Helper()
	if _, err := tc.client.CreateExternalVolumeClass(context.Background(), &ateapi.CreateExternalVolumeClassRequest{
		ExternalVolumeClass: &ateapi.ExternalVolumeClass{
			Metadata:   &ateapi.ResourceMetadata{Name: name},
			DriverName: "substrate.io/mock",
		},
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateExternalVolumeClass(%s) failed: %v", name, err)
	}
}

func TestCreateExternalVolumeClass(t *testing.T) {
	preview.InitForTest(t, "ExternalVolumes")
	ns := namespaceForTest("ns-create-evc")
	tc := setupPreviewTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	in := newExternalVolumeClass("nfs")
	in.Metadata.Uid = "caller-supplied-uid"
	in.Metadata.Version = 999
	created, err := tc.client.CreateExternalVolumeClass(ctx, &ateapi.CreateExternalVolumeClassRequest{ExternalVolumeClass: in})
	if err != nil {
		t.Fatalf("CreateExternalVolumeClass failed: %v", err)
	}
	if diff := cmp.Diff(newExternalVolumeClass("nfs"), created, protocmp.Transform(), ignoreServerMetadata, ignoreVersion); diff != "" {
		t.Errorf("CreateExternalVolumeClass mismatch (-want +got):\n%s", diff)
	}
	if md := created.GetMetadata(); md.GetVersion() != 1 || md.GetUid() == "" || md.GetUid() == "caller-supplied-uid" {
		t.Errorf("metadata = %v, want server-assigned uid and version 1", md)
	}

	got, err := tc.client.GetExternalVolumeClass(ctx, &ateapi.GetExternalVolumeClassRequest{ExternalVolumeClass: &ateapi.ObjectRef{Name: "nfs"}})
	if err != nil {
		t.Fatalf("GetExternalVolumeClass failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetExternalVolumeClass mismatch (-created +got):\n%s", diff)
	}

	_, err = tc.client.CreateExternalVolumeClass(ctx, &ateapi.CreateExternalVolumeClassRequest{ExternalVolumeClass: newExternalVolumeClass("nfs")})
	assertGrpcError(t, err, codes.AlreadyExists, "ExternalVolumeClass nfs already exists")

	// A client that does not opt in to preview features cannot set the field.
	_, err = tc.noOptInClient.CreateExternalVolumeClass(ctx, &ateapi.CreateExternalVolumeClassRequest{ExternalVolumeClass: newExternalVolumeClass("other")})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "external_volume_class: Forbidden")
}

func TestListExternalVolumeClasses(t *testing.T) {
	preview.InitForTest(t, "ExternalVolumes")
	ns := namespaceForTest("ns-list-evc")
	tc := setupPreviewTest(t, ns)
	defer tc.cleanup()

	want := []string{"class-a", "class-b", "class-c"}
	for _, n := range want {
		createExternalVolumeClass(t, tc, n)
	}
	var got []string
	token := ""
	for {
		resp, err := tc.client.ListExternalVolumeClasses(context.Background(), &ateapi.ListExternalVolumeClassesRequest{Options: &ateapi.ListOptions{PageSize: 2, PageToken: token}})
		if err != nil {
			t.Fatalf("ListExternalVolumeClasses failed: %v", err)
		}
		for _, c := range resp.GetExternalVolumeClasses() {
			got = append(got, c.GetMetadata().GetName())
		}
		if token = resp.GetNextPageToken(); token == "" {
			break
		}
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("listed classes mismatch (-want +got):\n%s", diff)
	}
}

func TestDeleteExternalVolumeClass(t *testing.T) {
	preview.InitForTest(t, "ExternalVolumes")
	ns := namespaceForTest("ns-delete-evc")
	tc := setupPreviewTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	ref := &ateapi.ObjectRef{Name: "nfs"}
	del := func(opts *ateapi.DeleteOptions) (*ateapi.ExternalVolumeClass, error) {
		return tc.client.DeleteExternalVolumeClass(ctx, &ateapi.DeleteExternalVolumeClassRequest{ExternalVolumeClass: ref, Options: opts})
	}

	created := createExternalVolumeClass(t, tc, "nfs")
	uid, version := created.GetMetadata().GetUid(), created.GetMetadata().GetVersion()
	_, err := del(&ateapi.DeleteOptions{Uid: uid, Version: version + 1})
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")
	_, err = del(&ateapi.DeleteOptions{Uid: foreignUID})
	assertGrpcError(t, err, codes.Aborted, "ExternalVolumeClass nfs does not have uid "+foreignUID)

	deleted, err := del(&ateapi.DeleteOptions{Uid: uid, Version: version})
	if err != nil {
		t.Fatalf("DeleteExternalVolumeClass failed: %v", err)
	}
	if deleted.GetMetadata().GetName() != "nfs" || deleted.GetMetadata().GetDeleteTime() == nil {
		t.Errorf("deleted class = %v, want nfs with delete_time", deleted.GetMetadata())
	}
	_, err = tc.client.GetExternalVolumeClass(ctx, &ateapi.GetExternalVolumeClassRequest{ExternalVolumeClass: ref})
	assertGrpcError(t, err, codes.NotFound, "ExternalVolumeClass nfs not found")
	_, err = del(nil)
	assertGrpcError(t, err, codes.NotFound, "ExternalVolumeClass nfs not found")
}

func TestValidation_ExternalVolumeClass(t *testing.T) {
	preview.InitForTest(t, "ExternalVolumes")
	ns := namespaceForTest("ns-validation-evc")
	tc := setupPreviewTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	t.Run("CreateExternalVolumeClass", func(t *testing.T) {
		bad := newExternalVolumeClass("nfs")
		bad.DriverName = "Bad_Driver"
		_, err := tc.client.CreateExternalVolumeClass(ctx, &ateapi.CreateExternalVolumeClassRequest{ExternalVolumeClass: bad})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "driver_name: Invalid value")
	})
	t.Run("GetExternalVolumeClass", func(t *testing.T) {
		_, err := tc.client.GetExternalVolumeClass(ctx, &ateapi.GetExternalVolumeClassRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "external_volume_class: Required value")
	})
	t.Run("ListExternalVolumeClasses", func(t *testing.T) {
		_, err := tc.client.ListExternalVolumeClasses(ctx, &ateapi.ListExternalVolumeClassesRequest{Options: &ateapi.ListOptions{PageSize: -1}})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "page_size: Invalid value")
	})
	t.Run("DeleteExternalVolumeClass", func(t *testing.T) {
		_, err := tc.client.DeleteExternalVolumeClass(ctx, &ateapi.DeleteExternalVolumeClassRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "external_volume_class: Required value")
	})
}
