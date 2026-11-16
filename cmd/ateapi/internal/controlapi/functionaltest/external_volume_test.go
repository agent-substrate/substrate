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
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/preview"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

func newExternalVolume(name string) *ateapi.ExternalVolume {
	return &ateapi.ExternalVolume{
		Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: name},
		VolumeId:      "nfs-server#exports#" + name,
		VolumeType:    "nfs.csi.k8s.io",
		VolumeContext: map[string]string{"server": "nfs-server.default.svc.cluster.local", "share": "/exports/" + name},
	}
}

func createExternalVolume(t *testing.T, tc *testContext, name string) *ateapi.ExternalVolume {
	t.Helper()
	created, err := tc.client.CreateExternalVolume(context.Background(), &ateapi.CreateExternalVolumeRequest{
		ExternalVolume: newExternalVolume(name),
	})
	if err != nil {
		t.Fatalf("CreateExternalVolume(%s) failed: %v", name, err)
	}
	return created
}

func getExternalVolume(t *testing.T, tc *testContext, name string) *ateapi.ExternalVolume {
	t.Helper()
	stored, err := tc.client.GetExternalVolume(context.Background(), &ateapi.GetExternalVolumeRequest{
		ExternalVolume: &ateapi.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetExternalVolume(%s) failed: %v", name, err)
	}
	return stored
}

func TestExternalVolume_Lifecycle(t *testing.T) {
	preview.InitForTest(t, "ExternalVolumes")
	ns := namespaceForTest("ns-extvol-lifecycle")
	tc := setupPreviewTest(t, ns)
	defer tc.cleanup()

	want := newExternalVolume("ctx")
	created := createExternalVolume(t, tc, "ctx")
	if created.GetMetadata().GetUid() == "" || created.GetMetadata().GetVersion() != 1 {
		t.Errorf("created metadata = %v, want a uid and version 1", created.GetMetadata())
	}
	if diff := cmp.Diff(want, created, protocmp.Transform(), protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "uid", "version", "create_time", "update_time")); diff != "" {
		t.Errorf("CreateExternalVolume mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(created, getExternalVolume(t, tc, "ctx"), protocmp.Transform()); diff != "" {
		t.Errorf("GetExternalVolume mismatch (-want +got):\n%s", diff)
	}

	_, err := tc.client.GetExternalVolume(context.Background(), &ateapi.GetExternalVolumeRequest{
		ExternalVolume: &ateapi.ObjectRef{Atespace: testAtespace, Name: "missing"},
	})
	assertGrpcError(t, err, codes.NotFound, fmt.Sprintf("ExternalVolume %s/missing not found", testAtespace))

	listed, err := tc.client.ListExternalVolumes(context.Background(), &ateapi.ListExternalVolumesRequest{Atespace: testAtespace})
	if err != nil {
		t.Fatalf("ListExternalVolumes failed: %v", err)
	}
	if diff := cmp.Diff([]*ateapi.ExternalVolume{created}, listed.GetExternalVolumes(), protocmp.Transform()); diff != "" {
		t.Errorf("ListExternalVolumes mismatch (-want +got):\n%s", diff)
	}

	// The atespace holds the record, so it cannot be deleted yet.
	_, err = tc.client.DeleteAtespace(context.Background(), &ateapi.DeleteAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: testAtespace}})
	assertGrpcError(t, err, codes.FailedPrecondition, fmt.Sprintf("Atespace %s is not empty", testAtespace))

	ref := &ateapi.ObjectRef{Atespace: testAtespace, Name: "ctx"}
	_, err = tc.client.DeleteExternalVolume(context.Background(), &ateapi.DeleteExternalVolumeRequest{
		ExternalVolume: ref,
		Options:        &ateapi.DeleteOptions{Version: created.GetMetadata().GetVersion() + 1},
	})
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")
	const otherUID = "8ba9b6ee-2e1a-4c0f-9f4e-2f0a0f1c3d55"
	_, err = tc.client.DeleteExternalVolume(context.Background(), &ateapi.DeleteExternalVolumeRequest{
		ExternalVolume: ref,
		Options:        &ateapi.DeleteOptions{Uid: otherUID},
	})
	assertGrpcError(t, err, codes.Aborted, fmt.Sprintf("ExternalVolume %s/ctx does not have uid %s", testAtespace, otherUID))

	deleted, err := tc.client.DeleteExternalVolume(context.Background(), &ateapi.DeleteExternalVolumeRequest{
		ExternalVolume: ref,
		Options:        &ateapi.DeleteOptions{Uid: created.GetMetadata().GetUid(), Version: created.GetMetadata().GetVersion()},
	})
	if err != nil {
		t.Fatalf("DeleteExternalVolume failed: %v", err)
	}
	if diff := cmp.Diff(created, deleted, protocmp.Transform()); diff != "" {
		t.Errorf("DeleteExternalVolume returned (-want +got):\n%s", diff)
	}
	_, err = tc.client.DeleteExternalVolume(context.Background(), &ateapi.DeleteExternalVolumeRequest{ExternalVolume: ref})
	assertGrpcError(t, err, codes.NotFound, fmt.Sprintf("ExternalVolume %s/ctx not found", testAtespace))
}

func TestCreateExternalVolume_Rejected(t *testing.T) {
	preview.InitForTest(t, "ExternalVolumes")
	ns := namespaceForTest("ns-extvol-rejected")
	tc := setupPreviewTest(t, ns)
	defer tc.cleanup()

	first := createExternalVolume(t, tc, "ctx")

	duplicate := newExternalVolume("ctx")
	duplicate.VolumeId = "nfs-server#exports#elsewhere"
	_, err := tc.client.CreateExternalVolume(context.Background(), &ateapi.CreateExternalVolumeRequest{ExternalVolume: duplicate})
	assertGrpcError(t, err, codes.AlreadyExists, fmt.Sprintf("ExternalVolume %s/ctx already exists", testAtespace))
	if diff := cmp.Diff(first, getExternalVolume(t, tc, "ctx"), protocmp.Transform()); diff != "" {
		t.Errorf("the rejected create changed the stored volume (-want +got):\n%s", diff)
	}

	untyped := newExternalVolume("untyped")
	untyped.VolumeType = ""
	_, err = tc.client.CreateExternalVolume(context.Background(), &ateapi.CreateExternalVolumeRequest{ExternalVolume: untyped})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "volume_type")

	unidentified := newExternalVolume("unidentified")
	unidentified.VolumeId = ""
	_, err = tc.client.CreateExternalVolume(context.Background(), &ateapi.CreateExternalVolumeRequest{ExternalVolume: unidentified})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "volume_id")

	elsewhere := newExternalVolume("ctx")
	elsewhere.Metadata.Atespace = "no-such-atespace"
	_, err = tc.client.CreateExternalVolume(context.Background(), &ateapi.CreateExternalVolumeRequest{ExternalVolume: elsewhere})
	assertGrpcError(t, err, codes.FailedPrecondition, "Atespace no-such-atespace not found")

	// A client that does not opt in to preview features cannot register a
	// volume, but it can still read and delete one.
	_, err = tc.noOptInClient.CreateExternalVolume(context.Background(), &ateapi.CreateExternalVolumeRequest{ExternalVolume: newExternalVolume("gated")})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "requires the ExternalVolumes preview gate")
	if _, err := tc.noOptInClient.GetExternalVolume(context.Background(), &ateapi.GetExternalVolumeRequest{
		ExternalVolume: &ateapi.ObjectRef{Atespace: testAtespace, Name: "ctx"},
	}); err != nil {
		t.Errorf("GetExternalVolume without preview opt-in failed: %v", err)
	}
	if _, err := tc.noOptInClient.DeleteExternalVolume(context.Background(), &ateapi.DeleteExternalVolumeRequest{
		ExternalVolume: &ateapi.ObjectRef{Atespace: testAtespace, Name: "ctx"},
	}); err != nil {
		t.Errorf("DeleteExternalVolume without preview opt-in failed: %v", err)
	}
}

func TestCreateExternalVolume_GateOff(t *testing.T) {
	preview.InitForTest(t)
	ns := namespaceForTest("ns-extvol-gate-off")
	tc := setupPreviewTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.CreateExternalVolume(context.Background(), &ateapi.CreateExternalVolumeRequest{ExternalVolume: newExternalVolume("ctx")})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "requires the ExternalVolumes preview gate")
}
