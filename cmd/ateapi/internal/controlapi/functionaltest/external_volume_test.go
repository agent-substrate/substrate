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
	"fmt"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

// externalVolumePlugin records every storage operation the control plane makes.
type externalVolumePlugin struct {
	volume.VolumePluginControlPlane

	mu       sync.Mutex
	deleted  []string
	attached []string
	detached []string
}

func (p *externalVolumePlugin) DeleteVolume(_ context.Context, volumeID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deleted = append(p.deleted, volumeID)
	return nil
}

func (p *externalVolumePlugin) AttachVolume(_ context.Context, req volume.AttachVolumeRequest) (volume.AttachVolumeResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attached = append(p.attached, req.VolumeID+"@"+req.Node)
	return volume.AttachVolumeResponse{}, nil
}

func (p *externalVolumePlugin) DetachVolume(_ context.Context, volumeID, node string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.detached = append(p.detached, volumeID+"@"+node)
	return nil
}

func (p *externalVolumePlugin) snapshot() (deleted, attached, detached []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.deleted...), append([]string(nil), p.attached...), append([]string(nil), p.detached...)
}

func setupExternalVolumeTest(t *testing.T, ns string) (*testContext, *externalVolumePlugin) {
	t.Helper()
	preview.InitForTest(t, "ExternalVolumes")
	plugin := &externalVolumePlugin{}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	return tc, plugin
}

func createExternalVolume(t *testing.T, tc *testContext, name string, trigger ateapi.DeleteTrigger) *ateapi.ExternalVolume {
	t.Helper()
	created, err := tc.client.CreateExternalVolume(context.Background(), &ateapi.CreateExternalVolumeRequest{
		ExternalVolume: &ateapi.ExternalVolume{
			Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: name},
			VolumeId:      "disk/" + name,
			VolumeType:    "substrate.io/mock",
			DeleteTrigger: trigger,
		},
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

func TestCreateExternalVolume_Registered(t *testing.T) {
	ns := namespaceForTest("ns-extvol-registered")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	created := createExternalVolume(t, tc, "adopted", ateapi.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED)
	if got, want := created.GetVolumeId(), "disk/adopted"; got != want {
		t.Errorf("volume_id = %q, want %q", got, want)
	}
	if got, want := created.GetVolumeType(), "substrate.io/mock"; got != want {
		t.Errorf("volume_type = %q, want %q", got, want)
	}
	if got := created.GetDeleteTrigger(); got != ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL {
		t.Errorf("delete_trigger = %v, want MANUAL", got)
	}

	if diff := cmp.Diff(created, getExternalVolume(t, tc, "adopted"), protocmp.Transform()); diff != "" {
		t.Errorf("GetExternalVolume mismatch (-want +got):\n%s", diff)
	}
	listed, err := tc.client.ListExternalVolumes(context.Background(), &ateapi.ListExternalVolumesRequest{Atespace: testAtespace})
	if err != nil {
		t.Fatalf("ListExternalVolumes failed: %v", err)
	}
	if diff := cmp.Diff([]*ateapi.ExternalVolume{created}, listed.GetExternalVolumes(), protocmp.Transform()); diff != "" {
		t.Errorf("ListExternalVolumes mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateExternalVolume_Rejected(t *testing.T) {
	ns := namespaceForTest("ns-extvol-rejected")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	first := createExternalVolume(t, tc, "shared", ateapi.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED)

	_, err := tc.client.CreateExternalVolume(context.Background(), &ateapi.CreateExternalVolumeRequest{
		ExternalVolume: &ateapi.ExternalVolume{
			Metadata:   &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "shared"},
			VolumeId:   "disk/elsewhere",
			VolumeType: "substrate.io/mock",
		},
	})
	assertGrpcError(t, err, codes.AlreadyExists, fmt.Sprintf("ExternalVolume %s/shared already exists", testAtespace))
	if diff := cmp.Diff(first, getExternalVolume(t, tc, "shared"), protocmp.Transform()); diff != "" {
		t.Errorf("the rejected create changed the stored volume (-want +got):\n%s", diff)
	}

	_, err = tc.client.CreateExternalVolume(context.Background(), &ateapi.CreateExternalVolumeRequest{
		ExternalVolume: &ateapi.ExternalVolume{
			Metadata: &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "untyped"},
			VolumeId: "disk/untyped",
		},
	})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "volume_type")
}

func TestUpdateExternalVolume(t *testing.T) {
	ns := namespaceForTest("ns-extvol-update")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	created := createExternalVolume(t, tc, "shared", ateapi.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR)
	toUpdate := func(in *ateapi.ExternalVolume) *ateapi.ExternalVolume {
		return &ateapi.ExternalVolume{
			Metadata:      in.GetMetadata(),
			DeleteTrigger: in.GetDeleteTrigger(),
			VolumeId:      in.GetVolumeId(),
			VolumeType:    in.GetVolumeType(),
			VolumeContext: in.GetVolumeContext(),
		}
	}

	req := toUpdate(created)
	req.DeleteTrigger = ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL
	updated, err := tc.client.UpdateExternalVolume(context.Background(), &ateapi.UpdateExternalVolumeRequest{ExternalVolume: req})
	if err != nil {
		t.Fatalf("UpdateExternalVolume failed: %v", err)
	}
	if got := updated.GetDeleteTrigger(); got != ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL {
		t.Errorf("delete_trigger = %v, want MANUAL", got)
	}

	req = toUpdate(updated)
	req.VolumeId = "disk/somewhere-else"
	_, err = tc.client.UpdateExternalVolume(context.Background(), &ateapi.UpdateExternalVolumeRequest{ExternalVolume: req})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "volume_id")
}

func TestDeleteExternalVolume(t *testing.T) {
	ns := namespaceForTest("ns-extvol-delete")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	created := createExternalVolume(t, tc, "shared", ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	ref := &ateapi.ObjectRef{Atespace: testAtespace, Name: "shared"}

	_, err := tc.client.DeleteExternalVolume(context.Background(), &ateapi.DeleteExternalVolumeRequest{
		ExternalVolume: ref,
		Options:        &ateapi.DeleteOptions{Version: created.GetMetadata().GetVersion() + 1},
	})
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")

	if _, err := tc.client.DeleteExternalVolume(context.Background(), &ateapi.DeleteExternalVolumeRequest{ExternalVolume: ref}); err != nil {
		t.Fatalf("DeleteExternalVolume failed: %v", err)
	}
	if deleted, _, _ := plugin.snapshot(); !cmp.Equal([]string{created.GetVolumeId()}, deleted) {
		t.Errorf("deleted disks = %v, want %v", deleted, []string{created.GetVolumeId()})
	}
	_, err = tc.client.DeleteExternalVolume(context.Background(), &ateapi.DeleteExternalVolumeRequest{ExternalVolume: ref})
	assertGrpcError(t, err, codes.NotFound, fmt.Sprintf("ExternalVolume %s/shared not found", testAtespace))
}
