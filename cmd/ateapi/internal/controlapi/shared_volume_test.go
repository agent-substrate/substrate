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

package controlapi

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// recordingVolumePlugin answers CreateVolume with a handle derived from the
// name it was asked for, and records both calls so a test can assert on what
// the workflow asked the storage system to do.
type recordingVolumePlugin struct {
	volume.VolumePluginControlPlane

	createErr  error
	deleteErr  error
	createdIDs []string
	deletedIDs []string
}

func (p *recordingVolumePlugin) CreateVolume(_ context.Context, req volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	if p.createErr != nil {
		return volume.CreateVolumeResponse{}, p.createErr
	}
	p.createdIDs = append(p.createdIDs, req.Name)
	return volume.CreateVolumeResponse{VolumeID: "disk/" + req.Name, VolumeContext: map[string]string{"capacity": req.Capacity}}, nil
}

func (p *recordingVolumePlugin) DeleteVolume(_ context.Context, volumeID string) error {
	if p.deleteErr != nil {
		return p.deleteErr
	}
	p.deletedIDs = append(p.deletedIDs, volumeID)
	return nil
}

// newTestSharedVolumeWorkflow builds a SharedVolumeWorkflow over a real
// store, with one StorageClass named "standard" whose reclaim policy the caller
// chooses, and a plugin registered under that class's provisioner.
func newTestSharedVolumeWorkflow(t *testing.T) (*SharedVolumeWorkflow, store.Interface, *recordingVolumePlugin) {
	t.Helper()
	persistence := newTestPersistence(t)
	plugin := &recordingVolumePlugin{}
	scLister := &fakeStorageClassLister{
		storageClasses: map[string]*storagev1.StorageClass{
			"standard": {
				ObjectMeta:  metav1.ObjectMeta{Name: "standard"},
				Provisioner: "mock",
				Parameters:  map[string]string{"type": "pd-ssd"},
			},
		},
	}
	registry := &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{"mock": plugin}}
	return NewSharedVolumeWorkflow(persistence, scLister, registry), persistence, plugin
}

// TestCreateSharedVolume_Provisioned checks the whole create: the record is
// reserved, the disk is asked for under an ID derived from the record's UID,
// and the handle the driver returned is stamped back.
func TestCreateSharedVolume_Provisioned(t *testing.T) {
	ctx := context.Background()
	w, _, plugin := newTestSharedVolumeWorkflow(t)

	got, err := w.CreateSharedVolume(ctx, &ateapipb.SharedVolume{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
		StorageClassName: "standard",
		Capacity:         "10Gi",
	})
	if err != nil {
		t.Fatalf("CreateSharedVolume: %v", err)
	}

	wantID := sharedVolumeID(got.GetMetadata().GetUid())
	if diff := cmp.Diff([]string{wantID}, plugin.createdIDs); diff != "" {
		t.Errorf("provisioned volume names mismatch (-want +got):\n%s", diff)
	}
	if want := "disk/" + wantID; got.GetVolumeId() != want {
		t.Errorf("volume_id = %q, want the handle the driver returned %q", got.GetVolumeId(), want)
	}
	if got.GetVolumeType() != "mock" {
		t.Errorf("volume_type = %q, want the class's provisioner %q", got.GetVolumeType(), "mock")
	}
	if want := ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR; got.GetDeleteTrigger() != want {
		t.Errorf("delete_trigger = %v, want %v", got.GetDeleteTrigger(), want)
	}
	if want := ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_READY; got.GetStatus().GetState() != want {
		t.Errorf("state = %v, want %v", got.GetStatus().GetState(), want)
	}
	if got.GetVolumeContext()["capacity"] != "10Gi" {
		t.Errorf("volume_context = %v, want the context the driver returned for a 10Gi disk", got.GetVolumeContext())
	}
}

func TestCreateSharedVolume_Registered(t *testing.T) {
	ctx := context.Background()
	w, _, plugin := newTestSharedVolumeWorkflow(t)

	got, err := w.CreateSharedVolume(ctx, &ateapipb.SharedVolume{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "adopted"},
		VolumeId:      "projects/p/zones/z/disks/preexisting",
		VolumeType:    "mock",
		VolumeContext: map[string]string{"fsType": "ext4"},
	})
	if err != nil {
		t.Fatalf("CreateSharedVolume: %v", err)
	}

	if len(plugin.createdIDs) != 0 {
		t.Errorf("provisioned %v, want nothing provisioned for a volume that already exists", plugin.createdIDs)
	}
	if want := "projects/p/zones/z/disks/preexisting"; got.GetVolumeId() != want {
		t.Errorf("volume_id = %q, want the supplied %q", got.GetVolumeId(), want)
	}
	if want := ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_READY; got.GetStatus().GetState() != want {
		t.Errorf("state = %v, want %v", got.GetStatus().GetState(), want)
	}
	if want := ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL; got.GetDeleteTrigger() != want {
		t.Errorf("delete_trigger = %v, want %v", got.GetDeleteTrigger(), want)
	}
}

func TestCreateSharedVolume_Errors(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		volume   *ateapipb.SharedVolume
		wantCode codes.Code
	}{
		{
			name: "unknown storage class",
			volume: &ateapipb.SharedVolume{
				Metadata:         &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "vol"},
				StorageClassName: "nonexistent",
				Capacity:         "1Gi",
			},
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "unknown atespace",
			volume: &ateapipb.SharedVolume{
				Metadata:         &ateapipb.ResourceMetadata{Atespace: "no-such-space", Name: "vol"},
				StorageClassName: "standard",
				Capacity:         "1Gi",
			},
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "no source at all",
			volume: &ateapipb.SharedVolume{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "vol"},
			},
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, _, _ := newTestSharedVolumeWorkflow(t)
			_, err := w.CreateSharedVolume(ctx, tc.volume)
			if got := apierror.Code(err); got != tc.wantCode {
				t.Fatalf("CreateSharedVolume error = %v (code %v), want code %v", err, got, tc.wantCode)
			}
		})
	}
}

func TestCreateSharedVolume_ReusedName(t *testing.T) {
	ctx := context.Background()
	w, _, plugin := newTestSharedVolumeWorkflow(t)

	toCreate := func() *ateapipb.SharedVolume {
		return &ateapipb.SharedVolume{
			Metadata:         &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
			StorageClassName: "standard",
			Capacity:         "10Gi",
		}
	}
	if _, err := w.CreateSharedVolume(ctx, toCreate()); err != nil {
		t.Fatalf("CreateSharedVolume: %v", err)
	}
	_, err := w.CreateSharedVolume(ctx, toCreate())
	if got := apierror.Code(err); got != codes.AlreadyExists {
		t.Fatalf("second CreateSharedVolume error = %v (code %v), want code %v", err, got, codes.AlreadyExists)
	}
	if len(plugin.createdIDs) != 1 {
		t.Errorf("provisioned %v, want only the first create to reach the storage system", plugin.createdIDs)
	}
}

func TestDeleteSharedVolume(t *testing.T) {
	ctx := context.Background()
	w, persistence, plugin := newTestSharedVolumeWorkflow(t)
	created, err := w.CreateSharedVolume(ctx, &ateapipb.SharedVolume{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
		StorageClassName: "standard",
		Capacity:         "10Gi",
	})
	if err != nil {
		t.Fatalf("CreateSharedVolume: %v", err)
	}
	volumeRef := resources.SharedVolumeRefFromSharedVolume(created)

	if _, err := w.DeleteSharedVolume(ctx, volumeRef, store.DeletePreconditions{Version: created.GetMetadata().GetVersion() + 1}); apierror.Code(err) != codes.Aborted {
		t.Fatalf("DeleteSharedVolume with a stale version error = %v, want code %v", err, codes.Aborted)
	}
	if len(plugin.deletedIDs) != 0 {
		t.Fatalf("collected %v before the precondition check passed", plugin.deletedIDs)
	}

	if _, err := w.DeleteSharedVolume(ctx, volumeRef, store.DeletePreconditions{UID: created.GetMetadata().GetUid()}); err != nil {
		t.Fatalf("DeleteSharedVolume: %v", err)
	}
	if diff := cmp.Diff([]string{created.GetVolumeId()}, plugin.deletedIDs); diff != "" {
		t.Errorf("collected disks mismatch (-want +got):\n%s", diff)
	}
	if _, err := persistence.GetSharedVolume(ctx, volumeRef); err == nil {
		t.Errorf("GetSharedVolume after delete succeeded, want the row gone")
	}
}

func TestDeleteSharedVolume_StillReferenced(t *testing.T) {
	ctx := context.Background()
	w, persistence, plugin := newTestSharedVolumeWorkflow(t)

	created, err := w.CreateSharedVolume(ctx, &ateapipb.SharedVolume{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
		StorageClassName: "standard",
		Capacity:         "10Gi",
	})
	if err != nil {
		t.Fatalf("CreateSharedVolume: %v", err)
	}
	volumeRef := resources.SharedVolumeRefFromSharedVolume(created)
	if _, err := persistence.UpdateSharedVolume(ctx, volumeRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.SharedVolume) error {
		toUpdate.Status.Refs = []*ateapipb.ActorRef{{ActorUid: "uid-1", ActorName: "producer"}}
		return nil
	}); err != nil {
		t.Fatalf("UpdateSharedVolume: %v", err)
	}

	_, err = w.DeleteSharedVolume(ctx, volumeRef, store.DeletePreconditions{})
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("DeleteSharedVolume error = %v (code %v), want code %v", err, got, codes.FailedPrecondition)
	}
	if len(plugin.deletedIDs) != 0 {
		t.Errorf("collected %v, want the disk left alone while an actor holds it", plugin.deletedIDs)
	}
}

func TestDeleteSharedVolume_Pending(t *testing.T) {
	ctx := context.Background()
	w, persistence, plugin := newTestSharedVolumeWorkflow(t)

	pending, err := persistence.CreateSharedVolume(ctx, &ateapipb.SharedVolume{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "stranded"},
		StorageClassName: "standard",
		VolumeType:       "mock",
		Status:           &ateapipb.SharedVolumeStatus{State: ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_PENDING},
	})
	if err != nil {
		t.Fatalf("CreateSharedVolume: %v", err)
	}

	if _, err := w.DeleteSharedVolume(ctx, resources.SharedVolumeRefFromSharedVolume(pending), store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteSharedVolume: %v", err)
	}
	want := []string{sharedVolumeID(pending.GetMetadata().GetUid())}
	if diff := cmp.Diff(want, plugin.deletedIDs); diff != "" {
		t.Errorf("collected disks mismatch (-want +got):\n%s", diff)
	}
}

func TestDeleteSharedVolume_NotFound(t *testing.T) {
	ctx := context.Background()
	w, _, _ := newTestSharedVolumeWorkflow(t)

	_, err := w.DeleteSharedVolume(ctx, resources.SharedVolumeRef{Atespace: "team-a", Name: "nope"}, store.DeletePreconditions{})
	if got := apierror.Code(err); got != codes.NotFound {
		t.Fatalf("DeleteSharedVolume error = %v (code %v), want code %v", err, got, codes.NotFound)
	}
}
