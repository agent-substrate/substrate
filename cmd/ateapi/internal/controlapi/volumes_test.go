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
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8slabels "k8s.io/apimachinery/pkg/labels"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

type fakeStorageClassLister struct {
	storageClasses map[string]*storagev1.StorageClass
}

func (f *fakeStorageClassLister) List(selector k8slabels.Selector) (ret []*storagev1.StorageClass, err error) {
	return nil, nil
}

func (f *fakeStorageClassLister) Get(name string) (*storagev1.StorageClass, error) {
	sc, ok := f.storageClasses[name]
	if !ok {
		return nil, k8serrors.NewNotFound(storagev1.Resource("storageclass"), name)
	}
	return sc, nil
}

var _ storagev1listers.StorageClassLister = (*fakeStorageClassLister)(nil)

func TestInitialActorVolumes_PendingState(t *testing.T) {
	tmpl := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{
			{
				Name: "data-vol-1",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "standard",
					AccessMode:       ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY,
				},
			},
			{
				Name: "scratch-vol",
			},
			{
				Name:       "durable-vol",
				DurableDir: &ateapipb.DurableDirVolumeSource{},
			},
			{
				Name: "data-vol-2",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "fast",
				},
			},
		},
	}

	want := []*ateapipb.ExternalVolume{
		{
			VolumeName: "data-vol-1",
			VolumeType: "mock-standard",
			Status:     ateapipb.ExternalVolume_STATUS_PENDING,
			AccessMode: ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY,
		},
		{
			VolumeName: "data-vol-2",
			VolumeType: "mock-fast",
			Status:     ateapipb.ExternalVolume_STATUS_PENDING,
		},
	}

	scLister := &fakeStorageClassLister{
		storageClasses: map[string]*storagev1.StorageClass{
			"standard": {
				ObjectMeta:  metav1.ObjectMeta{Name: "standard"},
				Provisioner: "mock-standard",
			},
			"fast": {
				ObjectMeta:  metav1.ObjectMeta{Name: "fast"},
				Provisioner: "mock-fast",
			},
		},
	}
	initVols, err := initialActorVolumes(context.Background(), scLister, tmpl)
	if err != nil {
		t.Fatalf("initialActorVolumes failed: %v", err)
	}
	if diff := cmp.Diff(want, initVols, protocmp.Transform()); diff != "" {
		t.Errorf("initialActorVolumes mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateActorVolumes(t *testing.T) {
	ctx := context.Background()

	standardTmpl := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{
			{
				Name: "data-vol",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "standard",
				},
			},
		},
	}

	multiVolTmpl := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{
			{
				Name: "vol1",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "standard",
				},
			},
			{
				Name: "vol2",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "standard",
				},
			},
			{
				Name: "vol3",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "standard",
				},
			},
		},
	}

	tests := []struct {
		name         string
		tmpl         *ateapipb.ActorTemplate
		inputVolumes []*ateapipb.ExternalVolume
		wantErr      bool
		wantRes      []*ateapipb.ExternalVolume
	}{
		{
			name: "partial failure returns error and preserves succeeded, failed, and remaining volumes",
			tmpl: multiVolTmpl,
			inputVolumes: []*ateapipb.ExternalVolume{
				{
					VolumeName: "vol1",
					VolumeType: "mock-standard",
					Status:     ateapipb.ExternalVolume_STATUS_PENDING,
				},
				{
					VolumeName: "vol2",
					Status:     ateapipb.ExternalVolume_STATUS_DELETING,
				},
				{
					VolumeName: "vol3",
					Status:     ateapipb.ExternalVolume_STATUS_PENDING,
				},
			},
			wantErr: true,
			wantRes: []*ateapipb.ExternalVolume{
				{
					VolumeName:      "vol1",
					StorageVolumeId: "mock-vol-substrate-actor-uid-123-vol1",
					VolumeType:      "mock-standard",
					Status:          ateapipb.ExternalVolume_STATUS_CREATED,
				},
				{
					VolumeName: "vol2",
					Status:     ateapipb.ExternalVolume_STATUS_DELETING,
				},
				{
					VolumeName: "vol3",
					Status:     ateapipb.ExternalVolume_STATUS_PENDING,
				},
			},
		},
		{
			name: "created volume status succeeds",
			tmpl: standardTmpl,
			inputVolumes: []*ateapipb.ExternalVolume{
				{
					VolumeName:      "data-vol",
					StorageVolumeId: "existing-vol-id",
					Status:          ateapipb.ExternalVolume_STATUS_CREATED,
				},
			},
			wantErr: false,
			wantRes: []*ateapipb.ExternalVolume{
				{
					VolumeName:      "data-vol",
					StorageVolumeId: "existing-vol-id",
					Status:          ateapipb.ExternalVolume_STATUS_CREATED,
				},
			},
		},
		{
			name: "unspecified volume status returns error",
			tmpl: standardTmpl,
			inputVolumes: []*ateapipb.ExternalVolume{
				{
					VolumeName: "data-vol",
					Status:     ateapipb.ExternalVolume_STATUS_UNSPECIFIED,
				},
			},
			wantErr: true,
			wantRes: []*ateapipb.ExternalVolume{
				{
					VolumeName: "data-vol",
					Status:     ateapipb.ExternalVolume_STATUS_UNSPECIFIED,
				},
			},
		},
		{
			name: "volume not found in template returns error",
			tmpl: &ateapipb.ActorTemplate{},
			inputVolumes: []*ateapipb.ExternalVolume{
				{
					VolumeName: "missing-vol",
					Status:     ateapipb.ExternalVolume_STATUS_PENDING,
				},
			},
			wantErr: true,
			wantRes: []*ateapipb.ExternalVolume{
				{
					VolumeName: "missing-vol",
					Status:     ateapipb.ExternalVolume_STATUS_PENDING,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := volume.NewMockVolumePlugin()
			registry := &mockPluginRegistry{
				plugins: map[string]volume.VolumePluginControlPlane{
					"mock-standard": plugin,
					"mock-fast":     plugin,
				},
			}
			scLister := &fakeStorageClassLister{
				storageClasses: map[string]*storagev1.StorageClass{
					"standard": {
						ObjectMeta:  metav1.ObjectMeta{Name: "standard"},
						Provisioner: "mock-standard",
					},
					"fast": {
						ObjectMeta:  metav1.ObjectMeta{Name: "fast"},
						Provisioner: "mock-fast",
					},
				},
			}
			res, err := createActorVolumes(ctx, registry, scLister, "actor-uid-123", tt.tmpl, tt.inputVolumes)
			if (err != nil) != tt.wantErr {
				t.Errorf("createActorVolumes() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.wantRes, res, protocmp.Transform()); diff != "" {
				t.Errorf("createActorVolumes() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

type trackingVolumePlugin struct {
	volume.VolumePluginControlPlane
	deletedIDs []string
}

func (t *trackingVolumePlugin) DeleteVolume(ctx context.Context, volumeID string) error {
	t.deletedIDs = append(t.deletedIDs, volumeID)
	return nil
}

func TestDeleteActorVolumes(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		actorUID    string
		volumes     []*ateapipb.ExternalVolume
		wantDeleted []string
		wantErr     bool
	}{
		{
			name:     "uses storage volume ID when present",
			actorUID: "uid-abc",
			volumes: []*ateapipb.ExternalVolume{
				{VolumeName: "vol1", StorageVolumeId: "storage-vol-123", VolumeType: "mock"},
			},
			wantDeleted: []string{"storage-vol-123"},
			wantErr:     false,
		},
		{
			name:     "falls back to actorVolumeID when storage volume ID is empty regardless of status",
			actorUID: "uid-abc",
			volumes: []*ateapipb.ExternalVolume{
				{VolumeName: "vol1", StorageVolumeId: "", Status: ateapipb.ExternalVolume_STATUS_CREATED, VolumeType: "mock"},
			},
			wantDeleted: []string{"substrate-uid-abc-vol1"},
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &trackingVolumePlugin{}
			registry := &mockPluginRegistry{
				plugins: map[string]volume.VolumePluginControlPlane{
					"mock": plugin,
				},
			}
			err := deleteActorVolumes(ctx, registry, tt.actorUID, tt.volumes)
			if (err != nil) != tt.wantErr {
				t.Fatalf("deleteActorVolumes() error = %v, wantErr %v", err, tt.wantErr)
			}

			if diff := cmp.Diff(tt.wantDeleted, plugin.deletedIDs); diff != "" {
				t.Errorf("deletedIDs mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

type mockPluginRegistry struct {
	plugins map[string]volume.VolumePluginControlPlane
}

func (m *mockPluginRegistry) GetPlugin(ctx context.Context, name string) (volume.VolumePluginControlPlane, error) {
	p, ok := m.plugins[name]
	if !ok {
		return nil, fmt.Errorf("plugin %q not found in mock registry", name)
	}
	return p, nil
}

type fakeDetachStore struct {
	worker      *ateapipb.Worker
	workerErr   error
	storedActor *ateapipb.Actor
	updateErr   error
}

func (f *fakeDetachStore) GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error) {
	if f.workerErr != nil {
		return nil, f.workerErr
	}
	return f.worker, nil
}

func (f *fakeDetachStore) UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.Actor) error) (*ateapipb.Actor, error) {
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	if err := mutate(f.storedActor); err != nil {
		return nil, err
	}
	return f.storedActor, nil
}

type detachTrackingVolumePlugin struct {
	volume.VolumePluginControlPlane
	detachedVolumes []string
	detachedNodes   []string
	detachErr       map[string]error
}

func (d *detachTrackingVolumePlugin) DetachVolume(ctx context.Context, volumeID, node string) error {
	d.detachedVolumes = append(d.detachedVolumes, volumeID)
	d.detachedNodes = append(d.detachedNodes, node)
	if d.detachErr != nil {
		if err, ok := d.detachErr[volumeID]; ok {
			return err
		}
	}
	return nil
}

func TestDetachActorVolumes_ClearsAndPersistsPublishContext(t *testing.T) {
	ctx := context.Background()

	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "test-space",
			Name:     "test-actor",
			Uid:      "uid-123",
		},
		Status: &ateapipb.ActorStatus{
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker: &ateapipb.ObjectRef{
					Name: "worker-1",
				},
			},
			ActorVolumes: []*ateapipb.ExternalVolume{
				{
					VolumeName:         "vol1",
					StorageVolumeId:    "storage-vol-1",
					VolumeType:         "mock",
					Status:             ateapipb.ExternalVolume_STATUS_CREATED,
					PublishContext:     map[string]string{"device": "/dev/sda"},
					PublishContextNode: "node-abc",
				},
			},
		},
	}

	template := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{
			{
				Name: "vol1",
			},
		},
		Containers: []*ateapipb.Container{
			{
				VolumeMounts: []*ateapipb.VolumeMount{
					{
						Name:      "vol1",
						MountPath: "/mnt/vol1",
					},
				},
			},
		},
	}

	fakeStore := &fakeDetachStore{
		worker: &ateapipb.Worker{
			NodeName: "node-abc",
		},
		storedActor: proto.Clone(actor).(*ateapipb.Actor),
	}

	plugin := &detachTrackingVolumePlugin{}
	registry := &mockPluginRegistry{
		plugins: map[string]volume.VolumePluginControlPlane{
			"mock": plugin,
		},
	}

	err := detachActorVolumes(ctx, fakeStore, registry, actor, template, "suspend")
	if err != nil {
		t.Fatalf("detachActorVolumes failed: %v", err)
	}

	// 1. Verify plugin DetachVolume was called
	if diff := cmp.Diff([]string{"storage-vol-1"}, plugin.detachedVolumes); diff != "" {
		t.Errorf("detached volume mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"node-abc"}, plugin.detachedNodes); diff != "" {
		t.Errorf("detached node mismatch (-want +got):\n%s", diff)
	}

	// 2. Verify actor passed in has PublishContext cleared in memory
	vol := actor.GetStatus().GetActorVolumes()[0]
	if vol.GetPublishContext() != nil {
		t.Errorf("expected actor in-memory PublishContext to be nil, got %v", vol.GetPublishContext())
	}
	if vol.GetPublishContextNode() != "" {
		t.Errorf("expected actor in-memory PublishContextNode to be empty, got %q", vol.GetPublishContextNode())
	}

	// 3. Verify store actor has PublishContext cleared and persisted
	storedVol := fakeStore.storedActor.GetStatus().GetActorVolumes()[0]
	if storedVol.GetPublishContext() != nil {
		t.Errorf("expected store persisted PublishContext to be nil, got %v", storedVol.GetPublishContext())
	}
	if storedVol.GetPublishContextNode() != "" {
		t.Errorf("expected store persisted PublishContextNode to be empty, got %q", storedVol.GetPublishContextNode())
	}
}

func TestDetachActorVolumes_PartialFailure_StillPersistsCleared(t *testing.T) {
	ctx := context.Background()

	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "test-space",
			Name:     "test-actor",
			Uid:      "uid-123",
		},
		Status: &ateapipb.ActorStatus{
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker: &ateapipb.ObjectRef{
					Name: "worker-1",
				},
			},
			ActorVolumes: []*ateapipb.ExternalVolume{
				{
					VolumeName:         "vol1",
					StorageVolumeId:    "storage-vol-1",
					VolumeType:         "mock",
					Status:             ateapipb.ExternalVolume_STATUS_CREATED,
					PublishContext:     map[string]string{"device": "/dev/sda"},
					PublishContextNode: "node-abc",
				},
				{
					VolumeName:         "vol2",
					StorageVolumeId:    "storage-vol-2",
					VolumeType:         "mock",
					Status:             ateapipb.ExternalVolume_STATUS_CREATED,
					PublishContext:     map[string]string{"device": "/dev/sdb"},
					PublishContextNode: "node-abc",
				},
			},
		},
	}

	template := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{
			{Name: "vol1"},
			{Name: "vol2"},
		},
		Containers: []*ateapipb.Container{
			{
				VolumeMounts: []*ateapipb.VolumeMount{
					{Name: "vol1", MountPath: "/mnt/vol1"},
					{Name: "vol2", MountPath: "/mnt/vol2"},
				},
			},
		},
	}

	fakeStore := &fakeDetachStore{
		worker: &ateapipb.Worker{
			NodeName: "node-abc",
		},
		storedActor: proto.Clone(actor).(*ateapipb.Actor),
	}

	// vol1 succeeds detach, vol2 fails detach
	plugin := &detachTrackingVolumePlugin{
		detachErr: map[string]error{
			"storage-vol-2": errors.New("rpc error: disk busy"),
		},
	}
	registry := &mockPluginRegistry{
		plugins: map[string]volume.VolumePluginControlPlane{
			"mock": plugin,
		},
	}

	err := detachActorVolumes(ctx, fakeStore, registry, actor, template, "suspend")
	if err == nil {
		t.Fatalf("expected error from failed vol2 detach, got nil")
	}

	// Verify vol1 publish context was cleared in the store write
	storedVol1 := fakeStore.storedActor.GetStatus().GetActorVolumes()[0]
	if storedVol1.GetPublishContext() != nil {
		t.Errorf("expected stored vol1 PublishContext to be nil, got %v", storedVol1.GetPublishContext())
	}
	if storedVol1.GetPublishContextNode() != "" {
		t.Errorf("expected stored vol1 PublishContextNode to be empty, got %q", storedVol1.GetPublishContextNode())
	}

	// Verify vol2 still retains publish context since detachment failed
	storedVol2 := fakeStore.storedActor.GetStatus().GetActorVolumes()[1]
	if storedVol2.GetPublishContext() == nil {
		t.Errorf("expected stored vol2 to retain PublishContext on failure, got nil")
	}
}

func TestDetachActorVolumes_StoreUpdateFails_DoesNotReturnError(t *testing.T) {
	ctx := context.Background()

	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "test-space",
			Name:     "test-actor",
			Uid:      "uid-123",
		},
		Status: &ateapipb.ActorStatus{
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker: &ateapipb.ObjectRef{
					Name: "worker-1",
				},
			},
			ActorVolumes: []*ateapipb.ExternalVolume{
				{
					VolumeName:         "vol1",
					StorageVolumeId:    "storage-vol-1",
					VolumeType:         "mock",
					Status:             ateapipb.ExternalVolume_STATUS_CREATED,
					PublishContext:     map[string]string{"device": "/dev/sda"},
					PublishContextNode: "node-abc",
				},
			},
		},
	}

	template := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{
			{Name: "vol1"},
		},
		Containers: []*ateapipb.Container{
			{
				VolumeMounts: []*ateapipb.VolumeMount{
					{Name: "vol1", MountPath: "/mnt/vol1"},
				},
			},
		},
	}

	fakeStore := &fakeDetachStore{
		worker: &ateapipb.Worker{
			NodeName: "node-abc",
		},
		storedActor: proto.Clone(actor).(*ateapipb.Actor),
		updateErr:   errors.New("store update conflict"),
	}

	plugin := &detachTrackingVolumePlugin{}
	registry := &mockPluginRegistry{
		plugins: map[string]volume.VolumePluginControlPlane{
			"mock": plugin,
		},
	}

	// In both delete and suspend paths, failing to persist cleared PublishContext is logged as a
	// warning and does not return an error, because the driver detachment already succeeded,
	// operations are idempotent, and subsequent attachments self-heal via maps.Equal.
	err := detachActorVolumes(ctx, fakeStore, registry, actor, template, "delete")
	if err != nil {
		t.Errorf("expected nil error during delete action even if UpdateActor fails, got: %v", err)
	}

	err = detachActorVolumes(ctx, fakeStore, registry, actor, template, "suspend")
	if err != nil {
		t.Errorf("expected nil error during suspend action even if UpdateActor fails, got: %v", err)
	}
}

type errorReturningVolumePlugin struct {
	volume.VolumePluginControlPlane
	createErr error
}

func (e *errorReturningVolumePlugin) CreateVolume(ctx context.Context, name, capacity, driverName string, parameters map[string]string, mode ateapipb.VolumeAccessMode) (string, map[string]string, error) {
	return "", nil, e.createErr
}

func TestCreateActorVolumes_ErrorCodePropagation(t *testing.T) {
	ctx := context.Background()

	tmpl := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{
			{
				Name: "vol1",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "standard",
					Capacity:         "10Gi",
					AccessMode:       ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY,
				},
			},
		},
	}

	scLister := &fakeStorageClassLister{
		storageClasses: map[string]*storagev1.StorageClass{
			"standard": {
				ObjectMeta:  metav1.ObjectMeta{Name: "standard"},
				Provisioner: "mock-standard",
			},
		},
	}

	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{
			name:     "InvalidArgument preserved",
			err:      status.Error(codes.InvalidArgument, "driver does not support ReadWriteMany"),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "OutOfRange preserved",
			err:      status.Error(codes.OutOfRange, "requested capacity exceeds limit"),
			wantCode: codes.OutOfRange,
		},
		{
			name:     "ResourceExhausted preserved",
			err:      status.Error(codes.ResourceExhausted, "quota exceeded"),
			wantCode: codes.ResourceExhausted,
		},
		{
			name:     "PermissionDenied preserved",
			err:      status.Error(codes.PermissionDenied, "unauthorized to create volume"),
			wantCode: codes.PermissionDenied,
		},
		{
			name:     "Unimplemented preserved",
			err:      status.Error(codes.Unimplemented, "provisioning not supported"),
			wantCode: codes.Unimplemented,
		},
		{
			name:     "AlreadyExists preserved",
			err:      status.Error(codes.AlreadyExists, "volume already exists with different parameters"),
			wantCode: codes.AlreadyExists,
		},
		{
			name:     "NotFound collapsed to Internal",
			err:      status.Error(codes.NotFound, "snapshot not found"),
			wantCode: codes.Internal,
		},
		{
			name:     "Unknown generic error collapsed to Internal",
			err:      errors.New("generic storage error"),
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputVols := []*ateapipb.ExternalVolume{
				{
					VolumeName: "vol1",
					VolumeType: "mock-standard",
					Status:     ateapipb.ExternalVolume_STATUS_PENDING,
					AccessMode: ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY,
				},
			}

			plugin := &errorReturningVolumePlugin{createErr: tt.err}
			registry := &mockPluginRegistry{
				plugins: map[string]volume.VolumePluginControlPlane{
					"mock-standard": plugin,
				},
			}

			_, err := createActorVolumes(ctx, registry, scLister, "test-uid", tmpl, inputVols)
			if got := status.Code(err); got != tt.wantCode {
				t.Errorf("status.Code(err) = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
		})
	}
}
