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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8slabels "k8s.io/apimachinery/pkg/labels"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

type fakeStorageClassLister struct {
	storageClasses map[string]*storagev1.StorageClass
	getErr         error
}

func (f *fakeStorageClassLister) List(selector k8slabels.Selector) (ret []*storagev1.StorageClass, err error) {
	return nil, nil
}

func (f *fakeStorageClassLister) Get(name string) (*storagev1.StorageClass, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	sc, ok := f.storageClasses[name]
	if !ok {
		return nil, k8serrors.NewNotFound(storagev1.Resource("storageclass"), name)
	}
	return sc, nil
}

var _ storagev1listers.StorageClassLister = (*fakeStorageClassLister)(nil)

func TestActorVolumesStorageClassErrors(t *testing.T) {
	ctx := context.Background()
	tmpl := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{{
			Name: "data-vol",
			ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
				StorageClassName: "standard",
			},
		}},
	}
	volumes := []*ateapipb.ExternalVolume{{
		Name:       "data-vol",
		VolumeType: "mock-standard",
		Status:     ateapipb.ExternalVolume_STATUS_PENDING,
	}}

	for _, tt := range []struct {
		name     string
		getErr   error
		wantCode codes.Code
	}{
		{
			name:     "missing StorageClass",
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "StorageClass lookup fails",
			getErr:   errors.New("storage class cache unavailable"),
			wantCode: codes.Internal,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lister := &fakeStorageClassLister{getErr: tt.getErr}
			t.Run("initial", func(t *testing.T) {
				_, err := initialActorVolumes(ctx, lister, tmpl)
				if got := apierror.Code(err); got != tt.wantCode {
					t.Fatalf("initialActorVolumes() code = %v, want %v; error = %v", got, tt.wantCode, err)
				}
				if !strings.Contains(err.Error(), `StorageClass "standard"`) {
					t.Errorf("initialActorVolumes() error does not name the StorageClass: %v", err)
				}
			})
			t.Run("create", func(t *testing.T) {
				res, err := createActorVolumes(ctx, &mockPluginRegistry{}, lister, "actor-uid-123", tmpl, volumes)
				if got := apierror.Code(err); got != tt.wantCode {
					t.Fatalf("createActorVolumes() code = %v, want %v; error = %v", got, tt.wantCode, err)
				}
				if !strings.Contains(err.Error(), `StorageClass "standard"`) {
					t.Errorf("createActorVolumes() error does not name the StorageClass: %v", err)
				}
				if diff := cmp.Diff(volumes, res, protocmp.Transform()); diff != "" {
					t.Errorf("createActorVolumes() did not preserve pending volumes (-want +got):\n%s", diff)
				}
			})
		})
	}
}

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
			Name:       "data-vol-1",
			VolumeType: "mock-standard",
			Status:     ateapipb.ExternalVolume_STATUS_PENDING,
			AccessMode: ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY,
		},
		{
			Name:       "data-vol-2",
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
		name           string
		tmpl           *ateapipb.ActorTemplate
		inputVolumes   []*ateapipb.ExternalVolume
		storageClasses map[string]*storagev1.StorageClass
		wantErr        bool
		wantRes        []*ateapipb.ExternalVolume
	}{
		{
			name: "partial failure returns error and preserves succeeded, failed, and remaining volumes",
			tmpl: multiVolTmpl,
			inputVolumes: []*ateapipb.ExternalVolume{
				{
					Name:       "vol1",
					VolumeType: "mock-standard",
					Status:     ateapipb.ExternalVolume_STATUS_PENDING,
				},
				{
					Name:   "vol2",
					Status: ateapipb.ExternalVolume_STATUS_DELETING,
				},
				{
					Name:   "vol3",
					Status: ateapipb.ExternalVolume_STATUS_PENDING,
				},
			},
			wantErr: true,
			wantRes: []*ateapipb.ExternalVolume{
				{
					Name:            "vol1",
					StorageVolumeId: "mock-vol-substrate-actor-uid-123-vol1",
					VolumeType:      "mock-standard",
					Status:          ateapipb.ExternalVolume_STATUS_CREATED,
				},
				{
					Name:   "vol2",
					Status: ateapipb.ExternalVolume_STATUS_DELETING,
				},
				{
					Name:   "vol3",
					Status: ateapipb.ExternalVolume_STATUS_PENDING,
				},
			},
		},
		{
			name: "created volume status succeeds",
			tmpl: standardTmpl,
			inputVolumes: []*ateapipb.ExternalVolume{
				{
					Name:            "data-vol",
					StorageVolumeId: "existing-vol-id",
					Status:          ateapipb.ExternalVolume_STATUS_CREATED,
				},
			},
			wantErr: false,
			wantRes: []*ateapipb.ExternalVolume{
				{
					Name:            "data-vol",
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
					Name:   "data-vol",
					Status: ateapipb.ExternalVolume_STATUS_UNSPECIFIED,
				},
			},
			wantErr: true,
			wantRes: []*ateapipb.ExternalVolume{
				{
					Name:   "data-vol",
					Status: ateapipb.ExternalVolume_STATUS_UNSPECIFIED,
				},
			},
		},
		{
			name: "volume not found in template returns error",
			tmpl: &ateapipb.ActorTemplate{},
			inputVolumes: []*ateapipb.ExternalVolume{
				{
					Name:   "missing-vol",
					Status: ateapipb.ExternalVolume_STATUS_PENDING,
				},
			},
			wantErr: true,
			wantRes: []*ateapipb.ExternalVolume{
				{
					Name:   "missing-vol",
					Status: ateapipb.ExternalVolume_STATUS_PENDING,
				},
			},
		},
		{
			name: "storage class parameters are propagated to volume context",
			tmpl: standardTmpl,
			inputVolumes: []*ateapipb.ExternalVolume{
				{
					Name:       "data-vol",
					VolumeType: "mock-standard",
					Status:     ateapipb.ExternalVolume_STATUS_PENDING,
				},
			},
			storageClasses: map[string]*storagev1.StorageClass{
				"standard": {
					ObjectMeta:  metav1.ObjectMeta{Name: "standard"},
					Provisioner: "mock-standard",
					Parameters: map[string]string{
						"type":                      "pd-ssd",
						"csi.storage.k8s.io/fstype": "ext4",
					},
				},
			},
			wantErr: false,
			wantRes: []*ateapipb.ExternalVolume{
				{
					Name:            "data-vol",
					StorageVolumeId: "mock-vol-substrate-actor-uid-123-data-vol",
					VolumeType:      "mock-standard",
					Status:          ateapipb.ExternalVolume_STATUS_CREATED,
					VolumeContext: map[string]string{
						"type":                      "pd-ssd",
						"csi.storage.k8s.io/fstype": "ext4",
					},
				},
			},
		},
		{
			name: "access mode is kept on the created volume",
			tmpl: standardTmpl,
			inputVolumes: []*ateapipb.ExternalVolume{
				{
					Name:       "data-vol",
					VolumeType: "mock-standard",
					Status:     ateapipb.ExternalVolume_STATUS_PENDING,
					AccessMode: ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY,
				},
			},
			wantErr: false,
			wantRes: []*ateapipb.ExternalVolume{
				{
					Name:            "data-vol",
					StorageVolumeId: "mock-vol-substrate-actor-uid-123-data-vol",
					VolumeType:      "mock-standard",
					Status:          ateapipb.ExternalVolume_STATUS_CREATED,
					AccessMode:      ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY,
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
			scs := tt.storageClasses
			if scs == nil {
				scs = map[string]*storagev1.StorageClass{
					"standard": {
						ObjectMeta:  metav1.ObjectMeta{Name: "standard"},
						Provisioner: "mock-standard",
					},
					"fast": {
						ObjectMeta:  metav1.ObjectMeta{Name: "fast"},
						Provisioner: "mock-fast",
					},
				}
			}
			scLister := &fakeStorageClassLister{storageClasses: scs}
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
				{Name: "vol1", StorageVolumeId: "storage-vol-123", VolumeType: "mock"},
			},
			wantDeleted: []string{"storage-vol-123"},
			wantErr:     false,
		},
		{
			name:     "falls back to actorVolumeID when storage volume ID is empty regardless of status",
			actorUID: "uid-abc",
			volumes: []*ateapipb.ExternalVolume{
				{Name: "vol1", StorageVolumeId: "", Status: ateapipb.ExternalVolume_STATUS_CREATED, VolumeType: "mock"},
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

type detachCall struct {
	VolumeID string
	Node     string
}

type mockDetachVolumePlugin struct {
	volume.VolumePluginControlPlane
	detachCalls []detachCall
	detachErrs  map[string]error
}

func (m *mockDetachVolumePlugin) DetachVolume(ctx context.Context, volumeID, node string) error {
	m.detachCalls = append(m.detachCalls, detachCall{VolumeID: volumeID, Node: node})
	if m.detachErrs != nil {
		if err, ok := m.detachErrs[volumeID]; ok {
			return err
		}
	}
	return nil
}

func TestDetachActorVolumes(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name            string
		actor           *ateapipb.Actor
		template        *ateapipb.ActorTemplate
		plugin          *mockDetachVolumePlugin
		pluginRegistry  *mockPluginRegistry
		wantDetachCalls []detachCall
		wantErr         bool
		wantErrContains string
	}{
		{
			name: "success with multiple mounted volumes",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapipb.ActorStatus{
					AssignedNode: "node-1",
					ExternalVolumes: []*ateapipb.ExternalVolume{
						{Name: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
						{Name: "vol2", StorageVolumeId: "storage-vol-2", VolumeType: "mock"},
					},
				},
			},
			template: &ateapipb.ActorTemplate{
				Volumes: []*ateapipb.Volume{
					{Name: "vol1"},
					{Name: "vol2"},
				},
				Containers: []*ateapipb.Container{
					{
						VolumeMounts: []*ateapipb.VolumeMount{
							{Name: "vol1"},
							{Name: "vol2"},
						},
					},
				},
			},
			wantDetachCalls: []detachCall{
				{VolumeID: "storage-vol-1", Node: "node-1"},
				{VolumeID: "storage-vol-2", Node: "node-1"},
			},
		},
		{
			name: "skips unmounted volume in template",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapipb.ActorStatus{
					AssignedNode: "node-1",
					ExternalVolumes: []*ateapipb.ExternalVolume{
						{Name: "mounted-vol", StorageVolumeId: "storage-vol-mounted", VolumeType: "mock"},
						{Name: "unmounted-vol", StorageVolumeId: "storage-vol-unmounted", VolumeType: "mock"},
					},
				},
			},
			template: &ateapipb.ActorTemplate{
				Volumes: []*ateapipb.Volume{
					{Name: "mounted-vol"},
					{Name: "unmounted-vol"},
				},
				Containers: []*ateapipb.Container{
					{
						VolumeMounts: []*ateapipb.VolumeMount{
							{Name: "mounted-vol"},
						},
					},
				},
			},
			wantDetachCalls: []detachCall{
				{VolumeID: "storage-vol-mounted", Node: "node-1"},
			},
		},
		{
			name: "skips volume with empty StorageVolumeId",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapipb.ActorStatus{
					AssignedNode: "node-1",
					ExternalVolumes: []*ateapipb.ExternalVolume{
						{Name: "vol1", StorageVolumeId: "", VolumeType: "mock"},
						{Name: "vol2", StorageVolumeId: "storage-vol-2", VolumeType: "mock"},
					},
				},
			},
			template: &ateapipb.ActorTemplate{
				Volumes: []*ateapipb.Volume{
					{Name: "vol1"},
					{Name: "vol2"},
				},
				Containers: []*ateapipb.Container{
					{
						VolumeMounts: []*ateapipb.VolumeMount{
							{Name: "vol1"},
							{Name: "vol2"},
						},
					},
				},
			},
			wantDetachCalls: []detachCall{
				{VolumeID: "storage-vol-2", Node: "node-1"},
			},
		},
		{
			name: "nil template falls back to detaching all actor volumes",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapipb.ActorStatus{
					AssignedNode: "node-1",
					ExternalVolumes: []*ateapipb.ExternalVolume{
						{Name: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
						{Name: "vol2", StorageVolumeId: "storage-vol-2", VolumeType: "mock"},
					},
				},
			},
			template: nil,
			wantDetachCalls: []detachCall{
				{VolumeID: "storage-vol-1", Node: "node-1"},
				{VolumeID: "storage-vol-2", Node: "node-1"},
			},
		},
		{
			name: "codes.NotFound from plugin is treated as already detached",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapipb.ActorStatus{
					AssignedNode: "node-1",
					ExternalVolumes: []*ateapipb.ExternalVolume{
						{Name: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
					},
				},
			},
			plugin: &mockDetachVolumePlugin{
				detachErrs: map[string]error{
					"storage-vol-1": status.Error(codes.NotFound, "volume not found"),
				},
			},
			wantDetachCalls: []detachCall{
				{VolumeID: "storage-vol-1", Node: "node-1"},
			},
			wantErr: false,
		},
		{
			name: "partial failure attempts all volumes and joins errors",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapipb.ActorStatus{
					AssignedNode: "node-1",
					ExternalVolumes: []*ateapipb.ExternalVolume{
						{Name: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
						{Name: "vol2", StorageVolumeId: "storage-vol-2", VolumeType: "mock"},
					},
				},
			},
			plugin: &mockDetachVolumePlugin{
				detachErrs: map[string]error{
					"storage-vol-1": status.Error(codes.Internal, "disk detach failed"),
				},
			},
			wantDetachCalls: []detachCall{
				{VolumeID: "storage-vol-1", Node: "node-1"},
				{VolumeID: "storage-vol-2", Node: "node-1"},
			},
			wantErr:         true,
			wantErrContains: "failed to detach volume \"storage-vol-1\"",
		},
		{
			name: "unknown plugin returns error",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapipb.ActorStatus{
					AssignedNode: "node-1",
					ExternalVolumes: []*ateapipb.ExternalVolume{
						{Name: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "unknown-plugin"},
					},
				},
			},
			wantErr:         true,
			wantErrContains: "failed to get volume plugin for \"unknown-plugin\"",
		},
		{
			name: "no assigned node skips detach",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapipb.ActorStatus{
					AssignedNode: "",
					ExternalVolumes: []*ateapipb.ExternalVolume{
						{Name: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
					},
				},
			},
			wantDetachCalls: nil,
			wantErr:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := tt.plugin
			if plugin == nil {
				plugin = &mockDetachVolumePlugin{}
			}
			registry := tt.pluginRegistry
			if registry == nil {
				registry = &mockPluginRegistry{
					plugins: map[string]volume.VolumePluginControlPlane{
						"mock": plugin,
					},
				}
			}

			err := detachActorVolumes(ctx, registry, tt.actor, tt.template, "test")
			if (err != nil) != tt.wantErr {
				t.Fatalf("detachActorVolumes() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Fatalf("detachActorVolumes() error = %v, want error containing %q", err, tt.wantErrContains)
				}
			}
			if diff := cmp.Diff(tt.wantDetachCalls, plugin.detachCalls); diff != "" {
				t.Errorf("detachCalls mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// createErrVolumePlugin fails every CreateVolume call with err and records the
// request it was given.
type createErrVolumePlugin struct {
	volume.VolumePluginControlPlane
	err     error
	lastReq volume.CreateVolumeRequest
}

func (p *createErrVolumePlugin) CreateVolume(ctx context.Context, req volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	p.lastReq = req
	return volume.CreateVolumeResponse{}, p.err
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
			name:     "InvalidArgument kept",
			err:      status.Error(codes.InvalidArgument, "driver does not support ReadWriteMany"),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "wrapped InvalidArgument kept",
			err:      fmt.Errorf("CSI CreateVolume failed: %w", status.Error(codes.InvalidArgument, "driver does not support ReadWriteMany")),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "OutOfRange becomes InvalidArgument",
			err:      status.Error(codes.OutOfRange, "requested capacity exceeds limit"),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "ResourceExhausted kept",
			err:      status.Error(codes.ResourceExhausted, "quota exceeded"),
			wantCode: codes.ResourceExhausted,
		},
		{
			name:     "AlreadyExists kept",
			err:      status.Error(codes.AlreadyExists, "volume already exists with different parameters"),
			wantCode: codes.AlreadyExists,
		},
		{
			name:     "PermissionDenied becomes Internal",
			err:      status.Error(codes.PermissionDenied, "unauthorized to create volume"),
			wantCode: codes.Internal,
		},
		{
			name:     "Unimplemented becomes Internal",
			err:      status.Error(codes.Unimplemented, "provisioning not supported"),
			wantCode: codes.Internal,
		},
		{
			name:     "NotFound becomes Internal",
			err:      status.Error(codes.NotFound, "snapshot not found"),
			wantCode: codes.Internal,
		},
		{
			name:     "plain error becomes Internal",
			err:      errors.New("generic storage error"),
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputVols := []*ateapipb.ExternalVolume{
				{
					Name:       "vol1",
					VolumeType: "mock-standard",
					Status:     ateapipb.ExternalVolume_STATUS_PENDING,
					AccessMode: ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY,
				},
			}
			plugin := &createErrVolumePlugin{err: tt.err}
			registry := &mockPluginRegistry{
				plugins: map[string]volume.VolumePluginControlPlane{
					"mock-standard": plugin,
				},
			}

			_, err := createActorVolumes(ctx, registry, scLister, "test-uid", tmpl, inputVols)
			if got := apierror.Code(err); got != tt.wantCode {
				t.Errorf("apierror.Code(err) = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			if got := plugin.lastReq.AccessMode; got != ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY {
				t.Errorf("CreateVolumeRequest.AccessMode = %v, want VOLUME_ACCESS_MODE_READ_WRITE_MANY", got)
			}
		})
	}
}
