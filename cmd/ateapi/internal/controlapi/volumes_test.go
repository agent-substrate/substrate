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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
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
		{
			name: "storage class parameters are propagated to volume context",
			tmpl: standardTmpl,
			inputVolumes: []*ateapipb.ExternalVolume{
				{
					VolumeName: "data-vol",
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
					VolumeName:      "data-vol",
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

type mockDetachStore struct {
	workers map[string]*ateapipb.Worker
	err     error
}

func (m *mockDetachStore) GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.workers == nil {
		return nil, store.ErrNotFound
	}
	w, ok := m.workers[name]
	if !ok {
		return nil, store.ErrNotFound
	}
	return w, nil
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

	baseWorker := &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: "worker-1"},
		NodeName: "node-1",
	}

	tests := []struct {
		name            string
		actor           *ateapipb.Actor
		template        *ateapipb.ActorTemplate
		store           *mockDetachStore
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
					WorkerAssignment: &ateapipb.WorkerAssignment{
						Worker: &ateapipb.ObjectRef{Name: "worker-1"},
					},
					ActorVolumes: []*ateapipb.ExternalVolume{
						{VolumeName: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
						{VolumeName: "vol2", StorageVolumeId: "storage-vol-2", VolumeType: "mock"},
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
			store: &mockDetachStore{
				workers: map[string]*ateapipb.Worker{"worker-1": baseWorker},
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
					WorkerAssignment: &ateapipb.WorkerAssignment{
						Worker: &ateapipb.ObjectRef{Name: "worker-1"},
					},
					ActorVolumes: []*ateapipb.ExternalVolume{
						{VolumeName: "mounted-vol", StorageVolumeId: "storage-vol-mounted", VolumeType: "mock"},
						{VolumeName: "unmounted-vol", StorageVolumeId: "storage-vol-unmounted", VolumeType: "mock"},
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
			store: &mockDetachStore{
				workers: map[string]*ateapipb.Worker{"worker-1": baseWorker},
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
					WorkerAssignment: &ateapipb.WorkerAssignment{
						Worker: &ateapipb.ObjectRef{Name: "worker-1"},
					},
					ActorVolumes: []*ateapipb.ExternalVolume{
						{VolumeName: "vol1", StorageVolumeId: "", VolumeType: "mock"},
						{VolumeName: "vol2", StorageVolumeId: "storage-vol-2", VolumeType: "mock"},
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
			store: &mockDetachStore{
				workers: map[string]*ateapipb.Worker{"worker-1": baseWorker},
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
					WorkerAssignment: &ateapipb.WorkerAssignment{
						Worker: &ateapipb.ObjectRef{Name: "worker-1"},
					},
					ActorVolumes: []*ateapipb.ExternalVolume{
						{VolumeName: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
						{VolumeName: "vol2", StorageVolumeId: "storage-vol-2", VolumeType: "mock"},
					},
				},
			},
			template: nil,
			store: &mockDetachStore{
				workers: map[string]*ateapipb.Worker{"worker-1": baseWorker},
			},
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
					WorkerAssignment: &ateapipb.WorkerAssignment{
						Worker: &ateapipb.ObjectRef{Name: "worker-1"},
					},
					ActorVolumes: []*ateapipb.ExternalVolume{
						{VolumeName: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
					},
				},
			},
			plugin: &mockDetachVolumePlugin{
				detachErrs: map[string]error{
					"storage-vol-1": status.Error(codes.NotFound, "volume not found"),
				},
			},
			store: &mockDetachStore{
				workers: map[string]*ateapipb.Worker{"worker-1": baseWorker},
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
					WorkerAssignment: &ateapipb.WorkerAssignment{
						Worker: &ateapipb.ObjectRef{Name: "worker-1"},
					},
					ActorVolumes: []*ateapipb.ExternalVolume{
						{VolumeName: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
						{VolumeName: "vol2", StorageVolumeId: "storage-vol-2", VolumeType: "mock"},
					},
				},
			},
			plugin: &mockDetachVolumePlugin{
				detachErrs: map[string]error{
					"storage-vol-1": status.Error(codes.Internal, "disk detach failed"),
				},
			},
			store: &mockDetachStore{
				workers: map[string]*ateapipb.Worker{"worker-1": baseWorker},
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
					WorkerAssignment: &ateapipb.WorkerAssignment{
						Worker: &ateapipb.ObjectRef{Name: "worker-1"},
					},
					ActorVolumes: []*ateapipb.ExternalVolume{
						{VolumeName: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "unknown-plugin"},
					},
				},
			},
			store: &mockDetachStore{
				workers: map[string]*ateapipb.Worker{"worker-1": baseWorker},
			},
			wantErr:         true,
			wantErrContains: "failed to get volume plugin for \"unknown-plugin\"",
		},
		{
			name: "no worker assignment skips detach",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapipb.ActorStatus{
					WorkerAssignment: nil,
					ActorVolumes: []*ateapipb.ExternalVolume{
						{VolumeName: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
					},
				},
			},
			store:           &mockDetachStore{},
			wantDetachCalls: nil,
			wantErr:         false,
		},
		{
			name: "worker not found in store skips detach",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapipb.ActorStatus{
					WorkerAssignment: &ateapipb.WorkerAssignment{
						Worker: &ateapipb.ObjectRef{Name: "nonexistent-worker"},
					},
					ActorVolumes: []*ateapipb.ExternalVolume{
						{VolumeName: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
					},
				},
			},
			store:           &mockDetachStore{workers: map[string]*ateapipb.Worker{}},
			wantDetachCalls: nil,
			wantErr:         false,
		},
		{
			name: "worker has empty node name skips detach",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapipb.ActorStatus{
					WorkerAssignment: &ateapipb.WorkerAssignment{
						Worker: &ateapipb.ObjectRef{Name: "worker-1"},
					},
					ActorVolumes: []*ateapipb.ExternalVolume{
						{VolumeName: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
					},
				},
			},
			store: &mockDetachStore{
				workers: map[string]*ateapipb.Worker{
					"worker-1": {
						Metadata: &ateapipb.ResourceMetadata{Name: "worker-1"},
						NodeName: "",
					},
				},
			},
			wantDetachCalls: nil,
			wantErr:         false,
		},
		{
			name: "store internal error returns error",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapipb.ActorStatus{
					WorkerAssignment: &ateapipb.WorkerAssignment{
						Worker: &ateapipb.ObjectRef{Name: "worker-1"},
					},
					ActorVolumes: []*ateapipb.ExternalVolume{
						{VolumeName: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
					},
				},
			},
			store: &mockDetachStore{
				err: errors.New("db connection failure"),
			},
			wantDetachCalls: nil,
			wantErr:         true,
			wantErrContains: "failed to get worker: db connection failure",
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

			err := detachActorVolumes(ctx, tt.store, registry, tt.actor, tt.template, "test")
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
