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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
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
	tmpl := &ateapi.ActorTemplate{
		Volumes: []*ateapi.Volume{{
			Name: "data-vol",
			External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
				StorageClassName: "standard",
			}},
		}},
	}
	volumes := []*ateapi.ExternalVolumeStatus{{
		Name:       "data-vol",
		VolumeType: "mock-standard",
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
				_, err := initialExternalVolumes(ctx, lister, tmpl)
				if got := apierror.Code(err); got != tt.wantCode {
					t.Fatalf("initialExternalVolumes() code = %v, want %v; error = %v", got, tt.wantCode, err)
				}
				if !strings.Contains(err.Error(), `StorageClass "standard"`) {
					t.Errorf("initialExternalVolumes() error does not name the StorageClass: %v", err)
				}
			})
			t.Run("create", func(t *testing.T) {
				res, err := createActorVolumes(ctx, &mockPluginRegistry{}, lister, "actor-uid-123", tmpl, volumes, nil)
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

func TestInitialActorVolumes(t *testing.T) {
	tmpl := &ateapi.ActorTemplate{
		Volumes: []*ateapi.Volume{
			{
				Name: "data-vol-1",
				External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
					StorageClassName: "standard",
				}},
			},
			{
				Name: "scratch-vol",
			},
			{
				Name:       "durable-vol",
				DurableDir: &ateapi.DurableDirVolumeSource{},
			},
			{
				Name: "data-vol-2",
				External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
					StorageClassName: "fast",
				}},
			},
		},
	}

	want := []*ateapi.ExternalVolumeStatus{
		{
			Name:       "data-vol-1",
			VolumeType: "mock-standard",
		},
		{
			Name:       "data-vol-2",
			VolumeType: "mock-fast",
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
	initVols, err := initialExternalVolumes(context.Background(), scLister, tmpl)
	if err != nil {
		t.Fatalf("initialExternalVolumes failed: %v", err)
	}
	if diff := cmp.Diff(want, initVols, protocmp.Transform()); diff != "" {
		t.Errorf("initialExternalVolumes mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateActorVolumes(t *testing.T) {
	ctx := context.Background()

	standardTmpl := &ateapi.ActorTemplate{
		Volumes: []*ateapi.Volume{
			{
				Name: "data-vol",
				External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
					StorageClassName: "standard",
				}},
			},
		},
	}

	multiVolTmpl := &ateapi.ActorTemplate{
		Volumes: []*ateapi.Volume{
			{
				Name: "vol1",
				External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
					StorageClassName: "standard",
				}},
			},
			{
				Name: "vol2",
				External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
					StorageClassName: "standard",
				}},
			},
			{
				Name: "vol3",
				External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
					StorageClassName: "standard",
				}},
			},
		},
	}

	tests := []struct {
		name           string
		tmpl           *ateapi.ActorTemplate
		inputVolumes   []*ateapi.ExternalVolumeStatus
		storageClasses map[string]*storagev1.StorageClass
		wantErr        bool
		wantRes        []*ateapi.ExternalVolumeStatus
	}{
		{
			name: "partial failure returns error and preserves succeeded, failed, and remaining volumes",
			tmpl: multiVolTmpl,
			inputVolumes: []*ateapi.ExternalVolumeStatus{
				{Name: "vol1", VolumeType: "mock-standard"},
				// Its type differs from its StorageClass's provisioner, so creating it fails.
				{Name: "vol2", VolumeType: "mock-fast"},
				{Name: "vol3", VolumeType: "mock-standard"},
			},
			wantErr: true,
			wantRes: []*ateapi.ExternalVolumeStatus{
				{
					Name:            "vol1",
					StorageVolumeId: "mock-vol-substrate-actor-uid-123-vol1",
					VolumeType:      "mock-standard",
				},
				{Name: "vol2", VolumeType: "mock-fast"},
				{Name: "vol3", VolumeType: "mock-standard"},
			},
		},
		{
			name: "volume with a storage volume id is not created again",
			tmpl: standardTmpl,
			inputVolumes: []*ateapi.ExternalVolumeStatus{
				{
					Name:            "data-vol",
					StorageVolumeId: "existing-vol-id",
				},
			},
			wantErr: false,
			wantRes: []*ateapi.ExternalVolumeStatus{
				{
					Name:            "data-vol",
					StorageVolumeId: "existing-vol-id",
				},
			},
		},
		{
			name: "volume not found in template returns error",
			tmpl: &ateapi.ActorTemplate{},
			inputVolumes: []*ateapi.ExternalVolumeStatus{
				{Name: "missing-vol"},
			},
			wantErr: true,
			wantRes: []*ateapi.ExternalVolumeStatus{
				{Name: "missing-vol"},
			},
		},
		{
			name: "storage class parameters are propagated to volume context",
			tmpl: standardTmpl,
			inputVolumes: []*ateapi.ExternalVolumeStatus{
				{
					Name:       "data-vol",
					VolumeType: "mock-standard",
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
			wantRes: []*ateapi.ExternalVolumeStatus{
				{
					Name:            "data-vol",
					StorageVolumeId: "mock-vol-substrate-actor-uid-123-data-vol",
					VolumeType:      "mock-standard",
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
			res, err := createActorVolumes(ctx, registry, scLister, "actor-uid-123", tt.tmpl, tt.inputVolumes, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("createActorVolumes() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.wantRes, res, protocmp.Transform()); diff != "" {
				t.Errorf("createActorVolumes() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// emptyIDVolumePlugin provisions volumes without returning an ID.
type emptyIDVolumePlugin struct {
	volume.VolumePluginControlPlane
}

func (emptyIDVolumePlugin) CreateVolume(context.Context, volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	return volume.CreateVolumeResponse{}, nil
}

// notFoundVolumePlugin fails every CreateVolume with NotFound, as a CSI driver
// does when the content source is gone.
type notFoundVolumePlugin struct {
	volume.VolumePluginControlPlane
}

func (notFoundVolumePlugin) CreateVolume(context.Context, volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	return volume.CreateVolumeResponse{}, fmt.Errorf("CSI CreateVolume failed: %w", status.Error(codes.NotFound, "source snapshot not found"))
}

func TestCreateActorVolumesNotFound(t *testing.T) {
	tmpl := &ateapi.ActorTemplate{
		Volumes: []*ateapi.Volume{{
			Name:     "data-vol",
			External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{StorageClassName: "standard"}},
		}},
	}
	registry := &mockPluginRegistry{
		plugins: map[string]volume.VolumePluginControlPlane{"mock-standard": notFoundVolumePlugin{}},
	}
	scLister := &fakeStorageClassLister{storageClasses: map[string]*storagev1.StorageClass{
		"standard": {ObjectMeta: metav1.ObjectMeta{Name: "standard"}, Provisioner: "mock-standard"},
	}}
	tests := []struct {
		name     string
		sources  []*ateapi.ExternalVolumeSnapshot
		wantCode codes.Code
	}{
		{
			name:     "restore from a deleted snapshot",
			sources:  []*ateapi.ExternalVolumeSnapshot{{SourceVolumeName: "data-vol", StorageSnapshotId: "snap-data", VolumeType: "mock-standard"}},
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "empty volume",
			wantCode: codes.Internal,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := []*ateapi.ExternalVolumeStatus{{Name: "data-vol", VolumeType: "mock-standard"}}
			_, err := createActorVolumes(context.Background(), registry, scLister, "actor-uid-123", tmpl, input, tt.sources)
			if got := apierror.Code(err); got != tt.wantCode {
				t.Fatalf("createActorVolumes() code = %v, want %v; error = %v", got, tt.wantCode, err)
			}
		})
	}
}

func TestCreateActorVolumesRejectsEmptyPluginID(t *testing.T) {
	tmpl := &ateapi.ActorTemplate{
		Volumes: []*ateapi.Volume{{
			Name:     "data-vol",
			External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{StorageClassName: "standard"}},
		}},
	}
	registry := &mockPluginRegistry{
		plugins: map[string]volume.VolumePluginControlPlane{"mock-standard": emptyIDVolumePlugin{}},
	}
	scLister := &fakeStorageClassLister{storageClasses: map[string]*storagev1.StorageClass{
		"standard": {ObjectMeta: metav1.ObjectMeta{Name: "standard"}, Provisioner: "mock-standard"},
	}}
	input := []*ateapi.ExternalVolumeStatus{{Name: "data-vol", VolumeType: "mock-standard"}}

	res, err := createActorVolumes(context.Background(), registry, scLister, "actor-uid-123", tmpl, input, nil)
	if got := apierror.Code(err); got != codes.Internal {
		t.Fatalf("createActorVolumes() code = %v, want %v; error = %v", got, codes.Internal, err)
	}
	// The volume stays unprovisioned, so a retry creates it again.
	if diff := cmp.Diff(input, res, protocmp.Transform()); diff != "" {
		t.Errorf("createActorVolumes() mismatch (-want +got):\n%s", diff)
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
		volumes     []*ateapi.ExternalVolumeStatus
		wantDeleted []string
		wantErr     bool
	}{
		{
			name:     "uses storage volume ID when present",
			actorUID: "uid-abc",
			volumes: []*ateapi.ExternalVolumeStatus{
				{Name: "vol1", StorageVolumeId: "storage-vol-123", VolumeType: "mock"},
			},
			wantDeleted: []string{"storage-vol-123"},
			wantErr:     false,
		},
		{
			name:     "falls back to actorVolumeID when storage volume ID is empty",
			actorUID: "uid-abc",
			volumes: []*ateapi.ExternalVolumeStatus{
				{Name: "vol1", StorageVolumeId: "", VolumeType: "mock"},
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
		actor           *ateapi.Actor
		template        *ateapi.ActorTemplate
		plugin          *mockDetachVolumePlugin
		pluginRegistry  *mockPluginRegistry
		wantDetachCalls []detachCall
		wantErr         bool
		wantErrContains string
	}{
		{
			name: "success with multiple mounted volumes",
			actor: &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapi.ActorStatus{
					AssignedNode: "node-1",
					ExternalVolumes: &ateapi.ExternalVolumes{Volumes: []*ateapi.ExternalVolumeStatus{
						{Name: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
						{Name: "vol2", StorageVolumeId: "storage-vol-2", VolumeType: "mock"},
					}},
				},
			},
			template: &ateapi.ActorTemplate{
				Volumes: []*ateapi.Volume{
					{Name: "vol1"},
					{Name: "vol2"},
				},
				Containers: []*ateapi.Container{
					{
						VolumeMounts: []*ateapi.VolumeMount{
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
			actor: &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapi.ActorStatus{
					AssignedNode: "node-1",
					ExternalVolumes: &ateapi.ExternalVolumes{Volumes: []*ateapi.ExternalVolumeStatus{
						{Name: "mounted-vol", StorageVolumeId: "storage-vol-mounted", VolumeType: "mock"},
						{Name: "unmounted-vol", StorageVolumeId: "storage-vol-unmounted", VolumeType: "mock"},
					}},
				},
			},
			template: &ateapi.ActorTemplate{
				Volumes: []*ateapi.Volume{
					{Name: "mounted-vol"},
					{Name: "unmounted-vol"},
				},
				Containers: []*ateapi.Container{
					{
						VolumeMounts: []*ateapi.VolumeMount{
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
			actor: &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapi.ActorStatus{
					AssignedNode: "node-1",
					ExternalVolumes: &ateapi.ExternalVolumes{Volumes: []*ateapi.ExternalVolumeStatus{
						{Name: "vol1", StorageVolumeId: "", VolumeType: "mock"},
						{Name: "vol2", StorageVolumeId: "storage-vol-2", VolumeType: "mock"},
					}},
				},
			},
			template: &ateapi.ActorTemplate{
				Volumes: []*ateapi.Volume{
					{Name: "vol1"},
					{Name: "vol2"},
				},
				Containers: []*ateapi.Container{
					{
						VolumeMounts: []*ateapi.VolumeMount{
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
			actor: &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapi.ActorStatus{
					AssignedNode: "node-1",
					ExternalVolumes: &ateapi.ExternalVolumes{Volumes: []*ateapi.ExternalVolumeStatus{
						{Name: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
						{Name: "vol2", StorageVolumeId: "storage-vol-2", VolumeType: "mock"},
					}},
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
			actor: &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapi.ActorStatus{
					AssignedNode: "node-1",
					ExternalVolumes: &ateapi.ExternalVolumes{Volumes: []*ateapi.ExternalVolumeStatus{
						{Name: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
					}},
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
			actor: &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapi.ActorStatus{
					AssignedNode: "node-1",
					ExternalVolumes: &ateapi.ExternalVolumes{Volumes: []*ateapi.ExternalVolumeStatus{
						{Name: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
						{Name: "vol2", StorageVolumeId: "storage-vol-2", VolumeType: "mock"},
					}},
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
			actor: &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapi.ActorStatus{
					AssignedNode: "node-1",
					ExternalVolumes: &ateapi.ExternalVolumes{Volumes: []*ateapi.ExternalVolumeStatus{
						{Name: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "unknown-plugin"},
					}},
				},
			},
			wantErr:         true,
			wantErrContains: "failed to get volume plugin for \"unknown-plugin\"",
		},
		{
			name: "no assigned node skips detach",
			actor: &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{Name: "actor-1", Atespace: "default"},
				Status: &ateapi.ActorStatus{
					AssignedNode: "",
					ExternalVolumes: &ateapi.ExternalVolumes{Volumes: []*ateapi.ExternalVolumeStatus{
						{Name: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"},
					}},
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

// snapshotLookupPlugin answers GetSnapshot with a fixed result and counts the
// calls.
type snapshotLookupPlugin struct {
	volume.VolumePluginControlPlane
	snap  volume.Snapshot
	found bool
	err   error
	calls int
}

func (p *snapshotLookupPlugin) GetSnapshot(context.Context, string) (volume.Snapshot, bool, error) {
	p.calls++
	return p.snap, p.found, p.err
}

func TestEnsureTagVolumeSnapshotsUsableReadiness(t *testing.T) {
	vols := []*ateapi.ExternalVolumeStatus{
		{Name: "data", VolumeType: "mock-standard"},
		{Name: "cache", VolumeType: "mock-standard"},
	}
	tests := []struct {
		name string
		// recordedReady is the data snapshot's ready_to_use on the tag. The
		// tag captures no cache snapshot, so cache is provisioned empty.
		recordedReady bool
		plugin        *snapshotLookupPlugin
		wantCode      codes.Code
		wantCalls     int
		// wantReady is ready_to_use on the returned and the stored tag.
		wantReady       bool
		wantStoredReady bool
	}{
		{
			name:            "snapshot recorded ready is used without asking the driver",
			recordedReady:   true,
			plugin:          &snapshotLookupPlugin{},
			wantReady:       true,
			wantStoredReady: true,
		},
		{
			name:            "snapshot the driver now reports ready is recorded on the tag",
			plugin:          &snapshotLookupPlugin{snap: volume.Snapshot{ReadyToUse: true}, found: true},
			wantCalls:       1,
			wantReady:       true,
			wantStoredReady: true,
		},
		{
			name:      "snapshot still copying",
			plugin:    &snapshotLookupPlugin{found: true},
			wantCode:  codes.FailedPrecondition,
			wantCalls: 1,
		},
		{
			name:      "snapshot deleted from the storage system",
			plugin:    &snapshotLookupPlugin{},
			wantCode:  codes.FailedPrecondition,
			wantCalls: 1,
		},
		{
			name:      "driver cannot look snapshots up",
			plugin:    &snapshotLookupPlugin{err: fmt.Errorf("lookup: %w", errors.ErrUnsupported)},
			wantCalls: 1,
		},
		{
			name:      "lookup fails",
			plugin:    &snapshotLookupPlugin{err: errors.New("driver unavailable")},
			wantCode:  codes.Internal,
			wantCalls: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			t.Cleanup(cleanup)
			actor := newTestSuspendedActor(t, ctx, persistence, "team-a", "source")
			seeded := newTestTag(t, "with-volumes", actor)
			seeded.Status.Snapshot.VolumeSnapshots = []*ateapi.ExternalVolumeSnapshot{
				{SourceVolumeName: "data", SourceVolumeId: "vol-data", StorageSnapshotId: "snap-data", VolumeType: "mock-standard", ReadyToUse: tt.recordedReady},
			}
			tag := storetest.MustCreateTag(t, ctx, persistence, seeded)
			impl := newServiceImpl(persistence, nil)
			impl.pluginRegistry = &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{"mock-standard": tt.plugin}}

			got, err := impl.ensureTagVolumeSnapshotsUsable(ctx, tag, vols)
			if code := apierror.Code(err); code != tt.wantCode {
				t.Fatalf("ensureTagVolumeSnapshotsUsable() error = %v, want code %v", err, tt.wantCode)
			}
			if tt.plugin.calls != tt.wantCalls {
				t.Errorf("GetSnapshot calls = %d, want %d", tt.plugin.calls, tt.wantCalls)
			}
			if err == nil {
				if ready := got.GetStatus().GetSnapshot().GetVolumeSnapshots()[0].GetReadyToUse(); ready != tt.wantReady {
					t.Errorf("returned tag ready_to_use = %v, want %v", ready, tt.wantReady)
				}
			}
			stored, err := persistence.GetTag(ctx, resources.TagRefFromTag(tag))
			if err != nil {
				t.Fatalf("GetTag: %v", err)
			}
			if ready := stored.GetStatus().GetSnapshot().GetVolumeSnapshots()[0].GetReadyToUse(); ready != tt.wantStoredReady {
				t.Errorf("stored tag ready_to_use = %v, want %v", ready, tt.wantStoredReady)
			}
		})
	}
}

func TestRefreshTagVolumeSnapshots(t *testing.T) {
	tests := []struct {
		name      string
		modify    func(*ateapi.Tag)
		plugin    *snapshotLookupPlugin
		wantCalls int
		wantReady bool
	}{
		{
			name:      "snapshot the driver now reports ready is recorded",
			plugin:    &snapshotLookupPlugin{snap: volume.Snapshot{ReadyToUse: true}, found: true},
			wantCalls: 1,
			wantReady: true,
		},
		{
			name:      "snapshot still copying stays not ready",
			plugin:    &snapshotLookupPlugin{found: true},
			wantCalls: 1,
		},
		{
			name:      "snapshot deleted from the storage system stays not ready",
			plugin:    &snapshotLookupPlugin{},
			wantCalls: 1,
		},
		{
			name:      "lookup failure stays not ready",
			plugin:    &snapshotLookupPlugin{err: errors.New("driver unavailable")},
			wantCalls: 1,
		},
		{
			name: "snapshot recorded ready is not looked up",
			modify: func(tag *ateapi.Tag) {
				tag.Status.Snapshot.VolumeSnapshots[0].ReadyToUse = true
			},
			plugin:    &snapshotLookupPlugin{},
			wantReady: true,
		},
		{
			name: "pending tag is not looked up",
			modify: func(tag *ateapi.Tag) {
				tag.Status.Snapshot.DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
			},
			plugin: &snapshotLookupPlugin{snap: volume.Snapshot{ReadyToUse: true}, found: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			t.Cleanup(cleanup)
			actor := newTestSuspendedActor(t, ctx, persistence, "team-a", "source")
			seeded := newTestTag(t, "with-volumes", actor)
			seeded.Status.Snapshot.VolumeSnapshots = []*ateapi.ExternalVolumeSnapshot{
				{SourceVolumeName: "data", SourceVolumeId: "vol-data", StorageSnapshotId: "snap-data", VolumeType: "mock-standard"},
			}
			if tt.modify != nil {
				tt.modify(seeded)
			}
			tag := storetest.MustCreateTag(t, ctx, persistence, seeded)
			impl := newServiceImpl(persistence, nil)
			impl.pluginRegistry = &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{"mock-standard": tt.plugin}}

			got := impl.refreshTagVolumeSnapshots(ctx, tag)
			if tt.plugin.calls != tt.wantCalls {
				t.Errorf("GetSnapshot calls = %d, want %d", tt.plugin.calls, tt.wantCalls)
			}
			if ready := got.GetStatus().GetSnapshot().GetVolumeSnapshots()[0].GetReadyToUse(); ready != tt.wantReady {
				t.Errorf("returned tag ready_to_use = %v, want %v", ready, tt.wantReady)
			}
			stored, err := persistence.GetTag(ctx, resources.TagRefFromTag(tag))
			if err != nil {
				t.Fatalf("GetTag: %v", err)
			}
			if ready := stored.GetStatus().GetSnapshot().GetVolumeSnapshots()[0].GetReadyToUse(); ready != tt.wantReady {
				t.Errorf("stored tag ready_to_use = %v, want %v", ready, tt.wantReady)
			}
		})
	}
}

func TestEnsureTagVolumeSnapshotsUsable(t *testing.T) {
	vols := []*ateapi.ExternalVolumeStatus{
		{Name: "data", VolumeType: "mock-standard"},
		{Name: "cache", VolumeType: "mock-standard"},
	}
	tagWith := func(snaps ...*ateapi.ExternalVolumeSnapshot) *ateapi.Tag {
		return &ateapi.Tag{
			Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "v1"},
			Status:   &ateapi.TagStatus{Snapshot: &ateapi.Snapshot{VolumeSnapshots: snaps}},
		}
	}
	snap := func(name, handle, driver string) *ateapi.ExternalVolumeSnapshot {
		return &ateapi.ExternalVolumeSnapshot{SourceVolumeName: name, StorageSnapshotId: handle, VolumeType: driver, ReadyToUse: true}
	}
	tests := []struct {
		name     string
		tag      *ateapi.Tag
		vols     []*ateapi.ExternalVolumeStatus
		wantCode codes.Code
	}{
		{name: "no tag", vols: vols},
		{name: "tag captured no volumes", tag: tagWith(), vols: vols},
		{name: "tag captured every volume", tag: tagWith(snap("data", "s1", "mock-standard"), snap("cache", "s2", "mock-standard")), vols: vols},
		{name: "tag captured a subset", tag: tagWith(snap("data", "s1", "mock-standard")), vols: vols},
		{name: "template has no external volumes", tag: tagWith(snap("data", "", "mock-standard"))},
		{name: "snapshot without a handle", tag: tagWith(snap("data", "s1", "mock-standard"), snap("cache", "", "mock-standard")), vols: vols, wantCode: codes.Internal},
		{name: "snapshot taken by another driver", tag: tagWith(snap("data", "s1", "mock-fast")), vols: vols, wantCode: codes.FailedPrecondition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Every snapshot is recorded ready, so nothing is looked up or written.
			_, err := newServiceImpl(nil, nil).ensureTagVolumeSnapshotsUsable(context.Background(), tt.tag, tt.vols)
			if code := apierror.Code(err); code != tt.wantCode {
				t.Errorf("ensureTagVolumeSnapshotsUsable() = %v, want code %v", err, tt.wantCode)
			}
		})
	}
}

// noSnapshotVolumePlugin is a driver without snapshot support.
type noSnapshotVolumePlugin struct {
	*volume.MockVolumePlugin
}

func (noSnapshotVolumePlugin) ControllerCapabilities(context.Context) (volume.Capabilities, error) {
	return volume.Capabilities{}, nil
}

func TestEnsureContentSourcesRestorable(t *testing.T) {
	externalVol := func(name, snapshotID string) *ateapi.Volume {
		tmpl := &ateapi.ExternalVolumeTemplate{StorageClassName: "standard"}
		if snapshotID != "" {
			tmpl.ContentSource = &ateapi.VolumeContentSource{Snapshot: &ateapi.SnapshotContentSource{StorageSnapshotId: snapshotID}}
		}
		return &ateapi.Volume{Name: name, External: &ateapi.ExternalVolumeSource{Template: tmpl}}
	}
	tmpl := &ateapi.ActorTemplate{Volumes: []*ateapi.Volume{externalVol("data", "imported"), externalVol("cache", "")}}
	tagWithData := &ateapi.Tag{Status: &ateapi.TagStatus{Snapshot: &ateapi.Snapshot{VolumeSnapshots: []*ateapi.ExternalVolumeSnapshot{
		{SourceVolumeName: "data", StorageSnapshotId: "tag-snap", VolumeType: "no-snapshots"},
	}}}}
	tests := []struct {
		name     string
		driver   string
		tag      *ateapi.Tag
		wantCode codes.Code
	}{
		{name: "driver supports snapshots", driver: "mock-standard"},
		{name: "driver without snapshots", driver: "no-snapshots", wantCode: codes.FailedPrecondition},
		{name: "tag captured the volume", driver: "no-snapshots", tag: tagWithData},
		{name: "unknown driver", driver: "missing", wantCode: codes.FailedPrecondition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newServiceImpl(nil, nil)
			s.pluginRegistry = &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{
				"mock-standard": volume.NewMockVolumePlugin(),
				"no-snapshots":  noSnapshotVolumePlugin{volume.NewMockVolumePlugin()},
			}}
			vols := []*ateapi.ExternalVolumeStatus{{Name: "data", VolumeType: tt.driver}, {Name: "cache", VolumeType: tt.driver}}
			err := s.ensureContentSourcesRestorable(context.Background(), tmpl, tt.tag, vols)
			if code := apierror.Code(err); code != tt.wantCode {
				t.Errorf("ensureContentSourcesRestorable() = %v, want code %v", err, tt.wantCode)
			}
		})
	}
}
