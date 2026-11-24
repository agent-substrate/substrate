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
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
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

// emptyIDVolumePlugin provisions volumes without returning an ID.
type emptyIDVolumePlugin struct {
	volume.VolumePluginControlPlane
}

func (emptyIDVolumePlugin) CreateVolume(context.Context, volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	return volume.CreateVolumeResponse{}, nil
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

	res, err := createActorVolumes(context.Background(), registry, scLister, "actor-uid-123", tmpl, input)
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

// restoringVolumePlugin records the snapshot each volume is created from,
// keyed by the volume's name in the storage system, and fails CreateVolume for
// the names in fail.
type restoringVolumePlugin struct {
	*volume.MockVolumePlugin
	fail map[string]error

	mu      sync.Mutex
	sources map[string]string
}

func (p *restoringVolumePlugin) CreateVolume(ctx context.Context, req volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	if err := p.fail[req.Name]; err != nil {
		return volume.CreateVolumeResponse{}, err
	}
	p.mu.Lock()
	p.sources[req.Name] = req.SourceSnapshotID
	p.mu.Unlock()
	return p.MockVolumePlugin.CreateVolume(ctx, req)
}

func TestCreateActorVolumesRestoresSourceSnapshots(t *testing.T) {
	tmpl := &ateapi.ActorTemplate{}
	for _, name := range []string{"vol1", "vol2", "vol3"} {
		tmpl.Volumes = append(tmpl.Volumes, &ateapi.Volume{
			Name:     name,
			External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{StorageClassName: "standard"}},
		})
	}
	scLister := &fakeStorageClassLister{storageClasses: map[string]*storagev1.StorageClass{
		"standard": {ObjectMeta: metav1.ObjectMeta{Name: "standard"}, Provisioner: "mock-standard"},
	}}
	tests := []struct {
		name        string
		input       []*ateapi.ExternalVolumeStatus
		fail        map[string]error
		wantCode    codes.Code
		wantSources map[string]string
		wantRes     []*ateapi.ExternalVolumeStatus
	}{
		{
			name: "only an unprovisioned volume with a source is restored",
			input: []*ateapi.ExternalVolumeStatus{
				{Name: "vol1", VolumeType: "mock-standard", SourceSnapshotId: "snap-1"},
				{Name: "vol2", VolumeType: "mock-standard"},
				// Provisioned by an earlier attempt.
				{Name: "vol3", VolumeType: "mock-standard", StorageVolumeId: "existing-vol3"},
			},
			wantSources: map[string]string{
				"substrate-actor-uid-123-vol1": "snap-1",
				"substrate-actor-uid-123-vol2": "",
			},
			wantRes: []*ateapi.ExternalVolumeStatus{
				{Name: "vol1", VolumeType: "mock-standard", StorageVolumeId: "mock-vol-substrate-actor-uid-123-vol1", SourceSnapshotId: "snap-1"},
				{Name: "vol2", VolumeType: "mock-standard", StorageVolumeId: "mock-vol-substrate-actor-uid-123-vol2"},
				{Name: "vol3", VolumeType: "mock-standard", StorageVolumeId: "existing-vol3"},
			},
		},
		{
			name: "a volume that fails to provision keeps its source for a retry",
			input: []*ateapi.ExternalVolumeStatus{
				{Name: "vol1", VolumeType: "mock-standard", SourceSnapshotId: "snap-1"},
				{Name: "vol2", VolumeType: "mock-standard", SourceSnapshotId: "snap-2"},
				{Name: "vol3", VolumeType: "mock-standard", SourceSnapshotId: "snap-3"},
			},
			fail:        map[string]error{"substrate-actor-uid-123-vol2": status.Error(codes.Unavailable, "driver restarting")},
			wantCode:    codes.Unavailable,
			wantSources: map[string]string{"substrate-actor-uid-123-vol1": "snap-1"},
			wantRes: []*ateapi.ExternalVolumeStatus{
				{Name: "vol1", VolumeType: "mock-standard", StorageVolumeId: "mock-vol-substrate-actor-uid-123-vol1", SourceSnapshotId: "snap-1"},
				{Name: "vol2", VolumeType: "mock-standard", SourceSnapshotId: "snap-2"},
				{Name: "vol3", VolumeType: "mock-standard", SourceSnapshotId: "snap-3"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &restoringVolumePlugin{MockVolumePlugin: volume.NewMockVolumePlugin(), fail: tt.fail, sources: map[string]string{}}
			registry := &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{"mock-standard": plugin}}
			res, err := createActorVolumes(context.Background(), registry, scLister, "actor-uid-123", tmpl, tt.input)
			if got := apierror.Code(err); got != tt.wantCode {
				t.Fatalf("createActorVolumes() code = %v, want %v; error = %v", got, tt.wantCode, err)
			}
			if diff := cmp.Diff(tt.wantSources, plugin.sources); diff != "" {
				t.Errorf("restore sources mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantRes, res, protocmp.Transform()); diff != "" {
				t.Errorf("createActorVolumes() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCreateActorVolumesErrorCodes(t *testing.T) {
	tmpl := &ateapi.ActorTemplate{
		Volumes: []*ateapi.Volume{{
			Name:     "data-vol",
			External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{StorageClassName: "standard"}},
		}},
	}
	scLister := &fakeStorageClassLister{storageClasses: map[string]*storagev1.StorageClass{
		"standard": {ObjectMeta: metav1.ObjectMeta{Name: "standard"}, Provisioner: "mock-standard"},
	}}
	tests := []struct {
		name     string
		source   string
		err      error
		wantCode codes.Code
	}{
		{name: "restore from a deleted snapshot", source: "snap-data", err: status.Error(codes.NotFound, "source snapshot not found"), wantCode: codes.FailedPrecondition},
		{name: "NotFound for an empty volume", err: status.Error(codes.NotFound, "not found"), wantCode: codes.Internal},
		{name: "restore from a snapshot that is not ready", source: "snap-data", err: status.Error(codes.FailedPrecondition, "snapshot not ready"), wantCode: codes.FailedPrecondition},
		{name: "FailedPrecondition for an empty volume", err: status.Error(codes.FailedPrecondition, "precondition"), wantCode: codes.Internal},
		{name: "restore aborted", source: "snap-data", err: status.Error(codes.Aborted, "operation pending"), wantCode: codes.Aborted},
		{name: "Aborted for an empty volume", err: status.Error(codes.Aborted, "operation pending"), wantCode: codes.Internal},
		{name: "capacity smaller than the snapshot", source: "snap-data", err: status.Error(codes.OutOfRange, "capacity too small"), wantCode: codes.FailedPrecondition},
		{name: "invalid parameters", err: status.Error(codes.InvalidArgument, "bad parameter"), wantCode: codes.FailedPrecondition},
		{name: "driver unavailable", err: status.Error(codes.Unavailable, "down"), wantCode: codes.Unavailable},
		{name: "driver timed out", err: status.Error(codes.DeadlineExceeded, "timeout"), wantCode: codes.DeadlineExceeded},
		{name: "quota", err: status.Error(codes.ResourceExhausted, "quota"), wantCode: codes.ResourceExhausted},
		{name: "error without a status", source: "snap-data", err: errors.New("boom"), wantCode: codes.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &restoringVolumePlugin{
				MockVolumePlugin: volume.NewMockVolumePlugin(),
				fail:             map[string]error{"substrate-actor-uid-123-data-vol": fmt.Errorf("CSI CreateVolume failed: %w", tt.err)},
			}
			registry := &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{"mock-standard": plugin}}
			input := []*ateapi.ExternalVolumeStatus{{Name: "data-vol", VolumeType: "mock-standard", SourceSnapshotId: tt.source}}
			_, err := createActorVolumes(context.Background(), registry, scLister, "actor-uid-123", tmpl, input)
			if got := apierror.Code(err); got != tt.wantCode {
				t.Fatalf("createActorVolumes() code = %v, want %v; error = %v", got, tt.wantCode, err)
			}
		})
	}
}

// lookupResult is what snapshotLookupPlugin answers GetSnapshot with.
type lookupResult struct {
	snap  volume.Snapshot
	found bool
	err   error
}

// snapshotLookupPlugin answers GetSnapshot from results, keyed by snapshot
// ID, and records the IDs it was asked about.
type snapshotLookupPlugin struct {
	volume.VolumePluginControlPlane
	results map[string]lookupResult

	mu    sync.Mutex
	calls []string
}

func (p *snapshotLookupPlugin) GetSnapshot(_ context.Context, snapshotID string) (volume.Snapshot, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, snapshotID)
	r := p.results[snapshotID]
	return r.snap, r.found, r.err
}

func (p *snapshotLookupPlugin) lookups() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Sorted(slices.Values(p.calls))
}

// volumeSnapshotStatus returns the status of a snapshot whose handle is
// recorded.
func volumeSnapshotStatus(snapshotID string, readyToUse bool) *ateapi.ExternalVolumeSnapshotStatus {
	return &ateapi.ExternalVolumeSnapshotStatus{SnapshotId: snapshotID, ReadyToUse: readyToUse}
}

// storedVolumeSnapshotsReady returns whether each snapshot tag owns is stored
// ready to use, keyed by source volume name.
func storedVolumeSnapshotsReady(t *testing.T, ctx context.Context, persistence store.Interface, tag *ateapi.Tag) map[string]bool {
	t.Helper()
	got := map[string]bool{}
	for name, status := range tagVolumeStatuses(t, ctx, persistence, tag) {
		got[name] = status.GetReadyToUse()
	}
	return got
}

func TestSetTagVolumeSources(t *testing.T) {
	ready := func(id string) *ateapi.ExternalVolumeSnapshotStatus { return volumeSnapshotStatus(id, true) }
	creating := func(id string) *ateapi.ExternalVolumeSnapshotStatus { return volumeSnapshotStatus(id, false) }
	readyNow := lookupResult{snap: volume.Snapshot{ReadyToUse: true, SizeBytes: 2048}, found: true}
	tests := []struct {
		name     string
		statuses map[string]*ateapi.ExternalVolumeSnapshotStatus
		// volumeType is the driver of the Actor's volumes, testVolumeDriver
		// when empty.
		volumeType string
		results    map[string]lookupResult
		noPlugin   bool
		// markDeleted marks the tag for deletion after the caller read it.
		markDeleted bool
		wantCode    codes.Code
		wantSources map[string]string
		wantLookups []string
		wantReady   map[string]bool
	}{
		{
			name:        "tag captured no volumes",
			wantSources: map[string]string{"data": "", "cache": ""},
			wantReady:   map[string]bool{},
		},
		{
			name:        "ready snapshots are used without a lookup",
			statuses:    map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": ready("snap-data"), "cache": ready("snap-cache")},
			wantSources: map[string]string{"data": "snap-data", "cache": "snap-cache"},
			wantReady:   map[string]bool{"data": true, "cache": true},
		},
		{
			name:        "a volume the tag left out is provisioned empty",
			statuses:    map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": ready("snap-data")},
			wantSources: map[string]string{"data": "snap-data", "cache": ""},
			wantReady:   map[string]bool{"data": true},
		},
		{
			name:        "a creating snapshot the driver now reports ready is used and recorded",
			statuses:    map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": creating("snap-data")},
			results:     map[string]lookupResult{"snap-data": readyNow},
			wantSources: map[string]string{"data": "snap-data", "cache": ""},
			wantLookups: []string{"snap-data"},
			wantReady:   map[string]bool{"data": true},
		},
		{
			name:        "a creating snapshot from a driver that cannot look snapshots up is used",
			statuses:    map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": creating("snap-data")},
			results:     map[string]lookupResult{"snap-data": {err: fmt.Errorf("lookup: %w", errors.ErrUnsupported)}},
			wantSources: map[string]string{"data": "snap-data", "cache": ""},
			wantLookups: []string{"snap-data"},
			wantReady:   map[string]bool{"data": false},
		},
		{
			name:        "a snapshot still copying is refused",
			statuses:    map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": creating("snap-data")},
			results:     map[string]lookupResult{"snap-data": {found: true}},
			wantCode:    codes.FailedPrecondition,
			wantLookups: []string{"snap-data"},
			wantReady:   map[string]bool{"data": false},
		},
		{
			name:        "a snapshot deleted from the storage system is refused",
			statuses:    map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": creating("snap-data")},
			wantCode:    codes.FailedPrecondition,
			wantLookups: []string{"snap-data"},
			wantReady:   map[string]bool{"data": false},
		},
		{
			name:        "a failed lookup is refused",
			statuses:    map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": creating("snap-data")},
			results:     map[string]lookupResult{"snap-data": {err: errors.New("driver unavailable")}},
			wantCode:    codes.Internal,
			wantLookups: []string{"snap-data"},
			wantReady:   map[string]bool{"data": false},
		},
		{
			name:        "a lookup the driver reports unavailable keeps its code",
			statuses:    map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": creating("snap-data")},
			results:     map[string]lookupResult{"snap-data": {err: fmt.Errorf("CSI ListSnapshots failed: %w", status.Error(codes.Unavailable, "down"))}},
			wantCode:    codes.Unavailable,
			wantLookups: []string{"snap-data"},
			wantReady:   map[string]bool{"data": false},
		},
		{
			name:        "a lookup the driver timed out keeps its code",
			statuses:    map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": creating("snap-data")},
			results:     map[string]lookupResult{"snap-data": {err: fmt.Errorf("CSI ListSnapshots failed: %w", status.Error(codes.DeadlineExceeded, "timeout"))}},
			wantCode:    codes.DeadlineExceeded,
			wantLookups: []string{"snap-data"},
			wantReady:   map[string]bool{"data": false},
		},
		{
			name:        "a lookup over the driver's quota keeps its code",
			statuses:    map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": creating("snap-data")},
			results:     map[string]lookupResult{"snap-data": {err: fmt.Errorf("CSI ListSnapshots failed: %w", status.Error(codes.ResourceExhausted, "quota"))}},
			wantCode:    codes.ResourceExhausted,
			wantLookups: []string{"snap-data"},
			wantReady:   map[string]bool{"data": false},
		},
		{
			name:      "a driver without a plugin is refused",
			statuses:  map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": creating("snap-data")},
			noPlugin:  true,
			wantCode:  codes.FailedPrecondition,
			wantReady: map[string]bool{"data": false},
		},
		{
			name:        "a snapshot ready now is recorded even when another is still copying",
			statuses:    map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": creating("snap-data"), "cache": creating("snap-cache")},
			results:     map[string]lookupResult{"snap-data": readyNow, "snap-cache": {found: true}},
			wantCode:    codes.FailedPrecondition,
			wantLookups: []string{"snap-cache", "snap-data"},
			wantReady:   map[string]bool{"data": true, "cache": false},
		},
		{
			name:      "a pending snapshot fails",
			statuses:  map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": ready("snap-data"), "cache": {}},
			wantCode:  codes.Internal,
			wantReady: map[string]bool{"data": true, "cache": false},
		},
		{
			name:      "a failed snapshot fails",
			statuses:  map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": {ErrorMessage: "simulated failure"}},
			wantCode:  codes.Internal,
			wantReady: map[string]bool{"data": false},
		},
		{
			name:       "a snapshot another driver took is refused",
			statuses:   map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": ready("snap-data")},
			volumeType: "other.io/driver",
			wantCode:   codes.FailedPrecondition,
			wantReady:  map[string]bool{"data": true},
		},
		{
			name:        "a tag marked for deletion after it was read is refused",
			statuses:    map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": ready("snap-data")},
			markDeleted: true,
			wantCode:    codes.FailedPrecondition,
			wantReady:   map[string]bool{"data": true},
		},
		{
			name:        "a tag marked for deletion with no snapshots left is refused",
			markDeleted: true,
			wantCode:    codes.FailedPrecondition,
			wantReady:   map[string]bool{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			t.Cleanup(cleanup)
			tag := seedCreatingVolumeTag(t, ctx, persistence, tt.statuses)
			if tt.markDeleted {
				if _, err := persistence.MarkTagForDeletion(ctx, resources.TagRefFromTag(tag), store.DeletePreconditions{}, nil); err != nil {
					t.Fatalf("MarkTagForDeletion: %v", err)
				}
			}
			plugin := &snapshotLookupPlugin{results: tt.results}
			impl := newServiceImpl(persistence, nil)
			impl.pluginRegistry = &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{testVolumeDriver: plugin}}
			if tt.noPlugin {
				impl.pluginRegistry = &mockPluginRegistry{}
			}
			volumeType := testVolumeDriver
			if tt.volumeType != "" {
				volumeType = tt.volumeType
			}
			vols := []*ateapi.ExternalVolumeStatus{
				{Name: "data", VolumeType: volumeType},
				{Name: "cache", VolumeType: volumeType},
			}

			err := impl.setTagVolumeSources(ctx, tag, vols)
			if got := apierror.Code(err); got != tt.wantCode {
				t.Fatalf("setTagVolumeSources() = %v, want code %v", err, tt.wantCode)
			}
			if err == nil {
				got := map[string]string{}
				for _, vol := range vols {
					got[vol.GetName()] = vol.GetSourceSnapshotId()
				}
				if diff := cmp.Diff(tt.wantSources, got); diff != "" {
					t.Errorf("source snapshots mismatch (-want +got):\n%s", diff)
				}
			}
			if diff := cmp.Diff(tt.wantLookups, plugin.lookups(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("GetSnapshot calls mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantReady, storedVolumeSnapshotsReady(t, ctx, persistence, tag)); diff != "" {
				t.Errorf("stored ready_to_use mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestRecordVolumeSnapshotReady verifies that recording readiness never fails
// the caller: a write that loses a race is retried against the stored
// snapshot, a failed write returns the snapshot as ready to use without
// storing it, and a snapshot deleted meanwhile is returned as it was.
func TestRecordVolumeSnapshotReady(t *testing.T) {
	observed := volume.Snapshot{ReadyToUse: true, SizeBytes: 2048}
	tests := []struct {
		name string
		// stale records the snapshot once before the call, so the call's
		// copy is out of date.
		stale bool
		// negativeSize has the driver report a negative size, which is
		// not recorded.
		negativeSize    bool
		failUpdate      bool
		deleted         bool
		wantReady       bool
		wantStoredReady bool
	}{
		{name: "recorded", wantReady: true, wantStoredReady: true},
		{name: "stale copy is retried", stale: true, wantReady: true, wantStoredReady: true},
		{name: "failed write", failUpdate: true, wantReady: true},
		{name: "negative size", negativeSize: true, wantReady: true, wantStoredReady: true},
		{name: "negative size and failed write", negativeSize: true, failUpdate: true, wantReady: true},
		{name: "deleted meanwhile", deleted: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			t.Cleanup(cleanup)
			tag := seedCreatingVolumeTag(t, ctx, persistence, map[string]*ateapi.ExternalVolumeSnapshotStatus{
				"data": volumeSnapshotStatus("snap-data", false),
			})
			snap := tagVolumeSnapshots(t, ctx, persistence, tag)["data"]
			ref := resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(snap)
			faulty := &faultyEVSStore{Interface: persistence}
			impl := newServiceImpl(faulty, nil)
			if tt.stale {
				impl.recordVolumeSnapshotReady(ctx, snap, observed)
			}
			if tt.deleted {
				if _, err := persistence.DeleteExternalVolumeSnapshot(ctx, ref, store.DeletePreconditions{}); err != nil {
					t.Fatalf("DeleteExternalVolumeSnapshot: %v", err)
				}
			}
			faulty.failUpdate = tt.failUpdate

			obs, wantSize := observed, observed.SizeBytes
			if tt.negativeSize {
				obs.SizeBytes, wantSize = -1, 0
			}
			got := impl.recordVolumeSnapshotReady(ctx, snap, obs)
			if got.GetStatus().GetReadyToUse() != tt.wantReady {
				t.Errorf("returned ready_to_use = %v, want %v", got.GetStatus().GetReadyToUse(), tt.wantReady)
			}
			if tt.wantReady && got.GetStatus().GetRestoreSizeBytes() != wantSize {
				t.Errorf("returned restore_size_bytes = %d, want %d", got.GetStatus().GetRestoreSizeBytes(), wantSize)
			}
			stored, err := persistence.GetExternalVolumeSnapshot(ctx, ref)
			if tt.deleted {
				if !errors.Is(err, store.ErrNotFound) {
					t.Errorf("GetExternalVolumeSnapshot = %v, want ErrNotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetExternalVolumeSnapshot: %v", err)
			}
			if stored.GetStatus().GetReadyToUse() != tt.wantStoredReady {
				t.Errorf("stored ready_to_use = %v, want %v", stored.GetStatus().GetReadyToUse(), tt.wantStoredReady)
			}
			if tt.wantStoredReady && stored.GetStatus().GetRestoreSizeBytes() != wantSize {
				t.Errorf("stored restore_size_bytes = %d, want %d", stored.GetStatus().GetRestoreSizeBytes(), wantSize)
			}
		})
	}
}

// completeTag marks the tag's snapshot COMPLETED, as CreateTag does last.
func completeTag(t *testing.T, ctx context.Context, persistence store.Interface, tag *ateapi.Tag) *ateapi.Tag {
	t.Helper()
	completed, err := persistence.UpdateTag(ctx, resources.TagRefFromTag(tag), store.PreconditionFrom(tag), func(toUpdate *ateapi.Tag) error {
		toUpdate.Status.Snapshot.DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
		return nil
	})
	if err != nil {
		t.Fatalf("completing tag: %v", err)
	}
	return completed
}

func TestRefreshTagVolumeSnapshots(t *testing.T) {
	tests := []struct {
		name   string
		status *ateapi.ExternalVolumeSnapshotStatus
		result lookupResult
		// pending leaves the tag's snapshot PENDING.
		pending bool
		// deleting marks the tag for deletion.
		deleting    bool
		wantLookups []string
		wantReady   bool
	}{
		{
			name:        "a snapshot the driver now reports ready is recorded",
			status:      volumeSnapshotStatus("snap-data", false),
			result:      lookupResult{snap: volume.Snapshot{ReadyToUse: true}, found: true},
			wantLookups: []string{"snap-data"},
			wantReady:   true,
		},
		{
			name:        "a snapshot still copying stays not ready",
			status:      volumeSnapshotStatus("snap-data", false),
			result:      lookupResult{found: true},
			wantLookups: []string{"snap-data"},
		},
		{
			name:        "a snapshot deleted from the storage system stays not ready",
			status:      volumeSnapshotStatus("snap-data", false),
			wantLookups: []string{"snap-data"},
		},
		{
			name:        "a failed lookup leaves the snapshot not ready",
			status:      volumeSnapshotStatus("snap-data", false),
			result:      lookupResult{err: errors.New("driver unavailable")},
			wantLookups: []string{"snap-data"},
		},
		{
			name:        "a driver that cannot look snapshots up leaves the snapshot not ready",
			status:      volumeSnapshotStatus("snap-data", false),
			result:      lookupResult{err: fmt.Errorf("lookup: %w", errors.ErrUnsupported)},
			wantLookups: []string{"snap-data"},
		},
		{
			name:      "a ready snapshot is not looked up",
			status:    volumeSnapshotStatus("snap-data", true),
			wantReady: true,
		},
		{
			name:    "a pending tag is not looked up",
			status:  volumeSnapshotStatus("snap-data", false),
			result:  lookupResult{snap: volume.Snapshot{ReadyToUse: true}, found: true},
			pending: true,
		},
		{
			name:     "a tag being deleted is not looked up",
			status:   volumeSnapshotStatus("snap-data", false),
			result:   lookupResult{snap: volume.Snapshot{ReadyToUse: true}, found: true},
			deleting: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			t.Cleanup(cleanup)
			tag := seedCreatingVolumeTag(t, ctx, persistence, map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": tt.status})
			if !tt.pending {
				tag = completeTag(t, ctx, persistence, tag)
			}
			if tt.deleting {
				var err error
				if tag, err = persistence.MarkTagForDeletion(ctx, resources.TagRefFromTag(tag), store.DeletePreconditions{}, nil); err != nil {
					t.Fatalf("MarkTagForDeletion: %v", err)
				}
			}
			plugin := &snapshotLookupPlugin{results: map[string]lookupResult{"snap-data": tt.result}}
			impl := newServiceImpl(persistence, nil)
			impl.pluginRegistry = &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{testVolumeDriver: plugin}}

			impl.refreshTagVolumeSnapshots(ctx, tag)
			if diff := cmp.Diff(tt.wantLookups, plugin.lookups(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("GetSnapshot calls mismatch (-want +got):\n%s", diff)
			}
			if got := storedVolumeSnapshotsReady(t, ctx, persistence, tag)["data"]; got != tt.wantReady {
				t.Errorf("stored ready_to_use = %v, want %v", got, tt.wantReady)
			}
		})
	}
}

func TestRefreshVolumeSnapshot(t *testing.T) {
	ready := lookupResult{snap: volume.Snapshot{ReadyToUse: true, SizeBytes: 2048}, found: true}
	tests := []struct {
		name   string
		status *ateapi.ExternalVolumeSnapshotStatus
		result lookupResult
		// pending leaves the tag's snapshot PENDING.
		pending  bool
		deleting bool
		// wantReady is the ready_to_use returned and stored.
		wantReady   bool
		wantLookups []string
	}{
		{
			name:        "a snapshot the driver now reports ready is recorded",
			status:      volumeSnapshotStatus("snap-data", false),
			result:      ready,
			wantReady:   true,
			wantLookups: []string{"snap-data"},
		},
		{
			name:        "a snapshot still copying stays not ready",
			status:      volumeSnapshotStatus("snap-data", false),
			result:      lookupResult{found: true},
			wantLookups: []string{"snap-data"},
		},
		{
			name:      "a ready snapshot is not looked up",
			status:    volumeSnapshotStatus("snap-data", true),
			wantReady: true,
		},
		{
			name:     "a snapshot of a tag being deleted is not looked up",
			status:   volumeSnapshotStatus("snap-data", false),
			result:   ready,
			deleting: true,
		},
		{
			name:    "a snapshot of a pending tag is not looked up",
			status:  volumeSnapshotStatus("snap-data", false),
			result:  ready,
			pending: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			t.Cleanup(cleanup)
			tag := seedCreatingVolumeTag(t, ctx, persistence, map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": tt.status})
			if !tt.pending {
				tag = completeTag(t, ctx, persistence, tag)
			}
			if tt.deleting {
				if _, err := persistence.MarkTagForDeletion(ctx, resources.TagRefFromTag(tag), store.DeletePreconditions{}, nil); err != nil {
					t.Fatalf("MarkTagForDeletion: %v", err)
				}
			}
			plugin := &snapshotLookupPlugin{results: map[string]lookupResult{"snap-data": tt.result}}
			impl := newServiceImpl(persistence, nil)
			impl.pluginRegistry = &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{testVolumeDriver: plugin}}

			got := impl.refreshVolumeSnapshot(ctx, tagVolumeSnapshots(t, ctx, persistence, tag)["data"])
			if got.GetStatus().GetReadyToUse() != tt.wantReady {
				t.Errorf("returned ready_to_use = %v, want %v", got.GetStatus().GetReadyToUse(), tt.wantReady)
			}
			if diff := cmp.Diff(tt.wantLookups, plugin.lookups(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("GetSnapshot calls mismatch (-want +got):\n%s", diff)
			}
			if stored := storedVolumeSnapshotsReady(t, ctx, persistence, tag)["data"]; stored != tt.wantReady {
				t.Errorf("stored ready_to_use = %v, want %v", stored, tt.wantReady)
			}
		})
	}
}
