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

// recordedVolume returns a provisioned volume whose attached_node is node.
func recordedVolume(name, storageID, volumeType, node string) *ateapipb.ExternalVolume {
	return &ateapipb.ExternalVolume{
		VolumeName:      name,
		StorageVolumeId: storageID,
		VolumeType:      volumeType,
		Status:          ateapipb.ExternalVolume_STATUS_CREATED,
		AttachedNode:    node,
	}
}

func TestDetachActorVolumes(t *testing.T) {
	ctx := context.Background()

	actorWith := func(assignment *ateapipb.WorkerAssignment, volumes ...*ateapipb.ExternalVolume) *ateapipb.Actor {
		return &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "default"},
			Status: &ateapipb.ActorStatus{
				WorkerAssignment: assignment,
				ActorVolumes:     volumes,
			},
		}
	}
	assignedTo := func(node string) *ateapipb.WorkerAssignment {
		return &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: "worker-1"}, NodeName: node}
	}

	tests := []struct {
		name            string
		actor           *ateapipb.Actor
		plugin          *mockDetachVolumePlugin
		wantDetachCalls []detachCall
		wantErrContains string
		wantCode        codes.Code
	}{
		{
			name:            "recorded node without a worker assignment is detached",
			actor:           actorWith(nil, recordedVolume("vol1", "storage-vol-1", "mock", "node-1")),
			wantDetachCalls: []detachCall{{VolumeID: "storage-vol-1", Node: "node-1"}},
		},
		{
			name:            "recorded node wins over the assignment's node",
			actor:           actorWith(assignedTo("node-2"), recordedVolume("vol1", "storage-vol-1", "mock", "node-1")),
			wantDetachCalls: []detachCall{{VolumeID: "storage-vol-1", Node: "node-1"}},
		},
		{
			// No fallback to the assignment: a volume with no recorded node
			// was never published.
			name:            "empty attached_node is not detached even with an assignment",
			actor:           actorWith(assignedTo("node-1"), recordedVolume("vol1", "storage-vol-1", "mock", "")),
			wantDetachCalls: nil,
		},
		{
			name: "only volumes with a recorded node are detached",
			actor: actorWith(assignedTo("node-1"),
				recordedVolume("vol1", "storage-vol-1", "mock", "node-1"),
				recordedVolume("vol2", "storage-vol-2", "mock", "")),
			wantDetachCalls: []detachCall{{VolumeID: "storage-vol-1", Node: "node-1"}},
		},
		{
			name: "each volume is detached from its own recorded node",
			actor: actorWith(nil,
				recordedVolume("vol1", "storage-vol-1", "mock", "node-1"),
				recordedVolume("vol2", "storage-vol-2", "mock", "node-2")),
			wantDetachCalls: []detachCall{
				{VolumeID: "storage-vol-1", Node: "node-1"},
				{VolumeID: "storage-vol-2", Node: "node-2"},
			},
		},
		{
			name: "recorded volume without a storage volume ID is skipped",
			actor: actorWith(nil,
				recordedVolume("vol1", "", "mock", "node-1"),
				recordedVolume("vol2", "storage-vol-2", "mock", "node-1")),
			wantDetachCalls: []detachCall{{VolumeID: "storage-vol-2", Node: "node-1"}},
		},
		{
			name:  "codes.NotFound from the plugin counts as already detached",
			actor: actorWith(nil, recordedVolume("vol1", "storage-vol-1", "mock", "node-1")),
			plugin: &mockDetachVolumePlugin{detachErrs: map[string]error{
				"storage-vol-1": status.Error(codes.NotFound, "volume not found"),
			}},
			wantDetachCalls: []detachCall{{VolumeID: "storage-vol-1", Node: "node-1"}},
		},
		{
			name: "partial failure attempts every volume and joins the errors",
			actor: actorWith(nil,
				recordedVolume("vol1", "storage-vol-1", "mock", "node-1"),
				recordedVolume("vol2", "storage-vol-2", "mock", "node-2")),
			plugin: &mockDetachVolumePlugin{detachErrs: map[string]error{
				"storage-vol-1": status.Error(codes.Internal, "disk detach failed"),
			}},
			wantDetachCalls: []detachCall{
				{VolumeID: "storage-vol-1", Node: "node-1"},
				{VolumeID: "storage-vol-2", Node: "node-2"},
			},
			wantErrContains: `failed to detach volume "storage-vol-1" from node "node-1"`,
		},
		{
			name:  "plugin's API error code survives the join",
			actor: actorWith(nil, recordedVolume("vol1", "storage-vol-1", "mock", "node-1")),
			plugin: &mockDetachVolumePlugin{detachErrs: map[string]error{
				"storage-vol-1": apierror.FailedPrecondition("driver cannot unpublish"),
			}},
			wantDetachCalls: []detachCall{{VolumeID: "storage-vol-1", Node: "node-1"}},
			wantErrContains: "driver cannot unpublish",
			wantCode:        codes.FailedPrecondition,
		},
		{
			name:            "unknown plugin returns an error",
			actor:           actorWith(nil, recordedVolume("vol1", "storage-vol-1", "unknown-plugin", "node-1")),
			wantErrContains: `failed to get volume plugin for "unknown-plugin"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := tt.plugin
			if plugin == nil {
				plugin = &mockDetachVolumePlugin{}
			}
			registry := &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{"mock": plugin}}

			err := detachActorVolumes(ctx, registry, tt.actor, "test")
			if tt.wantErrContains == "" {
				if err != nil {
					t.Fatalf("detachActorVolumes() error = %v, want nil", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Fatalf("detachActorVolumes() error = %v, want error containing %q", err, tt.wantErrContains)
			}
			if tt.wantCode != codes.OK {
				if got := apierror.Code(err); got != tt.wantCode {
					t.Errorf("apierror.Code(detachActorVolumes()) = %v, want %v", got, tt.wantCode)
				}
			}
			if diff := cmp.Diff(tt.wantDetachCalls, plugin.detachCalls); diff != "" {
				t.Errorf("detachCalls mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestClearVolumeAttachments(t *testing.T) {
	got := &ateapipb.ActorStatus{ActorVolumes: []*ateapipb.ExternalVolume{
		recordedVolume("vol1", "storage-vol-1", "mock", "node-1"),
		recordedVolume("vol2", "storage-vol-2", "mock", "node-2"),
		recordedVolume("vol3", "storage-vol-3", "mock", ""),
	}}
	clearVolumeAttachments(got)

	// Only attached_node changes.
	want := &ateapipb.ActorStatus{ActorVolumes: []*ateapipb.ExternalVolume{
		recordedVolume("vol1", "storage-vol-1", "mock", ""),
		recordedVolume("vol2", "storage-vol-2", "mock", ""),
		recordedVolume("vol3", "storage-vol-3", "mock", ""),
	}}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("status after clearVolumeAttachments mismatch (-want +got):\n%s", diff)
	}

	// A status without volumes, or none at all, is left alone.
	clearVolumeAttachments(&ateapipb.ActorStatus{})
	clearVolumeAttachments(nil)
}
