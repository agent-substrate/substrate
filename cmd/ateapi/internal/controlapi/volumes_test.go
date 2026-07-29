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

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/volume"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestInitialActorVolumes_PendingState(t *testing.T) {
	tmpl := &atev1alpha1.ActorTemplate{
		Spec: atev1alpha1.ActorTemplateSpec{
			Volumes: []atev1alpha1.Volume{
				{
					Name: "data-vol-1",
					VolumeSource: atev1alpha1.VolumeSource{
						ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
							StorageClassName: "standard",
						},
					},
				},
				{
					Name: "scratch-vol",
				},
				{
					Name: "durable-vol",
					VolumeSource: atev1alpha1.VolumeSource{
						DurableDir: &atev1alpha1.DurableDirVolumeSource{},
					},
				},
				{
					Name: "data-vol-2",
					VolumeSource: atev1alpha1.VolumeSource{
						ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
							StorageClassName: "fast",
						},
					},
				},
			},
		},
	}

	want := []*ateapipb.ExternalVolume{
		{
			VolumeName: "data-vol-1",
			VolumeType: "mock",
			Status:     ateapipb.ExternalVolume_STATUS_PENDING,
		},
		{
			VolumeName: "data-vol-2",
			VolumeType: "mock",
			Status:     ateapipb.ExternalVolume_STATUS_PENDING,
		},
	}

	initVols := initialActorVolumes(tmpl)
	if diff := cmp.Diff(want, initVols, protocmp.Transform()); diff != "" {
		t.Errorf("initialActorVolumes mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateActorVolumes(t *testing.T) {
	ctx := context.Background()

	standardTmpl := &atev1alpha1.ActorTemplate{
		Spec: atev1alpha1.ActorTemplateSpec{
			Volumes: []atev1alpha1.Volume{
				{
					Name: "data-vol",
					VolumeSource: atev1alpha1.VolumeSource{
						ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
							StorageClassName: "standard",
						},
					},
				},
			},
		},
	}

	multiVolTmpl := &atev1alpha1.ActorTemplate{
		Spec: atev1alpha1.ActorTemplateSpec{
			Volumes: []atev1alpha1.Volume{
				{
					Name: "vol1",
					VolumeSource: atev1alpha1.VolumeSource{
						ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
							StorageClassName: "standard",
						},
					},
				},
				{
					Name: "vol2",
					VolumeSource: atev1alpha1.VolumeSource{
						ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
							StorageClassName: "standard",
						},
					},
				},
				{
					Name: "vol3",
					VolumeSource: atev1alpha1.VolumeSource{
						ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
							StorageClassName: "standard",
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name         string
		tmpl         *atev1alpha1.ActorTemplate
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
					VolumeType: "mock",
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
					VolumeType:      "mock",
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
			tmpl: &atev1alpha1.ActorTemplate{
				Spec: atev1alpha1.ActorTemplateSpec{
					Volumes: []atev1alpha1.Volume{},
				},
			},
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
			globalVolumePlugin = volume.NewMockVolumePlugin()
			res, err := createActorVolumes(ctx, "actor-uid-123", tt.tmpl, tt.inputVolumes)
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
				{VolumeName: "vol1", StorageVolumeId: "storage-vol-123"},
			},
			wantDeleted: []string{"storage-vol-123"},
			wantErr:     false,
		},
		{
			name:     "falls back to actorVolumeID when storage volume ID is empty regardless of status",
			actorUID: "uid-abc",
			volumes: []*ateapipb.ExternalVolume{
				{VolumeName: "vol1", StorageVolumeId: "", Status: ateapipb.ExternalVolume_STATUS_CREATED},
			},
			wantDeleted: []string{"substrate-uid-abc-vol1"},
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &trackingVolumePlugin{}
			oldGlobalPlugin := globalVolumePlugin
			globalVolumePlugin = plugin
			defer func() { globalVolumePlugin = oldGlobalPlugin }()
			err := deleteActorVolumes(ctx, tt.actorUID, tt.volumes)
			if (err != nil) != tt.wantErr {
				t.Fatalf("deleteActorVolumes() error = %v, wantErr %v", err, tt.wantErr)
			}

			if diff := cmp.Diff(tt.wantDeleted, plugin.deletedIDs); diff != "" {
				t.Errorf("deletedIDs mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
