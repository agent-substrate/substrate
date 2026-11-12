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
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/volume"
)

// fakeExternalVolumeClassGetter serves classes from a map and reports a
// missing name the way ServiceImpl does.
type fakeExternalVolumeClassGetter struct {
	classes map[string]*ateapi.ExternalVolumeClass
	getErr  error
}

func (f *fakeExternalVolumeClassGetter) GetExternalVolumeClass(_ context.Context, name string) (*ateapi.ExternalVolumeClass, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	class, ok := f.classes[name]
	if !ok {
		return nil, apierror.NotFound("ExternalVolumeClass %s not found", name)
	}
	return class, nil
}

func TestInitialActorVolumes(t *testing.T) {
	tmpl := &ateapi.ActorTemplate{
		Volumes: []*ateapi.Volume{
			{
				Name: "data-vol-1",
				External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
					ExternalVolumeClass: &ateapi.ObjectRef{Name: "standard"},
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
					ExternalVolumeClass: &ateapi.ObjectRef{Name: "fast"},
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
			Name:         "data-vol-2",
			VolumeType:   "mock-fast",
			MountOptions: []string{"noatime"},
		},
	}

	classes := &fakeExternalVolumeClassGetter{
		classes: map[string]*ateapi.ExternalVolumeClass{
			"standard": {
				DriverName: "mock-standard",
			},
			"fast": {
				DriverName:   "mock-fast",
				MountOptions: []string{"noatime"},
			},
		},
	}
	initVols, err := initialExternalVolumes(context.Background(), classes, tmpl)
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
					ExternalVolumeClass: &ateapi.ObjectRef{Name: "standard"},
				}},
			},
		},
	}

	multiVolTmpl := &ateapi.ActorTemplate{
		Volumes: []*ateapi.Volume{
			{
				Name: "vol1",
				External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
					ExternalVolumeClass: &ateapi.ObjectRef{Name: "standard"},
				}},
			},
			{
				Name: "vol2",
				External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
					ExternalVolumeClass: &ateapi.ObjectRef{Name: "standard"},
				}},
			},
			{
				Name: "vol3",
				External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
					ExternalVolumeClass: &ateapi.ObjectRef{Name: "standard"},
				}},
			},
		},
	}

	tests := []struct {
		name         string
		tmpl         *ateapi.ActorTemplate
		inputVolumes []*ateapi.ExternalVolumeStatus
		classes      map[string]*ateapi.ExternalVolumeClass
		wantErr      bool
		wantRes      []*ateapi.ExternalVolumeStatus
	}{
		{
			name: "partial failure returns error and preserves succeeded, failed, and remaining volumes",
			tmpl: multiVolTmpl,
			inputVolumes: []*ateapi.ExternalVolumeStatus{
				{Name: "vol1", VolumeType: "mock-standard"},
				// Its type differs from its ExternalVolumeClass's driver, so creating it fails.
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
			name: "class parameters and mount options are propagated",
			tmpl: standardTmpl,
			inputVolumes: []*ateapi.ExternalVolumeStatus{
				{
					Name:       "data-vol",
					VolumeType: "mock-standard",
				},
			},
			classes: map[string]*ateapi.ExternalVolumeClass{
				"standard": {
					DriverName: "mock-standard",
					Parameters: map[string]string{
						"type":                      "pd-ssd",
						"csi.storage.k8s.io/fstype": "ext4",
					},
					MountOptions: []string{"rw", "noatime"},
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
					MountOptions: []string{"rw", "noatime"},
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
			cls := tt.classes
			if cls == nil {
				cls = map[string]*ateapi.ExternalVolumeClass{
					"standard": {DriverName: "mock-standard"},
					"fast":     {DriverName: "mock-fast"},
				}
			}
			getter := &fakeExternalVolumeClassGetter{classes: cls}
			res, err := createActorVolumes(ctx, registry, getter, "actor-uid-123", tt.tmpl, tt.inputVolumes)
			if (err != nil) != tt.wantErr {
				t.Errorf("createActorVolumes() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.wantRes, res, protocmp.Transform()); diff != "" {
				t.Errorf("createActorVolumes() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolveVolumeClass(t *testing.T) {
	tmpl := &ateapi.ExternalVolumeTemplate{
		ExternalVolumeClass: &ateapi.ObjectRef{Name: "gold"},
	}
	for _, tt := range []struct {
		name             string
		getter           *fakeExternalVolumeClassGetter
		wantCode         codes.Code
		wantDriver       string
		wantParams       map[string]string
		wantMountOptions []string
	}{{
		name: "found",
		getter: &fakeExternalVolumeClassGetter{classes: map[string]*ateapi.ExternalVolumeClass{
			"gold": {
				DriverName:   "mock-standard",
				Parameters:   map[string]string{"tier": "gold"},
				MountOptions: []string{"nfsvers=4.1", "noatime"},
			},
		}},
		wantCode:         codes.OK,
		wantDriver:       "mock-standard",
		wantParams:       map[string]string{"tier": "gold"},
		wantMountOptions: []string{"nfsvers=4.1", "noatime"},
	}, {
		name:     "missing",
		getter:   &fakeExternalVolumeClassGetter{},
		wantCode: codes.FailedPrecondition,
	}, {
		name:     "lookup fails",
		getter:   &fakeExternalVolumeClassGetter{getErr: errors.New("database unavailable")},
		wantCode: codes.Internal,
	}} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveVolumeClass(context.Background(), tt.getter, tmpl)
			if code := apierror.Code(err); code != tt.wantCode {
				t.Fatalf("resolveVolumeClass() code = %v, want %v; error = %v", code, tt.wantCode, err)
			}
			if err != nil {
				if !strings.Contains(err.Error(), `ExternalVolumeClass "gold"`) {
					t.Errorf("resolveVolumeClass() error does not name the class: %v", err)
				}
				return
			}
			if got.name != "gold" {
				t.Errorf("resolveVolumeClass() name = %q, want %q", got.name, "gold")
			}
			if got.driver != tt.wantDriver {
				t.Errorf("resolveVolumeClass() driver = %q, want %q", got.driver, tt.wantDriver)
			}
			if diff := cmp.Diff(tt.wantParams, got.params); diff != "" {
				t.Errorf("resolveVolumeClass() params mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantMountOptions, got.mountOptions); diff != "" {
				t.Errorf("resolveVolumeClass() mountOptions mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestActorVolumesFromExternalVolumeClass(t *testing.T) {
	ctx := context.Background()
	tmpl := &ateapi.ActorTemplate{
		Volumes: []*ateapi.Volume{{
			Name: "data-vol",
			External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
				ExternalVolumeClass: &ateapi.ObjectRef{Name: "gold"},
				CapacityBytes:       1 << 30,
			}},
		}},
	}
	classes := &fakeExternalVolumeClassGetter{classes: map[string]*ateapi.ExternalVolumeClass{
		"gold": {
			DriverName:   "mock-standard",
			Parameters:   map[string]string{"tier": "gold"},
			MountOptions: []string{"nfsvers=4.1"},
		},
	}}
	registry := &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{
		"mock-standard": volume.NewMockVolumePlugin(),
	}}

	t.Run("initial volume takes the class driver and mount options", func(t *testing.T) {
		got, err := initialExternalVolumes(ctx, classes, tmpl)
		if err != nil {
			t.Fatalf("initialExternalVolumes() failed: %v", err)
		}
		want := []*ateapi.ExternalVolumeStatus{{
			Name:         "data-vol",
			VolumeType:   "mock-standard",
			MountOptions: []string{"nfsvers=4.1"},
		}}
		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Errorf("initialExternalVolumes() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("create passes the class parameters and mount options", func(t *testing.T) {
		input := []*ateapi.ExternalVolumeStatus{{Name: "data-vol", VolumeType: "mock-standard"}}
		got, err := createActorVolumes(ctx, registry, classes, "actor-uid-123", tmpl, input)
		if err != nil {
			t.Fatalf("createActorVolumes() failed: %v", err)
		}
		want := []*ateapi.ExternalVolumeStatus{{
			Name:            "data-vol",
			StorageVolumeId: "mock-vol-substrate-actor-uid-123-data-vol",
			VolumeType:      "mock-standard",
			VolumeContext:   map[string]string{"tier": "gold"},
			MountOptions:    []string{"nfsvers=4.1"},
		}}
		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Errorf("createActorVolumes() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("create rejects a volume whose type differs from the class driver", func(t *testing.T) {
		input := []*ateapi.ExternalVolumeStatus{{Name: "data-vol", VolumeType: "mock-fast"}}
		got, err := createActorVolumes(ctx, registry, classes, "actor-uid-123", tmpl, input)
		if code := apierror.Code(err); code != codes.FailedPrecondition {
			t.Fatalf("createActorVolumes() code = %v, want %v; error = %v", code, codes.FailedPrecondition, err)
		}
		if !strings.Contains(err.Error(), `from ExternalVolumeClass "gold"`) {
			t.Errorf("createActorVolumes() error does not name the class: %v", err)
		}
		if diff := cmp.Diff(input, got, protocmp.Transform()); diff != "" {
			t.Errorf("createActorVolumes() did not preserve pending volumes (-want +got):\n%s", diff)
		}
	})

	t.Run("missing class is a failed precondition", func(t *testing.T) {
		missing := &fakeExternalVolumeClassGetter{}
		if _, err := initialExternalVolumes(ctx, missing, tmpl); apierror.Code(err) != codes.FailedPrecondition {
			t.Errorf("initialExternalVolumes() error = %v, want FailedPrecondition", err)
		}
		input := []*ateapi.ExternalVolumeStatus{{Name: "data-vol", VolumeType: "mock-standard"}}
		got, err := createActorVolumes(ctx, registry, missing, "actor-uid-123", tmpl, input)
		if code := apierror.Code(err); code != codes.FailedPrecondition {
			t.Fatalf("createActorVolumes() code = %v, want %v; error = %v", code, codes.FailedPrecondition, err)
		}
		if diff := cmp.Diff(input, got, protocmp.Transform()); diff != "" {
			t.Errorf("createActorVolumes() did not preserve pending volumes (-want +got):\n%s", diff)
		}
	})

	t.Run("template with no class is a failed precondition", func(t *testing.T) {
		noClass := &ateapi.ActorTemplate{Volumes: []*ateapi.Volume{{
			Name:     "data-vol",
			External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{CapacityBytes: 1 << 30}},
		}}}
		if _, err := initialExternalVolumes(ctx, classes, noClass); apierror.Code(err) != codes.FailedPrecondition {
			t.Errorf("initialExternalVolumes() error = %v, want FailedPrecondition", err)
		}
	})

	t.Run("stored legacy template with storage_class_name is a failed precondition", func(t *testing.T) {
		legacy := &ateapi.ActorTemplate{Volumes: []*ateapi.Volume{{
			Name:     "data-vol",
			External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{StorageClassName: "legacy-sc", CapacityBytes: 1 << 30}}, //nolint:staticcheck // SA1019: stored 1.0 preview templates may still carry storage_class_name
		}}}
		_, err := initialExternalVolumes(ctx, classes, legacy)
		if code := apierror.Code(err); code != codes.FailedPrecondition {
			t.Fatalf("initialExternalVolumes() code = %v, want %v; error = %v", code, codes.FailedPrecondition, err)
		}
		if !strings.Contains(err.Error(), `"legacy-sc"`) || !strings.Contains(err.Error(), "external_volume_class") {
			t.Errorf("initialExternalVolumes() error = %v, want mention of legacy-sc and external_volume_class", err)
		}

		input := []*ateapi.ExternalVolumeStatus{{Name: "data-vol", VolumeType: "mock-standard"}}
		got, err := createActorVolumes(ctx, registry, classes, "actor-uid-123", legacy, input)
		if code := apierror.Code(err); code != codes.FailedPrecondition {
			t.Fatalf("createActorVolumes() code = %v, want %v; error = %v", code, codes.FailedPrecondition, err)
		}
		if !strings.Contains(err.Error(), `"legacy-sc"`) || !strings.Contains(err.Error(), "external_volume_class") {
			t.Errorf("createActorVolumes() error = %v, want mention of legacy-sc and external_volume_class", err)
		}
		if diff := cmp.Diff(input, got, protocmp.Transform()); diff != "" {
			t.Errorf("createActorVolumes() did not preserve pending volumes (-want +got):\n%s", diff)
		}
	})
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
			External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{ExternalVolumeClass: &ateapi.ObjectRef{Name: "standard"}}},
		}},
	}
	registry := &mockPluginRegistry{
		plugins: map[string]volume.VolumePluginControlPlane{"mock-standard": emptyIDVolumePlugin{}},
	}
	classes := &fakeExternalVolumeClassGetter{classes: map[string]*ateapi.ExternalVolumeClass{
		"standard": {DriverName: "mock-standard"},
	}}
	input := []*ateapi.ExternalVolumeStatus{{Name: "data-vol", VolumeType: "mock-standard"}}

	res, err := createActorVolumes(context.Background(), registry, classes, "actor-uid-123", tmpl, input)
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
