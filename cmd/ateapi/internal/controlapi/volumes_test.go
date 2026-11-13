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
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
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

// volumeTestActor is the actor the volume tests provision for, in the
// atespace newTestPersistence creates.
func volumeTestActor() *ateapi.Actor {
	return &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor-1", Uid: "actor-uid-123"}}
}

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
				res, err := createActorVolumes(ctx, &mockPluginRegistry{}, lister, nil, volumeTestActor(), tmpl, volumes)
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

	actor := volumeTestActor()
	vol1Record := provisionedExternalVolumeName(actor.GetMetadata().GetUid(), "vol1")
	dataVolRecord := provisionedExternalVolumeName(actor.GetMetadata().GetUid(), "data-vol")

	tests := []struct {
		name           string
		tmpl           *ateapi.ActorTemplate
		inputVolumes   []*ateapi.ExternalVolumeStatus
		storageClasses map[string]*storagev1.StorageClass
		// existing is a record an earlier attempt left in the store.
		existing *ateapi.ExternalVolume
		wantCode codes.Code
		wantRes  []*ateapi.ExternalVolumeStatus
		// wantRecords are the ExternalVolumes expected in the store afterwards.
		wantRecords []*ateapi.ExternalVolume
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
			wantCode: codes.FailedPrecondition,
			wantRes: []*ateapi.ExternalVolumeStatus{
				{
					Name:               "vol1",
					StorageVolumeId:    "mock-vol-substrate-actor-uid-123-vol1",
					VolumeType:         "mock-standard",
					ExternalVolumeName: vol1Record,
				},
				{Name: "vol2", VolumeType: "mock-fast"},
				{Name: "vol3", VolumeType: "mock-standard"},
			},
			wantRecords: []*ateapi.ExternalVolume{{
				Metadata:   &ateapi.ResourceMetadata{Atespace: "team-a", Name: vol1Record},
				VolumeId:   "mock-vol-substrate-actor-uid-123-vol1",
				VolumeType: "mock-standard",
			}},
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
			wantCode: codes.NotFound,
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
			wantRes: []*ateapi.ExternalVolumeStatus{
				{
					Name:            "data-vol",
					StorageVolumeId: "mock-vol-substrate-actor-uid-123-data-vol",
					VolumeType:      "mock-standard",
					VolumeContext: map[string]string{
						"type":                      "pd-ssd",
						"csi.storage.k8s.io/fstype": "ext4",
					},
					ExternalVolumeName: dataVolRecord,
				},
			},
			wantRecords: []*ateapi.ExternalVolume{{
				Metadata:   &ateapi.ResourceMetadata{Atespace: "team-a", Name: dataVolRecord},
				VolumeId:   "mock-vol-substrate-actor-uid-123-data-vol",
				VolumeType: "mock-standard",
				VolumeContext: map[string]string{
					"type":                      "pd-ssd",
					"csi.storage.k8s.io/fstype": "ext4",
				},
			}},
		},
		{
			// A crash between the CSI call and the actor write leaves the
			// record behind; the retry gets the same volume and reuses it.
			name: "record left by an earlier attempt is reused",
			tmpl: standardTmpl,
			inputVolumes: []*ateapi.ExternalVolumeStatus{
				{Name: "data-vol", VolumeType: "mock-standard"},
			},
			existing: &ateapi.ExternalVolume{
				Metadata:   &ateapi.ResourceMetadata{Atespace: "team-a", Name: dataVolRecord},
				VolumeId:   "mock-vol-substrate-actor-uid-123-data-vol",
				VolumeType: "mock-standard",
			},
			wantRes: []*ateapi.ExternalVolumeStatus{
				{
					Name:               "data-vol",
					StorageVolumeId:    "mock-vol-substrate-actor-uid-123-data-vol",
					VolumeType:         "mock-standard",
					ExternalVolumeName: dataVolRecord,
				},
			},
			wantRecords: []*ateapi.ExternalVolume{{
				Metadata:   &ateapi.ResourceMetadata{Atespace: "team-a", Name: dataVolRecord},
				VolumeId:   "mock-vol-substrate-actor-uid-123-data-vol",
				VolumeType: "mock-standard",
			}},
		},
		{
			name: "record naming other storage fails the volume",
			tmpl: standardTmpl,
			inputVolumes: []*ateapi.ExternalVolumeStatus{
				{Name: "data-vol", VolumeType: "mock-standard"},
			},
			existing: &ateapi.ExternalVolume{
				Metadata:   &ateapi.ResourceMetadata{Atespace: "team-a", Name: dataVolRecord},
				VolumeId:   "somebody-elses-disk",
				VolumeType: "mock-standard",
			},
			wantCode: codes.FailedPrecondition,
			wantRes: []*ateapi.ExternalVolumeStatus{
				{Name: "data-vol", VolumeType: "mock-standard"},
			},
			wantRecords: []*ateapi.ExternalVolume{{
				Metadata:   &ateapi.ResourceMetadata{Atespace: "team-a", Name: dataVolRecord},
				VolumeId:   "somebody-elses-disk",
				VolumeType: "mock-standard",
			}},
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
			persistence := newTestPersistence(t)
			if tt.existing != nil {
				if _, err := persistence.CreateExternalVolume(ctx, tt.existing); err != nil {
					t.Fatalf("CreateExternalVolume failed: %v", err)
				}
			}
			res, err := createActorVolumes(ctx, registry, scLister, persistence, actor, tt.tmpl, tt.inputVolumes)
			if got := apierror.Code(err); got != tt.wantCode {
				t.Errorf("createActorVolumes() code = %v, want %v; error = %v", got, tt.wantCode, err)
			}
			if diff := cmp.Diff(tt.wantRes, res, protocmp.Transform()); diff != "" {
				t.Errorf("createActorVolumes() mismatch (-want +got):\n%s", diff)
			}
			records, err := persistence.ListExternalVolumes(ctx, "team-a", store.ListOptions{})
			if err != nil {
				t.Fatalf("ListExternalVolumes failed: %v", err)
			}
			if diff := cmp.Diff(tt.wantRecords, records.Items, protocmp.Transform(), protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "uid", "version", "create_time", "update_time")); diff != "" {
				t.Errorf("ExternalVolumes after createActorVolumes() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestProvisionedExternalVolumeName(t *testing.T) {
	const uid = "8ba9b6ee-2e1a-4c0f-9f4e-2f0a0f1c3d55"
	short := provisionedExternalVolumeName(uid, "data")
	if !strings.HasPrefix(short, "data-") || len(short) != len("data-")+16 {
		t.Errorf("provisionedExternalVolumeName(uid, data) = %q, want the volume name and a 16 character hash", short)
	}
	if again := provisionedExternalVolumeName(uid, "data"); again != short {
		t.Errorf("provisionedExternalVolumeName is not deterministic: %q then %q", short, again)
	}
	if other := provisionedExternalVolumeName("0f1c3d55-2f0a-4c0f-9f4e-8ba9b6ee2e1a", "data"); other == short {
		t.Errorf("provisionedExternalVolumeName gives two actors the same name %q", short)
	}
	if other := provisionedExternalVolumeName(uid, "data-2"); other == short {
		t.Errorf("provisionedExternalVolumeName gives two volumes the same name %q", short)
	}
	// Both the volume name and the result are DNS labels, so a long volume
	// name is cut to fit, without leaving a dash before the hash.
	for _, volumeName := range []string{"data", strings.Repeat("v", 63), strings.Repeat("v", 45) + "-vol"} {
		got := provisionedExternalVolumeName(uid, volumeName)
		if errs := resources.ValidateResourceName(got, nil); len(errs) > 0 {
			t.Errorf("provisionedExternalVolumeName(uid, %q) = %q, want a resource name: %v", volumeName, got, errs)
		}
		if strings.Contains(got, "--") {
			t.Errorf("provisionedExternalVolumeName(uid, %q) = %q, want no double dash", volumeName, got)
		}
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

	res, err := createActorVolumes(context.Background(), registry, scLister, nil, volumeTestActor(), tmpl, input)
	if got := apierror.Code(err); got != codes.Internal {
		t.Fatalf("createActorVolumes() code = %v, want %v; error = %v", got, codes.Internal, err)
	}
	// The volume stays unprovisioned, so a retry creates it again.
	if diff := cmp.Diff(input, res, protocmp.Transform()); diff != "" {
		t.Errorf("createActorVolumes() mismatch (-want +got):\n%s", diff)
	}
}

// failingExternalVolumeStore wraps a store, failing the calls whose error is
// set.
type failingExternalVolumeStore struct {
	externalVolumeStore
	createErr, getErr, deleteErr error
}

func (f *failingExternalVolumeStore) CreateExternalVolume(ctx context.Context, volume *ateapi.ExternalVolume) (*ateapi.ExternalVolume, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	return f.externalVolumeStore.CreateExternalVolume(ctx, volume)
}

func (f *failingExternalVolumeStore) GetExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapi.ExternalVolume, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.externalVolumeStore.GetExternalVolume(ctx, volumeRef)
}

func (f *failingExternalVolumeStore) DeleteExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.DeletePreconditions) (*ateapi.ExternalVolume, error) {
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return f.externalVolumeStore.DeleteExternalVolume(ctx, volumeRef, precondition)
}

func TestCreateActorVolumesRecordErrors(t *testing.T) {
	ctx := context.Background()
	actor := volumeTestActor()
	tmpl := &ateapi.ActorTemplate{
		Volumes: []*ateapi.Volume{{
			Name:     "data-vol",
			External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{StorageClassName: "standard"}},
		}},
	}
	registry := &mockPluginRegistry{
		plugins: map[string]volume.VolumePluginControlPlane{"mock-standard": volume.NewMockVolumePlugin()},
	}
	scLister := &fakeStorageClassLister{storageClasses: map[string]*storagev1.StorageClass{
		"standard": {ObjectMeta: metav1.ObjectMeta{Name: "standard"}, Provisioner: "mock-standard"},
	}}
	input := []*ateapi.ExternalVolumeStatus{{Name: "data-vol", VolumeType: "mock-standard"}}
	existing := &ateapi.ExternalVolume{
		Metadata:   &ateapi.ResourceMetadata{Atespace: "team-a", Name: provisionedExternalVolumeName(actor.GetMetadata().GetUid(), "data-vol")},
		VolumeId:   "mock-vol-substrate-actor-uid-123-data-vol",
		VolumeType: "mock-standard",
	}

	tests := []struct {
		name string
		// existing is a record an earlier attempt left in the store.
		existing *ateapi.ExternalVolume
		store    failingExternalVolumeStore
	}{
		{
			name:  "create fails",
			store: failingExternalVolumeStore{createErr: errors.New("connection reset")},
		},
		{
			name:     "read of the existing record fails",
			existing: existing,
			store:    failingExternalVolumeStore{getErr: errors.New("connection reset")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			persistence := newTestPersistence(t)
			if tt.existing != nil {
				if _, err := persistence.CreateExternalVolume(ctx, tt.existing); err != nil {
					t.Fatalf("CreateExternalVolume failed: %v", err)
				}
			}
			tt.store.externalVolumeStore = persistence
			res, err := createActorVolumes(ctx, registry, scLister, &tt.store, actor, tmpl, input)
			if got := apierror.Code(err); got != codes.Internal {
				t.Fatalf("createActorVolumes() code = %v, want %v; error = %v", got, codes.Internal, err)
			}
			// The volume stays unprovisioned in the status, so a retry
			// provisions it again and gets the same volume back.
			if diff := cmp.Diff(input, res, protocmp.Transform()); diff != "" {
				t.Errorf("createActorVolumes() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

type trackingVolumePlugin struct {
	volume.VolumePluginControlPlane
	deletedIDs []string
	// deleteErr is returned by every DeleteVolume call.
	deleteErr error
}

func (t *trackingVolumePlugin) DeleteVolume(ctx context.Context, volumeID string) error {
	t.deletedIDs = append(t.deletedIDs, volumeID)
	return t.deleteErr
}

func TestDeleteActorVolumes(t *testing.T) {
	ctx := context.Background()
	actor := volumeTestActor()
	derived := provisionedExternalVolumeName(actor.GetMetadata().GetUid(), "vol1")
	record := func(name string) *ateapi.ExternalVolume {
		return &ateapi.ExternalVolume{
			Metadata:   &ateapi.ResourceMetadata{Atespace: "team-a", Name: name},
			VolumeId:   "storage-vol-123",
			VolumeType: "mock",
		}
	}

	tests := []struct {
		name    string
		volumes []*ateapi.ExternalVolumeStatus
		// records are the ExternalVolumes in the store beforehand.
		records []*ateapi.ExternalVolume
		// storageErr is what the plugin's DeleteVolume returns.
		storageErr error
		// recordErr is what the store's DeleteExternalVolume returns.
		recordErr   error
		wantErr     bool
		wantDeleted []string
		// wantRecords are the ExternalVolumes left afterwards.
		wantRecords []string
	}{
		{
			name: "uses storage volume ID when present and deletes the record the status names",
			volumes: []*ateapi.ExternalVolumeStatus{
				{Name: "vol1", StorageVolumeId: "storage-vol-123", VolumeType: "mock", ExternalVolumeName: "vol1-record"},
			},
			records:     []*ateapi.ExternalVolume{record("vol1-record"), record("other")},
			wantDeleted: []string{"storage-vol-123"},
			wantRecords: []string{"other"},
		},
		{
			// The actor write after provisioning failed, so the status names
			// neither the volume nor its record.
			name: "falls back to the derived volume ID and record name",
			volumes: []*ateapi.ExternalVolumeStatus{
				{Name: "vol1", VolumeType: "mock"},
			},
			records:     []*ateapi.ExternalVolume{record(derived)},
			wantDeleted: []string{"substrate-actor-uid-123-vol1"},
		},
		{
			// Actors from before records were kept have none.
			name: "a missing record is not an error",
			volumes: []*ateapi.ExternalVolumeStatus{
				{Name: "vol1", StorageVolumeId: "storage-vol-123", VolumeType: "mock", ExternalVolumeName: "vol1-record"},
			},
			wantDeleted: []string{"storage-vol-123"},
		},
		{
			// A re-entered workflow whose earlier run deleted the storage but
			// not the record still removes the record.
			name: "a volume already gone from storage still loses its record",
			volumes: []*ateapi.ExternalVolumeStatus{
				{Name: "vol1", StorageVolumeId: "storage-vol-123", VolumeType: "mock", ExternalVolumeName: "vol1-record"},
			},
			records:     []*ateapi.ExternalVolume{record("vol1-record")},
			storageErr:  status.Error(codes.NotFound, "no such volume"),
			wantDeleted: []string{"storage-vol-123"},
		},
		{
			name: "a failed storage delete keeps the record",
			volumes: []*ateapi.ExternalVolumeStatus{
				{Name: "vol1", StorageVolumeId: "storage-vol-123", VolumeType: "mock", ExternalVolumeName: "vol1-record"},
			},
			records:     []*ateapi.ExternalVolume{record("vol1-record")},
			storageErr:  status.Error(codes.Unavailable, "storage down"),
			wantErr:     true,
			wantDeleted: []string{"storage-vol-123"},
			wantRecords: []string{"vol1-record"},
		},
		{
			name: "a failed record delete is reported",
			volumes: []*ateapi.ExternalVolumeStatus{
				{Name: "vol1", StorageVolumeId: "storage-vol-123", VolumeType: "mock", ExternalVolumeName: "vol1-record"},
			},
			records:     []*ateapi.ExternalVolume{record("vol1-record")},
			recordErr:   errors.New("connection reset"),
			wantErr:     true,
			wantDeleted: []string{"storage-vol-123"},
			wantRecords: []string{"vol1-record"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &trackingVolumePlugin{deleteErr: tt.storageErr}
			registry := &mockPluginRegistry{
				plugins: map[string]volume.VolumePluginControlPlane{
					"mock": plugin,
				},
			}
			persistence := newTestPersistence(t)
			for _, r := range tt.records {
				if _, err := persistence.CreateExternalVolume(ctx, r); err != nil {
					t.Fatalf("CreateExternalVolume failed: %v", err)
				}
			}
			volumeStore := &failingExternalVolumeStore{externalVolumeStore: persistence, deleteErr: tt.recordErr}
			if err := deleteActorVolumes(ctx, registry, volumeStore, actor, tt.volumes); (err != nil) != tt.wantErr {
				t.Fatalf("deleteActorVolumes() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.wantDeleted, plugin.deletedIDs); diff != "" {
				t.Errorf("deletedIDs mismatch (-want +got):\n%s", diff)
			}
			left, err := persistence.ListExternalVolumes(ctx, "team-a", store.ListOptions{})
			if err != nil {
				t.Fatalf("ListExternalVolumes failed: %v", err)
			}
			var names []string
			for _, r := range left.Items {
				names = append(names, r.GetMetadata().GetName())
			}
			if diff := cmp.Diff(tt.wantRecords, names); diff != "" {
				t.Errorf("ExternalVolumes left after deleteActorVolumes() mismatch (-want +got):\n%s", diff)
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
