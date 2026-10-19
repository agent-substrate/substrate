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

package apivalidation

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateCustomCreateSharedVolumeRequest(t *testing.T) {
	tests := []struct {
		name       string
		volume     *ateapipb.SharedVolume
		wantFields []string
	}{
		{
			name:   "provisioning from a class",
			volume: &ateapipb.SharedVolume{StorageClassName: "standard", Capacity: "10Gi"},
		},
		{
			name:   "registering an existing volume",
			volume: &ateapipb.SharedVolume{VolumeId: "disk-1", VolumeType: "mock"},
		},
		{
			name:   "registering an existing volume under a named class",
			volume: &ateapipb.SharedVolume{VolumeId: "disk-1", StorageClassName: "standard"},
		},
		{
			name:       "neither a class nor an ID",
			volume:     &ateapipb.SharedVolume{},
			wantFields: []string{"shared_volume.storage_class_name"},
		},
		{
			name:       "a size for a volume that already exists",
			volume:     &ateapipb.SharedVolume{VolumeId: "disk-1", VolumeType: "mock", Capacity: "10Gi"},
			wantFields: []string{"shared_volume.capacity"},
		},
		{
			name:       "an ID with no driver to reach it through",
			volume:     &ateapipb.SharedVolume{VolumeId: "disk-1"},
			wantFields: []string{"shared_volume.volume_type"},
		},
		{
			name:       "a driver alongside the class that supplies one",
			volume:     &ateapipb.SharedVolume{StorageClassName: "standard", VolumeType: "mock", Capacity: "10Gi"},
			wantFields: []string{"shared_volume.volume_type"},
		},
		{
			name:       "driver metadata for a volume that does not exist yet",
			volume:     &ateapipb.SharedVolume{StorageClassName: "standard", Capacity: "10Gi", VolumeContext: map[string]string{"fsType": "ext4"}},
			wantFields: []string{"shared_volume.volume_context"},
		},
		{
			name:       "provisioning without a size",
			volume:     &ateapipb.SharedVolume{StorageClassName: "standard"},
			wantFields: []string{"shared_volume.capacity"},
		},
		{
			name:       "read write many is not supported yet",
			volume:     &ateapipb.SharedVolume{StorageClassName: "standard", Capacity: "10Gi", AccessMode: ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_MANY},
			wantFields: []string{"shared_volume.access_mode"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &ateapipb.CreateSharedVolumeRequest{SharedVolume: tc.volume}
			errs := ValidateCustom_CreateSharedVolumeRequest(context.Background(), operation.Operation{Type: operation.Create}, nil, req, nil)

			var gotFields []string
			for _, err := range errs {
				gotFields = append(gotFields, err.Field)
			}
			if diff := cmp.Diff(tc.wantFields, gotFields); diff != "" {
				t.Errorf("offending fields mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidateCustomSharedVolumeCapacity(t *testing.T) {
	tests := []struct {
		value   string
		wantErr bool
	}{
		{value: ""},
		{value: "10Gi"},
		{value: "1500m"},
		{value: "ten gigs", wantErr: true},
		{value: "10GiB", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			value := tc.value
			errs := ValidateCustom_SharedVolume_Capacity(context.Background(), operation.Operation{Type: operation.Create}, field.NewPath("capacity"), &value, nil)
			if gotErr := len(errs) > 0; gotErr != tc.wantErr {
				t.Errorf("ValidateCustom_SharedVolume_Capacity(%q) errors = %v, wantErr %v", tc.value, errs, tc.wantErr)
			}
		})
	}
}
