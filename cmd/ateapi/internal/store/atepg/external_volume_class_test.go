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

package atepg

import (
	"context"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
)

func TestCreateExternalVolumeClass_StoresDriverName(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	if _, err := s.CreateExternalVolumeClass(ctx, &ateapi.ExternalVolumeClass{
		Metadata:   &ateapi.ResourceMetadata{Name: "nfs"},
		DriverName: "nfs.csi.k8s.io",
	}); err != nil {
		t.Fatalf("CreateExternalVolumeClass failed: %v", err)
	}

	var driverName string
	if err := s.pool.QueryRow(ctx, `SELECT driver_name FROM external_volume_classes WHERE name = $1`, "nfs").Scan(&driverName); err != nil {
		t.Fatalf("reading driver_name: %v", err)
	}
	if driverName != "nfs.csi.k8s.io" {
		t.Errorf("driver_name column = %q, want %q", driverName, "nfs.csi.k8s.io")
	}
}
