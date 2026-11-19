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

package atepg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
)

func TestCreateCsiDriverConfig_UniqueConstraintMessages(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	if _, err := s.CreateCsiDriverConfig(ctx, &ateapi.CsiDriverConfig{
		Metadata: &ateapi.ResourceMetadata{Name: "csi-nfs"}, DriverName: "nfs.csi.k8s.io", ControllerEndpoint: "tcp://10.0.0.1:12345",
	}); err != nil {
		t.Fatalf("CreateCsiDriverConfig failed: %v", err)
	}

	_, err := s.CreateCsiDriverConfig(ctx, &ateapi.CsiDriverConfig{
		Metadata: &ateapi.ResourceMetadata{Name: "csi-nfs"}, DriverName: "other.csi.k8s.io", ControllerEndpoint: "tcp://10.0.0.1:12345",
	})
	if !errors.Is(err, store.ErrAlreadyExists) || !strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "is already registered") {
		t.Errorf("duplicate name error = %v, want ErrAlreadyExists with 'already exists'", err)
	}

	_, err = s.CreateCsiDriverConfig(ctx, &ateapi.CsiDriverConfig{
		Metadata: &ateapi.ResourceMetadata{Name: "csi-nfs-2"}, DriverName: "nfs.csi.k8s.io", ControllerEndpoint: "tcp://10.0.0.1:12345",
	})
	if !errors.Is(err, store.ErrAlreadyExists) || !strings.Contains(err.Error(), `driver_name "nfs.csi.k8s.io" is already registered`) {
		t.Errorf("duplicate driver_name error = %v, want ErrAlreadyExists naming registered driver_name", err)
	}
}

func TestUpdateCsiDriverConfig_ConcurrentWriteReturnsConflict(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	created, err := s.CreateCsiDriverConfig(ctx, &ateapi.CsiDriverConfig{
		Metadata: &ateapi.ResourceMetadata{Name: "csi-nfs"}, DriverName: "nfs.csi.k8s.io", ControllerEndpoint: "tcp://10.0.0.1:12345",
	})
	if err != nil {
		t.Fatalf("CreateCsiDriverConfig failed: %v", err)
	}
	_, err = s.UpdateCsiDriverConfig(ctx, "csi-nfs", store.PreconditionFrom(created), func(toUpdate *ateapi.CsiDriverConfig) error {
		if _, err := s.UpdateCsiDriverConfig(ctx, "csi-nfs", store.PreconditionFrom(created), func(c *ateapi.CsiDriverConfig) error {
			c.ControllerEndpoint = "tcp://10.0.0.2:12345"
			return nil
		}); err != nil {
			return fmt.Errorf("concurrent update: %w", err)
		}
		toUpdate.ControllerEndpoint = "tcp://10.0.0.3:12345"
		return nil
	})
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("UpdateCsiDriverConfig error = %v, want ErrVersionConflict", err)
	}
	if stored, err := s.GetCsiDriverConfig(ctx, "csi-nfs"); err != nil || stored.GetControllerEndpoint() != "tcp://10.0.0.2:12345" {
		t.Errorf("GetCsiDriverConfig = (%v, %v), want winning endpoint", stored, err)
	}
}

func TestCsiDriverConfig_CorruptAndColumnTamper(t *testing.T) {
	cases := []struct {
		name, tamperSQL, lookupDriver string
		tamperArg                     any
	}{
		{"corrupt proto", `UPDATE csi_driver_configs SET proto = $1 WHERE name = 'csi-nfs'`, "nfs.csi.k8s.io", []byte{0xff}},
		{"uid mismatch", `UPDATE csi_driver_configs SET uid = $1 WHERE name = 'csi-nfs'`, "nfs.csi.k8s.io", "tampered-uid"},
		{"version mismatch", `UPDATE csi_driver_configs SET version = $1 WHERE name = 'csi-nfs'`, "nfs.csi.k8s.io", int64(99)},
		{"driver_name mismatch", `UPDATE csi_driver_configs SET driver_name = $1 WHERE name = 'csi-nfs'`, "tampered.csi.k8s.io", "tampered.csi.k8s.io"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := setupPostgresPersistence(t)
			ctx := context.Background()
			created, err := s.CreateCsiDriverConfig(ctx, &ateapi.CsiDriverConfig{
				Metadata: &ateapi.ResourceMetadata{Name: "csi-nfs"}, DriverName: "nfs.csi.k8s.io", ControllerEndpoint: "tcp://10.0.0.1:12345",
			})
			if err != nil {
				t.Fatalf("CreateCsiDriverConfig failed: %v", err)
			}
			if _, err := s.pool.Exec(ctx, tc.tamperSQL, tc.tamperArg); err != nil {
				t.Fatalf("tampering csi_driver_configs row: %v", err)
			}
			for op, fn := range map[string]func() error{
				"GetCsiDriverConfig":             func() error { _, err := s.GetCsiDriverConfig(ctx, "csi-nfs"); return err },
				"GetCsiDriverConfigByDriverName": func() error { _, err := s.GetCsiDriverConfigByDriverName(ctx, tc.lookupDriver); return err },
				"ListCsiDriverConfigs":           func() error { _, err := s.ListCsiDriverConfigs(ctx, store.ListOptions{PageSize: 10}); return err },
				"UpdateCsiDriverConfig": func() error {
					_, err := s.UpdateCsiDriverConfig(ctx, "csi-nfs", store.PreconditionFrom(created), func(*ateapi.CsiDriverConfig) error { return nil })
					return err
				},
				"DeleteCsiDriverConfig": func() error {
					_, err := s.DeleteCsiDriverConfig(ctx, "csi-nfs", store.DeletePreconditions{})
					return err
				},
			} {
				if err := fn(); err == nil {
					t.Errorf("%s succeeded on tampered row, want error", op)
				}
			}
		})
	}
}

func TestExternalVolumeClass_DriverNameColumnAndLegacyEmpty(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	if _, err := s.CreateCsiDriverConfig(ctx, &ateapi.CsiDriverConfig{
		Metadata: &ateapi.ResourceMetadata{Name: "csi-nfs"}, DriverName: "nfs.csi.k8s.io", ControllerEndpoint: "tcp://10.0.0.1:12345",
	}); err != nil {
		t.Fatalf("CreateCsiDriverConfig failed: %v", err)
	}
	if _, err := s.CreateExternalVolumeClass(ctx, &ateapi.ExternalVolumeClass{Metadata: &ateapi.ResourceMetadata{Name: "other"}, DriverName: "other.csi.k8s.io"}); err != nil {
		t.Fatalf("CreateExternalVolumeClass(other) failed: %v", err)
	}
	if _, err := s.CreateExternalVolumeClass(ctx, &ateapi.ExternalVolumeClass{Metadata: &ateapi.ResourceMetadata{Name: "nfs"}, DriverName: "nfs.csi.k8s.io"}); err != nil {
		t.Fatalf("CreateExternalVolumeClass(nfs) failed: %v", err)
	}
	var driverName string
	if err := s.pool.QueryRow(ctx, `SELECT driver_name FROM external_volume_classes WHERE name = $1`, "nfs").Scan(&driverName); err != nil || driverName != "nfs.csi.k8s.io" {
		t.Fatalf("driver_name column = (%q, %v), want nfs.csi.k8s.io", driverName, err)
	}

	// Simulate pre-000003 / older-replica rows whose driver_name column is still ''.
	if _, err := s.pool.Exec(ctx, `UPDATE external_volume_classes SET driver_name = '' WHERE name = $1`, "other"); err != nil {
		t.Fatalf("clearing other driver_name: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE external_volume_classes SET driver_name = '' WHERE name = $1`, "nfs"); err != nil {
		t.Fatalf("clearing nfs driver_name: %v", err)
	}
	if got, err := s.GetExternalVolumeClass(ctx, "nfs"); err != nil || got.GetDriverName() != "nfs.csi.k8s.io" {
		t.Fatalf("GetExternalVolumeClass on legacy row = (%v, %v), want nfs.csi.k8s.io", got, err)
	}
	if page, err := s.ListExternalVolumeClasses(ctx, store.ListOptions{PageSize: 10}); err != nil || len(page.Items) != 2 {
		t.Fatalf("ListExternalVolumeClasses on legacy rows = (%d items, %v), want 2", len(page.Items), err)
	}
	if _, err := s.DeleteCsiDriverConfig(ctx, "csi-nfs", store.DeletePreconditions{}); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("DeleteCsiDriverConfig while referenced by legacy row = %v, want ErrFailedPrecondition", err)
	}
	if _, err := s.DeleteExternalVolumeClass(ctx, "nfs", store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteExternalVolumeClass(nfs) failed: %v", err)
	}
	if _, err := s.DeleteCsiDriverConfig(ctx, "csi-nfs", store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteCsiDriverConfig after legacy row deleted = %v, want nil", err)
	}

	if _, err := s.pool.Exec(ctx, `UPDATE external_volume_classes SET driver_name = $1 WHERE name = 'other'`, "tampered.csi.k8s.io"); err != nil {
		t.Fatalf("tampering external_volume_classes row: %v", err)
	}
	if _, err := s.GetExternalVolumeClass(ctx, "other"); err == nil {
		t.Error("GetExternalVolumeClass succeeded on tampered driver_name, want error")
	}
	if _, err := s.ListExternalVolumeClasses(ctx, store.ListOptions{PageSize: 10}); err == nil {
		t.Error("ListExternalVolumeClasses succeeded on tampered driver_name, want error")
	}
}
