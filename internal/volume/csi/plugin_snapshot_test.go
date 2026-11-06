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

package csi

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"go.ate.dev/substrate/internal/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type controllerCapsFunc func(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error)

// reportCaps returns a ControllerGetCapabilities handler that reports rpcs.
func reportCaps(rpcs ...csi.ControllerServiceCapability_RPC_Type) controllerCapsFunc {
	return func(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
		resp := &csi.ControllerGetCapabilitiesResponse{}
		for _, rpc := range rpcs {
			resp.Capabilities = append(resp.Capabilities, &csi.ControllerServiceCapability{
				Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: rpc}},
			})
		}
		return resp, nil
	}
}

// failCaps is a ControllerGetCapabilities handler that fails, so the plugin
// assumes every RPC is supported.
func failCaps(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return nil, status.Error(codes.Unavailable, "down")
}

// newSnapshotTestPlugin serves driver on a unix socket and returns a plugin
// for it whose controller capabilities have been initialized.
func newSnapshotTestPlugin(t *testing.T, driver *mockCSIDriver) *Plugin {
	t.Helper()
	endpoint, cleanup := startMockCSIDriver(t, driver)
	t.Cleanup(cleanup)

	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	plugin := NewPlugin(client)
	plugin.InitControllerCapabilities(context.Background())
	return plugin
}

func TestPlugin_ControllerCapabilities(t *testing.T) {
	tests := []struct {
		name string
		caps controllerCapsFunc
		want volume.Capabilities
	}{
		{
			name: "create and delete only",
			caps: reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME, csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT),
			want: volume.Capabilities{CreateDeleteSnapshot: true},
		},
		{
			name: "create, delete and list",
			caps: reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT, csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS),
			want: volume.Capabilities{CreateDeleteSnapshot: true, ListSnapshots: true},
		},
		{
			name: "no snapshot support",
			caps: reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME),
			want: volume.Capabilities{},
		},
		{
			// Discovery is best-effort: a driver that cannot report its
			// capabilities is assumed to support everything.
			name: "query fails",
			caps: failCaps,
			want: volume.Capabilities{CreateDeleteSnapshot: true, ListSnapshots: true},
		},
		{
			name: "reports nothing",
			caps: reportCaps(),
			want: volume.Capabilities{CreateDeleteSnapshot: true, ListSnapshots: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := newSnapshotTestPlugin(t, &mockCSIDriver{controllerGetCapabilitiesFunc: tt.caps})
			got, err := plugin.ControllerCapabilities(context.Background())
			if err != nil {
				t.Fatalf("ControllerCapabilities: %v", err)
			}
			if got != tt.want {
				t.Errorf("ControllerCapabilities = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestPlugin_ControllerCapabilities_Uninitialized(t *testing.T) {
	got, err := NewPlugin(nil).ControllerCapabilities(context.Background())
	if err != nil {
		t.Fatalf("ControllerCapabilities: %v", err)
	}
	if got != (volume.Capabilities{}) {
		t.Errorf("ControllerCapabilities before init = %+v, want none", got)
	}
}

func TestPlugin_CreateSnapshot(t *testing.T) {
	tests := []struct {
		name string
		caps controllerCapsFunc
		// driverID is the snapshot ID the driver returns.
		driverID  string
		driverErr error
		wantErr   bool
		wantCalls int
		// wantCap is whether CreateDeleteSnapshot is reported afterwards.
		wantCap bool
	}{
		{
			name:      "creates",
			caps:      reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT),
			driverID:  "snap-s",
			wantCalls: 1,
			wantCap:   true,
		},
		{
			name:    "refused without the capability",
			caps:    reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME),
			wantErr: true,
		},
		{
			name:      "empty ID from the driver",
			caps:      reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT),
			wantErr:   true,
			wantCalls: 1,
			wantCap:   true,
		},
		{
			name:      "Unimplemented disables the capability",
			caps:      failCaps,
			driverErr: status.Error(codes.Unimplemented, "unimplemented"),
			wantErr:   true,
			wantCalls: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls int
			plugin := newSnapshotTestPlugin(t, &mockCSIDriver{
				controllerGetCapabilitiesFunc: tt.caps,
				createSnapshotFunc: func(_ context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
					calls++
					if tt.driverErr != nil {
						return nil, tt.driverErr
					}
					return &csi.CreateSnapshotResponse{Snapshot: &csi.Snapshot{
						SnapshotId:     tt.driverID,
						SourceVolumeId: req.GetSourceVolumeId(),
						ReadyToUse:     true,
					}}, nil
				},
			})
			ctx := context.Background()

			snap, err := plugin.CreateSnapshot(ctx, volume.CreateSnapshotRequest{Name: "s", SourceVolumeID: "v"})
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("CreateSnapshot error = %v, wantErr %v", err, tt.wantErr)
			}
			if want := (volume.Snapshot{SnapshotID: "snap-s", SourceVolumeID: "v", ReadyToUse: true}); !tt.wantErr && snap != want {
				t.Errorf("CreateSnapshot = %+v, want %+v", snap, want)
			}
			if calls != tt.wantCalls {
				t.Errorf("CreateSnapshot reached the driver %d times, want %d", calls, tt.wantCalls)
			}
			if caps, _ := plugin.ControllerCapabilities(ctx); caps.CreateDeleteSnapshot != tt.wantCap {
				t.Errorf("CreateDeleteSnapshot = %v afterwards, want %v", caps.CreateDeleteSnapshot, tt.wantCap)
			}
		})
	}
}

func TestPlugin_GetSnapshot(t *testing.T) {
	tests := []struct {
		name       string
		caps       controllerCapsFunc
		snapshotID string
		listErr    error
		// emptyList makes ListSnapshots return no entries.
		emptyList       bool
		wantFound       bool
		wantErr         bool
		wantUnsupported bool
		wantCalls       int
		// wantCap is whether ListSnapshots is reported afterwards.
		wantCap bool
	}{
		{
			name:       "found",
			caps:       reportCaps(csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS),
			snapshotID: "snap-1",
			wantFound:  true,
			wantCalls:  1,
			wantCap:    true,
		},
		{
			name:       "empty list is not found",
			caps:       reportCaps(csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS),
			snapshotID: "snap-1",
			emptyList:  true,
			wantCalls:  1,
			wantCap:    true,
		},
		{
			name:       "NotFound from the driver is not found",
			caps:       reportCaps(csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS),
			snapshotID: "snap-1",
			listErr:    status.Error(codes.NotFound, "no such snapshot"),
			wantCalls:  1,
			wantCap:    true,
		},
		{
			name:       "other driver errors are returned",
			caps:       reportCaps(csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS),
			snapshotID: "snap-1",
			listErr:    status.Error(codes.Unavailable, "down"),
			wantErr:    true,
			wantCalls:  1,
			wantCap:    true,
		},
		{
			name:            "unsupported without the capability",
			caps:            reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT),
			snapshotID:      "snap-1",
			wantErr:         true,
			wantUnsupported: true,
		},
		{
			name:            "Unimplemented is unsupported and disables the capability",
			caps:            failCaps,
			snapshotID:      "snap-1",
			listErr:         status.Error(codes.Unimplemented, "unimplemented"),
			wantErr:         true,
			wantUnsupported: true,
			wantCalls:       1,
		},
		{
			name:    "empty ID is rejected",
			caps:    reportCaps(csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS),
			wantErr: true,
			wantCap: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls int
			plugin := newSnapshotTestPlugin(t, &mockCSIDriver{
				controllerGetCapabilitiesFunc: tt.caps,
				listSnapshotsFunc: func(_ context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
					calls++
					if tt.listErr != nil {
						return nil, tt.listErr
					}
					if tt.emptyList {
						return &csi.ListSnapshotsResponse{}, nil
					}
					return &csi.ListSnapshotsResponse{Entries: []*csi.ListSnapshotsResponse_Entry{
						{Snapshot: &csi.Snapshot{SnapshotId: req.GetSnapshotId(), ReadyToUse: true}},
					}}, nil
				},
			})
			ctx := context.Background()

			snap, found, err := plugin.GetSnapshot(ctx, tt.snapshotID)
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("GetSnapshot error = %v, wantErr %v", err, tt.wantErr)
			}
			if got := errors.Is(err, errors.ErrUnsupported); got != tt.wantUnsupported {
				t.Errorf("GetSnapshot error = %v, want ErrUnsupported %v", err, tt.wantUnsupported)
			}
			if found != tt.wantFound {
				t.Errorf("GetSnapshot found = %v, want %v", found, tt.wantFound)
			}
			if want := (volume.Snapshot{SnapshotID: tt.snapshotID, ReadyToUse: true}); found && snap != want {
				t.Errorf("GetSnapshot = %+v, want %+v", snap, want)
			}
			if calls != tt.wantCalls {
				t.Errorf("ListSnapshots reached the driver %d times, want %d", calls, tt.wantCalls)
			}
			if caps, _ := plugin.ControllerCapabilities(ctx); caps.ListSnapshots != tt.wantCap {
				t.Errorf("ListSnapshots = %v afterwards, want %v", caps.ListSnapshots, tt.wantCap)
			}
		})
	}
}

func TestPlugin_DeleteSnapshot(t *testing.T) {
	tests := []struct {
		name       string
		caps       controllerCapsFunc
		snapshotID string
		driverErr  error
		wantErr    bool
		wantCalls  int
	}{
		{
			name:       "deletes",
			caps:       reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT),
			snapshotID: "snap-1",
			wantCalls:  1,
		},
		{
			name:       "not found is success",
			caps:       reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT),
			snapshotID: "snap-1",
			driverErr:  status.Error(codes.NotFound, "gone"),
			wantCalls:  1,
		},
		{
			name:       "other errors are returned",
			caps:       reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT),
			snapshotID: "snap-1",
			driverErr:  status.Error(codes.Internal, "boom"),
			wantErr:    true,
			wantCalls:  1,
		},
		{
			// The caller holds a handle the driver issued, and skipping the
			// call would leak it.
			name:       "sent without the capability",
			caps:       reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME),
			snapshotID: "snap-1",
			wantCalls:  1,
		},
		{
			name:    "empty ID is rejected",
			caps:    reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls int
			plugin := newSnapshotTestPlugin(t, &mockCSIDriver{
				controllerGetCapabilitiesFunc: tt.caps,
				deleteSnapshotFunc: func(context.Context, *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
					calls++
					if tt.driverErr != nil {
						return nil, tt.driverErr
					}
					return &csi.DeleteSnapshotResponse{}, nil
				},
			})

			err := plugin.DeleteSnapshot(context.Background(), tt.snapshotID)
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Errorf("DeleteSnapshot error = %v, wantErr %v", err, tt.wantErr)
			}
			if calls != tt.wantCalls {
				t.Errorf("DeleteSnapshot reached the driver %d times, want %d", calls, tt.wantCalls)
			}
		})
	}
}

func TestPlugin_CreateVolume_RestoreSource(t *testing.T) {
	snapshotCaps := reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT)
	tests := []struct {
		name   string
		caps   controllerCapsFunc
		source string
		// reportedSource, if set, replaces the content source the driver
		// reports. dropSource reports none, as a driver that ignores the
		// requested source would. emptySourceID reports a snapshot source
		// with no ID.
		reportedSource string
		dropSource     bool
		emptySourceID  bool
		deleteErr      error
		wantErr        bool
		// wantErrContains is a substring the error must contain.
		wantErrContains string
		wantSource      string
		wantCreateCalls int
		wantDeleted     []string
	}{
		{
			name:            "no source",
			caps:            snapshotCaps,
			wantCreateCalls: 1,
		},
		{
			name:            "driver reports the requested source",
			caps:            snapshotCaps,
			source:          "snap-1",
			wantSource:      "snap-1",
			wantCreateCalls: 1,
		},
		{
			name:            "driver reports no source",
			caps:            snapshotCaps,
			source:          "snap-1",
			dropSource:      true,
			wantErr:         true,
			wantCreateCalls: 1,
			wantDeleted:     []string{"vol-v"},
		},
		{
			name:            "driver reports a snapshot source with no ID",
			caps:            snapshotCaps,
			source:          "snap-1",
			emptySourceID:   true,
			wantErr:         true,
			wantCreateCalls: 1,
			wantDeleted:     []string{"vol-v"},
		},
		{
			name:            "deleting the unrestored volume fails",
			caps:            snapshotCaps,
			source:          "snap-1",
			dropSource:      true,
			deleteErr:       status.Error(codes.Internal, "boom"),
			wantErr:         true,
			wantErrContains: "vol-v",
			wantCreateCalls: 1,
			wantDeleted:     []string{"vol-v"},
		},
		{
			// Drivers may report the source in their own format, as GCE PD
			// reports a numeric ID for a projects/.../snapshots/... handle.
			name:            "driver reports the source in its own format",
			caps:            snapshotCaps,
			source:          "projects/p/global/snapshots/snap-1",
			reportedSource:  "8123456789012345678",
			wantSource:      "8123456789012345678",
			wantCreateCalls: 1,
		},
		{
			name:    "restore refused without snapshot support",
			caps:    reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME),
			source:  "snap-1",
			wantErr: true,
		},
		{
			name:            "empty volume needs no snapshot support",
			caps:            reportCaps(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME),
			wantCreateCalls: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var createCalls int
			var deleted []string
			plugin := newSnapshotTestPlugin(t, &mockCSIDriver{
				controllerGetCapabilitiesFunc: tt.caps,
				createVolumeFunc: func(_ context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
					createCalls++
					vol := &csi.Volume{VolumeId: "vol-" + req.GetName()}
					source := req.GetVolumeContentSource().GetSnapshot().GetSnapshotId()
					if tt.reportedSource != "" {
						source = tt.reportedSource
					}
					if tt.emptySourceID {
						vol.ContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
							Snapshot: &csi.VolumeContentSource_SnapshotSource{},
						}}
					} else if source != "" && !tt.dropSource {
						vol.ContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
							Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: source},
						}}
					}
					return &csi.CreateVolumeResponse{Volume: vol}, nil
				},
				deleteVolumeFunc: func(_ context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
					deleted = append(deleted, req.GetVolumeId())
					if tt.deleteErr != nil {
						return nil, tt.deleteErr
					}
					return &csi.DeleteVolumeResponse{}, nil
				},
			})

			resp, err := plugin.CreateVolume(context.Background(), volume.CreateVolumeRequest{
				Name:             "v",
				CapacityBytes:    1 << 30,
				SourceSnapshotID: tt.source,
			})
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("CreateVolume error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErrContains != "" && !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Errorf("CreateVolume error = %v, want it to contain %q", err, tt.wantErrContains)
			}
			if resp.ContentSourceSnapshotID != tt.wantSource {
				t.Errorf("ContentSourceSnapshotID = %q, want %q", resp.ContentSourceSnapshotID, tt.wantSource)
			}
			if createCalls != tt.wantCreateCalls {
				t.Errorf("CreateVolume reached the driver %d times, want %d", createCalls, tt.wantCreateCalls)
			}
			if !slices.Equal(deleted, tt.wantDeleted) {
				t.Errorf("deleted volumes = %v, want %v", deleted, tt.wantDeleted)
			}
		})
	}
}
