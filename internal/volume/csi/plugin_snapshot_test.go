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
	"net"
	"path/filepath"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// snapshotCSIController is a CSI controller that serves the capability and
// snapshot RPCs, and counts the snapshot calls that reach it so a test can
// tell a skipped call from a made one.
type snapshotCSIController struct {
	csi.UnimplementedControllerServer

	// capsErr fails ControllerGetCapabilities; otherwise caps is reported.
	capsErr error
	caps    []csi.ControllerServiceCapability_RPC_Type
	// createErr and listErr fail CreateSnapshot and ListSnapshots.
	createErr error
	listErr   error

	mu          sync.Mutex
	createCalls int
	listCalls   int
	deleteCalls int
}

func (c *snapshotCSIController) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	if c.capsErr != nil {
		return nil, c.capsErr
	}
	resp := &csi.ControllerGetCapabilitiesResponse{}
	for _, rpc := range c.caps {
		resp.Capabilities = append(resp.Capabilities, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: rpc}},
		})
	}
	return resp, nil
}

func (c *snapshotCSIController) CreateSnapshot(_ context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.createCalls++
	if c.createErr != nil {
		return nil, c.createErr
	}
	return &csi.CreateSnapshotResponse{Snapshot: &csi.Snapshot{
		SnapshotId:     "snap-" + req.GetName(),
		SourceVolumeId: req.GetSourceVolumeId(),
		ReadyToUse:     true,
	}}, nil
}

func (c *snapshotCSIController) ListSnapshots(_ context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listCalls++
	if c.listErr != nil {
		return nil, c.listErr
	}
	return &csi.ListSnapshotsResponse{Entries: []*csi.ListSnapshotsResponse_Entry{
		{Snapshot: &csi.Snapshot{SnapshotId: req.GetSnapshotId(), ReadyToUse: true}},
	}}, nil
}

func (c *snapshotCSIController) DeleteSnapshot(context.Context, *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteCalls++
	return &csi.DeleteSnapshotResponse{}, nil
}

func (c *snapshotCSIController) calls() (create, list, del int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.createCalls, c.listCalls, c.deleteCalls
}

// newSnapshotTestPlugin serves controller on a unix socket and returns a
// plugin for it whose controller capabilities have been initialized.
func newSnapshotTestPlugin(t *testing.T, controller *snapshotCSIController) *Plugin {
	t.Helper()
	socketPath := filepath.Join(t.TempDir(), "csi.sock")
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to listen on socket %q: %v", socketPath, err)
	}
	s := grpc.NewServer()
	csi.RegisterIdentityServer(s, &mockCSIDriver{})
	csi.RegisterControllerServer(s, controller)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	client, err := NewCSIClient("unix://"+socketPath, nil)
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
		name       string
		controller *snapshotCSIController
		want       volume.Capabilities
	}{
		{
			name: "create and delete only",
			controller: &snapshotCSIController{caps: []csi.ControllerServiceCapability_RPC_Type{
				csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
				csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
			}},
			want: volume.Capabilities{CreateDeleteSnapshot: true},
		},
		{
			name: "create, delete and list",
			controller: &snapshotCSIController{caps: []csi.ControllerServiceCapability_RPC_Type{
				csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
				csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
			}},
			want: volume.Capabilities{CreateDeleteSnapshot: true, ListSnapshots: true},
		},
		{
			name: "no snapshot support",
			controller: &snapshotCSIController{caps: []csi.ControllerServiceCapability_RPC_Type{
				csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
			}},
			want: volume.Capabilities{},
		},
		{
			// Discovery is best-effort: a driver that cannot report its
			// capabilities is assumed to support everything.
			name:       "query fails",
			controller: &snapshotCSIController{capsErr: status.Error(codes.Unavailable, "down")},
			want:       volume.Capabilities{CreateDeleteSnapshot: true, ListSnapshots: true},
		},
		{
			name:       "reports nothing",
			controller: &snapshotCSIController{},
			want:       volume.Capabilities{CreateDeleteSnapshot: true, ListSnapshots: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := newSnapshotTestPlugin(t, tt.controller)
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

func TestPlugin_CreateSnapshot_SkippedWhenUnsupported(t *testing.T) {
	controller := &snapshotCSIController{caps: []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	}}
	plugin := newSnapshotTestPlugin(t, controller)

	if _, err := plugin.CreateSnapshot(context.Background(), volume.CreateSnapshotRequest{Name: "s", SourceVolumeID: "v"}); err == nil {
		t.Fatal("CreateSnapshot succeeded on a driver that does not support snapshots")
	}
	if create, _, _ := controller.calls(); create != 0 {
		t.Errorf("CreateSnapshot reached the driver %d times, want 0", create)
	}
}

func TestPlugin_CreateSnapshot_UnimplementedDisablesCapability(t *testing.T) {
	controller := &snapshotCSIController{
		capsErr:   errors.New("no capabilities"),
		createErr: status.Error(codes.Unimplemented, "unimplemented"),
	}
	plugin := newSnapshotTestPlugin(t, controller)
	ctx := context.Background()
	req := volume.CreateSnapshotRequest{Name: "s", SourceVolumeID: "v"}

	if _, err := plugin.CreateSnapshot(ctx, req); err == nil {
		t.Fatal("CreateSnapshot succeeded, want the driver's Unimplemented")
	}
	if caps, _ := plugin.ControllerCapabilities(ctx); caps.CreateDeleteSnapshot {
		t.Error("CreateDeleteSnapshot still reported after the driver returned Unimplemented")
	}
	if _, err := plugin.CreateSnapshot(ctx, req); err == nil {
		t.Fatal("second CreateSnapshot succeeded, want it refused")
	}
	if create, _, _ := controller.calls(); create != 1 {
		t.Errorf("CreateSnapshot reached the driver %d times, want 1", create)
	}
}

func TestPlugin_CreateSnapshot(t *testing.T) {
	controller := &snapshotCSIController{caps: []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
	}}
	plugin := newSnapshotTestPlugin(t, controller)

	snap, err := plugin.CreateSnapshot(context.Background(), volume.CreateSnapshotRequest{Name: "s", SourceVolumeID: "v"})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snap.SnapshotID != "snap-s" || snap.SourceVolumeID != "v" || !snap.ReadyToUse {
		t.Errorf("CreateSnapshot = %+v, want snap-s of v, ready", snap)
	}
}

func TestPlugin_GetSnapshot_SkippedWhenUnsupported(t *testing.T) {
	controller := &snapshotCSIController{caps: []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
	}}
	plugin := newSnapshotTestPlugin(t, controller)

	_, found, err := plugin.GetSnapshot(context.Background(), "snap-1")
	if err != nil || found {
		t.Fatalf("GetSnapshot = (found %v, %v), want not found and no error", found, err)
	}
	if _, list, _ := controller.calls(); list != 0 {
		t.Errorf("ListSnapshots reached the driver %d times, want 0", list)
	}
}

func TestPlugin_GetSnapshot_UnimplementedDisablesCapability(t *testing.T) {
	controller := &snapshotCSIController{
		capsErr: errors.New("no capabilities"),
		listErr: status.Error(codes.Unimplemented, "unimplemented"),
	}
	plugin := newSnapshotTestPlugin(t, controller)
	ctx := context.Background()

	for range 2 {
		if _, found, err := plugin.GetSnapshot(ctx, "snap-1"); err != nil || found {
			t.Fatalf("GetSnapshot = (found %v, %v), want not found and no error", found, err)
		}
	}
	if caps, _ := plugin.ControllerCapabilities(ctx); caps.ListSnapshots {
		t.Error("ListSnapshots still reported after the driver returned Unimplemented")
	}
	if _, list, _ := controller.calls(); list != 1 {
		t.Errorf("ListSnapshots reached the driver %d times, want 1", list)
	}
}

func TestPlugin_GetSnapshot(t *testing.T) {
	controller := &snapshotCSIController{caps: []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
	}}
	plugin := newSnapshotTestPlugin(t, controller)

	snap, found, err := plugin.GetSnapshot(context.Background(), "snap-1")
	if err != nil || !found {
		t.Fatalf("GetSnapshot = (found %v, %v), want found", found, err)
	}
	if snap.SnapshotID != "snap-1" || !snap.ReadyToUse {
		t.Errorf("GetSnapshot = %+v, want snap-1, ready", snap)
	}
}

// TestPlugin_DeleteSnapshot_NotSkipped verifies a delete always reaches the
// driver, even when the cached capabilities say it cannot snapshot: the
// caller holds a handle the driver issued, and skipping would leak it.
func TestPlugin_DeleteSnapshot_NotSkipped(t *testing.T) {
	controller := &snapshotCSIController{caps: []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	}}
	plugin := newSnapshotTestPlugin(t, controller)

	if err := plugin.DeleteSnapshot(context.Background(), "snap-1"); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
	if _, _, del := controller.calls(); del != 1 {
		t.Errorf("DeleteSnapshot reached the driver %d times, want 1", del)
	}
}
