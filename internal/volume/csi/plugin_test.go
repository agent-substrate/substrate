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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockCSIDriver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer

	createVolumeFunc              func(context.Context, *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error)
	deleteVolumeFunc              func(context.Context, *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error)
	controllerPublishVolumeFunc   func(context.Context, *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error)
	controllerUnpublishVolumeFunc func(context.Context, *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error)
	nodeStageVolumeFunc           func(context.Context, *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error)
	nodeUnstageVolumeFunc         func(context.Context, *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error)
	nodePublishVolumeFunc         func(context.Context, *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error)
	nodeUnpublishVolumeFunc       func(context.Context, *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error)

	controllerGetCapabilitiesFunc func(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error)
	nodeGetCapabilitiesFunc       func(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error)
	getPluginCapabilitiesFunc     func(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error)
	probeFunc                     func(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error)
}

func (m *mockCSIDriver) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          "mock-driver",
		VendorVersion: "v1.0.0",
	}, nil
}

func (m *mockCSIDriver) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	if m.getPluginCapabilitiesFunc != nil {
		return m.getPluginCapabilitiesFunc(ctx, req)
	}
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
		},
	}, nil
}

func (m *mockCSIDriver) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	if m.probeFunc != nil {
		return m.probeFunc(ctx, req)
	}
	return &csi.ProbeResponse{}, nil
}

func (m *mockCSIDriver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if m.createVolumeFunc != nil {
		return m.createVolumeFunc(ctx, req)
	}
	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      req.GetName(),
			CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
		},
	}, nil
}

func (m *mockCSIDriver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if m.deleteVolumeFunc != nil {
		return m.deleteVolumeFunc(ctx, req)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

func (m *mockCSIDriver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if m.controllerPublishVolumeFunc != nil {
		return m.controllerPublishVolumeFunc(ctx, req)
	}
	return &csi.ControllerPublishVolumeResponse{}, nil
}

func (m *mockCSIDriver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if m.controllerUnpublishVolumeFunc != nil {
		return m.controllerUnpublishVolumeFunc(ctx, req)
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (m *mockCSIDriver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	if m.controllerGetCapabilitiesFunc != nil {
		return m.controllerGetCapabilitiesFunc(ctx, req)
	}
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
					},
				},
			},
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_PUBLISH_READONLY,
					},
				},
			},
		},
	}, nil
}

func (m *mockCSIDriver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	if m.nodeGetCapabilitiesFunc != nil {
		return m.nodeGetCapabilitiesFunc(ctx, req)
	}
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
		},
	}, nil
}

func (m *mockCSIDriver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if m.nodeStageVolumeFunc != nil {
		return m.nodeStageVolumeFunc(ctx, req)
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

func (m *mockCSIDriver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if m.nodeUnstageVolumeFunc != nil {
		return m.nodeUnstageVolumeFunc(ctx, req)
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (m *mockCSIDriver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if m.nodePublishVolumeFunc != nil {
		return m.nodePublishVolumeFunc(ctx, req)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (m *mockCSIDriver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if m.nodeUnpublishVolumeFunc != nil {
		return m.nodeUnpublishVolumeFunc(ctx, req)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func startMockCSIDriver(t *testing.T, driver *mockCSIDriver) (string, func()) {
	tmpDir, err := os.MkdirTemp("", "csi-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	socketPath := filepath.Join(tmpDir, "csi.sock")
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to listen on socket %q: %v", socketPath, err)
	}

	s := grpc.NewServer()
	csi.RegisterIdentityServer(s, driver)
	csi.RegisterControllerServer(s, driver)
	csi.RegisterNodeServer(s, driver)

	go func() {
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("grpc server failed: %v", err)
		}
	}()

	cleanup := func() {
		s.GracefulStop()
		lis.Close()
		os.RemoveAll(tmpDir)
	}

	return "unix://" + socketPath, cleanup
}

func TestPlugin_CreateVolume(t *testing.T) {
	driver := &mockCSIDriver{}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	plugin := NewPlugin(client)

	ctx := context.Background()
	resp, err := plugin.CreateVolume(ctx, volume.CreateVolumeRequest{
		Name:     "test-vol",
		Capacity: "1Gi",
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	if resp.VolumeID != "test-vol" {
		t.Errorf("expected volume ID %q, got %q", "test-vol", resp.VolumeID)
	}
}

func TestPlugin_DeleteVolume(t *testing.T) {
	driver := &mockCSIDriver{}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	plugin := NewPlugin(client)

	ctx := context.Background()
	err = plugin.DeleteVolume(ctx, "test-vol")
	if err != nil {
		t.Fatalf("DeleteVolume failed: %v", err)
	}
}

func TestPlugin_AttachVolume(t *testing.T) {
	driver := &mockCSIDriver{}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	plugin := NewPlugin(client)

	ctx := context.Background()
	plugin.InitControllerCapabilities(ctx)
	resp, err := plugin.AttachVolume(ctx, volume.AttachVolumeRequest{VolumeID: "test-vol", Node: "node-1"})
	if err != nil {
		t.Fatalf("AttachVolume failed: %v", err)
	}
	if len(resp.PublishContext) != 0 {
		t.Errorf("expected no publish context from a driver that returns none, got %v", resp.PublishContext)
	}

	driver.controllerPublishVolumeFunc = func(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
		return &csi.ControllerPublishVolumeResponse{
			PublishContext: map[string]string{"devicePath": "/dev/xvdba"},
		}, nil
	}
	resp, err = plugin.AttachVolume(ctx, volume.AttachVolumeRequest{VolumeID: "test-vol", Node: "node-1"})
	if err != nil {
		t.Fatalf("AttachVolume failed: %v", err)
	}
	wantPublishCtx := map[string]string{"devicePath": "/dev/xvdba"}
	if diff := cmp.Diff(wantPublishCtx, resp.PublishContext); diff != "" {
		t.Errorf("publish context mismatch (-want +got):\n%s", diff)
	}

	// ReadOnlyMany attaches read-only, since the mock advertises PUBLISH_READONLY.
	var receivedReq *csi.ControllerPublishVolumeRequest
	driver.controllerPublishVolumeFunc = func(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
		receivedReq = req
		return &csi.ControllerPublishVolumeResponse{}, nil
	}
	if _, err := plugin.AttachVolume(ctx, volume.AttachVolumeRequest{VolumeID: "test-vol", Node: "node-1", AccessMode: ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY}); err != nil {
		t.Fatalf("AttachVolume failed: %v", err)
	}
	if !receivedReq.GetReadonly() {
		t.Errorf("expected Readonly to be true for ReadOnlyMany")
	}
	if got := receivedReq.GetVolumeCapability().GetAccessMode().GetMode(); got != csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		t.Errorf("expected MULTI_NODE_READER_ONLY, got %v", got)
	}

	// Test Unimplemented warning bypass
	driver.controllerPublishVolumeFunc = func(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
		return nil, status.Error(codes.Unimplemented, "unimplemented")
	}
	resp, err = plugin.AttachVolume(ctx, volume.AttachVolumeRequest{VolumeID: "test-vol", Node: "node-1"})
	if err != nil {
		t.Errorf("AttachVolume should have ignored Unimplemented error, got: %v", err)
	}
	if len(resp.PublishContext) != 0 {
		t.Errorf("expected no publish context from an unimplemented attach, got %v", resp.PublishContext)
	}
}

func TestPlugin_DetachVolume(t *testing.T) {
	driver := &mockCSIDriver{}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	plugin := NewPlugin(client)

	ctx := context.Background()
	plugin.InitControllerCapabilities(ctx)
	err = plugin.DetachVolume(ctx, "test-vol", "node-1")
	if err != nil {
		t.Fatalf("DetachVolume failed: %v", err)
	}

	// Test Unimplemented warning bypass
	driver.controllerUnpublishVolumeFunc = func(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
		return nil, status.Error(codes.Unimplemented, "unimplemented")
	}
	err = plugin.DetachVolume(ctx, "test-vol", "node-1")
	if err != nil {
		t.Errorf("DetachVolume should have ignored Unimplemented error, got: %v", err)
	}
}

func TestPlugin_MountVolume(t *testing.T) {
	var receivedStageReq *csi.NodeStageVolumeRequest
	var receivedPublishReq *csi.NodePublishVolumeRequest
	driver := &mockCSIDriver{
		nodeStageVolumeFunc: func(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
			receivedStageReq = req
			return &csi.NodeStageVolumeResponse{}, nil
		},
		nodePublishVolumeFunc: func(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
			receivedPublishReq = req
			return &csi.NodePublishVolumeResponse{}, nil
		},
	}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	plugin := NewPlugin(client)
	tmpDir, err := os.MkdirTemp("", "csi-mount-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin.stagingDirPrefix = filepath.Join(tmpDir, "staging")
	targetPath := filepath.Join(tmpDir, "target")

	ctx := context.Background()
	plugin.InitNodeCapabilities(ctx)
	err = plugin.MountVolume(ctx, volume.MountVolumeRequest{
		VolumeID:       "test-vol",
		TargetPath:     targetPath,
		PublishContext: map[string]string{"devicePath": "/dev/xvdba"},
		AccessMode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY,
	})
	if err != nil {
		t.Fatalf("MountVolume failed: %v", err)
	}

	// Verify staging directory was created
	stagingPath := filepath.Join(plugin.stagingDirPrefix, "test-vol")
	if _, err := os.Stat(stagingPath); os.IsNotExist(err) {
		t.Errorf("staging directory %q was not created", stagingPath)
	}

	// ReadOnlyMany stages and publishes with MULTI_NODE_READER_ONLY, and
	// publishes read-only.
	if got := receivedStageReq.GetVolumeCapability().GetAccessMode().GetMode(); got != csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		t.Errorf("expected MULTI_NODE_READER_ONLY in NodeStageVolume, got %v", got)
	}
	if got := receivedPublishReq.GetVolumeCapability().GetAccessMode().GetMode(); got != csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		t.Errorf("expected MULTI_NODE_READER_ONLY in NodePublishVolume, got %v", got)
	}
	if !receivedPublishReq.GetReadonly() {
		t.Errorf("expected Readonly to be true in NodePublishVolume for ReadOnlyMany")
	}

	// Test NodeStageVolume Unimplemented bypass
	driver.nodeStageVolumeFunc = func(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
		return nil, status.Error(codes.Unimplemented, "unimplemented")
	}
	// Reset dir
	os.RemoveAll(tmpDir)
	os.MkdirAll(plugin.stagingDirPrefix, 0750)

	err = plugin.MountVolume(ctx, volume.MountVolumeRequest{VolumeID: "test-vol-2", TargetPath: targetPath})
	if err != nil {
		t.Errorf("MountVolume should have succeeded when NodeStageVolume is unimplemented, got: %v", err)
	}
}

func TestPlugin_UnmountVolume(t *testing.T) {
	driver := &mockCSIDriver{}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	plugin := NewPlugin(client)
	tmpDir, err := os.MkdirTemp("", "csi-unmount-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin.stagingDirPrefix = filepath.Join(tmpDir, "staging")
	targetPath := filepath.Join(tmpDir, "target")

	// Pre-create staging directory to test cleanup
	stagingPath := filepath.Join(plugin.stagingDirPrefix, "test-vol")
	if err := os.MkdirAll(stagingPath, 0750); err != nil {
		t.Fatalf("failed to create staging path: %v", err)
	}

	ctx := context.Background()
	plugin.InitNodeCapabilities(ctx)
	err = plugin.UnmountVolume(ctx, "test-vol", targetPath)
	if err != nil {
		t.Fatalf("UnmountVolume failed: %v", err)
	}

	// Verify staging directory was deleted
	if _, err := os.Stat(stagingPath); !os.IsNotExist(err) {
		t.Errorf("staging directory %q was not deleted", stagingPath)
	}

	// Test NodeUnstageVolume Unimplemented bypass
	driver.nodeUnstageVolumeFunc = func(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
		return nil, status.Error(codes.Unimplemented, "unimplemented")
	}
	// Re-create staging dir
	if err := os.MkdirAll(stagingPath, 0750); err != nil {
		t.Fatalf("failed to create staging path: %v", err)
	}
	err = plugin.UnmountVolume(ctx, "test-vol", targetPath)
	if err != nil {
		t.Errorf("UnmountVolume should have succeeded when NodeUnstageVolume is unimplemented, got: %v", err)
	}
}

func TestClient_Identity(t *testing.T) {
	driver := &mockCSIDriver{}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Test GetPluginInfo
	info, err := client.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo failed: %v", err)
	}
	if info.GetName() != "mock-driver" {
		t.Errorf("expected plugin name %q, got %q", "mock-driver", info.GetName())
	}

	// Test GetPluginCapabilities
	caps, err := client.GetPluginCapabilities(ctx, &csi.GetPluginCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("GetPluginCapabilities failed: %v", err)
	}
	if len(caps.GetCapabilities()) == 0 {
		t.Errorf("expected capabilities, got none")
	}

	// Test Probe
	_, err = client.Probe(ctx, &csi.ProbeRequest{})
	if err != nil {
		t.Fatalf("Probe failed: %v", err)
	}
}

func TestPlugin_Capabilities_SkipAttachDetachWhenUnsupported(t *testing.T) {
	var publishCalled, unpublishCalled int
	driver := &mockCSIDriver{
		controllerGetCapabilitiesFunc: func(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
			// Return capabilities WITHOUT PUBLISH_UNPUBLISH_VOLUME (like hostpath driver)
			return &csi.ControllerGetCapabilitiesResponse{
				Capabilities: []*csi.ControllerServiceCapability{
					{
						Type: &csi.ControllerServiceCapability_Rpc{
							Rpc: &csi.ControllerServiceCapability_RPC{
								Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
							},
						},
					},
				},
			}, nil
		},
		controllerPublishVolumeFunc: func(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
			publishCalled++
			return &csi.ControllerPublishVolumeResponse{}, nil
		},
		controllerUnpublishVolumeFunc: func(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
			unpublishCalled++
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		},
	}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	plugin := NewPlugin(client)
	ctx := context.Background()

	plugin.InitControllerCapabilities(ctx)

	if plugin.SupportsControllerCapability(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME) {
		t.Errorf("expected PUBLISH_UNPUBLISH_VOLUME to be false")
	}

	// AttachVolume should skip calling ControllerPublishVolume entirely
	if _, err := plugin.AttachVolume(ctx, volume.AttachVolumeRequest{VolumeID: "test-vol", Node: "node-1"}); err != nil {
		t.Errorf("AttachVolume failed: %v", err)
	}
	if publishCalled != 0 {
		t.Errorf("expected 0 calls to ControllerPublishVolume, got %d", publishCalled)
	}

	// DetachVolume should skip calling ControllerUnpublishVolume entirely
	if err := plugin.DetachVolume(ctx, "test-vol", "node-1"); err != nil {
		t.Errorf("DetachVolume failed: %v", err)
	}
	if unpublishCalled != 0 {
		t.Errorf("expected 0 calls to ControllerUnpublishVolume, got %d", unpublishCalled)
	}
}

func TestPlugin_Capabilities_SkipStageUnstageWhenUnsupported(t *testing.T) {
	var stageCalled, unstageCalled int
	driver := &mockCSIDriver{
		nodeGetCapabilitiesFunc: func(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
			// Return capabilities WITHOUT STAGE_UNSTAGE_VOLUME
			return &csi.NodeGetCapabilitiesResponse{
				Capabilities: []*csi.NodeServiceCapability{
					{
						Type: &csi.NodeServiceCapability_Rpc{
							Rpc: &csi.NodeServiceCapability_RPC{
								Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
							},
						},
					},
				},
			}, nil
		},
		nodeStageVolumeFunc: func(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
			stageCalled++
			return &csi.NodeStageVolumeResponse{}, nil
		},
		nodeUnstageVolumeFunc: func(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
			unstageCalled++
			return &csi.NodeUnstageVolumeResponse{}, nil
		},
	}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	plugin := NewPlugin(client)
	ctx := context.Background()

	plugin.InitNodeCapabilities(ctx)

	if plugin.SupportsNodeCapability(csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME) {
		t.Errorf("expected STAGE_UNSTAGE_VOLUME to be false")
	}

	tmpDir := t.TempDir()
	targetPath := filepath.Join(tmpDir, "target")
	if err := os.MkdirAll(targetPath, 0750); err != nil {
		t.Fatalf("failed to create target path: %v", err)
	}

	// MountVolume should skip NodeStageVolume entirely
	if err := plugin.MountVolume(ctx, volume.MountVolumeRequest{VolumeID: "test-vol", TargetPath: targetPath}); err != nil {
		t.Errorf("MountVolume failed: %v", err)
	}
	if stageCalled != 0 {
		t.Errorf("expected 0 calls to NodeStageVolume, got %d", stageCalled)
	}

	// UnmountVolume should skip NodeUnstageVolume entirely
	if err := plugin.UnmountVolume(ctx, "test-vol", targetPath); err != nil {
		t.Errorf("UnmountVolume failed: %v", err)
	}
	if unstageCalled != 0 {
		t.Errorf("expected 0 calls to NodeUnstageVolume, got %d", unstageCalled)
	}
}

func TestPlugin_VolumeCapability(t *testing.T) {
	// Guard against enum drift: VolumeCapability must map every access mode.
	if got := len(ateapipb.VolumeAccessMode_name) - 1; got != 3 {
		t.Fatalf("VolumeAccessMode has %d modes besides UNSPECIFIED, want 3; update VolumeCapability", got)
	}

	tests := []struct {
		name     string
		mode     ateapipb.VolumeAccessMode
		wantMode csi.VolumeCapability_AccessMode_Mode
		wantErr  bool
	}{
		{
			name:     "unspecified defaults to SINGLE_NODE_WRITER",
			mode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_UNSPECIFIED,
			wantMode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
		{
			name:     "ReadWriteOnce maps to SINGLE_NODE_WRITER",
			mode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_ONCE,
			wantMode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
		{
			name:     "ReadOnlyMany maps to MULTI_NODE_READER_ONLY",
			mode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY,
			wantMode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		},
		{
			name:     "ReadWriteMany maps to MULTI_NODE_MULTI_WRITER",
			mode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY,
			wantMode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		},
		{
			name:    "negative mode rejected",
			mode:    ateapipb.VolumeAccessMode(-1),
			wantErr: true,
		},
		{
			name:    "out-of-range mode rejected",
			mode:    ateapipb.VolumeAccessMode(99),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			volCap, err := VolumeCapability(tt.mode)
			if tt.wantErr {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("VolumeCapability(%v) error = %v, want InvalidArgument", tt.mode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("VolumeCapability(%v) failed: %v", tt.mode, err)
			}
			if got := volCap.GetAccessMode().GetMode(); got != tt.wantMode {
				t.Errorf("VolumeCapability(%v) mode = %v, want %v", tt.mode, got, tt.wantMode)
			}
			if volCap.GetMount() == nil {
				t.Errorf("VolumeCapability(%v) has no mount access type", tt.mode)
			}
		})
	}
}

func TestPlugin_AttachVolume_ReadWriteMany(t *testing.T) {
	var receivedReq *csi.ControllerPublishVolumeRequest
	driver := &mockCSIDriver{
		controllerPublishVolumeFunc: func(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
			receivedReq = req
			return &csi.ControllerPublishVolumeResponse{}, nil
		},
	}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	plugin := NewPlugin(client)
	ctx := context.Background()
	plugin.InitControllerCapabilities(ctx)

	if _, err := plugin.AttachVolume(ctx, volume.AttachVolumeRequest{VolumeID: "shared-vol", Node: "node-1", AccessMode: ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY}); err != nil {
		t.Fatalf("AttachVolume failed: %v", err)
	}
	if receivedReq.GetReadonly() {
		t.Errorf("expected Readonly to be false for ReadWriteMany")
	}
	if got := receivedReq.GetVolumeCapability().GetAccessMode().GetMode(); got != csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
		t.Errorf("expected MULTI_NODE_MULTI_WRITER, got %v", got)
	}
}

func TestPlugin_Capabilities_ReadOnlyValidation(t *testing.T) {
	tests := []struct {
		name                 string
		caps                 []csi.ControllerServiceCapability_RPC_Type
		mode                 ateapipb.VolumeAccessMode
		wantErr              bool
		wantSupportsReadonly bool
		wantPublishReadonly  bool
	}{
		{
			name: "publish readonly supported",
			caps: []csi.ControllerServiceCapability_RPC_Type{
				csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
				csi.ControllerServiceCapability_RPC_PUBLISH_READONLY,
			},
			mode:                 ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY,
			wantSupportsReadonly: true,
			wantPublishReadonly:  true,
		},
		{
			name: "publish readonly not supported attaches read-write",
			caps: []csi.ControllerServiceCapability_RPC_Type{
				csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
			},
			mode:                 ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY,
			wantSupportsReadonly: false,
			wantPublishReadonly:  false,
		},
		{
			name: "invalid mode rejected",
			caps: []csi.ControllerServiceCapability_RPC_Type{
				csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
			},
			mode:    ateapipb.VolumeAccessMode(99),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var caps []*csi.ControllerServiceCapability
			for _, rpc := range tt.caps {
				caps = append(caps, &csi.ControllerServiceCapability{
					Type: &csi.ControllerServiceCapability_Rpc{
						Rpc: &csi.ControllerServiceCapability_RPC{Type: rpc},
					},
				})
			}
			var receivedPublishReq *csi.ControllerPublishVolumeRequest
			driver := &mockCSIDriver{
				controllerGetCapabilitiesFunc: func(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
					return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
				},
				controllerPublishVolumeFunc: func(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
					receivedPublishReq = req
					return &csi.ControllerPublishVolumeResponse{}, nil
				},
			}
			endpoint, cleanup := startMockCSIDriver(t, driver)
			defer cleanup()

			client, err := NewCSIClient(endpoint, nil)
			if err != nil {
				t.Fatalf("failed to create CSI client: %v", err)
			}
			defer client.Close()

			plugin := NewPlugin(client)
			ctx := context.Background()
			plugin.InitControllerCapabilities(ctx)

			if got := plugin.SupportsControllerCapability(csi.ControllerServiceCapability_RPC_PUBLISH_READONLY); got != tt.wantSupportsReadonly {
				t.Errorf("PUBLISH_READONLY supported = %v, want %v", got, tt.wantSupportsReadonly)
			}

			_, err = plugin.AttachVolume(ctx, volume.AttachVolumeRequest{VolumeID: "test-vol", Node: "node-1", AccessMode: tt.mode})
			if tt.wantErr {
				if status.Code(err) != codes.InvalidArgument {
					t.Errorf("AttachVolume() error = %v, want InvalidArgument", err)
				}
				if receivedPublishReq != nil {
					t.Errorf("expected ControllerPublishVolume not to be called for an invalid mode")
				}
				return
			}
			if err != nil {
				t.Fatalf("AttachVolume failed: %v", err)
			}
			if receivedPublishReq == nil {
				t.Fatalf("expected ControllerPublishVolume to be called")
			}
			if got := receivedPublishReq.GetReadonly(); got != tt.wantPublishReadonly {
				t.Errorf("ControllerPublishVolumeRequest.Readonly = %v, want %v", got, tt.wantPublishReadonly)
			}
			if got := receivedPublishReq.GetVolumeCapability().GetAccessMode().GetMode(); got != csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
				t.Errorf("expected MULTI_NODE_READER_ONLY, got %v", got)
			}
		})
	}
}

func TestPlugin_CreateVolume_AccessModes(t *testing.T) {
	var receivedReq *csi.CreateVolumeRequest
	driver := &mockCSIDriver{
		createVolumeFunc: func(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
			receivedReq = req
			return &csi.CreateVolumeResponse{
				Volume: &csi.Volume{VolumeId: req.GetName()},
			}, nil
		},
	}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	plugin := NewPlugin(client)
	ctx := context.Background()

	tests := []struct {
		name     string
		mode     ateapipb.VolumeAccessMode
		wantMode csi.VolumeCapability_AccessMode_Mode
	}{
		{
			name:     "Unspecified",
			mode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_UNSPECIFIED,
			wantMode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
		{
			name:     "ReadWriteOnce",
			mode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_ONCE,
			wantMode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
		{
			name:     "ReadOnlyMany",
			mode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY,
			wantMode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		},
		{
			name:     "ReadWriteMany",
			mode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY,
			wantMode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			receivedReq = nil
			if _, err := plugin.CreateVolume(ctx, volume.CreateVolumeRequest{Name: "test-vol", Capacity: "1Gi", AccessMode: tt.mode}); err != nil {
				t.Fatalf("CreateVolume failed: %v", err)
			}
			volCaps := receivedReq.GetVolumeCapabilities()
			if len(volCaps) != 1 {
				t.Fatalf("expected 1 volume capability, got %d", len(volCaps))
			}
			if got := volCaps[0].GetAccessMode().GetMode(); got != tt.wantMode {
				t.Errorf("capability mode = %v, want %v", got, tt.wantMode)
			}
		})
	}

	t.Run("invalid mode rejected before calling the driver", func(t *testing.T) {
		receivedReq = nil
		_, err := plugin.CreateVolume(ctx, volume.CreateVolumeRequest{Name: "test-vol", Capacity: "1Gi", AccessMode: ateapipb.VolumeAccessMode(99)})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("CreateVolume() error = %v, want InvalidArgument", err)
		}
		if receivedReq != nil {
			t.Errorf("expected CreateVolume not to reach the driver for an invalid mode")
		}
	})

	t.Run("driver InvalidArgument is kept", func(t *testing.T) {
		driver.createVolumeFunc = func(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
			return nil, status.Error(codes.InvalidArgument, "driver does not support MULTI_NODE_MULTI_WRITER")
		}
		_, err := plugin.CreateVolume(ctx, volume.CreateVolumeRequest{Name: "test-vol", Capacity: "1Gi", AccessMode: ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("CreateVolume() error = %v (code %v), want InvalidArgument", err, status.Code(err))
		}
	})
}

func TestPlugin_Capabilities_AllEnabledWhenQueryFailsOrEmpty(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "Unimplemented", err: status.Error(codes.Unimplemented, "failed")},
		{name: "Unavailable", err: status.Error(codes.Unavailable, "failed")},
		{name: "Empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			driver := &mockCSIDriver{
				controllerGetCapabilitiesFunc: func(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
					if tc.err != nil {
						return nil, tc.err
					}
					return &csi.ControllerGetCapabilitiesResponse{}, nil
				},
				nodeGetCapabilitiesFunc: func(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
					if tc.err != nil {
						return nil, tc.err
					}
					return &csi.NodeGetCapabilitiesResponse{}, nil
				},
			}
			endpoint, cleanup := startMockCSIDriver(t, driver)
			defer cleanup()

			client, err := NewCSIClient(endpoint, nil)
			if err != nil {
				t.Fatalf("failed to create CSI client: %v", err)
			}
			defer client.Close()

			plugin := NewPlugin(client)
			ctx := context.Background()
			plugin.InitControllerCapabilities(ctx)
			plugin.InitNodeCapabilities(ctx)

			if !plugin.SupportsControllerCapability(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME) {
				t.Errorf("expected PUBLISH_UNPUBLISH_VOLUME to be true")
			}
			if !plugin.SupportsNodeCapability(csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME) {
				t.Errorf("expected STAGE_UNSTAGE_VOLUME to be true")
			}
		})
	}
}

func TestPlugin_Capabilities_DisableAttachDetachAfterUnimplemented(t *testing.T) {
	var publishCalled, unpublishCalled int
	driver := &mockCSIDriver{
		controllerPublishVolumeFunc: func(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
			publishCalled++
			return nil, status.Error(codes.Unimplemented, "unimplemented")
		},
		controllerUnpublishVolumeFunc: func(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
			unpublishCalled++
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		},
	}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	plugin := NewPlugin(client)
	ctx := context.Background()
	plugin.InitControllerCapabilities(ctx)

	for range 2 {
		if _, err := plugin.AttachVolume(ctx, volume.AttachVolumeRequest{VolumeID: "test-vol", Node: "node-1"}); err != nil {
			t.Errorf("AttachVolume failed: %v", err)
		}
	}
	if publishCalled != 1 {
		t.Errorf("expected 1 call to ControllerPublishVolume, got %d", publishCalled)
	}
	if plugin.SupportsControllerCapability(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME) {
		t.Errorf("expected PUBLISH_UNPUBLISH_VOLUME to be false after Unimplemented")
	}

	if err := plugin.DetachVolume(ctx, "test-vol", "node-1"); err != nil {
		t.Errorf("DetachVolume failed: %v", err)
	}
	if unpublishCalled != 0 {
		t.Errorf("expected 0 calls to ControllerUnpublishVolume, got %d", unpublishCalled)
	}
}

func TestPlugin_Capabilities_DisableStageUnstageAfterUnimplemented(t *testing.T) {
	var stageCalled, unstageCalled int
	driver := &mockCSIDriver{
		nodeStageVolumeFunc: func(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
			stageCalled++
			return nil, status.Error(codes.Unimplemented, "unimplemented")
		},
		nodeUnstageVolumeFunc: func(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
			unstageCalled++
			return &csi.NodeUnstageVolumeResponse{}, nil
		},
		nodePublishVolumeFunc: func(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
			if req.GetStagingTargetPath() != "" {
				t.Errorf("expected empty StagingTargetPath, got %q", req.GetStagingTargetPath())
			}
			return &csi.NodePublishVolumeResponse{}, nil
		},
	}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	plugin := NewPlugin(client)
	tmpDir := t.TempDir()
	plugin.stagingDirPrefix = filepath.Join(tmpDir, "staging")
	targetPath := filepath.Join(tmpDir, "target")
	ctx := context.Background()
	plugin.InitNodeCapabilities(ctx)

	for range 2 {
		if err := plugin.MountVolume(ctx, volume.MountVolumeRequest{VolumeID: "test-vol", TargetPath: targetPath}); err != nil {
			t.Errorf("MountVolume failed: %v", err)
		}
	}
	if stageCalled != 1 {
		t.Errorf("expected 1 call to NodeStageVolume, got %d", stageCalled)
	}
	if plugin.SupportsNodeCapability(csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME) {
		t.Errorf("expected STAGE_UNSTAGE_VOLUME to be false after Unimplemented")
	}
	stagingPath := filepath.Join(plugin.stagingDirPrefix, "test-vol")
	if _, err := os.Stat(stagingPath); !os.IsNotExist(err) {
		t.Errorf("expected staging directory %q to be removed, got err: %v", stagingPath, err)
	}

	if err := plugin.UnmountVolume(ctx, "test-vol", targetPath); err != nil {
		t.Errorf("UnmountVolume failed: %v", err)
	}
	if unstageCalled != 0 {
		t.Errorf("expected 0 calls to NodeUnstageVolume, got %d", unstageCalled)
	}
}

func TestNewCSIPlugin_CapabilityQueryFailureIsNotFatal(t *testing.T) {
	for _, isController := range []bool{true, false} {
		t.Run(fmt.Sprintf("isController=%t", isController), func(t *testing.T) {
			driver := &mockCSIDriver{
				controllerGetCapabilitiesFunc: func(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
					return nil, status.Error(codes.Unavailable, "unavailable")
				},
				nodeGetCapabilitiesFunc: func(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
					return nil, status.Error(codes.Unavailable, "unavailable")
				},
			}
			endpoint, cleanup := startMockCSIDriver(t, driver)
			defer cleanup()

			cfg := driverConfig(endpoint, nil)
			cfg.Spec.NodeSocketOverride = endpoint
			plugin, err := newCSIPlugin(t.Context(), &mockLister{cfg: cfg}, mockDriverName, isController, tlsPaths{})
			if err != nil {
				t.Fatalf("newCSIPlugin failed: %v", err)
			}
			defer plugin.client.Close()

			if isController && !plugin.SupportsControllerCapability(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME) {
				t.Errorf("expected PUBLISH_UNPUBLISH_VOLUME to be true")
			}
			if !isController && !plugin.SupportsNodeCapability(csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME) {
				t.Errorf("expected STAGE_UNSTAGE_VOLUME to be true")
			}
		})
	}
}
