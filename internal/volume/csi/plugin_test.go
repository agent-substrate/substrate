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
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/container-storage-interface/spec/lib/go/csi"
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

	getPluginCapabilitiesFunc func(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error)
	probeFunc                 func(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error)
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
	var receivedReq *csi.CreateVolumeRequest
	driver := &mockCSIDriver{
		createVolumeFunc: func(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
			receivedReq = req
			return &csi.CreateVolumeResponse{
				Volume: &csi.Volume{
					VolumeId:      req.GetName(),
					CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
				},
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
			name:     "ReadWriteOnce",
			mode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_ONCE,
			wantMode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
		{
			name:     "ReadWriteMany",
			mode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY,
			wantMode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		},
		{
			name:     "ReadOnlyMany",
			mode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY,
			wantMode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			volID, _, err := plugin.CreateVolume(ctx, "test-vol", "1Gi", "standard", nil, tt.mode)
			if err != nil {
				t.Fatalf("CreateVolume failed: %v", err)
			}
			if volID != "test-vol" {
				t.Errorf("expected volume ID %q, got %q", "test-vol", volID)
			}
			if len(receivedReq.GetVolumeCapabilities()) != 1 {
				t.Fatalf("expected 1 capability, got %d", len(receivedReq.GetVolumeCapabilities()))
			}
			gotMode := receivedReq.GetVolumeCapabilities()[0].GetAccessMode().GetMode()
			if gotMode != tt.wantMode {
				t.Errorf("expected capability mode %v, got %v", tt.wantMode, gotMode)
			}
		})
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
	var receivedReq *csi.ControllerPublishVolumeRequest
	driver := &mockCSIDriver{
		controllerPublishVolumeFunc: func(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
			receivedReq = req
			return &csi.ControllerPublishVolumeResponse{
				PublishContext: map[string]string{"device_path": "/dev/xvda"},
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
	pubCtx, err := plugin.AttachVolume(ctx, "test-vol", "node-1", ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY)
	if err != nil {
		t.Fatalf("AttachVolume failed: %v", err)
	}
	if pubCtx["device_path"] != "/dev/xvda" {
		t.Errorf("expected publish context device_path to be /dev/xvda, got %v", pubCtx)
	}
	if !receivedReq.GetReadonly() {
		t.Errorf("expected Readonly to be true for ReadOnlyMany")
	}
	if receivedReq.GetVolumeCapability().GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		t.Errorf("expected MULTI_NODE_READER_ONLY, got %v", receivedReq.GetVolumeCapability().GetAccessMode().GetMode())
	}

	// Test Unimplemented warning bypass
	driver.controllerPublishVolumeFunc = func(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
		return nil, status.Error(codes.Unimplemented, "unimplemented")
	}
	pubCtx, err = plugin.AttachVolume(ctx, "test-vol", "node-1", ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_ONCE)
	if err != nil {
		t.Errorf("AttachVolume should have ignored Unimplemented error, got: %v", err)
	}
	if pubCtx != nil {
		t.Errorf("expected nil publish context on unimplemented, got %v", pubCtx)
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
	tmpDir := t.TempDir()

	plugin.stagingDirPrefix = filepath.Join(tmpDir, "staging")
	targetPath := filepath.Join(tmpDir, "target")

	ctx := context.Background()
	pubCtx := map[string]string{"device_path": "/dev/xvda"}
	err = plugin.MountVolume(ctx, "test-vol", targetPath, nil, pubCtx, ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY)
	if err != nil {
		t.Fatalf("MountVolume failed: %v", err)
	}

	// Verify staging directory was created
	stagingPath := filepath.Join(plugin.stagingDirPrefix, "test-vol")
	if _, err := os.Stat(stagingPath); os.IsNotExist(err) {
		t.Errorf("staging directory %q was not created", stagingPath)
	}

	// Verify publishContext and access mode were propagated to NodeStageVolume
	if receivedStageReq == nil || receivedStageReq.GetPublishContext()["device_path"] != "/dev/xvda" {
		t.Errorf("expected publishContext to be propagated to NodeStageVolume, got %v", receivedStageReq)
	}
	if receivedStageReq.GetVolumeCapability().GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		t.Errorf("expected MULTI_NODE_READER_ONLY in stage, got %v", receivedStageReq.GetVolumeCapability().GetAccessMode().GetMode())
	}

	// Verify publishContext, access mode, and readonly were propagated to NodePublishVolume
	if receivedPublishReq == nil || receivedPublishReq.GetPublishContext()["device_path"] != "/dev/xvda" {
		t.Errorf("expected publishContext to be propagated to NodePublishVolume, got %v", receivedPublishReq)
	}
	if !receivedPublishReq.GetReadonly() {
		t.Errorf("expected Readonly to be true in NodePublishVolume for ReadOnlyMany")
	}
	if receivedPublishReq.GetVolumeCapability().GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		t.Errorf("expected MULTI_NODE_READER_ONLY in publish, got %v", receivedPublishReq.GetVolumeCapability().GetAccessMode().GetMode())
	}

	// Test NodeStageVolume Unimplemented bypass
	driver.nodeStageVolumeFunc = func(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
		return nil, status.Error(codes.Unimplemented, "unimplemented")
	}
	// Reset dir
	os.RemoveAll(tmpDir)
	os.MkdirAll(plugin.stagingDirPrefix, 0750)

	err = plugin.MountVolume(ctx, "test-vol-2", targetPath, nil, nil, ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_ONCE)
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

	if err := plugin.InitControllerCapabilities(ctx); err != nil {
		t.Fatalf("InitControllerCapabilities failed: %v", err)
	}

	if plugin.SupportsControllerPublish() {
		t.Errorf("expected SupportsControllerPublish to be false")
	}

	// AttachVolume should skip calling ControllerPublishVolume entirely
	if _, err := plugin.AttachVolume(ctx, "test-vol", "node-1", ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_ONCE); err != nil {
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
				Capabilities: []*csi.NodeServiceCapability{},
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

	if err := plugin.InitNodeCapabilities(ctx); err != nil {
		t.Fatalf("InitNodeCapabilities failed: %v", err)
	}

	if plugin.SupportsNodeStage() {
		t.Errorf("expected SupportsNodeStage to be false")
	}

	tmpDir := t.TempDir()
	targetPath := filepath.Join(tmpDir, "target")
	if err := os.MkdirAll(targetPath, 0750); err != nil {
		t.Fatalf("failed to create target path: %v", err)
	}

	// MountVolume should skip NodeStageVolume entirely
	if err := plugin.MountVolume(ctx, "test-vol", targetPath, nil, nil, ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_ONCE); err != nil {
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
	// Guard against enum drift: exactly 3 explicit access modes besides UNSPECIFIED.
	if len(ateapipb.VolumeAccessMode_name)-1 != 3 {
		t.Fatalf("VolumeAccessMode enum count changed: expected 3 non-unspecified modes, got %d", len(ateapipb.VolumeAccessMode_name)-1)
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
			wantErr:  false,
		},
		{
			name:     "ReadWriteOnce maps to SINGLE_NODE_WRITER",
			mode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_ONCE,
			wantMode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			wantErr:  false,
		},
		{
			name:     "ReadOnlyMany maps to MULTI_NODE_READER_ONLY",
			mode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY,
			wantMode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
			wantErr:  false,
		},
		{
			name:     "ReadWriteMany maps to MULTI_NODE_MULTI_WRITER",
			mode:     ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY,
			wantMode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			wantErr:  false,
		},
		{
			name:    "negative mode rejected",
			mode:    ateapipb.VolumeAccessMode(-1),
			wantErr: true,
		},
		{
			name:    "invalid out-of-range mode rejected",
			mode:    ateapipb.VolumeAccessMode(99),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cap, err := VolumeCapability(tt.mode)
			if (err != nil) != tt.wantErr {
				t.Fatalf("VolumeCapability(%v) error = %v, wantErr %v", tt.mode, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if cap == nil {
				t.Fatalf("VolumeCapability returned nil")
			}
			if cap.GetAccessMode().GetMode() != tt.wantMode {
				t.Errorf("VolumeCapability(%v) mode = %v, want %v", tt.mode, cap.GetAccessMode().GetMode(), tt.wantMode)
			}
			if cap.GetMount() == nil {
				t.Errorf("VolumeCapability(%v) mount is nil", tt.mode)
			}
		})
	}
}

func TestPlugin_AttachVolume_ReadWriteMany(t *testing.T) {
	var receivedReq *csi.ControllerPublishVolumeRequest
	driver := &mockCSIDriver{
		controllerPublishVolumeFunc: func(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
			receivedReq = req
			return &csi.ControllerPublishVolumeResponse{
				PublishContext: map[string]string{"shared_disk": "true"},
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

	pubCtx, err := plugin.AttachVolume(ctx, "shared-vol", "node-1", ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY)
	if err != nil {
		t.Fatalf("AttachVolume failed: %v", err)
	}
	if pubCtx["shared_disk"] != "true" {
		t.Errorf("expected publish context shared_disk=true, got %v", pubCtx)
	}
	if receivedReq.GetReadonly() {
		t.Errorf("expected Readonly=false for ReadWriteMany")
	}
	if receivedReq.GetVolumeCapability().GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
		t.Errorf("expected MULTI_NODE_MULTI_WRITER, got %v", receivedReq.GetVolumeCapability().GetAccessMode().GetMode())
	}
}

func TestPlugin_Capabilities_ReadOnlyValidation(t *testing.T) {
	tests := []struct {
		name                 string
		caps                 []*csi.ControllerServiceCapability
		mode                 ateapipb.VolumeAccessMode
		wantErr              bool
		wantSupportsReadonly bool
		wantPublishReadonly  bool
	}{
		{
			name: "publish readonly supported",
			caps: []*csi.ControllerServiceCapability{
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
			mode:                 ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY,
			wantErr:              false,
			wantSupportsReadonly: true,
			wantPublishReadonly:  true,
		},
		{
			name: "publish readonly not supported",
			caps: []*csi.ControllerServiceCapability{
				{
					Type: &csi.ControllerServiceCapability_Rpc{
						Rpc: &csi.ControllerServiceCapability_RPC{
							Type: csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
						},
					},
				},
			},
			mode:                 ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY,
			wantErr:              false,
			wantSupportsReadonly: false,
			wantPublishReadonly:  false,
		},
		{
			name: "invalid mode rejected",
			caps: []*csi.ControllerServiceCapability{
				{
					Type: &csi.ControllerServiceCapability_Rpc{
						Rpc: &csi.ControllerServiceCapability_RPC{
							Type: csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
						},
					},
				},
			},
			mode:                 ateapipb.VolumeAccessMode(99),
			wantErr:              true,
			wantSupportsReadonly: false,
			wantPublishReadonly:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var receivedPublishReq *csi.ControllerPublishVolumeRequest
			driver := &mockCSIDriver{
				controllerGetCapabilitiesFunc: func(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
					return &csi.ControllerGetCapabilitiesResponse{
						Capabilities: tt.caps,
					}, nil
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

			if err := plugin.InitControllerCapabilities(ctx); err != nil {
				t.Fatalf("InitControllerCapabilities failed: %v", err)
			}

			if got := plugin.SupportsPublishReadonly(); got != tt.wantSupportsReadonly {
				t.Errorf("SupportsPublishReadonly() = %v, want %v", got, tt.wantSupportsReadonly)
			}

			_, err = plugin.AttachVolume(ctx, "test-vol", "node-1", tt.mode)
			if (err != nil) != tt.wantErr {
				t.Errorf("AttachVolume() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && receivedPublishReq != nil {
				if receivedPublishReq.GetReadonly() != tt.wantPublishReadonly {
					t.Errorf("ControllerPublishVolumeRequest.Readonly = %v, want %v", receivedPublishReq.GetReadonly(), tt.wantPublishReadonly)
				}
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
				Volume: &csi.Volume{
					VolumeId: "new-vol-123",
				},
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

	// 1. Create with ReadWriteMany
	volID, _, err := plugin.CreateVolume(ctx, "vol-rwx", "10Gi", "mock.driver", nil, ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	if volID != "new-vol-123" {
		t.Errorf("volID = %q, want new-vol-123", volID)
	}
	if len(receivedReq.GetVolumeCapabilities()) != 1 {
		t.Fatalf("expected 1 capability, got %d", len(receivedReq.GetVolumeCapabilities()))
	}
	if receivedReq.GetVolumeCapabilities()[0].GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
		t.Errorf("expected MULTI_NODE_MULTI_WRITER, got %v", receivedReq.GetVolumeCapabilities()[0].GetAccessMode().GetMode())
	}

	// 2. Create with ReadOnlyMany
	volID, _, err = plugin.CreateVolume(ctx, "vol-ro", "5Gi", "mock.driver", nil, ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	if receivedReq.GetVolumeCapabilities()[0].GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		t.Errorf("expected MULTI_NODE_READER_ONLY, got %v", receivedReq.GetVolumeCapabilities()[0].GetAccessMode().GetMode())
	}

	// 3. Driver returns InvalidArgument for unsupported capability
	driver.createVolumeFunc = func(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
		return nil, status.Error(codes.InvalidArgument, "driver does not support MULTI_NODE_MULTI_WRITER")
	}
	_, _, err = plugin.CreateVolume(ctx, "vol-unsupported", "10Gi", "mock.driver", nil, ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected codes.InvalidArgument, got %v (code %v)", err, status.Code(err))
	}
}
