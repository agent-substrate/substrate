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

	client, err := NewCSIClient(endpoint)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	plugin := NewPlugin(client)

	ctx := context.Background()
	volID, err := plugin.CreateVolume(ctx, "test-vol", "1Gi", "standard")
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	if volID != "test-vol" {
		t.Errorf("expected volume ID %q, got %q", "test-vol", volID)
	}
}

func TestPlugin_DeleteVolume(t *testing.T) {
	driver := &mockCSIDriver{}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint)
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

	client, err := NewCSIClient(endpoint)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	plugin := NewPlugin(client)

	ctx := context.Background()
	err = plugin.AttachVolume(ctx, "test-vol", "node-1")
	if err != nil {
		t.Fatalf("AttachVolume failed: %v", err)
	}

	// Test Unimplemented warning bypass
	driver.controllerPublishVolumeFunc = func(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
		return nil, status.Error(codes.Unimplemented, "unimplemented")
	}
	err = plugin.AttachVolume(ctx, "test-vol", "node-1")
	if err != nil {
		t.Errorf("AttachVolume should have ignored Unimplemented error, got: %v", err)
	}
}

func TestPlugin_DetachVolume(t *testing.T) {
	driver := &mockCSIDriver{}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint)
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
	driver := &mockCSIDriver{}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint)
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
	err = plugin.MountVolume(ctx, "test-vol", targetPath)
	if err != nil {
		t.Fatalf("MountVolume failed: %v", err)
	}

	// Verify staging directory was created
	stagingPath := filepath.Join(plugin.stagingDirPrefix, "test-vol")
	if _, err := os.Stat(stagingPath); os.IsNotExist(err) {
		t.Errorf("staging directory %q was not created", stagingPath)
	}

	// Test NodeStageVolume Unimplemented bypass
	driver.nodeStageVolumeFunc = func(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
		return nil, status.Error(codes.Unimplemented, "unimplemented")
	}
	// Reset dir
	os.RemoveAll(tmpDir)
	os.MkdirAll(plugin.stagingDirPrefix, 0750)

	err = plugin.MountVolume(ctx, "test-vol-2", targetPath)
	if err != nil {
		t.Errorf("MountVolume should have succeeded when NodeStageVolume is unimplemented, got: %v", err)
	}
}

func TestPlugin_UnmountVolume(t *testing.T) {
	driver := &mockCSIDriver{}
	endpoint, cleanup := startMockCSIDriver(t, driver)
	defer cleanup()

	client, err := NewCSIClient(endpoint)
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

	client, err := NewCSIClient(endpoint)
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
