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
	"log/slog"
	"os"
	"path/filepath"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/volume"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Plugin implements volume.VolumePluginWorkerPlane using the CSI Client.
type Plugin struct {
	client           *Client
	stagingDirPrefix string
}

// Ensure Plugin implements volume.VolumePluginControlPlane and VolumePluginWorkerPlane
var _ volume.VolumePluginControlPlane = (*Plugin)(nil)
var _ volume.VolumePluginWorkerPlane = (*Plugin)(nil)

// NewPlugin creates a new Plugin adapter.
func NewPlugin(client *Client) *Plugin {
	return &Plugin{
		client:           client,
		stagingDirPrefix: ateompath.StagingDirPrefix(),
	}
}

// DriverName returns the driver name obtained from the CSI plugin.
func (p *Plugin) DriverName(ctx context.Context) (string, error) {
	resp, err := p.client.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if err != nil {
		return "", err
	}
	return resp.GetName(), nil
}

// CreateVolume maps to CSI Controller CreateVolume.
func (p *Plugin) CreateVolume(ctx context.Context, name string, capacity string, driverName string, parameters map[string]string) (string, map[string]string, error) {
	qty, err := resource.ParseQuantity(capacity)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse capacity %q: %w", capacity, err)
	}
	capBytes := qty.Value()

	req := &csi.CreateVolumeRequest{
		Name: name,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: capBytes,
		},
		VolumeCapabilities: getStandardCapabilities(),
		Parameters:         parameters,
	}

	resp, err := p.client.CreateVolume(ctx, req)
	if err != nil {
		return "", nil, fmt.Errorf("CSI CreateVolume failed: %w", err)
	}

	if resp.GetVolume() == nil {
		return "", nil, fmt.Errorf("CSI CreateVolume response returned nil volume")
	}

	return resp.GetVolume().GetVolumeId(), resp.GetVolume().GetVolumeContext(), nil
}

// DeleteVolume maps to CSI Controller DeleteVolume.
func (p *Plugin) DeleteVolume(ctx context.Context, volumeID string) error {
	req := &csi.DeleteVolumeRequest{
		VolumeId: volumeID,
	}

	_, err := p.client.DeleteVolume(ctx, req)
	if err != nil {
		return fmt.Errorf("CSI DeleteVolume failed: %w", err)
	}
	return nil
}

// AttachVolume maps to CSI Controller ControllerPublishVolume.
func (p *Plugin) AttachVolume(ctx context.Context, volumeID string, node string) error {
	req := &csi.ControllerPublishVolumeRequest{
		VolumeId:         volumeID,
		NodeId:           node,
		VolumeCapability: getStandardCapabilities()[0], // Use primary capability
		Readonly:         false,
	}

	resp, err := p.client.ControllerPublishVolume(ctx, req)
	if err != nil {
		// TODO: Query CSI driver capabilities ahead of time (e.g. during plugin initialization)
		// to avoid calling unimplemented methods and generating spammy logs.
		if status.Code(err) == codes.Unimplemented {
			slog.WarnContext(ctx, "CSI ControllerPublishVolume is unimplemented by driver; skipping attach", slog.String("volume_id", volumeID), slog.String("node", node))
			return nil
		}
		return fmt.Errorf("CSI ControllerPublishVolume failed: %w", err)
	}

	// NOTE: CSI ControllerPublishVolume returns PublishContext (metadata needed for mounting).
	// Currently, Substrate VolumePlugin interface does not support returning PublishContext.
	// We might need to store this context if the driver requires it (e.g. AWS EBS attachment info).
	// TODO: Extend Substrate's VolumePlugin interface to return and propagate
	// PublishContext if required by the driver for mounting.
	if resp != nil {
		_ = resp.GetPublishContext()
	}

	return nil
}

// DetachVolume maps to CSI Controller ControllerUnpublishVolume.
func (p *Plugin) DetachVolume(ctx context.Context, volumeID string, node string) error {
	req := &csi.ControllerUnpublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   node,
	}

	_, err := p.client.ControllerUnpublishVolume(ctx, req)
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			slog.WarnContext(ctx, "CSI ControllerUnpublishVolume is unimplemented by driver; skipping detach", slog.String("volume_id", volumeID), slog.String("node", node))
			return nil
		}
		return fmt.Errorf("CSI ControllerUnpublishVolume failed: %w", err)
	}
	return nil
}

// MountVolume maps to CSI Node NodePublishVolume.
// It also handles NodeStageVolume staging if required by the driver.
func (p *Plugin) MountVolume(ctx context.Context, volumeID string, targetPath string, volumeContext map[string]string) error {
	// 1. Stage the volume
	stagingPath := filepath.Join(p.stagingDirPrefix, volumeID)
	if err := os.MkdirAll(stagingPath, 0750); err != nil {
		return fmt.Errorf("failed to create staging directory %q: %w", stagingPath, err)
	}

	stageReq := &csi.NodeStageVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: stagingPath,
		VolumeCapability:  getStandardCapabilities()[0], // Use primary capability
		VolumeContext:     volumeContext,
	}

	_, err := p.client.NodeStageVolume(ctx, stageReq)
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			slog.WarnContext(ctx, "CSI NodeStageVolume is unimplemented by driver; skipping staging", slog.String("volume_id", volumeID))
			stagingPath = ""
		} else {
			return fmt.Errorf("CSI NodeStageVolume failed: %w", err)
		}
	}

	// 2. Publish (Mount) the volume
	req := &csi.NodePublishVolumeRequest{
		VolumeId:         volumeID,
		TargetPath:       targetPath,
		VolumeCapability: getStandardCapabilities()[0],
		Readonly:         false,
		VolumeContext:    volumeContext,
	}
	if stagingPath != "" {
		req.StagingTargetPath = stagingPath
	}

	_, err = p.client.NodePublishVolume(ctx, req)
	if err != nil {
		return fmt.Errorf("CSI NodePublishVolume failed: %w", err)
	}
	return nil
}

// UnmountVolume maps to CSI Node NodeUnpublishVolume.
// It also handles NodeUnstageVolume if staging was used.
func (p *Plugin) UnmountVolume(ctx context.Context, volumeID string, targetPath string) error {
	// 1. Unpublish (Unmount) the volume
	req := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetPath,
	}

	_, err := p.client.NodeUnpublishVolume(ctx, req)
	if err != nil {
		return fmt.Errorf("CSI NodeUnpublishVolume failed: %w", err)
	}

	// 2. Unstage the volume
	stagingPath := filepath.Join(p.stagingDirPrefix, volumeID)
	unstageReq := &csi.NodeUnstageVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: stagingPath,
	}

	_, err = p.client.NodeUnstageVolume(ctx, unstageReq)
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			slog.WarnContext(ctx, "CSI NodeUnstageVolume is unimplemented by driver; skipping unstaging", slog.String("volume_id", volumeID))
		} else {
			return fmt.Errorf("CSI NodeUnstageVolume failed: %w", err)
		}
	}

	// Clean up staging directory
	if err := os.Remove(stagingPath); err != nil && !os.IsNotExist(err) {
		slog.WarnContext(ctx, "failed to remove staging directory", slog.String("path", stagingPath), slog.Any("error", err))
	}

	return nil
}

// Helper to provide standard capabilities for general volume operations.
// TODO: Support and expose different volume access modes (e.g. ReadWriteMany, ReadOnlyMany)
// instead of hardcoding SingleNodeWriter.
func getStandardCapabilities() []*csi.VolumeCapability {
	return []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
		},
	}
}

// NewCSIPlugin establishes a CSI client and returns a verified Plugin instance.
func NewCSIPlugin(ctx context.Context, lister listersv1alpha1.CSIDriverConfigLister, driverName string, isController bool) (*Plugin, error) {
	if lister == nil {
		return nil, fmt.Errorf("missing csiDriverConfigLister")
	}

	cfg, err := lister.Get(driverName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve CSIDriverConfig for %q: %w", driverName, err)
	}

	var endpoint string
	switch {
	case isController:
		endpoint = cfg.Spec.ControllerEndpoint
	case cfg.Spec.NodeSocketOverride != "":
		endpoint = cfg.Spec.NodeSocketOverride
		slog.InfoContext(ctx, "Found CSIDriverConfig with NodeSocketOverride", slog.String("driver", driverName), slog.String("endpoint", endpoint))
	default:
		endpoint = "unix://" + ateompath.KubeletPluginSocketPath(driverName)
	}
	csiClient, err := NewCSIClient(endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize CSI client from endpoint %q: %w", endpoint, err)
	}
	csiPlugin := NewPlugin(csiClient)

	// Verify CSI plugin reported name matches requested name.
	reportedName, err := csiPlugin.DriverName(ctx)
	if err != nil {
		csiClient.Close()
		return nil, fmt.Errorf("failed to get driver name from plugin %q: %w", driverName, err)
	}
	if reportedName != driverName {
		csiClient.Close()
		return nil, fmt.Errorf("reported driver name %q does not match requested name %q", reportedName, driverName)
	}

	return csiPlugin, nil
}
