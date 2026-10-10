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
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/volume"
	v1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	// DefaultClientCertPath is the path to the pod identity credential bundle.
	DefaultClientCertPath = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"
	// DefaultCACertPath is the path to the servicedns trust bundle.
	DefaultCACertPath = "/run/servicedns.podcert.ate.dev/trust-bundle.pem"
)

type tlsPaths struct {
	clientCert string
	caCert     string
}

var defaultTLSPaths = tlsPaths{
	clientCert: DefaultClientCertPath,
	caCert:     DefaultCACertPath,
}

// Plugin implements volume.VolumePluginWorkerPlane using the CSI Client.
type Plugin struct {
	client           *Client
	stagingDirPrefix string

	// capsMu guards controllerCaps and nodeCaps. The maps are built by
	// InitControllerCapabilities and InitNodeCapabilities, and an RPC is
	// disabled once the driver returns Unimplemented for it.
	capsMu         sync.RWMutex
	controllerCaps map[csi.ControllerServiceCapability_RPC_Type]bool
	nodeCaps       map[csi.NodeServiceCapability_RPC_Type]bool
}

// Ensure Plugin implements volume.VolumePluginControlPlane and VolumePluginWorkerPlane
var _ volume.VolumePluginControlPlane = (*Plugin)(nil)
var _ volume.VolumePluginWorkerPlane = (*Plugin)(nil)

// NewPlugin creates a new Plugin adapter. Call InitControllerCapabilities or
// InitNodeCapabilities before use; until then, no optional RPC is reported as supported.
func NewPlugin(client *Client) *Plugin {
	return &Plugin{
		client:           client,
		stagingDirPrefix: filepath.Join(nodepath.BasePath, "staging"),
	}
}

// SupportsControllerCapability reports whether the driver supports the given controller RPC.
func (p *Plugin) SupportsControllerCapability(rpc csi.ControllerServiceCapability_RPC_Type) bool {
	p.capsMu.RLock()
	defer p.capsMu.RUnlock()
	return p.controllerCaps[rpc]
}

// SupportsNodeCapability reports whether the driver supports the given node RPC.
func (p *Plugin) SupportsNodeCapability(rpc csi.NodeServiceCapability_RPC_Type) bool {
	p.capsMu.RLock()
	defer p.capsMu.RUnlock()
	return p.nodeCaps[rpc]
}

// disableControllerCap records that the driver does not implement the given controller RPC.
func (p *Plugin) disableControllerCap(rpc csi.ControllerServiceCapability_RPC_Type) {
	p.capsMu.Lock()
	defer p.capsMu.Unlock()
	if p.controllerCaps == nil {
		p.controllerCaps = make(map[csi.ControllerServiceCapability_RPC_Type]bool)
	}
	p.controllerCaps[rpc] = false
}

// disableNodeCap records that the driver does not implement the given node RPC.
func (p *Plugin) disableNodeCap(rpc csi.NodeServiceCapability_RPC_Type) {
	p.capsMu.Lock()
	defer p.capsMu.Unlock()
	if p.nodeCaps == nil {
		p.nodeCaps = make(map[csi.NodeServiceCapability_RPC_Type]bool)
	}
	p.nodeCaps[rpc] = false
}

// InitControllerCapabilities queries and caches ControllerGetCapabilities from the driver.
// If the query fails or reports no capabilities, every controller RPC is assumed supported.
func (p *Plugin) InitControllerCapabilities(ctx context.Context) {
	caps := make(map[csi.ControllerServiceCapability_RPC_Type]bool)
	resp, err := p.client.ControllerGetCapabilities(ctx, &csi.ControllerGetCapabilitiesRequest{})
	for _, c := range resp.GetCapabilities() {
		if rpc := c.GetRpc(); rpc != nil {
			caps[rpc.GetType()] = true
		}
	}
	if err != nil || len(caps) == 0 {
		slog.WarnContext(ctx, "CSI ControllerGetCapabilities failed or reported no capabilities; assuming all controller RPCs are supported", slog.Any("error", err))
		for v := range csi.ControllerServiceCapability_RPC_Type_name {
			if rpc := csi.ControllerServiceCapability_RPC_Type(v); rpc != csi.ControllerServiceCapability_RPC_UNKNOWN {
				caps[rpc] = true
			}
		}
	}
	p.capsMu.Lock()
	defer p.capsMu.Unlock()
	p.controllerCaps = caps
}

// InitNodeCapabilities queries and caches NodeGetCapabilities from the driver.
// If the query fails or reports no capabilities, every node RPC is assumed supported.
func (p *Plugin) InitNodeCapabilities(ctx context.Context) {
	caps := make(map[csi.NodeServiceCapability_RPC_Type]bool)
	resp, err := p.client.NodeGetCapabilities(ctx, &csi.NodeGetCapabilitiesRequest{})
	for _, c := range resp.GetCapabilities() {
		if rpc := c.GetRpc(); rpc != nil {
			caps[rpc.GetType()] = true
		}
	}
	if err != nil || len(caps) == 0 {
		slog.WarnContext(ctx, "CSI NodeGetCapabilities failed or reported no capabilities; assuming all node RPCs are supported", slog.Any("error", err))
		for v := range csi.NodeServiceCapability_RPC_Type_name {
			if rpc := csi.NodeServiceCapability_RPC_Type(v); rpc != csi.NodeServiceCapability_RPC_UNKNOWN {
				caps[rpc] = true
			}
		}
	}
	p.capsMu.Lock()
	defer p.capsMu.Unlock()
	p.nodeCaps = caps
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
func (p *Plugin) CreateVolume(ctx context.Context, req volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	qty, err := resource.ParseQuantity(req.Capacity)
	if err != nil {
		return volume.CreateVolumeResponse{}, fmt.Errorf("failed to parse capacity %q: %w", req.Capacity, err)
	}
	capBytes := qty.Value()

	csiReq := &csi.CreateVolumeRequest{
		Name: req.Name,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: capBytes,
		},
		VolumeCapabilities: getStandardCapabilities(),
		Parameters:         req.Parameters,
	}

	resp, err := p.client.CreateVolume(ctx, csiReq)
	if err != nil {
		return volume.CreateVolumeResponse{}, fmt.Errorf("CSI CreateVolume failed: %w", err)
	}

	if resp.GetVolume() == nil {
		return volume.CreateVolumeResponse{}, fmt.Errorf("CSI CreateVolume response returned nil volume")
	}

	return volume.CreateVolumeResponse{
		VolumeID:      resp.GetVolume().GetVolumeId(),
		VolumeContext: resp.GetVolume().GetVolumeContext(),
	}, nil
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
func (p *Plugin) AttachVolume(ctx context.Context, req volume.AttachVolumeRequest) (volume.AttachVolumeResponse, error) {
	if !p.SupportsControllerCapability(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME) {
		slog.DebugContext(ctx, "Driver does not support ControllerPublishVolume; skipping attach", slog.String("volume_id", req.VolumeID), slog.String("node", req.Node))
		return volume.AttachVolumeResponse{}, nil
	}

	csiReq := &csi.ControllerPublishVolumeRequest{
		VolumeId:         req.VolumeID,
		NodeId:           req.Node,
		VolumeCapability: getStandardCapabilities()[0], // Use primary capability
		Readonly:         false,
	}

	resp, err := p.client.ControllerPublishVolume(ctx, csiReq)
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			slog.WarnContext(ctx, "CSI ControllerPublishVolume is unimplemented by driver; skipping attach", slog.String("volume_id", req.VolumeID), slog.String("node", req.Node))
			p.disableControllerCap(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME)
			return volume.AttachVolumeResponse{}, nil
		}
		return volume.AttachVolumeResponse{}, fmt.Errorf("CSI ControllerPublishVolume failed: %w", err)
	}

	return volume.AttachVolumeResponse{PublishContext: resp.GetPublishContext()}, nil
}

// DetachVolume maps to CSI Controller ControllerUnpublishVolume.
func (p *Plugin) DetachVolume(ctx context.Context, volumeID string, node string) error {
	if !p.SupportsControllerCapability(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME) {
		slog.DebugContext(ctx, "Driver does not support ControllerPublishVolume; skipping detach", slog.String("volume_id", volumeID), slog.String("node", node))
		return nil
	}

	req := &csi.ControllerUnpublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   node,
	}

	_, err := p.client.ControllerUnpublishVolume(ctx, req)
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			slog.WarnContext(ctx, "CSI ControllerUnpublishVolume is unimplemented by driver; skipping detach", slog.String("volume_id", volumeID), slog.String("node", node))
			p.disableControllerCap(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME)
			return nil
		}
		return fmt.Errorf("CSI ControllerUnpublishVolume failed: %w", err)
	}
	return nil
}

// MountVolume maps to CSI Node NodePublishVolume.
// It also handles NodeStageVolume staging if required by the driver.
func (p *Plugin) MountVolume(ctx context.Context, req volume.MountVolumeRequest) error {
	stagingPath, err := p.stageVolume(ctx, req)
	if err != nil {
		return err
	}

	csiReq := &csi.NodePublishVolumeRequest{
		VolumeId:          req.VolumeID,
		StagingTargetPath: stagingPath,
		TargetPath:        req.TargetPath,
		VolumeCapability:  getStandardCapabilities()[0],
		Readonly:          false,
		VolumeContext:     req.VolumeContext,
		PublishContext:    req.PublishContext,
	}

	if _, err := p.client.NodePublishVolume(ctx, csiReq); err != nil {
		return fmt.Errorf("CSI NodePublishVolume failed: %w", err)
	}
	return nil
}

// stageVolume calls NodeStageVolume if the driver supports it. It returns the staging
// path, or an empty string if the volume was not staged.
func (p *Plugin) stageVolume(ctx context.Context, req volume.MountVolumeRequest) (string, error) {
	if !p.SupportsNodeCapability(csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME) {
		return "", nil
	}

	stagingPath := filepath.Join(p.stagingDirPrefix, req.VolumeID)
	if err := os.MkdirAll(stagingPath, 0750); err != nil {
		return "", fmt.Errorf("failed to create staging directory %q: %w", stagingPath, err)
	}

	csiReq := &csi.NodeStageVolumeRequest{
		VolumeId:          req.VolumeID,
		StagingTargetPath: stagingPath,
		VolumeCapability:  getStandardCapabilities()[0], // Use primary capability
		VolumeContext:     req.VolumeContext,
		PublishContext:    req.PublishContext,
	}

	_, err := p.client.NodeStageVolume(ctx, csiReq)
	if status.Code(err) == codes.Unimplemented {
		slog.WarnContext(ctx, "CSI NodeStageVolume is unimplemented by driver; skipping staging", slog.String("volume_id", req.VolumeID))
		p.disableNodeCap(csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME)
		removeStagingDir(ctx, stagingPath)
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("CSI NodeStageVolume failed: %w", err)
	}
	return stagingPath, nil
}

// UnmountVolume maps to CSI Node NodeUnpublishVolume.
// It also handles NodeUnstageVolume if staging was used.
func (p *Plugin) UnmountVolume(ctx context.Context, volumeID string, targetPath string) error {
	req := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetPath,
	}

	if _, err := p.client.NodeUnpublishVolume(ctx, req); err != nil {
		return fmt.Errorf("CSI NodeUnpublishVolume failed: %w", err)
	}

	return p.unstageVolume(ctx, volumeID)
}

// unstageVolume calls NodeUnstageVolume and removes the staging directory if the
// driver supports staging.
func (p *Plugin) unstageVolume(ctx context.Context, volumeID string) error {
	if !p.SupportsNodeCapability(csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME) {
		return nil
	}

	stagingPath := filepath.Join(p.stagingDirPrefix, volumeID)
	req := &csi.NodeUnstageVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: stagingPath,
	}

	_, err := p.client.NodeUnstageVolume(ctx, req)
	if status.Code(err) == codes.Unimplemented {
		slog.WarnContext(ctx, "CSI NodeUnstageVolume is unimplemented by driver; skipping unstaging", slog.String("volume_id", volumeID))
		p.disableNodeCap(csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME)
	} else if err != nil {
		return fmt.Errorf("CSI NodeUnstageVolume failed: %w", err)
	}

	removeStagingDir(ctx, stagingPath)
	return nil
}

// removeStagingDir removes an empty staging directory, logging any failure.
func removeStagingDir(ctx context.Context, stagingPath string) {
	if err := os.Remove(stagingPath); err != nil && !os.IsNotExist(err) {
		slog.WarnContext(ctx, "failed to remove staging directory", slog.String("path", stagingPath), slog.Any("error", err))
	}
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

// CSIDriverConfigGetter provides access to retrieve a CSIDriverConfig by name.
// Both listersv1alpha1.CSIDriverConfigLister and direct client getters implement this interface.
type CSIDriverConfigGetter interface {
	Get(name string) (*v1alpha1.CSIDriverConfig, error)
}

// NewCSIPlugin establishes a CSI client and returns a verified Plugin instance.
func NewCSIPlugin(ctx context.Context, getter CSIDriverConfigGetter, driverName string, isController bool) (*Plugin, error) {
	return newCSIPlugin(ctx, getter, driverName, isController, defaultTLSPaths)
}

func newCSIPlugin(ctx context.Context, getter CSIDriverConfigGetter, driverName string, isController bool, paths tlsPaths) (*Plugin, error) {
	if getter == nil {
		return nil, fmt.Errorf("missing csiDriverConfigGetter")
	}

	cfg, err := getter.Get(driverName)
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
		endpoint = "unix://" + kubeletPluginSocketPath(driverName)
	}

	var tlsCfg *tls.Config
	if isController {
		var err error
		tlsCfg, err = resolveTLSConfig(cfg, paths)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve TLS config for %q: %w", driverName, err)
		}
	}

	csiClient, err := NewCSIClient(endpoint, tlsCfg)
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

	// Capability discovery is best-effort; if it fails or reports nothing, every RPC is assumed supported.
	if isController {
		csiPlugin.InitControllerCapabilities(ctx)
	} else {
		csiPlugin.InitNodeCapabilities(ctx)
	}

	return csiPlugin, nil
}

func resolveTLSConfig(cfg *v1alpha1.CSIDriverConfig, paths tlsPaths) (*tls.Config, error) {
	if cfg == nil || cfg.Spec.TLS == nil || !cfg.Spec.TLS.Enabled {
		return nil, nil
	}

	tlsCfg := cfg.Spec.TLS

	if !tlsCfg.UsePodIdentity {
		// TODO: Support manual certificates loaded from Secrets specified in the config.
		return nil, fmt.Errorf("only pod identity TLS is supported in this configuration")
	}

	// Pod Identity certificates are projected into the pod as files by the
	// kubelet (via Substrate's podcertcontroller), which handles rotation on
	// disk; PrepareClientTLSConfig reloads both the client certificate and the
	// CA trust bundle from disk per handshake, so rotation is picked up
	// without restarting the process.
	preparedCfg, err := credbundle.PrepareClientTLSConfig(credbundle.ClientConfig{
		GetClientCertificate: credbundle.ClientLoader(paths.clientCert),
		TrustBundlePath:      paths.caCert,
		ServerName:           tlsCfg.ServerName,
		// NextProtos configures ALPN h2 for gRPC over TLS.
		NextProtos: []string{"h2"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to load CA cert pool from %q: %w", paths.caCert, err)
	}
	return preparedCfg, nil
}

// kubeletPluginSocketPath is the CSI driver socket in the kubelet plugins directory.
func kubeletPluginSocketPath(driverName string) string {
	return filepath.Join("/var/lib/kubelet/plugins", driverName, "csi.sock")
}
