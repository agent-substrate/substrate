// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package volume

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

// Use a directory that is shared between atelet and ateom but not cleaned up by atelet
var mockVolumeDirectories string = filepath.Join(ateompath.BasePath, "mockvolumes")

var (
	_ VolumePluginControlPlane = (*MockVolumePlugin)(nil)
	_ VolumePluginWorkerPlane  = (*MockVolumePlugin)(nil)
)

// MockVolumePlugin is a simple implementation of VolumePluginControlPlane and VolumePluginWorkerPlane for testing purposes.
//
// It creates a subdirectory on the host for each actor. This only persists data if the actor
// is scheduled to the same host.
//
// This plugin also does not cleanup the subdirectories, so that has to be done by the test infrastructure.
type MockVolumePlugin struct {
	mu      sync.Mutex
	volumes map[string]*MockVolumeState
	counter int
}

// MockVolumeState tracks the state of a mock volume.
type MockVolumeState struct {
	ID           string
	Name         string
	Capacity     string
	StorageClass string
	Node         string
	Mounts       map[string]bool // targetPath -> mounted
}

// NewMockVolumePlugin creates a new MockVolumePlugin.
func NewMockVolumePlugin() *MockVolumePlugin {
	return &MockVolumePlugin{
		volumes: make(map[string]*MockVolumeState),
	}
}

// CreateVolume simulates volume provisioning.
func (p *MockVolumePlugin) CreateVolume(ctx context.Context, name string, capacity string, storageClass string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counter++
	volumeID := fmt.Sprintf("mock-vol-%d", p.counter)
	slog.InfoContext(ctx, "MockVolumePlugin.CreateVolume", slog.String("name", name), slog.String("capacity", capacity), slog.String("storageClass", storageClass), slog.String("volumeID", volumeID))
	p.volumes[volumeID] = &MockVolumeState{
		ID:           volumeID,
		Name:         name,
		Capacity:     capacity,
		StorageClass: storageClass,
		Mounts:       make(map[string]bool),
	}
	return volumeID, nil
}

// DeleteVolume simulates volume deletion.
func (p *MockVolumePlugin) DeleteVolume(ctx context.Context, volumeID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	slog.InfoContext(ctx, "MockVolumePlugin.DeleteVolume", slog.String("volumeID", volumeID))
	if _, ok := p.volumes[volumeID]; !ok {
		slog.ErrorContext(ctx, "MockVolumePlugin.DeleteVolume failed: volume not found", slog.String("volumeID", volumeID))
		return fmt.Errorf("volume %s not found", volumeID)
	}
	delete(p.volumes, volumeID)
	return nil
}

// AttachVolume simulates volume attachment to a node.
func (p *MockVolumePlugin) AttachVolume(ctx context.Context, volumeID string, node string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	slog.InfoContext(ctx, "MockVolumePlugin.AttachVolume", slog.String("volumeID", volumeID), slog.String("node", node))
	vol, ok := p.volumes[volumeID]
	if !ok {
		slog.ErrorContext(ctx, "MockVolumePlugin.AttachVolume failed: volume not found", slog.String("volumeID", volumeID))
		return fmt.Errorf("volume %s not found", volumeID)
	}
	vol.Node = node
	return nil
}

// DetachVolume simulates volume detachment from a node.
func (p *MockVolumePlugin) DetachVolume(ctx context.Context, volumeID string, node string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	slog.InfoContext(ctx, "MockVolumePlugin.DetachVolume", slog.String("volumeID", volumeID), slog.String("node", node))
	vol, ok := p.volumes[volumeID]
	if !ok {
		slog.ErrorContext(ctx, "MockVolumePlugin.DetachVolume failed: volume not found", slog.String("volumeID", volumeID))
		return fmt.Errorf("volume %s not found", volumeID)
	}
	if vol.Node != node {
		slog.ErrorContext(ctx, "MockVolumePlugin.DetachVolume failed: volume not attached to node", slog.String("volumeID", volumeID), slog.String("node", node), slog.String("attachedNode", vol.Node))
		return fmt.Errorf("volume %s not attached to node %s", volumeID, node)
	}
	vol.Node = ""
	return nil
}

// MountVolume simulates mounting volume on the host.
func (p *MockVolumePlugin) MountVolume(ctx context.Context, volumeID string, targetPath string) error {
	slog.InfoContext(ctx, "MockVolumePlugin.MountVolume", slog.String("volumeID", volumeID), slog.String("targetPath", targetPath))

	volumeDir := filepath.Join(mockVolumeDirectories, volumeID)
	if err := os.MkdirAll(volumeDir, 0755); err != nil {
		slog.ErrorContext(ctx, "MockVolumePlugin.MountVolume failed: mkdir error", slog.String("volumeID", volumeID), slog.Any("error", err))
		return fmt.Errorf("failed to create mock volume directory %q: %w", volumeDir, err)
	}

	testFilePath := filepath.Join(volumeDir, "test.txt")
	if err := os.WriteFile(testFilePath, []byte("test content\n"), 0644); err != nil {
		slog.ErrorContext(ctx, "MockVolumePlugin.MountVolume failed: create test file error", slog.String("volumeID", volumeID), slog.Any("error", err))
		return fmt.Errorf("failed to create test file in %q: %w", volumeDir, err)
	}

	// Use symlink instead of bind mount to avoid atelet requiring bidirectional mount propagation.
	_ = os.Remove(targetPath)
	if err := os.Symlink(volumeDir, targetPath); err != nil {
		return fmt.Errorf("failed to symlink %q to %q: %w", volumeDir, targetPath, err)
	}
	return nil
}

// UnmountVolume simulates unmounting volume from the host.
func (p *MockVolumePlugin) UnmountVolume(ctx context.Context, volumeID string, targetPath string) error {
	slog.InfoContext(ctx, "MockVolumePlugin.UnmountVolume", slog.String("volumeID", volumeID), slog.String("targetPath", targetPath))

	if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
		slog.ErrorContext(ctx, "MockVolumePlugin.UnmountVolume failed: remove error", slog.String("volumeID", volumeID), slog.String("targetPath", targetPath), slog.Any("error", err))
		return fmt.Errorf("failed to remove target path %q: %w", targetPath, err)
	}
	return nil
}

// GetVolumeState returns the state of a mock volume for verification in tests.
// This only works for controller methods.
func (p *MockVolumePlugin) GetVolumeState(volumeID string) (*MockVolumeState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	slog.Info("MockVolumePlugin.GetVolumeState", slog.String("volumeID", volumeID))
	vol, ok := p.volumes[volumeID]
	if !ok {
		slog.Error("MockVolumePlugin.GetVolumeState failed: volume not found", slog.String("volumeID", volumeID))
		return nil, fmt.Errorf("volume %s not found", volumeID)
	}
	// Return a copy to avoid concurrent access issues in tests
	mountsCopy := make(map[string]bool)
	for k, v := range vol.Mounts {
		mountsCopy[k] = v
	}
	return &MockVolumeState{
		ID:           vol.ID,
		Name:         vol.Name,
		Capacity:     vol.Capacity,
		StorageClass: vol.StorageClass,
		Node:         vol.Node,
		Mounts:       mountsCopy,
	}, nil
}
