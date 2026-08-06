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
//
// The control-plane methods (Create/Delete/Attach/DetachVolume) are
// intentionally stateless no-ops: they log and succeed, with no bookkeeping
// of which volumes exist or which node they're attached to. Nothing reads
// that state - the e2e test that exercises this plugin verifies the volume
// actually worked by checking the real file MountVolume writes on the node,
// not by querying the plugin. A stateful mock used to track this in an
// in-process map, which broke once ate-api-server ran multiple replicas
// (whichever replica handled a given RPC had no idea what a different
// replica's map contained); going stateless removes the bug class instead
// of syncing the state, since nothing actually needs it.
type MockVolumePlugin struct{}

// NewMockVolumePlugin creates a new MockVolumePlugin.
func NewMockVolumePlugin() *MockVolumePlugin {
	return &MockVolumePlugin{}
}

// CreateVolume simulates volume provisioning. The returned volumeID is
// derived deterministically from name so repeated calls (retries) are
// idempotent by construction, with no state needed to enforce that.
func (p *MockVolumePlugin) CreateVolume(ctx context.Context, name string, capacity string, storageClass string) (string, error) {
	volumeID := "mock-vol-" + name
	slog.InfoContext(ctx, "MockVolumePlugin.CreateVolume", slog.String("name", name), slog.String("capacity", capacity), slog.String("storageClass", storageClass), slog.String("volumeID", volumeID))
	return volumeID, nil
}

// DeleteVolume simulates volume deletion.
func (p *MockVolumePlugin) DeleteVolume(ctx context.Context, volumeID string) error {
	slog.InfoContext(ctx, "MockVolumePlugin.DeleteVolume", slog.String("volumeID", volumeID))
	return nil
}

// AttachVolume simulates volume attachment to a node.
func (p *MockVolumePlugin) AttachVolume(ctx context.Context, volumeID string, node string) error {
	slog.InfoContext(ctx, "MockVolumePlugin.AttachVolume", slog.String("volumeID", volumeID), slog.String("node", node))
	return nil
}

// DetachVolume simulates volume detachment from a node.
func (p *MockVolumePlugin) DetachVolume(ctx context.Context, volumeID string, node string) error {
	slog.InfoContext(ctx, "MockVolumePlugin.DetachVolume", slog.String("volumeID", volumeID), slog.String("node", node))
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
