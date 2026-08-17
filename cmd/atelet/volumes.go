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

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/internal/volume/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *AteomHerder) mountExternalVolumes(ctx context.Context, actorUID string, volumes []*ateletpb.Volume) error {
	for _, vol := range volumes {
		if vol.GetType() != ateletpb.VolumeType_VOLUME_TYPE_EXTERNAL {
			continue
		}
		ext := vol.GetExternal()
		if ext == nil {
			continue
		}
		hostPath := ateompath.VolumeHostPath(actorUID, vol.GetName())
		if err := os.MkdirAll(hostPath, 0o750); err != nil {
			return fmt.Errorf("failed to create mount point %q: %w", hostPath, err)
		}
		slog.InfoContext(ctx, "Mounting volume", slog.String("volume_id", ext.GetStorageVolumeId()), slog.String("host_path", hostPath), slog.String("volume_type", ext.GetVolumeType()))
		plugin, err := s.getPlugin(ctx, ext.GetVolumeType())
		if err != nil {
			return fmt.Errorf("failed to get volume plugin for %q: %w", ext.GetVolumeType(), err)
		}
		if err := plugin.MountVolume(ctx, ext.GetStorageVolumeId(), hostPath, ext.GetVolumeContext()); err != nil {
			return fmt.Errorf("failed to mount volume %q to %q: %w", ext.GetStorageVolumeId(), hostPath, err)
		}
	}
	return nil
}

func (s *AteomHerder) unmountExternalVolumes(ctx context.Context, actorUID string, volumes []*ateletpb.Volume) error {
	var errs []error
	for _, vol := range volumes {
		if vol.GetType() != ateletpb.VolumeType_VOLUME_TYPE_EXTERNAL {
			continue
		}
		ext := vol.GetExternal()
		if ext == nil {
			continue
		}
		hostPath := ateompath.VolumeHostPath(actorUID, vol.GetName())
		slog.InfoContext(ctx, "Unmounting volume", slog.String("volume_id", ext.GetStorageVolumeId()), slog.String("host_path", hostPath), slog.String("volume_type", ext.GetVolumeType()))
		// TODO: Standardize volume plugin lookup and error handling across control plane
		// and worker plane (e.g. via a shared helper).
		plugin, err := s.getPlugin(ctx, ext.GetVolumeType())
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to get volume plugin for %q (volume %q): %w", ext.GetVolumeType(), ext.GetStorageVolumeId(), err))
			continue
		}
		if err := plugin.UnmountVolume(ctx, ext.GetStorageVolumeId(), hostPath); err != nil {
			if status.Code(err) == codes.NotFound {
				slog.WarnContext(ctx, "Volume not found during unmount, assuming already unmounted", slog.String("volume_id", ext.GetStorageVolumeId()), slog.Any("error", err))
			} else {
				errs = append(errs, fmt.Errorf("failed to unmount volume %q from %q: %w", ext.GetStorageVolumeId(), hostPath, err))
			}
		}
	}
	return errors.Join(errs...)
}

func (s *AteomHerder) getPlugin(ctx context.Context, driverName string) (volume.VolumePluginWorkerPlane, error) {
	s.mu.RLock()
	plugin, ok := s.volumePlugins[driverName]
	s.mu.RUnlock()
	if ok {
		return plugin, nil
	}

	csiPlugin, err := csi.NewCSIPlugin(ctx, s.csiDriverConfigLister, driverName, false /*isController*/)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.volumePlugins[driverName] = csiPlugin
	s.mu.Unlock()
	return csiPlugin, nil
}
