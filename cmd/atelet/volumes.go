// Copyright 2026 The Agent Substrate Authors
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

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/internal/volume/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func (s *Atelet) mountExternalVolumes(ctx context.Context, actorUID string, volumes []*ateletpb.Volume) error {
	for _, vol := range volumes {
		// Given that external volumes has been in the codebase for a while and
		// its preview-ness is solely around the API, and that the impact of
		// disabling the gate while in use would be an orphaned volume
		// attachment, we are NOT checking the ExternalVolumes gate in the
		// mount/unmount logic.
		ext := vol.GetExternal()
		if ext == nil {
			continue
		}
		hostPath := ateletpath.ExternalVolumeHostPath(actorUID, vol.GetName())
		if err := os.MkdirAll(hostPath, 0o750); err != nil {
			return fmt.Errorf("failed to create mount point %q: %w", hostPath, err)
		}
		slog.InfoContext(ctx, "Mounting volume", slog.String("volume_id", ext.GetStorageVolumeId()), slog.String("host_path", hostPath), slog.String("volume_type", ext.GetVolumeType()))
		plugin, err := s.getPlugin(ctx, ext.GetVolumeType())
		if err != nil {
			return fmt.Errorf("failed to get volume plugin for %q: %w", ext.GetVolumeType(), err)
		}
		if err := plugin.MountVolume(ctx, volume.MountVolumeRequest{
			VolumeID:       ext.GetStorageVolumeId(),
			TargetPath:     hostPath,
			VolumeContext:  ext.GetVolumeContext(),
			PublishContext: ext.GetPublishContext(),
		}); err != nil {
			return fmt.Errorf("failed to mount volume %q to %q: %w", ext.GetStorageVolumeId(), hostPath, err)
		}
	}
	return nil
}

func (s *Atelet) unmountExternalVolumes(ctx context.Context, actorUID string, volumes []*ateletpb.Volume) error {
	var errs []error
	for _, vol := range volumes {
		// Given that external volumes has been in the codebase for a while and
		// its preview-ness is solely around the API, and that the impact of
		// disabling the gate while in use would be an orphaned volume
		// attachment, we are NOT checking the ExternalVolumes gate in the
		// mount/unmount logic.
		ext := vol.GetExternal()
		if ext == nil {
			continue
		}
		hostPath := ateletpath.ExternalVolumeHostPath(actorUID, vol.GetName())
		slog.InfoContext(ctx, "Unmounting volume", slog.String("volume_id", ext.GetStorageVolumeId()), slog.String("host_path", hostPath), slog.String("volume_type", ext.GetVolumeType()))
		// TODO: Standardize volume plugin lookup and error handling across control plane
		// and worker plane (e.g. via a shared helper).
		plugin, err := s.getPlugin(ctx, ext.GetVolumeType())
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to get volume plugin for %q (volume %q): %w", ext.GetVolumeType(), ext.GetStorageVolumeId(), err))
			continue
		}
		if err := plugin.UnmountVolume(ctx, ext.GetStorageVolumeId(), hostPath); err != nil {
			if status.Code(err) == codes.NotFound || errors.Is(err, os.ErrNotExist) {
				slog.WarnContext(ctx, "Volume not found during unmount, assuming already unmounted", slog.String("volume_id", ext.GetStorageVolumeId()), slog.Any("error", err))
			} else {
				errs = append(errs, fmt.Errorf("failed to unmount volume %q from %q: %w", ext.GetStorageVolumeId(), hostPath, err))
			}
		}
	}
	return errors.Join(errs...)
}

type csiNodePluginSlot struct {
	gen                uint64
	inflight           int
	nodeSocketOverride string
	plugin             *csi.Plugin
}

func (s *Atelet) evictNodeSlotLocked(driverName string, cur csiNodePluginSlot) (*csi.Plugin, uint64) {
	evicted := cur.plugin
	cur.plugin, cur.nodeSocketOverride = nil, ""
	cur.gen++
	s.csiPlugins[driverName] = cur
	return evicted, cur.gen
}

func (s *Atelet) getPluginOnce(ctx context.Context, driverName string) (volume.VolumePluginWorkerPlane, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	if s.csiPlugins == nil {
		s.csiPlugins = make(map[string]csiNodePluginSlot)
	}
	slot := s.csiPlugins[driverName]
	slot.inflight++
	startGen := slot.gen
	s.csiPlugins[driverName] = slot
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		cur := s.csiPlugins[driverName]
		cur.inflight--
		if cur.plugin == nil && cur.inflight == 0 {
			delete(s.csiPlugins, driverName)
		} else {
			s.csiPlugins[driverName] = cur
		}
		s.mu.Unlock()
	}()

	cfg, err := s.csiDriverConfigGetter.Get(ctx, driverName)
	if err != nil {
		evictOnErr := apierrors.IsNotFound(err) || status.Code(err) == codes.NotFound
		retErr := fmt.Errorf("failed to retrieve CsiDriverConfig for %q: %w", driverName, err)
		if !evictOnErr {
			return nil, false, retErr
		}
		s.mu.Lock()
		cur := s.csiPlugins[driverName]
		if cur.gen != startGen {
			retry := cur.plugin != nil
			s.mu.Unlock()
			return nil, retry, retErr
		}
		evicted, _ := s.evictNodeSlotLocked(driverName, cur)
		s.mu.Unlock()
		_ = evicted.Close()
		return nil, false, retErr
	}
	if cfg == nil {
		return nil, false, fmt.Errorf("missing CsiDriverConfig for %q", driverName)
	}

	socketOverride := cfg.GetNodeSocketOverride()
	s.mu.Lock()
	cur := s.csiPlugins[driverName]
	if cur.plugin != nil && cur.nodeSocketOverride == socketOverride {
		s.mu.Unlock()
		return cur.plugin, false, nil
	}
	if cur.gen != startGen {
		s.mu.Unlock()
		return nil, true, nil
	}
	var preEvicted *csi.Plugin
	if cur.plugin != nil {
		preEvicted, startGen = s.evictNodeSlotLocked(driverName, cur)
	}
	s.mu.Unlock()
	_ = preEvicted.Close()

	csiPlugin, err := csi.NewCSIPluginFromConfig(ctx, cfg, driverName, false /*isController*/)
	if err != nil {
		return nil, false, err
	}

	s.mu.Lock()
	cur = s.csiPlugins[driverName]
	if cur.plugin != nil && cur.nodeSocketOverride == socketOverride {
		matched := cur.plugin
		s.mu.Unlock()
		_ = csiPlugin.Close()
		return matched, false, nil
	}
	if cur.gen != startGen {
		s.mu.Unlock()
		_ = csiPlugin.Close()
		return nil, true, nil
	}
	evicted := cur.plugin
	cur.plugin, cur.nodeSocketOverride = csiPlugin, socketOverride
	cur.gen++
	s.csiPlugins[driverName] = cur
	s.mu.Unlock()
	_ = evicted.Close()
	return csiPlugin, false, nil
}

func (s *Atelet) getPlugin(ctx context.Context, driverName string) (volume.VolumePluginWorkerPlane, error) {
	s.mu.RLock()
	plugin, ok := s.volumePlugins[driverName]
	s.mu.RUnlock()
	if ok {
		return plugin, nil
	}
	if s.csiDriverConfigGetter == nil {
		return nil, errors.New("missing csiDriverConfigGetter")
	}
	for {
		plugin, retry, err := s.getPluginOnce(ctx, driverName)
		if !retry {
			return plugin, err
		}
	}
}
