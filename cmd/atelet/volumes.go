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
	"maps"
	"os"
	"sync"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/internal/volume/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const defaultCSIPluginConfigTTL = 30 * time.Second

func wrapPluginError(err error, format string, args ...any) error {
	if apierror.Code(err) == codes.Unavailable {
		return apierror.Unavailable(format, args...)
	}
	return fmt.Errorf(format, args...)
}

func (s *Atelet) mountExternalVolumes(ctx context.Context, actorUID string, volumes []*ateletpb.Volume) error {
	plugins := make(map[string]volume.VolumePluginWorkerPlane, len(volumes))
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for _, vol := range volumes {
		ext := vol.GetExternal()
		if ext == nil {
			continue
		}
		driverName := ext.GetVolumeType()
		if _, ok := plugins[driverName]; ok {
			continue
		}
		plugin, release, err := s.getPlugin(ctx, driverName)
		if err != nil {
			return wrapPluginError(err, "failed to get volume plugin for %q: %w", driverName, err)
		}
		plugins[driverName] = plugin
		releases = append(releases, release)
	}
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
		if err := plugins[ext.GetVolumeType()].MountVolume(ctx, volume.MountVolumeRequest{
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
		plugin, release, err := s.getPlugin(ctx, ext.GetVolumeType())
		if err != nil {
			errs = append(errs, wrapPluginError(err, "failed to get volume plugin for %q (volume %q): %w", ext.GetVolumeType(), ext.GetStorageVolumeId(), err))
			continue
		}
		unmountErr := plugin.UnmountVolume(ctx, ext.GetStorageVolumeId(), hostPath)
		release()
		if unmountErr != nil {
			if status.Code(unmountErr) == codes.NotFound || errors.Is(unmountErr, os.ErrNotExist) {
				slog.WarnContext(ctx, "Volume not found during unmount, assuming already unmounted", slog.String("volume_id", ext.GetStorageVolumeId()), slog.Any("error", unmountErr))
			} else {
				errs = append(errs, fmt.Errorf("failed to unmount volume %q from %q: %w", ext.GetStorageVolumeId(), hostPath, unmountErr))
			}
		}
	}
	if len(errs) == 0 {
		return nil
	}
	var hasUnavailable, hasFatal bool
	for _, err := range errs {
		switch apierror.Code(err) {
		case codes.Unavailable:
			hasUnavailable = true
		case codes.Canceled, codes.DeadlineExceeded:
		default:
			hasFatal = true
		}
	}
	if hasFatal {
		return apierror.Internal("%w", errors.Join(errs...))
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("%v: %w", errors.Join(errs...), ctxErr)
	}
	if !hasUnavailable {
		return errors.Join(errs...)
	}
	return apierror.Unavailable("%w", errors.Join(errs...))
}

type csiNodePluginEntry struct {
	mu      sync.Mutex
	refs    int
	retired bool
	plugin  *csi.Plugin
}

func newCSINodePluginEntry(p *csi.Plugin) *csiNodePluginEntry {
	return &csiNodePluginEntry{refs: 1, plugin: p}
}

func (e *csiNodePluginEntry) acquire() func() {
	e.mu.Lock()
	e.refs++
	e.mu.Unlock()
	return sync.OnceFunc(e.release)
}

func (e *csiNodePluginEntry) release() {
	e.mu.Lock()
	e.refs--
	shouldClose := e.retired && e.refs == 0
	p := e.plugin
	e.mu.Unlock()
	if shouldClose {
		_ = p.Close()
	}
}

func (e *csiNodePluginEntry) retire() {
	if e == nil {
		return
	}
	e.mu.Lock()
	if e.retired {
		e.mu.Unlock()
		return
	}
	e.retired = true
	e.refs--
	shouldClose := e.refs == 0
	p := e.plugin
	e.mu.Unlock()
	if shouldClose {
		_ = p.Close()
	}
}

func noopNodeRelease() {}

type csiNodePluginSlot struct {
	gen                uint64
	inflight           int
	checkedAt          time.Time
	nodeSocketOverride string
	entry              *csiNodePluginEntry
}

func (s *Atelet) closeCSIPlugins() {
	s.mu.Lock()
	slots := maps.Clone(s.csiPlugins)
	clear(s.csiPlugins)
	s.mu.Unlock()
	for _, slot := range slots {
		slot.entry.retire()
	}
}

func (s *Atelet) now() time.Time {
	if s.clock != nil {
		return s.clock.Now()
	}
	return time.Now()
}

func (s *Atelet) evictNodeSlotLocked(driverName string, cur csiNodePluginSlot) (*csiNodePluginEntry, uint64) {
	evicted := cur.entry
	cur.entry, cur.nodeSocketOverride, cur.checkedAt = nil, "", time.Time{}
	cur.gen++
	s.csiPlugins[driverName] = cur
	return evicted, cur.gen
}

func isTransientConfigLookupError(err error) bool {
	switch apierror.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	return apierrors.IsServiceUnavailable(err) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err)
}

func (s *Atelet) getPluginOnce(ctx context.Context, driverName string) (volume.VolumePluginWorkerPlane, func(), bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, false, err
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
		if cur.entry == nil && cur.inflight == 0 {
			delete(s.csiPlugins, driverName)
		} else {
			s.csiPlugins[driverName] = cur
		}
		s.mu.Unlock()
	}()

	cfg, err := s.csiDriverConfigGetter.Get(ctx, driverName)
	if err != nil {
		evictOnErr := apierrors.IsNotFound(err) || status.Code(err) == codes.NotFound || apierror.Code(err) == codes.NotFound
		retErr := wrapPluginError(err, "failed to retrieve CsiDriverConfig for %q: %w", driverName, err)
		if !evictOnErr {
			if ctx.Err() == nil && isTransientConfigLookupError(err) {
				s.mu.Lock()
				cur := s.csiPlugins[driverName]
				if cur.entry != nil {
					release := cur.entry.acquire()
					plugin := cur.entry.plugin
					s.mu.Unlock()
					slog.WarnContext(ctx, "Failed to revalidate CsiDriverConfig; continuing with cached CSI node plugin",
						slog.String("driver", driverName), slog.Any("error", err))
					return plugin, release, false, nil
				}
				s.mu.Unlock()
			}
			return nil, nil, false, retErr
		}
		s.mu.Lock()
		cur := s.csiPlugins[driverName]
		if cur.gen != startGen {
			retry := cur.entry != nil
			s.mu.Unlock()
			return nil, nil, retry, retErr
		}
		evicted, _ := s.evictNodeSlotLocked(driverName, cur)
		s.mu.Unlock()
		evicted.retire()
		return nil, nil, false, retErr
	}
	if cfg == nil {
		return nil, nil, false, fmt.Errorf("missing CsiDriverConfig for %q", driverName)
	}

	socketOverride := cfg.GetNodeSocketOverride()
	s.mu.Lock()
	cur := s.csiPlugins[driverName]
	if cur.entry != nil && cur.nodeSocketOverride == socketOverride {
		cur.checkedAt = s.now()
		release := cur.entry.acquire()
		plugin := cur.entry.plugin
		s.csiPlugins[driverName] = cur
		s.mu.Unlock()
		return plugin, release, false, nil
	}
	if cur.gen != startGen {
		s.mu.Unlock()
		return nil, nil, true, nil
	}
	s.mu.Unlock()

	csiPlugin, err := csi.NewCSIPluginFromConfig(ctx, cfg, driverName, false /*isController*/)
	if err != nil {
		return nil, nil, false, err
	}

	s.mu.Lock()
	cur = s.csiPlugins[driverName]
	if cur.entry != nil && cur.nodeSocketOverride == socketOverride {
		release := cur.entry.acquire()
		matched := cur.entry.plugin
		s.mu.Unlock()
		_ = csiPlugin.Close()
		return matched, release, false, nil
	}
	if cur.gen != startGen {
		s.mu.Unlock()
		_ = csiPlugin.Close()
		return nil, nil, true, nil
	}
	prev := cur.entry
	next := newCSINodePluginEntry(csiPlugin)
	release := next.acquire()
	cur.entry, cur.nodeSocketOverride, cur.checkedAt = next, socketOverride, s.now()
	cur.gen++
	s.csiPlugins[driverName] = cur
	s.mu.Unlock()
	prev.retire()
	return csiPlugin, release, false, nil
}

func (s *Atelet) getPlugin(ctx context.Context, driverName string) (volume.VolumePluginWorkerPlane, func(), error) {
	s.mu.RLock()
	if plugin, ok := s.volumePlugins[driverName]; ok {
		s.mu.RUnlock()
		return plugin, noopNodeRelease, nil
	}
	if slot, ok := s.csiPlugins[driverName]; ok && slot.entry != nil && !slot.checkedAt.IsZero() && s.now().Sub(slot.checkedAt) < defaultCSIPluginConfigTTL {
		release := slot.entry.acquire()
		plugin := slot.entry.plugin
		s.mu.RUnlock()
		return plugin, release, nil
	}
	s.mu.RUnlock()
	if s.csiDriverConfigGetter == nil {
		return nil, nil, errors.New("missing csiDriverConfigGetter")
	}
	for {
		plugin, release, retry, err := s.getPluginOnce(ctx, driverName)
		if !retry {
			return plugin, release, err
		}
	}
}
