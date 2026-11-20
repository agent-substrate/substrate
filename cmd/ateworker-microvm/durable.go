//go:build linux

// Copyright 2026 The Agent Substrate Authors
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

package main

// Durable-dir volumes for the micro-VM runtime.
//
// A durable-dir volume is a directory whose contents outlive the actor's process
// state: it survives suspend/resume and, under the VOLUMES snapshot fidelity, is the
// ONLY thing captured (the workload cold-starts on restore). The host side is
// owned by atelet, which creates one directory per volume under
// ActorDirs.durable_dir_volume_mounts_dir and wipes them when the actor's
// directories are reset.
//
// ateworker exposes that host directory to the guest under the single kataShared
// virtio-fs share at SharedDir(actorUID)/durable, where each container's bind
// is attached.
//
// Snapshots carry each volume as its own tar (durableTarFile), so the volume
// directories themselves are never taken from the snapshot. virtiofsd serves
// the share write-through (no --writeback), so once the guest is paused every
// completed guest write is already visible on the host and the tars are
// complete.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/tarutil"
)

// durableTarFile is the snapshot file holding the tar of one durable-dir
// volume: the contents of <volumeName> under
// ActorDirs.durable_dir_volume_mounts_dir, plus a root entry for the volume
// directory's own metadata. The volume directory itself comes from atelet,
// never from the snapshot: kata-agent bind-mounts it by path inside the guest,
// so a symlink planted there would expose whatever it points at in the guest
// rootfs to the container.
func durableTarFile(volumeName string) (string, error) {
	if volumeName == "" || volumeName == "." || strings.Contains(volumeName, "/") || !filepath.IsLocal(volumeName) {
		return "", fmt.Errorf("invalid durable-dir volume name %q", volumeName)
	}
	return "durable-dir-" + volumeName + ".tar", nil
}

// hasDurableVolumes reports whether any container mounts a durable-dir volume.
func hasDurableVolumes(containers []*ateworkerpb.Container) bool {
	for _, c := range containers {
		if len(c.GetDurableDirVolumeMounts()) > 0 {
			return true
		}
	}
	return false
}

// durableVolumeNames returns the sorted, deduplicated durable-dir volume names
// mounted by workload containers.
func durableVolumeNames(containers []*ateworkerpb.Container) []string {
	var names []string
	for _, c := range containers {
		for _, m := range c.GetDurableDirVolumeMounts() {
			names = append(names, m.GetName())
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// stageDurableVolumes bind-mounts src, the actor's host durable-dir directory,
// into the sandbox's shared virtio-fs tree at SharedDir(actorUID)/durable.
func (s *AteWorkerService) stageDurableVolumes(ctx context.Context, actorUID, src string) error {
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("while checking durable-dir volumes dir %q: %w", src, err)
	}
	if err := kata.BindIntoShare(ctx, src, actorUID, ocispec.ShareDurable); err != nil {
		return fmt.Errorf("while binding durable-dir volumes into the shared tree: %w", err)
	}
	return nil
}

// tarDurableVolumes archives each durable-dir volume under dir into the
// checkpoint directory, one tar per volume plus a socket sidecar manifest next
// to each tar that dropped sockets (see snapshotsockets.go), and returns the
// file names — data files, both kinds, since a manifest restores the volume
// like its tar does. The caller must have paused the guest first: virtiofsd is
// write-through, so a completed guest write is on the host by then, but a
// running guest could still add more after the walk.
//
// Sockets the workload left behind are skipped by the tar itself (tarutil
// logs them): they hold no data, and under the cold-boot fidelities the
// workload recreates them on start. A MEMORY restore does not — the daemons
// come back still listening on those paths — which is what the manifests are
// for.
func (s *AteWorkerService) tarDurableVolumes(ctx context.Context, dir, checkpointDir string, volumes []string) ([]string, error) {
	var files []string
	for _, vol := range volumes {
		name, err := durableTarFile(vol)
		if err != nil {
			return nil, err
		}
		volDir := filepath.Join(dir, vol)
		if err := s.archiveDir(ctx, filepath.Join(checkpointDir, name), volDir); err != nil {
			return nil, fmt.Errorf("while archiving durable-dir volume %q: %w", vol, err)
		}
		files = append(files, name)
		manifest := socketsManifestFile(name)
		written, err := captureSocketManifest(ctx, filepath.Join(checkpointDir, manifest), volDir)
		if err != nil {
			return nil, fmt.Errorf("while recording sockets of durable-dir volume %q: %w", vol, err)
		}
		if written {
			// The manifest is a data file like its tar: on VOLUMES the whole
			// snapshot is data files, and a file left out of the list would
			// never ship.
			files = append(files, manifest)
		}
	}
	return files, nil
}

// untarDurableVolumes restores each durable-dir volume from the snapshot into
// its directory under dir, which atelet has already created, empty. A volume
// with no tar in the snapshot (added to the template since) stays empty. It
// must run before the durable share's virtiofsd starts, so the guest never
// observes the directory mid-restore.
//
// restoreSockets recreates the socket inodes the tars dropped, from the sidecar
// manifests: pass true for a MEMORY restore, whose daemons come back still
// listening on those paths, false for a cold boot, whose workload re-creates
// its sockets on start.
func untarDurableVolumes(dir, snapshotDir string, volumes []string, restoreSockets bool) error {
	for _, vol := range volumes {
		name, err := durableTarFile(vol)
		if err != nil {
			return err
		}
		tarPath := filepath.Join(snapshotDir, name)
		if _, err := os.Stat(tarPath); errors.Is(err, fs.ErrNotExist) {
			continue
		}
		volDir := filepath.Join(dir, vol)
		if err := os.MkdirAll(volDir, 0o700); err != nil {
			return fmt.Errorf("while creating durable-dir volume dir %q: %w", volDir, err)
		}
		if err := tarutil.Extract(tarPath, volDir); err != nil {
			return fmt.Errorf("while restoring durable-dir volume %q: %w", vol, err)
		}
		if restoreSockets {
			if err := restoreSocketManifest(filepath.Join(snapshotDir, socketsManifestFile(name)), volDir); err != nil {
				return fmt.Errorf("while restoring sockets of durable-dir volume %q: %w", vol, err)
			}
		}
	}
	return nil
}
