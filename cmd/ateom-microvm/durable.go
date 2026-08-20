//go:build linux

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

package main

// Durable-dir volumes for the micro-VM runtime.
//
// A durable-dir volume is a directory whose contents outlive the actor's process
// state: it survives suspend/resume and, under the Data snapshot scope, is the
// ONLY thing captured (the workload cold-starts on restore). The host side is
// owned by atelet, which creates one directory per volume under
// ateompath.DurableDirVolumeMountsDir(actorUID) and wipes them when the actor's
// directories are reset.
//
// ateom exposes that host directory to the guest under the single kataShared
// virtio-fs share at SharedDir(actorUID)/durable (in-guest path:
// kata.GuestDurableVolumeDir(volume)), bind-mounted from there into every
// container that declares it. An actor may have any number of them: they cost a
// subdirectory each, not a device, so nothing here scales with the volume count.
//
// Snapshots carry the contents as a tar of the whole per-actor directory, so
// every volume rides along and the layout is reproduced verbatim on restore.
// virtiofsd serves the share write-through (no --writeback), so once the guest
// is paused every completed guest write is already visible on the host and the
// tar is complete.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/reaper"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/tarutil"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// durableTarFile is the snapshot file holding the tar of the actor's durable-dir
// volumes. Its entries are <volumeName>/... relative to
// ateompath.DurableDirVolumeMountsDir, so extraction restores the same layout.
// The name is shared with atelet, which uses it to carve durable data out of a
// FULL snapshot's file set when uploading a paused checkpoint as DATA.
const durableTarFile = ateompath.DurableDirTarFile

// hasDurableVolumes reports whether any container mounts a durable-dir volume.
func hasDurableVolumes(containers []*ateompb.Container) bool {
	for _, c := range containers {
		if len(c.GetDurableDirVolumeMounts()) > 0 {
			return true
		}
	}
	return false
}

// durableMounts returns the OCI mounts that expose a container's durable-dir
// volumes at the paths it declared. Each source is that volume's directory
// inside the guest's durable share, which the agent mounts at sandbox creation.
func durableMounts(mounts []*ateompb.DurableDirVolumeMount) []specs.Mount {
	out := make([]specs.Mount, 0, len(mounts))
	for _, m := range mounts {
		out = append(out, specs.Mount{
			Destination: m.GetMountPath(),
			Source:      kata.GuestDurableVolumeDir(m.GetVolumeName()),
			Type:        "bind",
			Options:     []string{"rbind", "rw"},
		})
	}
	return out
}

// workloadSpec returns the OCI spec to start a container with: the prepared
// spec, plus a bind for each durable-dir volume (writable), CSI volume,
// system-info volume (read-only), and image volume (read-only) it mounts.
//
// The spec is copied rather than mutated so the bundle's on-disk config.json
// stays as prepared — only the started container sees the binds.
func workloadSpec(c actorContainer) *specs.Spec {
	if len(c.durableMounts) == 0 && len(c.csiMounts) == 0 && len(c.systemInfoMounts) == 0 && len(c.imageMounts) == 0 {
		return c.spec
	}
	spec := *c.spec

	var mounts []specs.Mount
	mounts = append(mounts, c.spec.Mounts...)
	mounts = append(mounts, durableMounts(c.durableMounts)...)
	mounts = append(mounts, csiMounts(c.csiMounts)...)
	mounts = append(mounts, systemInfoMounts(c.systemInfoMounts)...)
	mounts = append(mounts, imageVolumeMounts(c.imageMounts, c.name)...)
	spec.Mounts = mounts
	return &spec
}

// stageDurableVolumes bind-mounts the actor's host durable-dir directory
// into the sandbox's shared virtio-fs tree at SharedDir(actorUID)/durable.
func (s *AteomService) stageDurableVolumes(ctx context.Context, actorUID string) error {
	src := ateompath.DurableDirVolumeMountsDir(actorUID)
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("while checking durable-dir volumes dir %q: %w", src, err)
	}
	dst := filepath.Join(kata.SharedDir(actorUID), "durable")
	// Drop any stale mount first (lazy if busy), then ensure clean mountpoint.
	if err := reaper.Run(exec.Command("umount", dst)); err != nil {
		_ = reaper.Run(exec.Command("umount", "-l", dst))
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("creating %q: %w", dst, err)
	}
	cmd := exec.CommandContext(ctx, "mount", "--bind", src, dst)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := reaper.Run(cmd); err != nil {
		return fmt.Errorf("bind-mounting durable-dir volumes at %q: %w (%s)", dst, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// tarDurableVolumes archives the actor's durable-dir volumes (dir) into the
// checkpoint directory. The caller must have paused the guest first: virtiofsd is
// write-through, so a completed guest write is on the host by then, but a
// running guest could still add more after the walk.
//
// Sockets the workload left behind are skipped rather than archived (tarutil
// logs them); they hold no data and the workload recreates them on start.
func tarDurableVolumes(ctx context.Context, dir, checkpointDir string) error {
	if err := tarutil.Create(ctx, filepath.Join(checkpointDir, durableTarFile), dir); err != nil {
		return fmt.Errorf("while archiving durable-dir volumes from %q: %w", dir, err)
	}
	return nil
}

// untarDurableVolumes restores the durable-dir volumes from a snapshot into the
// actor's host directory (dir, which atelet has already created, empty). It must
// run before the durable share's virtiofsd starts, so the guest never observes
// the directory mid-restore.
func untarDurableVolumes(dir, snapshotDir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("while creating durable-dir volumes dir %q: %w", dir, err)
	}
	if err := tarutil.Extract(filepath.Join(snapshotDir, durableTarFile), dir); err != nil {
		return fmt.Errorf("while restoring durable-dir volumes into %q: %w", dir, err)
	}
	return nil
}
