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
// ateom exposes that host directory to the guest over a SECOND virtiofsd — the
// kataShared share stays strictly read-only (it is the overlay lower, served
// with cache=always), so writable volumes get their own share. Each volume is a
// subdirectory of the one share, at kata.GuestDurableVolumeDir(volume),
// bind-mounted from there into every container that declares it. An actor may
// have any number of them: they cost a subdirectory each, not a device, so
// nothing here scales with the volume count (gVisor is the runtime that caps
// this at one, via the ActorTemplate CEL rules).
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

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
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

// workloadSpec returns the OCI spec to start a container's overlay workload
// with: the prepared spec, plus a bind for each durable-dir volume it mounts.
//
// The spec is copied rather than mutated so the bundle's on-disk config.json and
// the carrier's view stay as prepared — only the workload sees the binds.
func workloadSpec(c actorContainer) *specs.Spec {
	if len(c.durableMounts) == 0 {
		return c.spec
	}
	spec := *c.spec
	spec.Mounts = append(append([]specs.Mount(nil), c.spec.Mounts...), durableMounts(c.durableMounts)...)
	return &spec
}

// durableVirtiofsdLogPath is where the durable-dir share's virtiofsd logs,
// beside the overlay lower's (see virtiofsdLogPath) under the actor's VM dir.
func durableVirtiofsdLogPath(id string) string {
	return filepath.Join(kata.VMDir(id), "virtiofsd-durable.log")
}

// stageDurableShare starts the virtiofsd serving the actor's durable-dir volumes.
//
// It serves ateompath.DurableDirVolumeMountsDir directly — no bind into the
// kataShared tree — so teardown has nothing extra to unmount. Unlike the RO
// lower's virtiofsd this one runs with cache=auto: the host contents change
// underneath the guest whenever a snapshot is restored into them.
//
// The returned cmd outlives this call (CH talks to it for the VM's lifetime);
// the caller owns it (tracked on runningActor, killed in teardownActor).
func (s *AteomService) stageDurableShare(ctx context.Context, rr resolvedRuntime, actorUID string) (*exec.Cmd, error) {
	shared := ateompath.DurableDirVolumeMountsDir(actorUID)
	if _, err := os.Stat(shared); err != nil {
		return nil, fmt.Errorf("while checking durable-dir volumes dir %q: %w", shared, err)
	}
	log, _ := os.OpenFile(durableVirtiofsdLogPath(actorUID), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	cmd, err := kata.StartVirtiofsd(ctx, kata.VirtiofsdOptions{
		Binary:     rr.virtiofsd,
		SocketPath: kata.DurableVirtiofsdSocketPath(actorUID),
		SharedDir:  shared,
		Cache:      "auto",
		Log:        log,
	})
	if err != nil {
		return nil, fmt.Errorf("while starting durable-dir virtiofsd: %w", err)
	}
	return cmd, nil
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
