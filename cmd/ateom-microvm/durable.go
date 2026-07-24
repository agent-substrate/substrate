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
// with cache=always), so writable volumes get their own share, mounted at
// kata.GuestDurableVolumeDir(volume) and bind-mounted from there into each
// container that declares the volume.
//
// Snapshots carry the contents as a tar of the whole per-actor directory, so the
// volume names round-trip without ateom having to learn them from the wire (the
// ateom protocol carries mount paths only). virtiofsd serves the share
// write-through (no --writeback), so once the guest is paused every completed
// guest write is already visible on the host and the tar is complete.

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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// durableTarFile is the snapshot file holding the tar of the actor's durable-dir
// volumes. Its entries are <volumeName>/... relative to
// ateompath.DurableDirVolumeMountsDir, so extraction restores the same layout.
const durableTarFile = "durable-dir.tar"

// hasDurableVolumes reports whether any container mounts a durable-dir volume.
func hasDurableVolumes(containers []*ateompb.Container) bool {
	for _, c := range containers {
		if len(c.GetDurableDirVolumes()) > 0 {
			return true
		}
	}
	return false
}

// resolveDurableVolumeName returns the name of the actor's durable-dir volume,
// read from the directory atelet created for it (dir is
// ateompath.DurableDirVolumeMountsDir for the actor).
//
// The ateom protocol carries only the guest mount paths, not volume names, so
// the name comes from the host layout: the directory holds exactly one
// subdirectory per volume. The ActorTemplate API allows at most one durable-dir
// volume per template, so anything other than exactly one directory means ateom
// and atelet disagree about the actor — fail rather than guess.
//
// That makes this the one place ateom depends on atelet's on-disk layout, and
// the first thing to change if more than one durable-dir volume is ever
// supported: ateompb.Container would need to carry each mount's volume name
// alongside its path, and this lookup would go away.
func resolveDurableVolumeName(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", fmt.Errorf("while reading durable-dir volumes dir %q: %w", dir, err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	switch len(names) {
	case 1:
		return names[0], nil
	case 0:
		return "", status.Errorf(codes.FailedPrecondition,
			"actor declares durable-dir volume mounts but %q holds no volume directory", dir)
	default:
		return "", status.Errorf(codes.Unimplemented,
			"ateom-microvm supports at most one durable-dir volume, found %d in %q", len(names), dir)
	}
}

// durableMounts returns the OCI mounts that expose one durable volume at each of
// a container's declared mount paths. The source is the volume's directory inside
// the guest's durable share, which the agent mounts at sandbox creation.
func durableMounts(volumeName string, mountPaths []string) []specs.Mount {
	src := kata.GuestDurableVolumeDir(volumeName)
	mounts := make([]specs.Mount, 0, len(mountPaths))
	for _, p := range mountPaths {
		mounts = append(mounts, specs.Mount{
			Destination: p,
			Source:      src,
			Type:        "bind",
			Options:     []string{"rbind", "rw"},
		})
	}
	return mounts
}

// workloadSpec returns the OCI spec to start a container's overlay workload with:
// the prepared spec, plus the durable-dir binds when the actor has a volume and
// this container mounts it.
//
// The spec is copied rather than mutated so the bundle's on-disk config.json and
// the carrier's view stay as prepared — only the workload sees the binds.
func workloadSpec(c actorContainer, durableVolume string) *specs.Spec {
	if durableVolume == "" || len(c.durableMountPaths) == 0 {
		return c.spec
	}
	spec := *c.spec
	spec.Mounts = append(append([]specs.Mount(nil), c.spec.Mounts...), durableMounts(durableVolume, c.durableMountPaths)...)
	return &spec
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
	log, _ := os.OpenFile(filepath.Join(kata.VMDir(actorUID), "virtiofsd-durable.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
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
