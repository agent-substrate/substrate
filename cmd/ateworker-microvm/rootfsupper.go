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

// Host-side rootfs overlay uppers for the micro-VM runtime.
//
// Each container's rootfs is assembled ON THE HOST — the stock kata
// arrangement: overlay(lower = the OCI image bundle, upper/work = this
// per-actor directory), merged by the host kernel and served to the guest
// over the one kataShared virtio-fs share (see internal/kata/overlay_linux.go).
// Rootfs writes cost host disk, not guest RAM. The host kernel owns all
// overlay mechanics, so deletion metadata is the canonical kind: whiteouts as
// 0:0 char devices and opaque markers as trusted.overlay.* xattrs in the
// upper, with no special mount options and no guest xattr passthrough.
// (The retired alternatives: a guest tmpfs upper capped rootfs writes at the
// tmpfs size and pinned every written byte in guest memory; a guest-mounted
// overlay on a virtio-fs upper needed three kernel workarounds. Snapshots
// from the tmpfs era still restore, see below.)
//
// The directory is owned entirely by ateworker (atelet never touches it): created
// pristine at cold boot, re-materialized from the snapshot at restore, and
// removed at teardown — after CleanupSandboxState has dropped the overlay
// mounts that use it.
//
// Snapshots: the upper does not ride in guest memory, so ROOTFS and MEMORY
// snapshots ship it as one tar per container (rootfsUpperTarFile), taken while
// the guest is paused (the share is write-through, so a paused guest's
// completed writes are already in the upper). Each tar holds only the contents
// of that container's upperdir: ateworker creates the directory layout itself on
// restore, so nothing in the snapshot decides a path ateworker later mounts or
// wipes. A ROOTFS restore puts the upper back and cold-boots on it; a VOLUMES
// snapshot deliberately excludes rootfs state, so the workload cold-starts on a
// pristine upper.
//
// Containers using EROFS layers mount the overlay in the guest over this same
// directory (see erofs.go); their snapshot tar (rootfsUpperTarFile) archives both
// fs/ and work/ because the guest kernel holds the workdir open.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/tarutil"
)

// rootfsUpperTarFile is the snapshot file holding the tar of one container's
// rootfs upper. For a host-merged rootfs that is the upperdir alone (<cid>/fs
// under rootfsUpperDir, see kata.UpperWorkDirs): entries are relative to the
// upperdir, plus a root entry, as overlayfs presents the upperdir's own mode,
// ownership, and xattrs as the container's /. The workdir is never archived:
// with index=off pinned on the mount (see kata.StageMergedRootfs) a restored
// workdir would be inert, and StageMergedRootfs recreates it regardless.
// For a guest-merged (erofs) rootfs it is the whole <cid> directory, fs and
// work, under a different name: the guest kernel keeps the workdir open across
// snapshots, and the name tells a restore which layout the tar holds.
func rootfsUpperTarFile(containerID string, erofs bool) (string, error) {
	if containerID == "" || containerID == "." || strings.Contains(containerID, "/") || !filepath.IsLocal(containerID) {
		return "", fmt.Errorf("invalid container name %q", containerID)
	}
	if erofs {
		return "rootfs-rw-" + containerID + ".tar", nil
	}
	return "rootfs-upper-" + containerID + ".tar", nil
}

// containerNames returns the names of the actor's containers.
func containerNames(containers []*ateworkerpb.Container) []string {
	names := make([]string, len(containers))
	for i, c := range containers {
		names[i] = c.GetName()
	}
	return names
}

// stageRootfsUpper prepares the host upper dir for a cold boot: pristine when
// snapshotDir is empty, otherwise re-materialized from the per-container tars
// a ROOTFS or MEMORY snapshot left in snapshotDir. Either way any previous
// activation's contents are gone first. Must run after CleanupSandboxState
// has dropped the overlay mounts that used the dir.
func stageRootfsUpper(actorDirs *ateworkerpb.ActorDirs, snapshotDir string, containers []string, rec *erofsLayers) error {
	if snapshotDir == "" {
		return resetRootfsUpperDir(actorDirs)
	}
	return untarRootfsUpper(rootfsUpperDir(actorDirs), snapshotDir, containers, rec)
}

// resetRootfsUpperDir gives a cold boot a pristine upper directory: a cold
// boot must start from the bare image, and atelet's actor-dir reset does not
// know about this directory, so ateworker wipes any previous activation's contents
// itself. The per-container fs/work subdirectories are created by the overlay
// staging (kata.StageMergedRootfs).
func resetRootfsUpperDir(actorDirs *ateworkerpb.ActorDirs) error {
	dir := rootfsUpperDir(actorDirs)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("while clearing rootfs upper dir %q: %w", dir, err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("while creating rootfs upper dir %q: %w", dir, err)
	}
	return nil
}

// tarRootfsUpper archives each container's rootfs upper under dir into the
// checkpoint directory, one tar per container: the upperdir alone for a
// host-merged rootfs, the whole <cid> directory for a guest-merged one. The
// caller must have paused the guest first: virtiofsd is write-through, so a
// completed guest write has reached the host upper by then, but a running
// guest could still add more after the walk.
func tarRootfsUpper(ctx context.Context, dir, checkpointDir string, ctrs []actorContainer) error {
	for _, c := range ctrs {
		upper, _ := kata.UpperWorkDirs(dir, c.name)
		name, err := rootfsUpperTarFile(c.name, c.erofs)
		if err != nil {
			return err
		}
		src := upper
		if c.erofs {
			src = filepath.Dir(upper)
		}
		if err := tarutil.CreateWithRoot(ctx, filepath.Join(checkpointDir, name), src); err != nil {
			return fmt.Errorf("while archiving rootfs upper %q: %w", src, err)
		}
	}
	return nil
}

// untarRootfsUpper restores each container's rootfs upper from the snapshot
// into the actor's host directory, as the snapshot's record says it was
// archived (see tarRootfsUpper). It must run before the merged overlays are
// mounted or the rw binds staged (they consume these contents). The directory
// is recreated from scratch: nothing else owns it, and stale contents from a
// previous activation would corrupt the overlay state the guest's find-paths
// re-opens. Every directory above the upperdirs is created here, never taken
// from the tar.
func untarRootfsUpper(dir, snapshotDir string, containers []string, rec *erofsLayers) error {
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("while clearing rootfs upper dir %q: %w", dir, err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("while creating rootfs upper dir %q: %w", dir, err)
	}
	for _, cid := range containers {
		upper, _, err := kata.MkdirUpperWorkDirs(dir, cid)
		if err != nil {
			return fmt.Errorf("while creating rootfs upper for %q: %w", cid, err)
		}
		name, err := rootfsUpperTarFile(cid, rec.isEROFS(cid))
		if err != nil {
			return err
		}
		dst := upper
		if rec.isEROFS(cid) {
			dst = filepath.Dir(upper)
		}
		if err := tarutil.Extract(filepath.Join(snapshotDir, name), dst); err != nil {
			return fmt.Errorf("while restoring rootfs upper %q: %w", dst, err)
		}
	}
	return nil
}
