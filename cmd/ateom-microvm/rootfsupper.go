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
// The directory is owned entirely by ateom (atelet never touches it): created
// pristine at cold boot, re-materialized from the snapshot at restore, and
// removed at teardown — after CleanupSandboxState has dropped the overlay
// mounts that use it.
//
// Snapshots: the upper does not ride in guest memory, so a FULL snapshot
// ships it as a tar (rootfsUpperTarFile), taken while the guest is paused
// (the share is write-through, so a paused guest's completed writes are
// already in the upper). Every FULL snapshot carries one; restoring anything
// older fails at the untar. A DATA snapshot deliberately excludes rootfs state:
// the workload cold-starts on restore.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/tarutil"
)

// rootfsUpperTarFile is the snapshot file holding the tar of the actor's
// rootfs uppers. Its entries are <containerID>/fs/... relative to
// rootfsUpperDir — the upperdir half of the layout kata.UpperWorkDirs mounts —
// so extraction restores exactly the tree the merged overlays (and the guest's
// find-paths) expect. The workdir half is deliberately absent (see
// tarRootfsUpper); StageMergedRootfs recreates it at mount.
const rootfsUpperTarFile = "rootfs-upper.tar"

// rootfsUpperDir is the host directory backing the actor's rootfs overlay
// uppers: one subdirectory per container (see kata.UpperWorkDirs). Local to
// this binary — no other component touches it — hence not in ateompath.
func rootfsUpperDir(actorUID string) string {
	return filepath.Join(ateompath.ActorPath(actorUID), "rootfs-upper")
}

// resetRootfsUpperDir gives a cold boot a pristine upper directory: a cold
// boot must start from the bare image, and atelet's actor-dir reset does not
// know about this directory, so ateom wipes any previous activation's contents
// itself. The per-container fs/work subdirectories are created by the overlay
// staging (kata.StageMergedRootfs).
func resetRootfsUpperDir(actorUID string) error {
	dir := rootfsUpperDir(actorUID)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("while clearing rootfs upper dir %q: %w", dir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("while creating rootfs upper dir %q: %w", dir, err)
	}
	return nil
}

// tarRootfsUpper archives the actor's rootfs uppers (dir) into the checkpoint
// directory. The caller must have paused the guest first: virtiofsd is
// write-through, so a completed guest write has reached the host overlay's
// upper by then, but a running guest could still add more after the walk.
//
// Each container's overlay WORKDIR (<cid>/work) is excluded: with index=off
// pinned on the mount (see kata.StageMergedRootfs) a restored workdir is
// inert — overlayfs wipes and rebuilds it at mount, and StageMergedRootfs
// recreates the directory regardless — so archiving it is dead weight on the
// suspend path. Not always trivial weight, either: a copy-up in flight when
// the guest paused can leave file-sized temp data there.
func tarRootfsUpper(ctx context.Context, dir, checkpointDir string) error {
	skipWorkdir := func(rel string) bool {
		parts := strings.Split(rel, "/")
		return len(parts) == 2 && parts[1] == "work"
	}
	if err := tarutil.CreateFiltered(ctx, filepath.Join(checkpointDir, rootfsUpperTarFile), dir, skipWorkdir); err != nil {
		return fmt.Errorf("while archiving rootfs uppers from %q: %w", dir, err)
	}
	return nil
}

// untarRootfsUpper restores the rootfs uppers from a snapshot into the actor's
// host directory. It must run before the merged overlays are mounted (the
// mounts consume these contents). The directory is recreated from scratch:
// nothing else owns it, and stale contents from a previous activation would
// corrupt the overlay state the guest's find-paths re-opens.
func untarRootfsUpper(dir, snapshotDir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("while clearing rootfs upper dir %q: %w", dir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("while creating rootfs upper dir %q: %w", dir, err)
	}
	if err := tarutil.Extract(filepath.Join(snapshotDir, rootfsUpperTarFile), dir); err != nil {
		return fmt.Errorf("while restoring rootfs uppers into %q: %w", dir, err)
	}
	return nil
}
