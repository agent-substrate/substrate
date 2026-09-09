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
// virtio-fs share at SharedDir(actorUID)/durable, where each container's bind
// is attached.
//
// Snapshots carry the contents of the whole per-actor directory, so every
// volume rides along and the layout is reproduced verbatim on restore.
// virtiofsd serves the share write-through (no --writeback), so once the guest
// is paused every completed guest write is already visible on the host and the
// capture is complete.
//
// There are two arrangements for that capture, chosen by ATEOM_DURABLE_BACKEND:
//
//   - tar (the default): one archive of the whole directory. Sealing it reads
//     and rewrites every byte the actor holds, on the paused critical path.
//   - files: a metadata-only index tar plus one blob per non-empty regular
//     file, hard-linked out of the directory rather than copied. Sealing costs
//     one link per file instead of a copy of the tree, so the paused window
//     stops scaling with the actor's data. The price is a snapshot of many
//     objects rather than one, which a directory of many small files pays for
//     on upload.
//
// A restore dispatches on what the snapshot actually contains, not on the
// variable, so either arrangement reads back under either setting.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/tarutil"
)

// The snapshot files holding the actor's durable-dir volumes, in whichever
// arrangement wrote them. Entries are <volumeName>/... relative to
// ateompath.DurableDirVolumeMountsDir, so a restore reproduces the same layout.
// The names are shared with atelet, which uses them to carve durable data out
// of a FULL snapshot's file set when uploading a paused checkpoint as DATA.
const (
	durableTarFile    = ateompath.DurableDirTarFile
	durableIndexFile  = ateompath.DurableDirIndexFile
	durableBlobPrefix = ateompath.DurableDirBlobPrefix
)

// durableBackendEnvVar selects the arrangement a checkpoint writes. Only
// durableBackendFiles is recognized; anything else, including unset, keeps the
// tar.
const (
	durableBackendEnvVar = "ATEOM_DURABLE_BACKEND"
	durableBackendFiles  = "files"
)

// splitDurableCapture reports whether checkpoints should write the split
// arrangement.
func splitDurableCapture() bool {
	return os.Getenv(durableBackendEnvVar) == durableBackendFiles
}

// hasDurableVolumes reports whether any container mounts a durable-dir volume.
func hasDurableVolumes(containers []*ateompb.Container) bool {
	for _, c := range containers {
		if len(c.GetDurableDirVolumeMounts()) > 0 {
			return true
		}
	}
	return false
}

// stageDurableVolumes bind-mounts the actor's host durable-dir directory
// into the sandbox's shared virtio-fs tree at SharedDir(actorUID)/durable.
func (s *AteomService) stageDurableVolumes(ctx context.Context, actorUID string) error {
	src := ateompath.DurableDirVolumeMountsDir(actorUID)
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("while checking durable-dir volumes dir %q: %w", src, err)
	}
	if err := kata.BindIntoShare(ctx, src, actorUID, ocispec.ShareDurable); err != nil {
		return fmt.Errorf("while binding durable-dir volumes into the shared tree: %w", err)
	}
	return nil
}

// captureDurableVolumes writes the actor's durable-dir volumes (dir) into the
// checkpoint directory. The caller must have paused the guest first: virtiofsd is
// write-through, so a completed guest write is on the host by then, but a
// running guest could still add more after the walk.
//
// Sockets the workload left behind are skipped rather than archived (tarutil
// logs them); they hold no data and the workload recreates them on start.
//
// The split arrangement hardlinks file contents into the checkpoint directory,
// which means dir must not be written again afterwards. CheckpointWorkload
// guarantees that: the guest stays paused until terminateWorkload tears the
// sandbox down, and atelet resets the actor's directories after the RPC
// returns.
func captureDurableVolumes(ctx context.Context, dir, checkpointDir string) error {
	var err error
	if splitDurableCapture() {
		_, err = tarutil.CreateSplit(ctx, filepath.Join(checkpointDir, durableIndexFile), dir, checkpointDir, durableBlobPrefix)
	} else {
		err = tarutil.Create(ctx, filepath.Join(checkpointDir, durableTarFile), dir)
	}
	if err != nil {
		return fmt.Errorf("while archiving durable-dir volumes from %q: %w", dir, err)
	}
	return nil
}

// restoreDurableVolumes restores the durable-dir volumes from a snapshot into the
// actor's host directory (dir, which atelet has already created, empty). It must
// run before the durable share's virtiofsd starts, so the guest never observes
// the directory mid-restore.
//
// Which arrangement the snapshot is in is read off the snapshot itself: a node
// configured for one has to be able to restore an actor captured under the
// other, and the snapshot outlives whatever ateom wrote it.
//
// The split arrangement hands the guest the staged blobs themselves, hard-linked
// rather than copied, so the guest's writes land on snapshotDir's inodes. That
// is sound because snapshotDir belongs to this activation alone: atelet stages
// it by copying out of the retained checkpoint — so a write can never reach the
// checkpoint every later restore reads — nothing reads it once this call
// returns, and resetActorDirs deletes it together with dir.
func restoreDurableVolumes(dir, snapshotDir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("while creating durable-dir volumes dir %q: %w", dir, err)
	}
	index := filepath.Join(snapshotDir, durableIndexFile)
	if _, err := os.Stat(index); err == nil {
		if err := tarutil.ExtractSplit(index, snapshotDir, dir); err != nil {
			return fmt.Errorf("while restoring durable-dir volumes into %q: %w", dir, err)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("while checking for durable-dir index %q: %w", index, err)
	}
	if err := tarutil.Extract(filepath.Join(snapshotDir, durableTarFile), dir); err != nil {
		return fmt.Errorf("while restoring durable-dir volumes into %q: %w", dir, err)
	}
	return nil
}
