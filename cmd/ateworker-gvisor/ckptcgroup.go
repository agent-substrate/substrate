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

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/snapshotcache"
	"golang.org/x/sys/unix"
)

// During checkpoint, moving the sentry from <uid>-_sandbox into a sibling
// <uid>-_ckpt cgroup (memory.max = "max", cpu.max copied from <uid>-_sandbox)
// prevents newly written pages.img cache from evicting clean pages or
// OOM-killing the sentry under the actor's memory.max. Removing <uid>-_ckpt
// after sandbox teardown reparents the cache to the worker scope for fast
// local Resume, and dropActorCheckpointCacheAsync unlinks non-preserved files
// in the restore dir or evicts preserved checkpoint page cache via
// POSIX_FADV_DONTNEED once background restore page loading completes.

const (
	// checkpointCgroupSuffix uses a leading underscore like _sandbox so it cannot
	// collide with a DNS-label application container leaf (<uid>-<container>).
	checkpointCgroupSuffix = "-_ckpt"
)

type checkpointCgroup struct {
	leaf string
	dir  string
	pid  int
}

func checkpointCgroupDir(root, actorUID string) string {
	return filepath.Join(root, actorUID+checkpointCgroupSuffix)
}

// isolateCheckpointCache moves the actor's sentry from its sandbox leaf into a
// sibling <uid>-_ckpt cgroup, preserving the sandbox leaf's cpu.max so checkpoint
// serialization remains capped to the actor's CPU limit.
func isolateCheckpointCache(root string, actorDirs *ateworkerpb.ActorDirs, actorUID string) (*checkpointCgroup, error) {
	pid, err := readSandboxPID(actorDirs)
	if err != nil {
		return nil, err
	}

	leaf := filepath.Join(root, ocispec.GVisorCgroupLeaf(actorUID, ocispec.SandboxID))
	if !cgroupHasPID(leaf, pid) {
		return nil, fmt.Errorf("sandbox pid %d is not in cgroup %q", pid, leaf)
	}
	dir := checkpointCgroupDir(root, actorUID)
	if err := os.Mkdir(dir, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf("while creating cgroup %q: %w", dir, err)
	}
	if cpuMax, err := os.ReadFile(filepath.Join(leaf, "cpu.max")); err == nil {
		if err := os.WriteFile(filepath.Join(dir, "cpu.max"), cpuMax, 0o644); err != nil {
			slog.Debug("Failed to copy cpu.max to checkpoint cgroup", slog.String("cgroup", dir), slog.Any("err", err))
		}
	}
	c := &checkpointCgroup{leaf: leaf, dir: dir, pid: pid}
	if err := movePID(dir, pid); err != nil {
		return nil, errors.Join(err, c.remove())
	}
	return c, nil
}

func readSandboxPID(actorDirs *ateworkerpb.ActorDirs) (int, error) {
	pidFile := pidFilePath(actorDirs, ocispec.SandboxID)
	b, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, fmt.Errorf("while reading sandbox pid file %q: %w", pidFile, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("invalid sandbox pid %q in %q: %w", string(b), pidFile, err)
	}
	return pid, nil
}

func cgroupHasPID(dir string, pid int) bool {
	procs, err := os.ReadFile(filepath.Join(dir, "cgroup.procs"))
	if err != nil {
		return false
	}
	return slices.Contains(strings.Fields(string(procs)), strconv.Itoa(pid))
}

// movePID is a var so tests can model cgroup2 PID migration on a temp dir.
var movePID = writeCgroupPID

func writeCgroupPID(dir string, pid int) error {
	if err := os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte(strconv.Itoa(pid)), 0o644); err != nil {
		return fmt.Errorf("while moving PID %d into cgroup %q: %w", pid, dir, err)
	}
	return nil
}

// restore moves the sentry back into its sandbox leaf on checkpoint failure. It
// returns nil once the sentry has exited; any other failure leaves the sentry
// alive in the checkpoint cgroup, so the caller must terminate it.
func (c *checkpointCgroup) restore() error {
	if c == nil {
		return nil
	}
	if !cgroupHasPID(c.dir, c.pid) {
		return nil
	}
	err := movePID(c.leaf, c.pid)
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	return err
}

// remove deletes the empty sibling cgroup, reparenting its page cache.
func (c *checkpointCgroup) remove() error {
	if c == nil {
		return nil
	}
	if err := os.Remove(c.dir); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("while removing cgroup %q: %w", c.dir, err)
	}
	return nil
}

// checkpointIsolated isolates the sentry into a sibling cgroup (when the
// worker's cgroup is delegated), runs checkpoint, and tears down the workload
// before removing the sibling cgroup so its page cache reparents cleanly. If
// checkpoint fails, it moves the sentry back into its sandbox leaf (or terminates
// the workload if that rollback fails).
func checkpointIsolated(
	ctx context.Context,
	cgroupDelegated bool,
	cgroupRoot, actorUID string,
	actorDirs *ateworkerpb.ActorDirs,
	checkpoint func(context.Context) error,
	terminate func() error,
) error {
	var ckptCgroup *checkpointCgroup
	if cgroupDelegated {
		var err error
		ckptCgroup, err = isolateCheckpointCache(cgroupRoot, actorDirs, actorUID)
		if err != nil {
			slog.WarnContext(ctx, "Checkpointing without page cache isolation",
				slog.String("actorUID", actorUID), slog.Any("err", err))
		}
	}
	terminateOnExit := true
	defer func() {
		if terminateOnExit {
			_ = terminate()
		}
		err := ckptCgroup.remove()
		if err != nil {
			// If terminate left the sentry alive, move it back to its sandbox leaf.
			if rerr := ckptCgroup.restore(); rerr != nil {
				slog.WarnContext(ctx, "Failed to move the sentry back into its sandbox leaf",
					slog.String("actorUID", actorUID), slog.Any("err", rerr))
			} else {
				err = ckptCgroup.remove()
			}
		}
		if err != nil {
			slog.WarnContext(ctx, "Failed to remove the checkpoint cgroup",
				slog.String("actorUID", actorUID), slog.Any("err", err))
		}
	}()

	if err := checkpoint(ctx); err != nil {
		if rerr := ckptCgroup.restore(); rerr != nil {
			slog.WarnContext(ctx, "Failed to move the sentry back into its sandbox leaf; terminating workload",
				slog.String("actorUID", actorUID), slog.Any("err", rerr))
		} else {
			terminateOnExit = false
		}
		return err
	}
	return nil
}

// removeStaleCheckpointCgroups removes empty *-_ckpt cgroups left behind on restart.
func removeStaleCheckpointCgroups(ctx context.Context, root string) {
	dirs, err := filepath.Glob(filepath.Join(root, "*"+checkpointCgroupSuffix))
	if err != nil {
		return
	}
	for _, dir := range dirs {
		if err := os.Remove(dir); err != nil && !errors.Is(err, fs.ErrNotExist) {
			slog.WarnContext(ctx, "Failed to remove a stale checkpoint cgroup", slog.String("cgroup", dir), slog.Any("err", err))
		}
	}
}

func isCheckpointDataFile(name string) bool {
	ext := filepath.Ext(name)
	return ext == ".img" || ext == ".tar"
}

// evictCheckpointCache is a var so tests can observe eviction.
var evictCheckpointCache = snapshotcache.EvictWithRetries

// dropActorCheckpointCacheAsync pins the snapshot data files in restoreDir
// while the caller holds the actor lock. When the restore dir is not preserved
// and nothing else links the files, it unlinks them immediately: the kernel
// frees pages and blocks once the sentry closes its open FDs, and the actor's
// cgroup is no longer charged for them. When the restore dir is preserved, or
// a file has another link, such as atelet's snapshot cache entry it was staged
// from, so that unlinking would free nothing, it waits in the background
// (bounded by actorCtx) for async restore page loading to finish and evicts
// clean pages from the host page cache, then unlinks the files unless the
// dir is preserved.
func dropActorCheckpointCacheAsync(actorCtx context.Context, actorUID, restoreDir string, preserveRestoreDir bool, waitRestore func(context.Context) error) {
	targets := snapshotcache.FindFiles(restoreDir, isCheckpointDataFile)
	if len(targets) == 0 {
		return
	}
	if !preserveRestoreDir && !anyLinkedElsewhere(targets) {
		snapshotcache.DropFiles(targets)
		return
	}
	go func() {
		if waitRestore != nil {
			if err := waitRestore(actorCtx); err != nil {
				slog.Warn("Skipping checkpoint page cache eviction after restore wait error",
					slog.String("actorUID", actorUID), slog.Any("err", err))
				if !preserveRestoreDir {
					snapshotcache.DropFiles(targets)
				}
				return
			}
		}
		evictCheckpointCache(targets)
		if !preserveRestoreDir {
			snapshotcache.DropFiles(targets)
		}
	}()
}

// anyLinkedElsewhere reports whether any of files has a hard link besides
// the one in the restore dir.
func anyLinkedElsewhere(files []snapshotcache.File) bool {
	for _, f := range files {
		if st, ok := f.Info.Sys().(*syscall.Stat_t); ok && st.Nlink > 1 {
			return true
		}
	}
	return false
}
