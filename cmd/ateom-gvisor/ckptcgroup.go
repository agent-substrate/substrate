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
	"bytes"
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
	"time"

	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"golang.org/x/sys/unix"
)

// Checkpoint page cache isolation.
//
// During checkpoint, the sentry writes pages.img (~guest RAM size). If it
// stays in <uid>-_pause, the image page cache is charged against the actor's
// memory.max alongside unswappable guest memory, evicting clean image pages
// (forcing disk reads on local Resume) or OOM-killing the sentry when writeback
// lags.
//
// Moving the sentry into a sibling <uid>-_ckpt cgroup (memory.max = "max")
// before checkpoint charges newly written image pages to <uid>-_ckpt while
// existing guest memory remains charged to <uid>-_pause. Removing <uid>-_ckpt
// after sandbox teardown reparents the cache to the worker scope for fast
// local Resume, and dropActorCheckpointCacheAsync evicts the .img cache via
// POSIX_FADV_DONTNEED once Restore completes.

const (
	// checkpointCgroupSuffix uses a leading underscore like _pause so it cannot
	// collide with a DNS-label application container leaf (<uid>-<container>).
	checkpointCgroupSuffix = "-_ckpt"
	// sentryArgv0 identifies the sandbox process; systrap stubs have an empty
	// cmdline and gofers use "runsc-gofer".
	sentryArgv0        = "runsc-sandbox"
	defaultProcRoot    = "/proc"
	waitRestoreTimeout = 30 * time.Second
)

type checkpointCgroup struct {
	leaf     string
	dir      string
	procRoot string
}

func checkpointCgroupDir(root, actorUID string) string {
	return filepath.Join(root, actorUID+checkpointCgroupSuffix)
}

// isolateCheckpointCache moves the actor's sentry from its pause leaf into a
// sibling <uid>-_ckpt cgroup.
func isolateCheckpointCache(root, procRoot, actorUID string) (*checkpointCgroup, error) {
	leaf := filepath.Join(root, ocispec.GVisorCgroupLeaf(actorUID, ocispec.PauseContainer))
	pid, err := findSentry(leaf, procRoot)
	if err != nil {
		return nil, err
	}

	dir := checkpointCgroupDir(root, actorUID)
	if err := os.Mkdir(dir, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf("while creating cgroup %q: %w", dir, err)
	}
	c := &checkpointCgroup{leaf: leaf, dir: dir, procRoot: procRoot}
	if err := movePID(dir, pid); err != nil {
		return nil, errors.Join(err, c.remove())
	}
	return c, nil
}

func findSentry(leaf, procRoot string) (int, error) {
	procs, err := os.ReadFile(filepath.Join(leaf, "cgroup.procs"))
	if err != nil {
		return 0, fmt.Errorf("while listing the cgroup %q: %w", leaf, err)
	}
	for _, f := range strings.Fields(string(procs)) {
		pid, err := strconv.Atoi(f)
		if err != nil {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join(procRoot, f, "cmdline"))
		if err != nil {
			continue
		}
		argv0, _, _ := bytes.Cut(cmdline, []byte{0})
		if string(argv0) == sentryArgv0 {
			return pid, nil
		}
	}
	return 0, fmt.Errorf("no %s process in %q", sentryArgv0, leaf)
}

// movePID is a var so tests can model cgroup2 PID migration on a temp dir.
var movePID = writeCgroupPID

func writeCgroupPID(dir string, pid int) error {
	if err := os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte(strconv.Itoa(pid)), 0o644); err != nil {
		return fmt.Errorf("while moving PID %d into cgroup %q: %w", pid, dir, err)
	}
	return nil
}

// restore moves the sentry back into its pause leaf on checkpoint failure.
func (c *checkpointCgroup) restore() error {
	if c == nil {
		return nil
	}
	pid, err := findSentry(c.dir, c.procRoot)
	if err != nil {
		return nil
	}
	err = movePID(c.leaf, pid)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, unix.ESRCH) {
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

// checkpointFull isolates the sentry into a sibling cgroup, runs `runsc
// checkpoint` and optional durable-volume archiving, and tears down the
// workload before removing the sibling cgroup so its page cache reparents
// cleanly. If `runsc checkpoint` fails, it moves the sentry back into its pause
// leaf (or terminates the workload if that rollback fails).
func checkpointFull(
	ctx context.Context,
	cgroupRoot, procRoot, actorUID string,
	actorDirs *ateompb.ActorDirs,
	containers []*ateompb.Container,
	checkpoint func(context.Context, string, string) error,
	terminate func() error,
) error {
	ckptCgroup, err := isolateCheckpointCache(cgroupRoot, procRoot, actorUID)
	if err != nil {
		slog.WarnContext(ctx, "Checkpointing without page cache isolation",
			slog.String("actorUID", actorUID), slog.Any("err", err))
	}
	terminateOnExit := true
	defer func() {
		if terminateOnExit {
			_ = terminate()
		}
		if err := ckptCgroup.remove(); err != nil {
			slog.WarnContext(ctx, "Failed to remove the checkpoint cgroup",
				slog.String("actorUID", actorUID), slog.Any("err", err))
		}
	}()

	checkpointPath := actorDirs.GetCheckpointDir()
	// Checkpoint pause container (root of the sandbox).
	// TODO: Consider pause -> tar -> resume -> checkpoint order for better failure handling.
	if err := checkpoint(ctx, ocispec.PauseContainer, checkpointPath); err != nil {
		if rerr := ckptCgroup.restore(); rerr != nil {
			slog.WarnContext(ctx, "Failed to move the sentry back into its pause leaf; terminating workload",
				slog.String("actorUID", actorUID), slog.Any("err", rerr))
		} else {
			terminateOnExit = false
		}
		return fmt.Errorf("while checkpointing pause: %w", err)
	}
	if hasDurableVolumes(containers) {
		if err := tarDurableVolumes(ctx, actorDirs.GetDurableDirVolumeMountsDir(), checkpointPath); err != nil {
			return fmt.Errorf("while archiving durable-dir volumes: %w", err)
		}
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

type cachedImageFile struct {
	path string
	info fs.FileInfo
}

// dropActorCheckpointCacheAsync waits for background restore page loading to
// finish and then evicts the actor's .img files from page cache in the
// background, pinned by inode so delayed post-writeback passes cannot evict
// newer checkpoints.
func dropActorCheckpointCacheAsync(actorUID string, actorDirs *ateompb.ActorDirs, waitRestore func(context.Context) error) {
	targets := findCheckpointImages(actorDirs)
	if len(targets) == 0 {
		return
	}
	go func() {
		if waitRestore != nil {
			waitCtx, cancel := context.WithTimeout(context.Background(), waitRestoreTimeout)
			err := waitRestore(waitCtx)
			cancel()
			if err != nil {
				slog.Warn("Skipping checkpoint page cache eviction after wait -restore error",
					slog.String("actorUID", actorUID), slog.Any("err", err))
				return
			}
		}
		if !evictCheckpointImages(targets) {
			return
		}
		for _, delay := range []time.Duration{500 * time.Millisecond, 1500 * time.Millisecond} {
			time.Sleep(delay)
			if !evictCheckpointImages(targets) {
				return
			}
		}
	}()
}

func findCheckpointImages(actorDirs *ateompb.ActorDirs) []cachedImageFile {
	var targets []cachedImageFile
	for _, dir := range []string{actorDirs.GetRestoreDir(), localCheckpointsDir(actorDirs)} {
		if dir == "" {
			continue
		}
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || !d.Type().IsRegular() || filepath.Ext(d.Name()) != ".img" {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return nil
			}
			if slices.ContainsFunc(targets, func(t cachedImageFile) bool { return os.SameFile(t.info, info) }) {
				return nil
			}
			targets = append(targets, cachedImageFile{path: path, info: info})
			return nil
		})
	}
	return targets
}

func evictCheckpointImages(targets []cachedImageFile) bool {
	anyRemaining := false
	for _, t := range targets {
		f, err := os.Open(t.path)
		if err != nil {
			continue
		}
		info, err := f.Stat()
		if err == nil && os.SameFile(info, t.info) && info.ModTime().Equal(t.info.ModTime()) && info.Size() == t.info.Size() {
			anyRemaining = true
			_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
		}
		_ = f.Close()
	}
	return anyRemaining
}
