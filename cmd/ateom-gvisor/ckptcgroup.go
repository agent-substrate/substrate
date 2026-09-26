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
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/ocispec"
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
// Moving the sentry into a sibling <uid>-ckpt cgroup (memory.max = "max")
// before checkpoint charges newly written image pages to <uid>-ckpt while
// existing guest memory remains charged to <uid>-_pause. Removing <uid>-ckpt
// after sandbox teardown reparents the cache to the worker scope for fast
// local Resume, and dropActorCheckpointCacheAsync evicts the .img cache via
// POSIX_FADV_DONTNEED once Restore completes.

const (
	checkpointCgroupSuffix = "-ckpt"
	// sentryArgv0 identifies the sandbox process; systrap stubs have an empty
	// cmdline and gofers use "runsc-gofer".
	sentryArgv0     = "runsc-sandbox"
	defaultProcRoot = "/proc"
)

type checkpointCgroup struct {
	leaf string
	dir  string
	pid  int
}

func checkpointCgroupDir(root, actorUID string) string {
	return filepath.Join(root, actorUID+checkpointCgroupSuffix)
}

// isolateCheckpointCache moves the actor's sentry from its pause leaf into a
// sibling <uid>-ckpt cgroup.
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
	c := &checkpointCgroup{leaf: leaf, dir: dir, pid: pid}
	if err := movePID(dir, pid); err != nil {
		return nil, errors.Join(err, c.remove())
	}
	return c, nil
}

func findSentry(leaf, procRoot string) (int, error) {
	procs, err := os.ReadFile(filepath.Join(leaf, "cgroup.procs"))
	if err != nil {
		return 0, fmt.Errorf("while listing the pause leaf: %w", err)
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

func movePID(dir string, pid int) error {
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
	err := movePID(c.leaf, c.pid)
	if errors.Is(err, fs.ErrNotExist) {
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

// removeStaleCheckpointCgroups removes empty *-ckpt cgroups left behind on restart.
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

// dropActorCheckpointCacheAsync evicts the actor's .img files from page cache
// in the background, pinned by inode so delayed post-writeback passes cannot
// evict newer checkpoints.
func dropActorCheckpointCacheAsync(actorUID string, extraDirs ...string) {
	targets := findCheckpointImages(actorUID, extraDirs...)
	if len(targets) == 0 {
		return
	}
	go func() {
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

func findCheckpointImages(actorUID string, extraDirs ...string) []cachedImageFile {
	dirs := append([]string{
		ateompath.RestoreStateDir(actorUID),
		filepath.Join(ateompath.ActorPath(actorUID), "local-checkpoint"),
	}, extraDirs...)
	return findCheckpointImagesInDirs(dirs...)
}

func findCheckpointImagesInDirs(dirs ...string) []cachedImageFile {
	var targets []cachedImageFile
	seen := make(map[string]bool)
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || !d.Type().IsRegular() || filepath.Ext(d.Name()) != ".img" || seen[path] {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return nil
			}
			seen[path] = true
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
