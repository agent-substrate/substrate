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
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/snapshotcache"
	"golang.org/x/sys/unix"
)

const testActorUID = "0b5f3c1e-7a2d-4e8f-9c61-2d4a8b7e5f10"

type ckptFixture struct {
	root, leaf string
	actorDirs  *ateworkerpb.ActorDirs
}

func newCkptFixture(t *testing.T, sandboxPID string) ckptFixture {
	t.Helper()
	actorRoot := t.TempDir()
	f := ckptFixture{
		root: t.TempDir(),
		actorDirs: &ateworkerpb.ActorDirs{
			RootDir:       actorRoot,
			CheckpointDir: t.TempDir(),
		},
	}
	f.leaf = filepath.Join(f.root, ocispec.GVisorCgroupLeaf(testActorUID, ocispec.SandboxID))
	mustMkdir(t, f.leaf)
	mustWrite(t, filepath.Join(f.leaf, "cgroup.procs"), "100\n"+sandboxPID+"\n")
	mustWrite(t, filepath.Join(f.leaf, "memory.max"), "805306368\n")
	mustWrite(t, filepath.Join(f.leaf, "cpu.max"), "200000 100000\n")

	if sandboxPID != "" {
		pidFile := pidFilePath(f.actorDirs, ocispec.SandboxID)
		mustMkdir(t, filepath.Dir(pidFile))
		mustWrite(t, pidFile, sandboxPID+"\n")
	}

	origMovePID := movePID
	t.Cleanup(func() { movePID = origMovePID })
	movePID = func(dir string, pid int) error {
		if err := writeCgroupPID(dir, pid); err != nil {
			return err
		}
		// Emulate cgroup2 migration out of the sibling checkpoint cgroup so rmdir
		// succeeds once the sentry moves back into its sandbox leaf.
		ckptDir := checkpointCgroupDir(f.root, testActorUID)
		if dir != ckptDir {
			_ = os.Remove(filepath.Join(ckptDir, "cgroup.procs"))
			_ = os.Remove(filepath.Join(ckptDir, "cpu.max"))
		}
		return nil
	}
	return f
}

func mustMkdir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
}

func mustWrite(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestIsolateCheckpointCacheMovesOnlyTheSentry(t *testing.T) {
	f := newCkptFixture(t, "101")

	c, err := isolateCheckpointCache(f.root, f.actorDirs, testActorUID)
	if err != nil {
		t.Fatalf("isolateCheckpointCache: %v", err)
	}
	dir := checkpointCgroupDir(f.root, testActorUID)
	if got := mustRead(t, filepath.Join(dir, "cgroup.procs")); got != "101" {
		t.Errorf("checkpoint cgroup.procs = %q, want the sentry's PID 101", got)
	}
	if got := mustRead(t, filepath.Join(dir, "cpu.max")); got != "200000 100000\n" {
		t.Errorf("checkpoint cpu.max = %q, want \"200000 100000\\n\"", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "memory.max")); !os.IsNotExist(err) {
		t.Errorf("checkpoint memory.max should not be set, stat err = %v", err)
	}

	if err := c.restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got := mustRead(t, filepath.Join(f.leaf, "cgroup.procs")); got != "101" {
		t.Errorf("sandbox leaf cgroup.procs after restore = %q, want 101", got)
	}
	if err := c.remove(); err != nil {
		t.Fatalf("remove: %v", err)
	}
}

func TestIsolateCheckpointCacheWithoutPIDFile(t *testing.T) {
	f := newCkptFixture(t, "")

	if _, err := isolateCheckpointCache(f.root, f.actorDirs, testActorUID); err == nil {
		t.Fatal("isolateCheckpointCache succeeded with no sandbox pid file")
	}
	if _, err := os.Stat(checkpointCgroupDir(f.root, testActorUID)); !os.IsNotExist(err) {
		t.Errorf("checkpoint cgroup left behind: stat err = %v", err)
	}
}

func TestIsolateCheckpointCacheRejectsPIDOutsideSandboxLeaf(t *testing.T) {
	f := newCkptFixture(t, "101")
	mustWrite(t, filepath.Join(f.leaf, "cgroup.procs"), "100\n")

	if _, err := isolateCheckpointCache(f.root, f.actorDirs, testActorUID); err == nil {
		t.Fatal("isolateCheckpointCache succeeded with the pid outside the sandbox leaf")
	}
	if _, err := os.Stat(checkpointCgroupDir(f.root, testActorUID)); !os.IsNotExist(err) {
		t.Errorf("checkpoint cgroup created for a reused pid: stat err = %v", err)
	}
	if got := mustRead(t, filepath.Join(f.leaf, "cgroup.procs")); got != "100\n" {
		t.Errorf("sandbox leaf cgroup.procs = %q, want unchanged \"100\\n\"", got)
	}
}

func TestCheckpointCgroupRestoreAfterLeafGone(t *testing.T) {
	f := newCkptFixture(t, "101")
	c, err := isolateCheckpointCache(f.root, f.actorDirs, testActorUID)
	if err != nil {
		t.Fatalf("isolateCheckpointCache: %v", err)
	}
	if err := os.RemoveAll(f.leaf); err != nil {
		t.Fatal(err)
	}
	// The sentry is still alive in the checkpoint cgroup with nowhere to go
	// back to; the caller must terminate it rather than leave it unlimited.
	if err := c.restore(); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("restore with the leaf gone = %v, want ErrNotExist", err)
	}
	if got := mustRead(t, filepath.Join(c.dir, "cgroup.procs")); got != "101" {
		t.Errorf("checkpoint cgroup.procs = %q, want the live sentry 101", got)
	}
}

func TestCheckpointCgroupRestoreSkipsExitedSentry(t *testing.T) {
	f := newCkptFixture(t, "101")
	c, err := isolateCheckpointCache(f.root, f.actorDirs, testActorUID)
	if err != nil {
		t.Fatalf("isolateCheckpointCache: %v", err)
	}

	// If the sentry exited (no longer in ckpt cgroup.procs) and its PID was
	// recycled elsewhere, restore must not move it into the sandbox leaf.
	mustWrite(t, filepath.Join(c.dir, "cgroup.procs"), "")
	mustWrite(t, filepath.Join(f.leaf, "cgroup.procs"), "100\n")
	if err := c.restore(); err != nil {
		t.Fatalf("restore with exited sentry: %v", err)
	}
	if got := mustRead(t, filepath.Join(f.leaf, "cgroup.procs")); got != "100\n" {
		t.Errorf("sandbox leaf cgroup.procs = %q, want unchanged \"100\\n\"", got)
	}

	// If the sentry exits between cgroupHasPID and movePID (ESRCH), restore returns nil.
	mustWrite(t, filepath.Join(c.dir, "cgroup.procs"), "101\n")
	movePID = func(string, int) error { return unix.ESRCH }
	if err := c.restore(); err != nil {
		t.Errorf("restore on ESRCH = %v, want nil", err)
	}
}

func TestCheckpointCgroupNilIsNoop(t *testing.T) {
	var c *checkpointCgroup
	if err := c.restore(); err != nil {
		t.Errorf("nil restore = %v", err)
	}
	if err := c.remove(); err != nil {
		t.Errorf("nil remove = %v", err)
	}
}

func TestCheckpointIsolated(t *testing.T) {
	t.Run("terminates before removing checkpoint cgroup on success", func(t *testing.T) {
		f := newCkptFixture(t, "101")
		ckptDir := checkpointCgroupDir(f.root, testActorUID)
		var terminated bool

		err := checkpointIsolated(
			context.Background(),
			true, f.root, testActorUID, f.actorDirs,
			func(context.Context) error {
				if got := mustRead(t, filepath.Join(ckptDir, "cgroup.procs")); got != "101" {
					t.Errorf("checkpoint cgroup.procs during checkpoint = %q, want 101", got)
				}
				return nil
			},
			func() error {
				terminated = true
				// Emulate container teardown emptying the checkpoint cgroup so rmdir succeeds.
				_ = os.Remove(filepath.Join(ckptDir, "cpu.max"))
				return os.Remove(filepath.Join(ckptDir, "cgroup.procs"))
			},
		)
		if err != nil {
			t.Fatalf("checkpointIsolated: %v", err)
		}
		if !terminated {
			t.Error("terminate was not called")
		}
		if _, err := os.Stat(ckptDir); !os.IsNotExist(err) {
			t.Errorf("checkpoint cgroup still exists after checkpointIsolated: stat err = %v", err)
		}
	})

	t.Run("skips cgroup isolation when cgroup is not delegated", func(t *testing.T) {
		f := newCkptFixture(t, "")
		ckptDir := checkpointCgroupDir(f.root, testActorUID)
		var terminated bool

		err := checkpointIsolated(
			context.Background(),
			false, f.root, testActorUID, f.actorDirs,
			func(context.Context) error {
				if _, err := os.Stat(ckptDir); !os.IsNotExist(err) {
					t.Errorf("checkpoint cgroup should not be created when !cgroupDelegated: %v", err)
				}
				return nil
			},
			func() error {
				terminated = true
				return nil
			},
		)
		if err != nil {
			t.Fatalf("checkpointIsolated: %v", err)
		}
		if !terminated {
			t.Error("terminate was not called")
		}
	})

	t.Run("restores sentry and keeps workload alive when checkpoint fails", func(t *testing.T) {
		f := newCkptFixture(t, "101")
		ckptDir := checkpointCgroupDir(f.root, testActorUID)
		var terminated bool

		err := checkpointIsolated(
			context.Background(),
			true, f.root, testActorUID, f.actorDirs,
			func(context.Context) error {
				return errors.New("checkpoint boom")
			},
			func() error {
				terminated = true
				return nil
			},
		)
		if err == nil || !strings.Contains(err.Error(), "checkpoint boom") {
			t.Fatalf("checkpointIsolated err = %v, want checkpoint boom", err)
		}
		if terminated {
			t.Error("terminate called after successful cgroup rollback")
		}
		if got := mustRead(t, filepath.Join(f.leaf, "cgroup.procs")); got != "101" {
			t.Errorf("sandbox leaf cgroup.procs = %q, want 101", got)
		}
		if _, err := os.Stat(ckptDir); !os.IsNotExist(err) {
			t.Errorf("checkpoint cgroup left behind: stat err = %v", err)
		}
	})

	t.Run("terminates workload if both checkpoint and cgroup restore fail", func(t *testing.T) {
		f := newCkptFixture(t, "101")
		ckptDir := checkpointCgroupDir(f.root, testActorUID)
		var terminated bool

		err := checkpointIsolated(
			context.Background(),
			true, f.root, testActorUID, f.actorDirs,
			func(context.Context) error {
				movePID = func(string, int) error { return unix.EACCES }
				return errors.New("checkpoint boom")
			},
			func() error {
				terminated = true
				_ = os.Remove(filepath.Join(ckptDir, "cpu.max"))
				return os.Remove(filepath.Join(ckptDir, "cgroup.procs"))
			},
		)
		if err == nil {
			t.Fatal("checkpointIsolated succeeded on checkpoint error")
		}
		if !terminated {
			t.Error("terminate was not called when restore failed; sentry would stay in unlimited cgroup")
		}
		if _, err := os.Stat(ckptDir); !os.IsNotExist(err) {
			t.Errorf("checkpoint cgroup left behind: stat err = %v", err)
		}
	})

	t.Run("terminates workload if checkpoint fails and the sandbox leaf is gone", func(t *testing.T) {
		f := newCkptFixture(t, "101")
		ckptDir := checkpointCgroupDir(f.root, testActorUID)
		var terminated bool

		err := checkpointIsolated(
			context.Background(),
			true, f.root, testActorUID, f.actorDirs,
			func(context.Context) error {
				if err := os.RemoveAll(f.leaf); err != nil {
					t.Fatal(err)
				}
				return errors.New("checkpoint boom")
			},
			func() error {
				terminated = true
				_ = os.Remove(filepath.Join(ckptDir, "cpu.max"))
				return os.Remove(filepath.Join(ckptDir, "cgroup.procs"))
			},
		)
		if err == nil {
			t.Fatal("checkpointIsolated succeeded on checkpoint error")
		}
		if !terminated {
			t.Error("terminate was not called with the sandbox leaf gone; sentry would stay in unlimited cgroup")
		}
		if _, err := os.Stat(ckptDir); !os.IsNotExist(err) {
			t.Errorf("checkpoint cgroup left behind: stat err = %v", err)
		}
	})

	t.Run("moves the sentry back into its sandbox leaf when terminate leaves it running", func(t *testing.T) {
		f := newCkptFixture(t, "101")
		ckptDir := checkpointCgroupDir(f.root, testActorUID)

		err := checkpointIsolated(
			context.Background(),
			true, f.root, testActorUID, f.actorDirs,
			func(context.Context) error { return nil },
			func() error {
				// Non-empty ckptDir emulates a sentry still alive in the cgroup.
				return errors.New("terminate boom")
			},
		)
		if err != nil {
			t.Fatalf("checkpointIsolated: %v", err)
		}
		if got := mustRead(t, filepath.Join(f.leaf, "cgroup.procs")); got != "101" {
			t.Errorf("sandbox leaf cgroup.procs = %q, want the live sentry 101 back under its memory.max", got)
		}
		if _, err := os.Stat(ckptDir); !os.IsNotExist(err) {
			t.Errorf("checkpoint cgroup left behind: stat err = %v", err)
		}
	})
}

func TestRemoveStaleCheckpointCgroups(t *testing.T) {
	root := t.TempDir()
	stale := checkpointCgroupDir(root, testActorUID)
	mustMkdir(t, stale)
	busy := checkpointCgroupDir(root, "busy")
	mustMkdir(t, busy)
	// A non-empty directory stands in for a cgroup that still has processes:
	// rmdir fails on both.
	mustWrite(t, filepath.Join(busy, "cgroup.procs"), strconv.Itoa(42))
	leaf := filepath.Join(root, ocispec.GVisorCgroupLeaf(testActorUID, ocispec.SandboxID))
	mustMkdir(t, leaf)

	removeStaleCheckpointCgroups(context.Background(), root)

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale checkpoint cgroup not removed: stat err = %v", err)
	}
	if _, err := os.Stat(busy); err != nil {
		t.Errorf("busy checkpoint cgroup removed: %v", err)
	}
	if _, err := os.Stat(leaf); err != nil {
		t.Errorf("sandbox leaf removed: %v", err)
	}
}

func newRestoreDir(t *testing.T) string {
	t.Helper()
	restoreDir := filepath.Join(t.TempDir(), "restore-dir")
	mustMkdir(t, filepath.Join(restoreDir, "fs"))
	for _, name := range []string{"checkpoint.img", "pages.img", "fs/pages.img", "fs/fscheckpoint.pb", "manifest.json"} {
		mustWrite(t, filepath.Join(restoreDir, name), "staged data")
	}
	return restoreDir
}

func captureEviction(t *testing.T) <-chan []snapshotcache.File {
	t.Helper()
	evicted := make(chan []snapshotcache.File, 1)
	orig := evictCheckpointCache
	t.Cleanup(func() { evictCheckpointCache = orig })
	evictCheckpointCache = func(targets []snapshotcache.File) { evicted <- targets }
	return evicted
}

func TestDropStagedRestoreFiles(t *testing.T) {
	restoreDir := newRestoreDir(t)
	evicted := captureEviction(t)

	waitCalled := false
	dropActorCheckpointCacheAsync(context.Background(), testActorUID, restoreDir, false, func(context.Context) error {
		waitCalled = true
		return errors.New("waitRestore should not be called when !preserveRestoreDir")
	})
	if waitCalled {
		t.Fatal("waitRestore was called when !preserveRestoreDir")
	}

	for _, name := range []string{"checkpoint.img", "pages.img", "fs/pages.img"} {
		if _, err := os.Stat(filepath.Join(restoreDir, name)); !os.IsNotExist(err) {
			t.Fatalf("staged %s still exists after dropActorCheckpointCacheAsync: err=%v", name, err)
		}
	}
	for _, name := range []string{"fs/fscheckpoint.pb", "manifest.json"} {
		if _, err := os.Stat(filepath.Join(restoreDir, name)); err != nil {
			t.Fatalf("%s should be preserved: %v", name, err)
		}
	}
	select {
	case <-evicted:
		t.Fatal("dropped files were also evicted")
	default:
	}
}

// TestDropSharedRestoreFilesAfterEviction pins the case of a restore dir
// whose files are hard links into atelet's snapshot cache: unlinking them at
// once would free nothing, since the cache's link keeps the inode and its
// pages charged to the actor's cgroup, so the files are evicted from the page
// cache after the restore's page loading, as a preserved dir is, and only
// then unlinked.
func TestDropSharedRestoreFilesAfterEviction(t *testing.T) {
	restoreDir := newRestoreDir(t)
	cache := filepath.Join(t.TempDir(), "cache")
	mustMkdir(t, cache)
	for _, name := range []string{"checkpoint.img", "pages.img"} {
		if err := os.Link(filepath.Join(restoreDir, name), filepath.Join(cache, name)); err != nil {
			t.Fatal(err)
		}
	}
	evicted := captureEviction(t)
	restoreDone := make(chan struct{})

	dropActorCheckpointCacheAsync(context.Background(), testActorUID, restoreDir, false, func(ctx context.Context) error {
		select {
		case <-restoreDone:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	// Nothing happens while the restore still loads pages from the files.
	select {
	case <-evicted:
		t.Fatal("evicted before the restore wait completed")
	case <-time.After(50 * time.Millisecond):
	}
	for _, name := range []string{"checkpoint.img", "pages.img", "fs/pages.img"} {
		if _, err := os.Stat(filepath.Join(restoreDir, name)); err != nil {
			t.Fatalf("%s unlinked before the restore wait completed: %v", name, err)
		}
	}

	close(restoreDone)
	select {
	case targets := <-evicted:
		if len(targets) != 3 {
			t.Errorf("evicted %d files, want the 3 data files", len(targets))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("eviction never ran after the restore wait completed")
	}
	deadline := time.Now().Add(5 * time.Second)
	for _, name := range []string{"checkpoint.img", "pages.img", "fs/pages.img"} {
		for {
			if _, err := os.Stat(filepath.Join(restoreDir, name)); os.IsNotExist(err) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s still exists after eviction", name)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	for _, name := range []string{"checkpoint.img", "pages.img"} {
		if _, err := os.Stat(filepath.Join(cache, name)); err != nil {
			t.Errorf("the cache's link to %s went with the restore dir's: %v", name, err)
		}
	}
}

func TestEvictPreservedRestoreDir(t *testing.T) {
	t.Run("evicts once the restore wait completes", func(t *testing.T) {
		restoreDir := newRestoreDir(t)
		evicted := captureEviction(t)
		restoreDone := make(chan struct{})

		dropActorCheckpointCacheAsync(context.Background(), testActorUID, restoreDir, true, func(ctx context.Context) error {
			select {
			case <-restoreDone:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})

		select {
		case <-evicted:
			t.Fatal("evicted before the restore wait completed")
		case <-time.After(50 * time.Millisecond):
		}
		close(restoreDone)
		select {
		case targets := <-evicted:
			var names []string
			for _, f := range targets {
				rel, _ := filepath.Rel(restoreDir, f.Path)
				names = append(names, rel)
			}
			slices.Sort(names)
			if want := []string{"checkpoint.img", "fs/pages.img", "pages.img"}; !slices.Equal(names, want) {
				t.Errorf("evicted %v, want %v", names, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("eviction did not run after the restore wait completed")
		}
		for _, name := range []string{"checkpoint.img", "pages.img", "fs/pages.img"} {
			if _, err := os.Stat(filepath.Join(restoreDir, name)); err != nil {
				t.Errorf("preserved %s was dropped: %v", name, err)
			}
		}
	})

	t.Run("skips eviction when the restore wait fails", func(t *testing.T) {
		restoreDir := newRestoreDir(t)
		evicted := captureEviction(t)
		waited := make(chan struct{})

		dropActorCheckpointCacheAsync(context.Background(), testActorUID, restoreDir, true, func(context.Context) error {
			defer close(waited)
			return errors.New("runsc wait boom")
		})

		<-waited
		select {
		case <-evicted:
			t.Fatal("evicted after the restore wait failed")
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("waits no longer than the actor is hosted", func(t *testing.T) {
		restoreDir := newRestoreDir(t)
		evicted := captureEviction(t)
		actorCtx, unhost := context.WithCancel(context.Background())
		waited := make(chan error, 1)

		dropActorCheckpointCacheAsync(actorCtx, testActorUID, restoreDir, true, func(ctx context.Context) error {
			<-ctx.Done()
			waited <- ctx.Err()
			return ctx.Err()
		})

		unhost()
		select {
		case err := <-waited:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("restore wait ended with %v, want context.Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("restore wait did not end when the actor was unhosted")
		}
		select {
		case <-evicted:
			t.Fatal("evicted after the actor was unhosted")
		case <-time.After(50 * time.Millisecond):
		}
	})
}
