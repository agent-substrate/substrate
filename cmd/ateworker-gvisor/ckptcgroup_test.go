//go:build linux

// Copyright 2026 The Agent Substrate Authors
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
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/ateworker-gvisor/internal/cgroupstats"
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

// memoryHigh returns the value last written to dir's memory.high.
func memoryHigh(t *testing.T, dir string) string {
	t.Helper()
	value, _, _ := strings.Cut(mustRead(t, filepath.Join(dir, "memory.high")), "\n")
	return value
}

func TestIsolateCheckpointCacheMovesOnlyTheSentry(t *testing.T) {
	f := newCkptFixture(t, "101")

	c, err := isolateCheckpointCache(f.root, f.actorDirs, testActorUID, 0)
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

	if _, err := isolateCheckpointCache(f.root, f.actorDirs, testActorUID, 0); err == nil {
		t.Fatal("isolateCheckpointCache succeeded with no sandbox pid file")
	}
	if _, err := os.Stat(checkpointCgroupDir(f.root, testActorUID)); !os.IsNotExist(err) {
		t.Errorf("checkpoint cgroup left behind: stat err = %v", err)
	}
}

func TestIsolateCheckpointCacheRejectsPIDOutsideSandboxLeaf(t *testing.T) {
	f := newCkptFixture(t, "101")
	mustWrite(t, filepath.Join(f.leaf, "cgroup.procs"), "100\n")

	if _, err := isolateCheckpointCache(f.root, f.actorDirs, testActorUID, 0); err == nil {
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
	c, err := isolateCheckpointCache(f.root, f.actorDirs, testActorUID, 0)
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
	c, err := isolateCheckpointCache(f.root, f.actorDirs, testActorUID, 0)
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
			f.root, testActorUID, 0, f.actorDirs, nil, nil,
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

	t.Run("checkpoints without isolation when the sandbox pid is unknown", func(t *testing.T) {
		f := newCkptFixture(t, "")
		ckptDir := checkpointCgroupDir(f.root, testActorUID)
		var terminated bool

		err := checkpointIsolated(
			context.Background(),
			f.root, testActorUID, 0, f.actorDirs, nil, nil,
			func(context.Context) error {
				if _, err := os.Stat(ckptDir); !os.IsNotExist(err) {
					t.Errorf("checkpoint cgroup should not be created without a sandbox pid: %v", err)
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
			f.root, testActorUID, 0, f.actorDirs, nil, nil,
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
			f.root, testActorUID, 0, f.actorDirs, nil, nil,
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
			f.root, testActorUID, 0, f.actorDirs, nil, nil,
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
			f.root, testActorUID, 0, f.actorDirs, nil, nil,
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

// liftableActor returns a hosted actor where the sandbox leaf is
// limited by memory.high alone.
func liftableActor(t *testing.T, f ckptFixture) *hostedActor {
	t.Helper()
	mustWrite(t, filepath.Join(f.leaf, "memory.high"), "805306368\n")
	leafMemory(t, f, 524288000, 629145600)
	return readActor(t, f, "805306368")
}

// readActor returns a hosted actor with memoryHigh, and a reading of the
// fixture's sandbox leaf.
func readActor(t *testing.T, f ckptFixture, memoryHigh string) *hostedActor {
	t.Helper()
	h := &hostedActor{memoryHigh: memoryHigh}
	if _, err := h.leaf.read(f.leaf, cgroupstats.Read); err != nil {
		t.Fatal(err)
	}
	return h
}

// leafMemory sets the memory.current and memory.peak of the fixture's sandbox
// leaf.
func leafMemory(t *testing.T, f ckptFixture, current, peak uint64) {
	t.Helper()
	mustWrite(t, filepath.Join(f.leaf, "memory.current"), strconv.FormatUint(current, 10)+"\n")
	mustWrite(t, filepath.Join(f.leaf, "memory.peak"), strconv.FormatUint(peak, 10)+"\n")
}

const slowCheckpointMsg = "Slow checkpoint: moving the sentry to a checkpoint cgroup"

// resetSlowCheckpointWarnings resets the slowCheckpointReason sync.Once's.
func resetSlowCheckpointWarnings() {
	for _, r := range []*slowCheckpointReason{slowCacheLimit, slowNotHosted, slowMemoryMax, slowLiftFailed} {
		r.warned = sync.Once{}
	}
}

func TestCheckpointIsolatedLiftsMemoryHigh(t *testing.T) {
	t.Run("keeps the sentry in its leaf when the cache needs no limit", func(t *testing.T) {
		logs := captureWarnings(t)
		f := newCkptFixture(t, "101")
		ckptDir := checkpointCgroupDir(f.root, testActorUID)
		hosted := liftableActor(t, f)
		// The sandbox grew since that reading, which the frozen leaf repeats.
		leafMemory(t, f, 534773760, 629145600)
		var terminated bool

		err := checkpointIsolated(
			context.Background(),
			f.root, testActorUID, 0, f.actorDirs, hosted, nil,
			func(context.Context) error {
				if got := memoryHigh(t, f.leaf); got != "max" {
					t.Errorf("sandbox leaf memory.high during checkpoint = %q, want max", got)
				}
				// Emulate the snapshot's page cache.
				leafMemory(t, f, 1048576000, 1153433600)
				if got, err := hosted.leaf.read(f.leaf, cgroupstats.Read); err != nil || got.MemoryCurrentBytes != 524288000 || got.MemoryPeakBytes != 629145600 {
					t.Errorf("sandbox leaf usage during checkpoint = current %d peak %d, err %v, want current 524288000 peak 629145600 from before it",
						got.MemoryCurrentBytes, got.MemoryPeakBytes, err)
				}
				if _, err := os.Stat(ckptDir); !os.IsNotExist(err) {
					t.Errorf("checkpoint cgroup created: stat err = %v", err)
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
		if strings.Contains(logs.String(), slowCheckpointMsg) {
			t.Errorf("warned of a slow checkpoint without moving the sentry: %s", logs)
		}
		// runsc delete removes the leaf when terminate succeeds.
		// For the test fixture though, check nothing gets written
		// to it again.
		if got := memoryHigh(t, f.leaf); got != "max" {
			t.Errorf("sandbox leaf memory.high after terminate = %q, want max: written again", got)
		}
		if got, err := hosted.leaf.read(f.leaf, cgroupstats.Read); err != nil || got.MemoryCurrentBytes != 1048576000 {
			t.Errorf("sandbox leaf usage after terminate = current %d, err %v, want current 1048576000: still frozen", got.MemoryCurrentBytes, err)
		}
	})

	t.Run("writes nothing when the leaf has no memory.high to lift", func(t *testing.T) {
		f := newCkptFixture(t, "101")
		ckptDir := checkpointCgroupDir(f.root, testActorUID)

		err := checkpointIsolated(
			context.Background(),
			f.root, testActorUID, 0, f.actorDirs, &hostedActor{memoryHigh: "max"}, nil,
			func(context.Context) error {
				if _, err := os.Stat(ckptDir); !os.IsNotExist(err) {
					t.Errorf("checkpoint cgroup created: stat err = %v", err)
				}
				return nil
			},
			func() error { return nil },
		)
		if err != nil {
			t.Fatalf("checkpointIsolated: %v", err)
		}
	})

	t.Run("puts memory.high back when terminate leaves the sandbox leaf", func(t *testing.T) {
		f := newCkptFixture(t, "101")

		err := checkpointIsolated(
			context.Background(),
			f.root, testActorUID, 0, f.actorDirs, liftableActor(t, f), nil,
			func(context.Context) error { return nil },
			func() error { return errors.New("terminate boom") },
		)
		if err != nil {
			t.Fatalf("checkpointIsolated: %v", err)
		}
		if got := memoryHigh(t, f.leaf); got != "805306368" {
			t.Errorf("sandbox leaf memory.high after failed terminate = %q, want 805306368", got)
		}
	})

	saving := fmt.Errorf("runsc killed: %w", errSandboxSaving)
	for _, tc := range []struct {
		name string
		// err is what checkpoint fails with.
		err error
		// wantWait is whether it waits for the sandbox to exit, and exits
		// whether the sandbox does.
		wantWait, exits bool
	}{
		{name: "puts memory.high back and keeps workload alive when checkpoint fails", err: errors.New("checkpoint failed")},
		{name: "puts memory.high back when a sandbox left saving outlives the wait", err: saving, wantWait: true},
		{name: "keeps the readings of a sandbox left saving frozen once it exits", err: saving, wantWait: true, exits: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCkptFixture(t, "101")
			hosted := liftableActor(t, f)
			var terminated, waited bool

			err := checkpointIsolated(
				context.Background(),
				f.root, testActorUID, 0, f.actorDirs, hosted,
				func() bool {
					waited = true
					if got := memoryHigh(t, f.leaf); got != "max" {
						t.Errorf("sandbox leaf memory.high while the sandbox saves = %q, want max", got)
					}
					return tc.exits
				},
				func(context.Context) error {
					// Emulate the page cache of a partial snapshot, reclaimed since.
					leafMemory(t, f, 419430400, 1153433600)
					return tc.err
				},
				func() error {
					terminated = true
					return nil
				},
			)
			if !errors.Is(err, tc.err) {
				t.Fatalf("checkpointIsolated = %v, want %v", err, tc.err)
			}
			if waited != tc.wantWait {
				t.Errorf("waited for the sandbox to exit: %t, want %t", waited, tc.wantWait)
			}
			if terminated {
				t.Error("terminate was called after a failed checkpoint")
			}
			wantHigh, wantCurrent := "805306368", uint64(419430400)
			if tc.exits {
				wantHigh, wantCurrent = "max", 524288000
			}
			if got := memoryHigh(t, f.leaf); got != wantHigh {
				t.Errorf("sandbox leaf memory.high after failed checkpoint = %q, want %q", got, wantHigh)
			}
			got, err := hosted.leaf.read(f.leaf, cgroupstats.Read)
			if err != nil {
				t.Fatalf("reading the sandbox leaf after failed checkpoint: %v", err)
			}
			if got.MemoryCurrentBytes != wantCurrent {
				t.Errorf("sandbox leaf usage after failed checkpoint = current %d, want current %d", got.MemoryCurrentBytes, wantCurrent)
			}
		})
	}

	for _, tc := range []struct {
		name string
		// memoryHigh is what the worker holds as the leaf's memory.high, and
		// leafHigh the file. An empty leafHigh leaves the file out.
		memoryHigh, leafHigh string
		cacheHigh            int64
		// unhosted leaves the actor without a hostedActor.
		unhosted bool
		// wantReason is part of the reason the slow checkpoint warning gives,
		// and wantErr part of its error, if it has one.
		wantReason, wantErr string
	}{
		{name: "the cache needs a limit", memoryHigh: "805306368", leafHigh: "805306368\n", cacheHigh: 64 << 20, wantReason: "needs a memory limit"},
		{name: "the worker does not host the actor", leafHigh: "805306368\n", unhosted: true, wantReason: "does not host the actor"},
		{name: "the leaf may have a memory.max", leafHigh: "max\n", wantReason: "may have a memory.max"},
		{name: "the leaf's memory.high cannot be lifted", memoryHigh: "805306368", wantReason: "memory.high failed", wantErr: "no such file or directory"},
	} {
		t.Run("moves the sentry when "+tc.name, func(t *testing.T) {
			resetSlowCheckpointWarnings()
			logs := captureWarnings(t)
			f := newCkptFixture(t, "101")
			if tc.leafHigh != "" {
				mustWrite(t, filepath.Join(f.leaf, "memory.high"), tc.leafHigh)
			}
			leafMemory(t, f, 524288000, 629145600)
			ckptDir := checkpointCgroupDir(f.root, testActorUID)
			var hosted *hostedActor
			if !tc.unhosted {
				hosted = readActor(t, f, tc.memoryHigh)
			}

			err := checkpointIsolated(
				context.Background(),
				f.root, testActorUID, tc.cacheHigh, f.actorDirs, hosted, nil,
				func(context.Context) error {
					if got := mustRead(t, filepath.Join(ckptDir, "cgroup.procs")); got != "101" {
						t.Errorf("checkpoint cgroup.procs during checkpoint = %q, want 101", got)
					}
					if hosted != nil {
						leafMemory(t, f, 419430400, 629145600)
						if got, err := hosted.leaf.read(f.leaf, cgroupstats.Read); err != nil || got.MemoryCurrentBytes != 419430400 {
							t.Errorf("sandbox leaf usage during checkpoint = current %d, err %v, want current 419430400", got.MemoryCurrentBytes, err)
						}
					}
					if tc.leafHigh == "" {
						return nil
					}
					if got := mustRead(t, filepath.Join(f.leaf, "memory.high")); got != tc.leafHigh {
						t.Errorf("sandbox leaf memory.high during checkpoint = %q, want unchanged %q", got, tc.leafHigh)
					}
					return nil
				},
				func() error {
					// Emulate container teardown emptying the checkpoint cgroup so rmdir succeeds.
					_ = os.Remove(filepath.Join(ckptDir, "cpu.max"))
					return os.Remove(filepath.Join(ckptDir, "cgroup.procs"))
				},
			)
			if err != nil {
				t.Fatalf("checkpointIsolated: %v", err)
			}
			if got := logs.String(); !strings.Contains(got, slowCheckpointMsg) || !strings.Contains(got, tc.wantReason) || !strings.Contains(got, tc.wantErr) {
				t.Errorf("warnings = %s, want %q with a reason containing %q and an error containing %q", got, slowCheckpointMsg, tc.wantReason, tc.wantErr)
			}
		})
	}
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

func TestIsolateCheckpointCacheLimitsTheCache(t *testing.T) {
	f := newCkptFixture(t, "101")
	// cgroupfs provides memory.high itself
	dir := checkpointCgroupDir(f.root, testActorUID)
	mustMkdir(t, dir)
	mustWrite(t, filepath.Join(dir, "memory.high"), "max\n")
	if _, err := isolateCheckpointCache(f.root, f.actorDirs, testActorUID, 64<<20); err != nil {
		t.Fatalf("isolateCheckpointCache: %v", err)
	}
	if got := memoryHigh(t, dir); got != "67108864" {
		t.Errorf("checkpoint memory.high = %q, want 67108864", got)
	}
	// a leftover cgroup reused without a limit drops the old one
	if _, err := isolateCheckpointCache(f.root, f.actorDirs, testActorUID, 0); err != nil {
		t.Fatalf("isolateCheckpointCache: %v", err)
	}
	if got := memoryHigh(t, dir); got != "max" {
		t.Errorf("checkpoint memory.high = %q, want max", got)
	}
}
