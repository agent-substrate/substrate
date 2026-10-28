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
	"crypto/rand"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/child"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

const (
	// rootfsUpperDeletingPrefix names a rootfs upper that a checkpoint moved
	// aside for its background teardown to delete. No activation uses one.
	rootfsUpperDeletingPrefix = "rootfs-upper.deleting."
	// teardownReapTimeout bounds the wait for the killed VMM and virtiofsd.
	// The cgroup kill in cleanupSandboxState handles anything left after it.
	teardownReapTimeout = time.Minute
	// teardownCleanupTimeout bounds the cleanup that follows the reap.
	teardownCleanupTimeout = 30 * time.Second
)

// asideUpper is a rootfs upper moved aside, with the device and inode it had
// right after the rename.
type asideUpper struct {
	path     string
	dev, ino uint64
}

// beginAsyncTeardown runs the part of a checkpoint's teardown that must finish
// before the reply. The actor dir is shared by every worker pod on the node,
// so every change to it happens here: the VM is killed without waiting, the
// sandbox and bundle mounts are detached, and the rootfs upper is moved aside.
// The actor leaves the hosted set but keeps its draining slot and its memory
// booking.
//
// The returned func starts the rest in the background. That part touches only
// this worker's processes, /run dirs, cgroup and network namespace, and the
// moved-aside upper. It holds the actor's lock and the checkpoint's inFlight
// token until it ends, so the actor's next RPC and WaitIdle both wait for it.
func (s *AteWorkerService) beginAsyncTeardown(ctx context.Context, actorUID string, actorDirs *ateworkerpb.ActorDirs) func(unlock, releaseInFlight func()) {
	ra := s.runningVM(actorUID)
	s.setGuestStats(actorUID, nil)
	if ra != nil {
		ra.stopping.Store(true)
		s.actorsMu.Lock()
		agent := ra.guestAgent
		ra.guestAgent = nil
		s.actorsMu.Unlock()
		if agent != nil {
			_ = agent.Close()
		}
		if ra.chProc != nil {
			ra.chProc.Kill()
		}
		if ra.vfsdProc != nil {
			ra.vfsdProc.Kill()
		}
	}
	for _, dir := range []string{kata.SharedDir(actorUID), actorDirs.GetOciBundleDir()} {
		if err := imagecache.UnmountAllUnder(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.WarnContext(ctx, "Failed to detach the actor's mounts", slog.String("actorUID", actorUID), slog.String("dir", dir), slog.Any("err", err))
		}
	}
	aside := moveRootfsUpperAside(ctx, actorUID, actorDirs)
	hosted := s.detachForTeardown(actorUID)

	ctx = context.WithoutCancel(ctx)
	return func(unlock, releaseInFlight func()) {
		handedOff := time.Now()
		s.startTeardown(ctx, hosted, unlock, releaseInFlight, func() {
			start := time.Now()
			if ra != nil {
				waitReaped(ctx, actorUID, ra.chProc, ra.vfsdProc)
			}
			// The budget starts after the reap, so a slow reap still leaves
			// time for the cgroup and network cleanup.
			ctx, cancel := context.WithTimeout(ctx, teardownCleanupTimeout)
			defer cancel()
			s.cleanupSandboxState(ctx, actorUID)
			if hosted != nil {
				_ = s.finishUnhostActor(ctx, actorUID, hosted)
			}
			// A later activation, on this pod or another, may have made a new
			// live upper by now. Only the moved-aside ones are removed.
			_, err := os.Lstat(rootfsUpperDir(actorDirs))
			skipped := removeAsideUpper(ctx, actorUID, aside)
			swept := sweepAsideUppers(ctx, actorUID, actorDirs, aside)
			slog.InfoContext(ctx, "Checkpoint teardown finished", slog.String("actorUID", actorUID),
				slog.Duration("queued", start.Sub(handedOff)), slog.Duration("took", time.Since(start)),
				slog.Bool("rehosted", err == nil), slog.Bool("guard_skipped", skipped), slog.Int("stale_swept", swept))
		})
	}
}

// detachForTeardown is detachHostedActor for a background teardown. The
// actor's memory stays booked until releaseTeardown, since its killed VM may
// still hold it.
func (s *AteWorkerService) detachForTeardown(actorUID string) *hostedActor {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	hosted := s.detachHostedActorLocked(actorUID)
	if hosted != nil {
		s.teardownBooked += actorBooking(hosted.memoryBytes)
	}
	return hosted
}

// releaseTeardown returns the memory booking and draining slot that
// detachForTeardown kept.
func (s *AteWorkerService) releaseTeardown(hosted *hostedActor) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	s.teardownBooked -= actorBooking(hosted.memoryBytes)
	s.draining--
}

// startTeardown runs stageB in the background once a teardown slot is free.
// It then releases what detachForTeardown kept for hosted (if non-nil), the
// inFlight token and the actor's lock, in that order, even if stageB panics.
func (s *AteWorkerService) startTeardown(ctx context.Context, hosted *hostedActor, unlock, releaseInFlight func(), stageB func()) {
	go func() {
		defer unlock()
		defer releaseInFlight()
		if hosted != nil {
			defer s.releaseTeardown(hosted)
		}
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "Checkpoint teardown panicked", slog.Any("panic", r))
			}
		}()
		s.teardownSlots <- struct{}{}
		defer func() { <-s.teardownSlots }()
		stageB()
	}()
}

// waitReaped waits for the killed processes to be reaped, up to
// teardownReapTimeout.
func waitReaped(ctx context.Context, actorUID string, procs ...*child.Process) {
	timeout := time.NewTimer(teardownReapTimeout)
	defer timeout.Stop()
	for _, p := range procs {
		if p == nil {
			continue
		}
		select {
		case <-p.Done():
		case <-timeout.C:
			slog.WarnContext(ctx, "Killed VM process not reaped in time", slog.String("actorUID", actorUID))
			return
		}
	}
}

// moveRootfsUpperAside renames the rootfs upper to a path unique across the
// node's worker pods, in the same dir. If the rename fails, it removes the
// upper instead. It returns nil when nothing is left to delete later.
func moveRootfsUpperAside(ctx context.Context, actorUID string, actorDirs *ateworkerpb.ActorDirs) *asideUpper {
	live := rootfsUpperDir(actorDirs)
	path := filepath.Join(actorDirs.GetRootDir(), rootfsUpperDeletingPrefix+rand.Text())
	if err := os.Rename(live, path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.WarnContext(ctx, "Failed to move the rootfs upper aside; removing it now", slog.String("actorUID", actorUID), slog.Any("err", err))
			if err := os.RemoveAll(live); err != nil {
				slog.WarnContext(ctx, "Failed to remove rootfs upper dir", slog.String("actorUID", actorUID), slog.Any("err", err))
			}
		}
		return nil
	}
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		// Without its identity the guard cannot tell it apart; the sweep of a
		// later teardown removes it.
		slog.WarnContext(ctx, "Cannot stat the moved-aside rootfs upper", slog.String("actorUID", actorUID), slog.Any("err", err))
		return &asideUpper{path: path}
	}
	return &asideUpper{path: path, dev: uint64(st.Dev), ino: st.Ino}
}

// removeAsideUpper removes a moved-aside upper if it is still the directory
// moveRootfsUpperAside left there. It reports whether it skipped the removal.
func removeAsideUpper(ctx context.Context, actorUID string, a *asideUpper) bool {
	if a == nil {
		return false
	}
	var st unix.Stat_t
	if err := unix.Lstat(a.path, &st); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false
		}
		slog.WarnContext(ctx, "Cannot stat the moved-aside rootfs upper", slog.String("actorUID", actorUID), slog.Any("err", err))
		return true
	}
	if a.ino == 0 || st.Mode&unix.S_IFMT != unix.S_IFDIR || uint64(st.Dev) != a.dev || st.Ino != a.ino {
		slog.WarnContext(ctx, "Leaving a moved-aside rootfs upper that changed", slog.String("actorUID", actorUID), slog.String("path", a.path))
		return true
	}
	if err := os.RemoveAll(a.path); err != nil {
		slog.WarnContext(ctx, "Failed to remove the moved-aside rootfs upper", slog.String("actorUID", actorUID), slog.Any("err", err))
	}
	return false
}

// sweepAsideUppers removes the moved-aside uppers that earlier teardowns left,
// such as one whose worker exited first. It leaves keep, which
// removeAsideUpper has handled. It returns how many it removed.
func sweepAsideUppers(ctx context.Context, actorUID string, actorDirs *ateworkerpb.ActorDirs, keep *asideUpper) int {
	entries, err := os.ReadDir(actorDirs.GetRootDir())
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.WarnContext(ctx, "Cannot list the actor dir", slog.String("actorUID", actorUID), slog.Any("err", err))
		}
		return 0
	}
	n := 0
	for _, e := range entries {
		path := filepath.Join(actorDirs.GetRootDir(), e.Name())
		if !strings.HasPrefix(e.Name(), rootfsUpperDeletingPrefix) || (keep != nil && path == keep.path) {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			slog.WarnContext(ctx, "Failed to remove a stale rootfs upper", slog.String("actorUID", actorUID), slog.Any("err", err))
			continue
		}
		n++
	}
	return n
}
