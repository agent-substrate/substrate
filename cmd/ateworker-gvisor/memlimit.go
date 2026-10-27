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
	"strconv"
	"strings"
	"time"

	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/ateworker/cgroup"
	"go.ate.dev/substrate/internal/ocispec"
)

// With runc 1.5.2+ on systemd 253-258 the worker container's scope has
// OOMPolicy=kill, so any kernel OOM kill under it stops the whole worker, even
// one confined to a single actor's leaf. So the sandbox leaf gets memory.high
// at the actor's limit, which throttles without invoking the OOM killer, and
// we kill a sandbox that stays over with cgroup.kill, which systemd ignores.
// https://github.com/opencontainers/cgroups/blob/19a241326c83a5100dff94732733791a0c9e4720/systemd/v2.go#L366-L382

// memoryWatchInterval is how often sandbox leaves are checked.
const memoryWatchInterval = 250 * time.Millisecond

// throttleSandboxMemory swaps the memory.max runsc set on the sandbox leaf for
// a memory.high at the same limit. runsc reads memory.max back to size the
// sentry's --total-memory when create launches the sandbox, so this runs after
// the sandbox's create. On failure the leaf keeps its memory.max.
func (s *AteWorkerService) throttleSandboxMemory(ctx context.Context, hosted *hostedActor) {
	if !s.throttleMemory {
		return
	}
	limit, err := swapMemoryMaxForHigh(s.sandboxLeaf(hosted.attribution.UID))
	if err != nil {
		attrs := append(ateattr.ActorLogAttrs(hosted.attribution), slog.Any("err", err))
		slog.LogAttrs(ctx, slog.LevelWarn, "Sandbox keeps its memory.max, an OOM kill in it will restart the worker", attrs...)
		return
	}
	hosted.memoryLimit.Store(limit)
}

// psiEnabled reports whether the kernel tracks memory pressure for cgroups
// under root, which the watch needs to tell a stuck sandbox from a busy one.
func psiEnabled(root string) bool {
	_, err := os.Stat(filepath.Join(root, "memory.pressure"))
	return err == nil
}

// removeStaleSandboxLeaf removes a sandbox leaf an earlier activation of the
// actor left behind when its runsc delete never ran. runsc joins an existing
// cgroup as is, so the new sandbox would get no memory.max, a sentry sized to
// the host's RAM, and no watch.
// https://github.com/google/gvisor/blob/0068142bdeb9de91fe72bf9794204de6dec92cb4/runsc/cgroup/cgroup_v2.go#L138-L153
func (s *AteWorkerService) removeStaleSandboxLeaf(ctx context.Context, actorUID string) error {
	dir := s.sandboxLeaf(actorUID)
	killCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cgroup.KillLeafUnder(killCtx, s.cgroupRoot, filepath.Base(dir)); err != nil {
		return fmt.Errorf("while clearing a leftover sandbox cgroup: %w", err)
	}
	if err := os.Remove(dir); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("while removing leftover sandbox cgroup %q: %w", dir, err)
	}
	return nil
}

func (s *AteWorkerService) sandboxLeaf(actorUID string) string {
	return filepath.Join(s.cgroupRoot, ocispec.GVisorCgroupLeaf(actorUID, sandboxCgroupContainer))
}

// swapMemoryMaxForHigh returns the limit it moved, or 0 if the leaf has none.
func swapMemoryMaxForHigh(dir string) (uint64, error) {
	b, err := os.ReadFile(filepath.Join(dir, "memory.max"))
	if errors.Is(err, fs.ErrNotExist) {
		// memory controller not delegated
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	v := strings.TrimSpace(string(b))
	if v == "max" {
		return 0, nil
	}
	limit, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing memory.max %q: %w", v, err)
	}
	// high first, so the leaf is never unlimited
	if err := os.WriteFile(filepath.Join(dir, "memory.high"), []byte(v), 0o644); err != nil {
		return 0, err
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max"), 0o644); err != nil {
		return 0, err
	}
	return limit, nil
}

// watchMemory kills sandboxes that stay over their memory limit, until ctx is
// done.
func (s *AteWorkerService) watchMemory(ctx context.Context) {
	ticker := time.NewTicker(memoryWatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, h := range s.hostedActors() {
				s.checkMemory(ctx, h)
			}
		}
	}
}

// checkMemory kills h's sandbox if the watch finds it stuck at its limit. The
// kill runs in the background, holding h.memoryMu until the sandbox is gone.
func (s *AteWorkerService) checkMemory(ctx context.Context, h *hostedActor) {
	if !h.memoryMu.TryLock() {
		// being killed, or paused for a checkpoint
		return
	}
	limit := h.memoryLimit.Load()
	if limit == 0 || h.stopping.Load() || h.oomKilled.Load() {
		h.memoryMu.Unlock()
		return
	}
	dir := s.sandboxLeaf(h.attribution.UID)
	over, current, err := h.memoryWatch.Check(dir, limit, time.Now())
	if err != nil || !over {
		// a read fails when the sandbox is going away
		h.memoryMu.Unlock()
		return
	}
	h.oomKilled.Store(true)
	attrs := append(ateattr.ActorLogAttrs(h.attribution),
		slog.Uint64("memoryLimitBytes", limit),
		slog.Uint64(string(ateattr.StatsMemoryUsageKey), current))
	slog.LogAttrs(ctx, slog.LevelWarn, "Killing a sandbox over its memory limit", attrs...)
	go func() {
		defer h.memoryMu.Unlock()
		killCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := cgroup.KillLeafUnder(killCtx, s.cgroupRoot, filepath.Base(dir)); err != nil {
			attrs = append(attrs, slog.Any("err", err))
			slog.LogAttrs(ctx, slog.LevelError, "Failed to kill a sandbox over its memory limit", attrs...)
		}
	}()
}

// pauseMemoryWatch stops the watch on h's sandbox leaf, e.g. while its sentry
// sits in the checkpoint cgroup, waiting out a kill in progress. It returns
// the limit to resume with.
func pauseMemoryWatch(h *hostedActor) uint64 {
	h.memoryMu.Lock()
	defer h.memoryMu.Unlock()
	return h.memoryLimit.Swap(0)
}

// resumeMemoryWatch watches h's sandbox leaf again, from a fresh window.
func resumeMemoryWatch(h *hostedActor, limit uint64) {
	h.memoryMu.Lock()
	defer h.memoryMu.Unlock()
	h.memoryWatch = cgroup.MemoryWatch{}
	h.memoryLimit.Store(limit)
}
