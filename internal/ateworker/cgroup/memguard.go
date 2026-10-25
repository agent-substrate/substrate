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

package cgroup

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// With runc 1.5.2+ on systemd 253-258 the worker container's scope has
// OOMPolicy=kill, so any kernel OOM kill under it stops the whole worker, even
// one confined to a single actor's cgroup. So an actor's cgroup gets
// memory.high at its limit, which throttles without invoking the OOM killer,
// and a MemoryGuard kills one that stays stuck there with cgroup.kill, which
// systemd ignores.
// https://github.com/opencontainers/cgroups/blob/19a241326c83a5100dff94732733791a0c9e4720/systemd/v2.go#L366-L382

// MemoryGuardInterval is how often a MemoryGuard should be checked.
const MemoryGuardInterval = 250 * time.Millisecond

// MemoryGuard kills one cgroup once a MemoryWatch finds it stuck at its
// memory.high. It does nothing until Resume arms it with the limit.
type MemoryGuard struct {
	limit  atomic.Uint64
	killed atomic.Bool
	// mu is held while a check or a kill runs, and guards watch
	mu    sync.Mutex
	watch MemoryWatch
}

// Kill is a kill a MemoryGuard started.
type Kill struct {
	Limit, Current uint64
	// Done receives the kill's result once the cgroup is empty or the kill
	// gives up.
	Done <-chan error
}

// Check kills the cgroup root/name in the background if it is stuck at its
// memory.high, and reports whether it did. A guard that is paused, already
// killing, or has killed does nothing.
func (g *MemoryGuard) Check(ctx context.Context, root, name string) (Kill, bool) {
	if !g.mu.TryLock() {
		return Kill{}, false
	}
	limit := g.limit.Load()
	if limit == 0 || g.killed.Load() {
		g.mu.Unlock()
		return Kill{}, false
	}
	over, current, err := g.watch.Check(filepath.Join(root, name), limit, time.Now())
	if err != nil || !over {
		// a read fails when the cgroup is going away
		g.mu.Unlock()
		return Kill{}, false
	}
	g.killed.Store(true)
	done := make(chan error, 1)
	go func() {
		defer g.mu.Unlock()
		killCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		done <- KillLeafUnder(killCtx, root, name)
	}()
	return Kill{Limit: limit, Current: current, Done: done}, true
}

// Killed reports whether the guard killed its cgroup.
func (g *MemoryGuard) Killed() bool {
	return g.killed.Load()
}

// Pause stops the guard, waiting out a kill in progress, and returns the limit
// to resume with.
func (g *MemoryGuard) Pause() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.limit.Swap(0)
}

// Resume arms the guard for limit, from a fresh window. A limit of 0 leaves it
// paused.
func (g *MemoryGuard) Resume(limit uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.watch = MemoryWatch{}
	g.limit.Store(limit)
}

// PSIEnabled reports whether the kernel tracks memory pressure for cgroups
// under root, which a MemoryWatch needs to tell a stuck cgroup from a busy one.
func PSIEnabled(root string) bool {
	_, err := os.Stat(filepath.Join(root, "memory.pressure"))
	return err == nil
}
