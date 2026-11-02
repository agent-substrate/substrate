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
	"log/slog"
	"time"

	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/ateworker/cgroup"
)

// leafMemoryHigh is the memory.high for an actor's cgroup, or 0 for none: what
// the actor books on the worker. The guest can't use more than its memory
// limit, so past that it's cloud-hypervisor or virtiofsd growing, which would
// otherwise take the whole worker pod to its limit (see cgroup.MemoryGuard).
func (s *AteWorkerService) leafMemoryHigh(memoryBytes int64) int64 {
	if !s.throttleMemory || memoryBytes <= 0 {
		return 0
	}
	return actorBooking(memoryBytes)
}

// watchMemory kills actors stuck at their cgroup's memory.high, until ctx is
// done.
func (s *AteWorkerService) watchMemory(ctx context.Context) {
	ticker := time.NewTicker(cgroup.MemoryGuardInterval)
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

// checkMemory kills h's VMM and virtiofsd if their cgroup is stuck at its
// memory.high. watchVM then reports the actor crashed.
func (s *AteWorkerService) checkMemory(ctx context.Context, h *hostedActor) {
	s.actorsMu.RLock()
	ra := h.vm
	s.actorsMu.RUnlock()
	if ra == nil || ra.stopping.Load() {
		return
	}
	kill, ok := ra.memory.Check(ctx, cgroup.Root, h.attribution.UID)
	if !ok {
		return
	}
	attrs := append(ateattr.ActorLogAttrs(h.attribution),
		slog.Uint64("memoryLimitBytes", kill.Limit),
		slog.Uint64(string(ateattr.StatsMemoryUsageKey), kill.Current))
	slog.LogAttrs(ctx, slog.LevelWarn, "Killing a microVM over its memory limit", attrs...)
	go func() {
		if err := <-kill.Done; err != nil {
			attrs = append(attrs, slog.Any("err", err))
			slog.LogAttrs(ctx, slog.LevelError, "Failed to kill a microVM over its memory limit", attrs...)
		}
	}()
}
