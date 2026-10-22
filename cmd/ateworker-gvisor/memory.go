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
	"go.ate.dev/substrate/internal/ateworker/cgroup"
	"go.ate.dev/substrate/internal/ocispec"
)

// checkpointWriteHeadroom is how far a checkpoint's page cache may grow when
// the actor's booking leaves less than that, and checkpointWriteSlots how many
// checkpoints may write at once. memoryReservedBytes covers both.
const (
	checkpointWriteHeadroom = 16 << 20
	checkpointWriteSlots    = 2
)

// memoryReservedBytes is what the worker keeps from actors: its own process,
// and the page cache of the checkpoints it lets write at once, which leaves
// the actor's cgroup with its sentry (see isolateCheckpointCache).
const memoryReservedBytes = 32<<20 + checkpointWriteSlots*checkpointWriteHeadroom

// unbookedMemory is the worker's reported memory capacity less what its
// actors booked.
func (s *AteWorkerService) unbookedMemory(podLimit int64) int64 {
	unbooked := podLimit - memoryReservedBytes
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	for _, h := range s.actors {
		unbooked -= h.memoryBytes
	}
	return unbooked
}

// checkpointCacheLimit returns the memory.high for the actor's checkpoint
// cgroup, 0 for none, and what it borrowed for it.
//
// The checkpoint writes about as much as the sandbox holds through the page
// cache, charged to the checkpoint cgroup. At the worker pod's memory limit
// that can OOM-kill the whole pod, so the cache may only grow into what the
// actor booked and the sandbox left untouched, plus what it can borrow. When
// the pod can lend all of it there is no limit: memory.high also shrinks the
// cgroup's dirty page limit, which throttles the write to the disk.
func (s *AteWorkerService) checkpointCacheLimit(hosted *hostedActor, actorUID string) (high, borrowed int64) {
	used, err := cgroup.ActorLeafMemory(ocispec.GVisorCgroupLeaf(actorUID, ocispec.SandboxID))
	if err != nil {
		return checkpointWriteHeadroom, 0
	}
	borrowed = s.borrow.Take(used)
	if borrowed >= used {
		return 0, borrowed
	}
	var booked int64
	if hosted != nil {
		booked = hosted.memoryBytes
	}
	return max(checkpointWriteHeadroom, booked-used) + borrowed, borrowed
}
