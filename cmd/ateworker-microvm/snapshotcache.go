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
	"log/slog"
	"strings"

	"github.com/agent-substrate/substrate/internal/snapshotcache"
)

const memoryRangesFile = "memory-ranges"

func isSnapshotDataFile(name string, includeMemory bool) bool {
	if includeMemory && name == memoryRangesFile {
		return true
	}
	return strings.HasSuffix(name, ".tar")
}

// dropStagedMemoryImage drops the staged memory-ranges file and already-extracted
// tar archives in a scratch restoreDir once an eager ("Copy") restore has
// finished reading guest memory.
func dropStagedMemoryImage(ctx context.Context, restoreDir string) {
	targets := snapshotcache.FindFiles(restoreDir, func(name string) bool {
		return isSnapshotDataFile(name, true)
	})
	if len(targets) == 0 {
		return
	}
	snapshotcache.DropFiles(targets)
	slog.InfoContext(ctx, "dropped the staged memory image (eager restore needs no merge base)")
}

// dropActorSnapshotCacheAsync pins the actor's snapshot files in restoreDir
// while the caller holds the actor lock, then evicts their clean pages from the
// host page cache in the background.
func dropActorSnapshotCacheAsync(restoreDir string, includeMemory bool) {
	targets := snapshotcache.FindFiles(restoreDir, func(name string) bool {
		return isSnapshotDataFile(name, includeMemory)
	})
	if len(targets) == 0 {
		return
	}
	go snapshotcache.EvictWithRetries(targets)
}
