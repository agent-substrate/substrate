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
	"io/fs"
	"log/slog"
	"math"
	"path/filepath"

	"go.ate.dev/substrate/internal/snapshotcache"
	"go.ate.dev/substrate/internal/tarutil"
)

// unbookedMemory is the worker's reported memory capacity less what its
// actors booked, overhead included.
func (s *AteWorkerService) unbookedMemory(podLimit int64) int64 {
	unbooked := podLimit - memoryReservedBytes
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	for _, h := range s.actors {
		unbooked -= actorBooking(h.memoryBytes)
	}
	return unbooked
}

// actorBooking is what the control plane books for an actor with this memory
// limit: the limit plus actorOverhead.
func actorBooking(memoryBytes int64) int64 {
	const gib = 1 << 30
	perGiB := actorOverhead.GetMemoryBytesPerGib()
	return memoryBytes + actorOverhead.GetMemoryBytes() + memoryBytes/gib*perGiB + memoryBytes%gib*perGiB/gib
}

// archiveDir archives srcDir to tarPath. When the pod can lend the whole
// directory, the archive goes to the page cache without a write-behind, and
// the loan comes back once it's on disk. Otherwise its dirty pages are bounded.
func (s *AteWorkerService) archiveDir(ctx context.Context, tarPath, srcDir string) error {
	var size int64
	lent := false
	if s.borrow != nil {
		var err error
		if size, err = dirSize(ctx, srcDir); err == nil {
			lent = s.borrow.TakeAll(size)
		}
	}
	if !lent {
		return tarutil.CreateWithRoot(ctx, tarPath, srcDir)
	}
	if err := tarutil.CreateWithRoot(ctx, tarPath, srcDir, tarutil.WriteBehind(0)); err != nil {
		s.borrow.Return(size)
		return err
	}
	if err := snapshotcache.StartWriteback([]string{tarPath}, func() { s.borrow.Return(size) }); err != nil {
		s.borrow.Return(size)
		slog.WarnContext(ctx, "Failed to start archive writeback", slog.String("path", tarPath), slog.Any("err", err))
	}
	return nil
}

// dirSize estimates how big a tar of dir is: each regular file's data padded
// to a 512 byte block, plus 2KiB per entry for its header and a PAX header
// carrying its xattrs. Hardlinks count once per link, which leaves room for
// the archive's end blocks.
func dirSize(ctx context.Context, dir string) (int64, error) {
	var size int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		n := int64(2 << 10)
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			n += (info.Size() + 511) &^ 511
		}
		// sparse files can claim far more than the disk holds
		if size > math.MaxInt64-n {
			size = math.MaxInt64
			return filepath.SkipAll
		}
		size += n
		return nil
	})
	return size, err
}
