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

package imagecache

// Size accounting and last-use recording: the data foundations for cache
// eviction. Both facts live in the filesystem — a per-layer "allocated" file
// and the image record's mtime — so they survive atelet restarts without
// in-memory state.

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// statBlockSize is the size of one st_blocks unit. Linux and macOS count
// st_blocks in 512-byte units, not in the filesystem's block size.
const statBlockSize = 512

// countingReader counts bytes read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// touchRecord refreshes the record's mtime (the persisted last-use
// timestamp). Best-effort: a failed touch only costs LRU accuracy.
func (s *Store) touchRecord(digest v1.Hash) {
	now := time.Now()
	if err := os.Chtimes(s.recordPath(digest), now, now); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Warn("Failed to touch image record", slog.String("digest", digest.String()), slog.Any("err", err))
	}
}

// layerSize returns the layer's recorded allocated bytes, backfilling the
// record for layers unpacked before it existed.
func (s *Store) layerSize(layerDir string) (int64, error) {
	if n, ok, err := recordedLayerSize(layerDir); err != nil || ok {
		return n, err
	}
	total := walkLayerSize(layerDir)
	// Best-effort write; the walked total is valid regardless. The write
	// bumps the dir mtime and deliberately does NOT restore it: restoring
	// could rewind a concurrent ensureLayer reuse-touch and defeat
	// retireLayer's freshness veto. The bump just makes a freshly
	// backfilled layer read as recently used for one min-age window.
	_ = os.WriteFile(filepath.Join(layerDir, layerAllocatedFileName), []byte(strconv.FormatInt(total, 10)+"\n"), 0o600)
	return total, nil
}

// layerSizeReadOnly is layerSize without the backfill write, for dry-run
// eviction: "dry run mutates nothing" includes derived metadata.
func (s *Store) layerSizeReadOnly(layerDir string) (int64, error) {
	if n, ok, err := recordedLayerSize(layerDir); err != nil || ok {
		return n, err
	}
	return walkLayerSize(layerDir), nil
}

// recordedLayerSize reads the layer's allocated-bytes file. ok is false when
// the file is absent or corrupt (the caller falls back to walking the tree).
func recordedLayerSize(layerDir string) (n int64, ok bool, err error) {
	b, err := os.ReadFile(filepath.Join(layerDir, layerAllocatedFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("while reading layer size: %w", err)
	}
	n, perr := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if perr != nil {
		return 0, false, nil
	}
	return n, true, nil
}

// walkLayerSize returns the allocated bytes of the layer's fs tree.
func walkLayerSize(layerDir string) int64 {
	return allocatedBytes(os.DirFS(filepath.Join(layerDir, layerFSDirName)))
}

// allocatedBytes sums the disk blocks allocated to every entry in fsys,
// directories included, counting each inode once so hardlinks count once.
// Blocks, not file sizes, because GC compares the result with statfs.
func allocatedBytes(fsys fs.FS) int64 {
	var total int64
	seen := map[[2]uint64]bool{}
	// The callback swallows every error (skip-and-under-count contract),
	// so WalkDir cannot return one.
	_ = fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// Images legitimately ship unreadable dirs (0111, 0000) and
			// atelet lacks CAP_DAC_READ_SEARCH, so skip and under-count
			// rather than abort.
			if p != "." && d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			total += info.Size()
			return nil
		}
		key := [2]uint64{uint64(stat.Dev), stat.Ino}
		if !seen[key] {
			seen[key] = true
			total += int64(stat.Blocks) * statBlockSize
		}
		return nil
	})
	return total
}

// CacheSize returns the sum of every layer's recorded allocated bytes (the
// accounting behind --image-cache-max-bytes, distinct from volume usage).
func (s *Store) CacheSize() (int64, error) {
	entries, err := os.ReadDir(s.layersDir())
	if err != nil {
		return 0, fmt.Errorf("while listing layer pool: %w", err)
	}
	var total int64
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") || !e.IsDir() {
			continue
		}
		n, err := s.layerSize(filepath.Join(s.layersDir(), e.Name()))
		if err != nil {
			slog.Warn("Failed to size layer", slog.String("layer", e.Name()), slog.Any("err", err))
			continue
		}
		total += n
	}
	return total, nil
}
