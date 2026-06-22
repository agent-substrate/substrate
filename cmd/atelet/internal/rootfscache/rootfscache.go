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

// Package rootfscache provides a node-local, digest-keyed cache of extracted
// OCI rootfs directories.  On a cache hit the caller can set up an overlayfs
// mount instead of re-extracting the image tarball, reducing per-restore
// latency from seconds to sub-second.
package rootfscache

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// DefaultMaxCacheBytes is the default disk budget for the rootfs cache (20 GiB).
const DefaultMaxCacheBytes int64 = 20 * 1024 * 1024 * 1024

// readySentinel is the name of the sentinel file written atomically after a
// rootfs has been fully extracted.  Its presence is the cache-hit signal.
const readySentinel = ".ready"

// lastAccessFile is the name of the file that stores the unix-timestamp of the
// most recent cache hit, used for LRU eviction.
const lastAccessFile = ".last_access"

// entryState tracks one cached digest.  All fields are immutable after
// construction except lastAccess, which is updated on cache hits.
type entryState struct {
	digest     string
	lowerDir   string
	sizeBytes  int64
	lastAccess time.Time
}

// Cache is a node-local, digest-keyed rootfs cache.  It is safe for
// concurrent use.
type Cache struct {
	basePath     string
	maxCacheBytes int64

	mu      sync.Mutex
	entries map[string]*entryState // keyed by digest

	// inflight deduplicates concurrent EnsureRootfs calls for the same
	// digest: the first goroutine extracts while others wait.
	inflight map[string]*inflightEntry

	// metrics
	cacheHits   metric.Int64Counter
	cacheMisses metric.Int64Counter
}

type inflightEntry struct {
	done chan struct{}
	// result is set before done is closed.
	lowerDir string
	cached   bool
	err      error
}

// New creates a Cache rooted at basePath.  The directory is created if it does
// not exist.  maxCacheBytes caps total disk usage; pass 0 for
// DefaultMaxCacheBytes.
func New(ctx context.Context, basePath string, maxCacheBytes int64) (*Cache, error) {
	if maxCacheBytes <= 0 {
		maxCacheBytes = DefaultMaxCacheBytes
	}
	if err := os.MkdirAll(basePath, 0o700); err != nil {
		return nil, fmt.Errorf("creating rootfs cache dir: %w", err)
	}

	meter := otel.Meter("atelet")
	cacheHits, err := meter.Int64Counter("atelet.rootfs_cache.hit",
		metric.WithDescription("Rootfs cache hits (overlay path taken)"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating hit metric: %w", err)
	}
	cacheMisses, err := meter.Int64Counter("atelet.rootfs_cache.miss",
		metric.WithDescription("Rootfs cache misses (untar required)"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating miss metric: %w", err)
	}

	c := &Cache{
		basePath:      basePath,
		maxCacheBytes: maxCacheBytes,
		entries:       make(map[string]*entryState),
		inflight:      make(map[string]*inflightEntry),
		cacheHits:     cacheHits,
		cacheMisses:   cacheMisses,
	}

	// Load existing cache entries from disk.
	if err := c.loadIndex(ctx); err != nil {
		slog.WarnContext(ctx, "Failed to load rootfs cache index, starting empty", slog.Any("err", err))
	}

	return c, nil
}

// LowerDir returns the read-only rootfs directory for the given digest, or ""
// if the digest is not cached.
func (c *Cache) LowerDir(digest string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[digest]; ok {
		return e.lowerDir
	}
	return ""
}

// EnsureRootfs guarantees that the rootfs for digest is extracted into the
// cache.  On a cache hit it returns the lowerDir immediately without reading
// tarData.  On a miss it consumes tarData to populate the cache.
//
// Returns:
//
//	lowerDir – the read-only rootfs path (non-empty on success)
//	cached   – true if the cache was hit (tarData was NOT consumed)
//	err      – any error
//
// The digest MUST be a valid directory name (hex-encoded sha256, no slashes).
func (c *Cache) EnsureRootfs(ctx context.Context, digest string, tarData io.Reader) (string, bool, error) {
	tracer := otel.Tracer("rootfscache")
	ctx, span := tracer.Start(ctx, "EnsureRootfs")
	span.SetAttributes(attribute.String("digest", digest))
	defer span.End()

	if err := validateDigest(digest); err != nil {
		return "", false, err
	}

	// Fast path: already cached.
	c.mu.Lock()
	if e, ok := c.entries[digest]; ok {
		c.mu.Unlock()
		c.cacheHits.Add(ctx, 1)
		span.SetAttributes(attribute.Bool("hit", true))
		_ = c.touchAccess(digest)
		return e.lowerDir, true, nil
	}

	// Deduplicate concurrent requests for the same digest.
	if infl, ok := c.inflight[digest]; ok {
		c.mu.Unlock()
		select {
		case <-infl.done:
			return infl.lowerDir, infl.cached, infl.err
		case <-ctx.Done():
			return "", false, ctx.Err()
		}
	}

	// We are the first goroutine for this digest — set up the inflight entry
	// so others can wait on us.
	infl := &inflightEntry{done: make(chan struct{})}
	c.inflight[digest] = infl
	c.mu.Unlock()

	// Do the actual extraction outside the lock.
	lowerDir, err := c.extract(ctx, digest, tarData)

	c.mu.Lock()
	delete(c.inflight, digest)
	c.mu.Unlock()

	if err != nil {
		infl.lowerDir = ""
		infl.cached = false
		infl.err = err
		close(infl.done)
		return "", false, err
	}

	c.cacheMisses.Add(ctx, 1)
	span.SetAttributes(attribute.Bool("hit", false))

	infl.lowerDir = lowerDir
	infl.cached = false
	infl.err = nil
	close(infl.done)
	return lowerDir, false, nil
}

// extract untars tarData into the cache directory for digest, writes the
// .ready sentinel and .last_access file, and returns the lowerDir path.
func (c *Cache) extract(ctx context.Context, digest string, tarData io.Reader) (string, error) {
	lowerDir := filepath.Join(c.basePath, digest, "lower")

	// Clean up any partial extraction from a previous crash.
	if err := os.RemoveAll(filepath.Join(c.basePath, digest)); err != nil {
		return "", fmt.Errorf("cleaning partial cache entry: %w", err)
	}
	if err := os.MkdirAll(lowerDir, 0o700); err != nil {
		return "", fmt.Errorf("creating cache lower dir: %w", err)
	}

	slog.InfoContext(ctx, "Rootfs cache miss, extracting",
		slog.String("digest", digest),
		slog.String("lowerDir", lowerDir),
	)

	if err := Untar(ctx, tarData, lowerDir); err != nil {
		// Clean up on failure so the next attempt starts fresh.
		_ = os.RemoveAll(filepath.Join(c.basePath, digest))
		return "", fmt.Errorf("extracting rootfs: %w", err)
	}

	// Make the lower dir read-only (best effort; overlayfs lowerdir is
	// inherently read-only, but this adds a layer of defense).
	_ = chmodRecursive(lowerDir, 0o555)

	// Write the .ready sentinel atomically.
	readyPath := filepath.Join(c.basePath, digest, readySentinel)
	if err := os.WriteFile(readyPath, []byte(time.Now().Format(time.RFC3339)), 0o444); err != nil {
		return "", fmt.Errorf("writing ready sentinel: %w", err)
	}

	// Write the .last_access file.
	if err := c.touchAccess(digest); err != nil {
		return "", fmt.Errorf("writing last_access: %w", err)
	}

	// Register in the in-memory index.
	size := dirSize(lowerDir)
	c.mu.Lock()
	c.entries[digest] = &entryState{
		digest:     digest,
		lowerDir:   lowerDir,
		sizeBytes:  size,
		lastAccess: time.Now(),
	}
	c.mu.Unlock()

	// Best-effort eviction.
	go c.evictIfNeeded(context.Background())

	return lowerDir, nil
}

// loadIndex scans the basePath for completed cache entries (those with a
// .ready sentinel) and populates the in-memory index.
func (c *Cache) loadIndex(ctx context.Context) error {
	dirs, err := os.ReadDir(c.basePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	var totalSize int64
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		digest := d.Name()
		readyPath := filepath.Join(c.basePath, digest, readySentinel)
		if _, err := os.Stat(readyPath); err != nil {
			// Incomplete entry from a previous crash — remove it.
			_ = os.RemoveAll(filepath.Join(c.basePath, digest))
			continue
		}
		lowerDir := filepath.Join(c.basePath, digest, "lower")
		size := dirSize(lowerDir)

		// Read last_access time; fall back to ready file mtime.
		lastAccess := readAccessTime(filepath.Join(c.basePath, digest))

		c.entries[digest] = &entryState{
			digest:     digest,
			lowerDir:   lowerDir,
			sizeBytes:  size,
			lastAccess: lastAccess,
		}
		totalSize += size
	}

	slog.InfoContext(ctx, "Loaded rootfs cache index",
		slog.Int("entries", len(c.entries)),
		slog.Int64("totalBytes", totalSize),
	)
	return nil
}

// touchAccess updates the .last_access file and in-memory timestamp for
// digest.
func (c *Cache) touchAccess(digest string) error {
	now := time.Now()
	path := filepath.Join(c.basePath, digest, lastAccessFile)
	if err := os.WriteFile(path, []byte(now.Format(time.RFC3339Nano)), 0o644); err != nil {
		return err
	}
	c.mu.Lock()
	if e, ok := c.entries[digest]; ok {
		e.lastAccess = now
	}
	c.mu.Unlock()
	return nil
}

// evictIfNeeded removes the oldest entries until total cache size is within
// the budget.  It is called asynchronously after each extraction.
func (c *Cache) evictIfNeeded(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var total int64
	for _, e := range c.entries {
		total += e.sizeBytes
	}

	for total > c.maxCacheBytes && len(c.entries) > 0 {
		// Find the entry with the oldest lastAccess.
		var oldest *entryState
		for _, e := range c.entries {
			if oldest == nil || e.lastAccess.Before(oldest.lastAccess) {
				oldest = e
			}
		}
		if oldest == nil {
			break
		}

		slog.InfoContext(ctx, "Evicting rootfs cache entry",
			slog.String("digest", oldest.digest),
			slog.Int64("sizeBytes", oldest.sizeBytes),
			slog.Time("lastAccess", oldest.lastAccess),
		)

		entryDir := filepath.Join(c.basePath, oldest.digest)
		if err := os.RemoveAll(entryDir); err != nil {
			slog.WarnContext(ctx, "Failed to evict cache entry", slog.String("digest", oldest.digest), slog.Any("err", err))
			break
		}
		total -= oldest.sizeBytes
		delete(c.entries, oldest.digest)
	}
}

// EvictLRU removes the least-recently-used cache entry and returns its digest
// and size, or ("", 0) if the cache is empty.  This is exported for tests.
func (c *Cache) EvictLRU() (string, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var oldest *entryState
	for _, e := range c.entries {
		if oldest == nil || e.lastAccess.Before(oldest.lastAccess) {
			oldest = e
		}
	}
	if oldest == nil {
		return "", 0
	}

	entryDir := filepath.Join(c.basePath, oldest.digest)
	size := oldest.sizeBytes
	_ = os.RemoveAll(entryDir)
	delete(c.entries, oldest.digest)
	return oldest.digest, size
}

// Size returns the total size of all cached entries in bytes.
func (c *Cache) Size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var total int64
	for _, e := range c.entries {
		total += e.sizeBytes
	}
	return total
}

// Count returns the number of cached entries.
func (c *Cache) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// --- Untar implementation -------------------------------------------------

// Untar extracts a tar stream into rootPath.  It is a self-contained copy of
// the untar logic from cmd/atelet/oci.go, using os.OpenRoot for path-traversal
// safety.
func Untar(ctx context.Context, tarData io.Reader, rootPath string) error {
	tracer := otel.Tracer("rootfscache")
	_, span := tracer.Start(ctx, "Untar")
	defer span.End()

	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return fmt.Errorf("opening rootfs %q as os.Root: %w", rootPath, err)
	}
	defer root.Close()

	tarReader := tar.NewReader(tarData)
	for {
		hdr, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("in tarReader.Next: %w", err)
		}

		name, skip, err := ValidateTarName(hdr.Name)
		if err != nil {
			return fmt.Errorf("invalid tar entry: %w", err)
		}
		if skip {
			continue
		}

		mode := hdr.FileInfo().Mode().Perm()

		switch hdr.Typeflag {
		case tar.TypeReg:
			if _, err := root.Lstat(name); err == nil {
				if err := root.RemoveAll(name); err != nil {
					return fmt.Errorf("while replacing existing path at %q before regular file: %w", name, err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("while checking existing path at %q before regular file: %w", name, err)
			}

			outFile, err := root.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, mode)
			if err != nil {
				return fmt.Errorf("while creating file %q: %w", name, err)
			}
			_, err = io.Copy(outFile, tarReader)
			closeErr := outFile.Close()
			if err != nil {
				return fmt.Errorf("while writing contents of %q from tar stream: %w", name, err)
			}
			if closeErr != nil {
				return fmt.Errorf("while closing file %q: %w", name, closeErr)
			}

		case tar.TypeDir:
			err := root.Mkdir(name, mode)
			if errors.Is(err, os.ErrExist) {
				// Tolerate repeated directory entries.
			} else if err != nil {
				return fmt.Errorf("while creating directory=%q, mode=%v: %w", name, mode, err)
			}

		case tar.TypeSymlink:
			if existing, err := root.Lstat(name); err == nil {
				if existing.Mode()&os.ModeSymlink != 0 {
					if cur, rerr := root.Readlink(name); rerr == nil && cur == hdr.Linkname {
						continue
					}
				}
				if err := root.RemoveAll(name); err != nil {
					return fmt.Errorf("while replacing existing path at %q before symlink: %w", name, err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("while checking existing path at %q before symlink: %w", name, err)
			}
			if err := root.Symlink(hdr.Linkname, name); err != nil {
				return fmt.Errorf("while creating symlink src=%q target=%q: %w", name, hdr.Linkname, err)
			}

		case tar.TypeLink:
			linkname, linkSkip, err := ValidateTarName(hdr.Linkname)
			if err != nil {
				return fmt.Errorf("invalid hardlink target for %q: %w", name, err)
			}
			if linkSkip {
				return fmt.Errorf("invalid hardlink target for %q: empty", name)
			}
			if _, err := root.Lstat(name); err == nil {
				if err := root.RemoveAll(name); err != nil {
					return fmt.Errorf("while replacing existing path at %q before hardlink: %w", name, err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("while checking existing path at %q before hardlink: %w", name, err)
			}
			if err := root.Link(linkname, name); err != nil {
				return fmt.Errorf("while creating hardlink src=%q target=%q: %w", name, linkname, err)
			}

		default:
			tfStr := string([]byte{hdr.Typeflag})
			slog.ErrorContext(ctx, "Unhandled tar entry typeflag", slog.String("typeflag", tfStr), slog.Any("hdr", hdr))
			return fmt.Errorf("unhandled tar entry typeflag %q", tfStr)
		}
	}

	return nil
}

// --- helpers --------------------------------------------------------------

// ValidateTarName cleans and validates a tar entry name.  It is exported so
// the main package's tests can call it without importing the internals.
func ValidateTarName(name string) (cleaned string, skip bool, err error) {
	if name == "" {
		return "", true, nil
	}
	cleaned = filepath.Clean(name)
	if cleaned == "." {
		return "", true, nil
	}
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "" || cleaned == "." {
		return "", true, nil
	}
	if !filepath.IsLocal(cleaned) {
		return "", false, fmt.Errorf("not a local path: %q", name)
	}
	return cleaned, false, nil
}

func validateDigest(digest string) error {
	if digest == "" {
		return fmt.Errorf("digest must not be empty")
	}
	if strings.ContainsAny(digest, "/\\..") {
		return fmt.Errorf("digest contains invalid characters: %q", digest)
	}
	return nil
}

// dirSize returns the total size of all regular files under dir.
func dirSize(dir string) int64 {
	var size int64
	_ = filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			if info, err := d.Info(); err == nil {
				size += info.Size()
			}
		}
		return nil
	})
	return size
}

// readAccessTime reads the .last_access file or falls back to the directory
// mtime.
func readAccessTime(entryDir string) time.Time {
	data, err := os.ReadFile(filepath.Join(entryDir, lastAccessFile))
	if err == nil {
		s := strings.TrimSpace(string(data))
		if t, perr := time.Parse(time.RFC3339Nano, s); perr == nil {
			return t
		}
		if t, perr := time.Parse(time.RFC3339, s); perr == nil {
			return t
		}
	}
	// Fallback: directory mtime.
	if info, err := os.Stat(entryDir); err == nil {
		return info.ModTime()
	}
	return time.Time{}
}

// chmodRecursive sets the permission bits on every file and directory under
// root.  Errors are silently ignored (best-effort).
func chmodRecursive(root string, perm os.FileMode) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		_ = os.Chmod(path, perm)
		return nil
	})
}

// SortedDigests returns the cached digests sorted by last-access time
// (oldest first).  Useful for diagnostics and eviction.
func (c *Cache) SortedDigests() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	type entry struct {
		digest     string
		lastAccess time.Time
	}
	var entries []entry
	for _, e := range c.entries {
		entries = append(entries, entry{e.digest, e.lastAccess})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lastAccess.Before(entries[j].lastAccess)
	})
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.digest
	}
	return out
}
