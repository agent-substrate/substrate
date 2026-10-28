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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
)

const (
	goldenTmpfsMountDir         = "/run/ateworker-golden-slots"
	goldenTmpfsSize             = 8 << 30
	goldenTmpfsShmDir           = "/dev/shm/ate-microvm-slots"
	goldenTmpfsMinFree          = 2 << 30
	goldenSlotPoolCap           = 8
	goldenSlotPrewarmCount      = 6
	goldenTmpfsSlotPoolCap      = 12
	goldenTmpfsSlotPrewarmCount = 8

	zeroCopyNoGoldenDelta   = "no_golden_delta"
	zeroCopyMergedOnDisk    = "merged_on_disk"
	zeroCopyDiskSlot        = "disk_slot"
	zeroCopyNoIdleSlot      = "no_idle_slot"
	zeroCopyPreviousFailure = "previous_attempt_failed"
	zeroCopyLayout          = "layout"
	zeroCopyRewriteFailed   = "rewrite_failed"
)

var (
	goldenTmpfsRoot        string
	goldenZeroCopyDisabled bool
	goldenPools            sync.Map
	goldenPrewarmStarted   sync.Map
	zeroCopyBackoff        sync.Map
)

// setupGoldenSlotTmpfs mounts or locates a tmpfs directory for golden base memory slots and reports whether it can back guest RAM directly.
func setupGoldenSlotTmpfs(ctx context.Context, zeroCopy bool) (dir string, guestRAMReady bool) {
	size := int64(goldenTmpfsSize)
	if zeroCopy {
		if mt, err := memTotalBytes(); err != nil {
			slog.WarnContext(ctx, "Could not read node memory size; zero-copy restores stay off", slog.Any("err", err))
			zeroCopy = false
		} else {
			size = mt
		}
	}
	dir, huge, mountErr := mountGoldenSlotTmpfs(goldenTmpfsMountDir, size, zeroCopy)
	if mountErr == nil {
		slog.InfoContext(ctx, "Golden slot tmpfs mounted", slog.String("dir", dir), slog.Int64("size_bytes", size), slog.String("huge", huge))
		return dir, zeroCopy && huge == "within_size"
	}
	if free, ok := tmpfsFreeBytes("/dev/shm"); ok && free >= goldenTmpfsMinFree {
		if os.MkdirAll(goldenTmpfsShmDir, 0o700) == nil {
			slog.InfoContext(ctx, "Golden slot tmpfs: using /dev/shm", slog.String("dir", goldenTmpfsShmDir), slog.Int64("free_bytes", free))
			return goldenTmpfsShmDir, false
		}
	}
	slog.WarnContext(ctx, "Golden slot tmpfs unavailable; golden slot pool stays on node disk", slog.Any("mount_err", mountErr))
	return "", false
}

// mountGoldenSlotTmpfs mounts or remounts a dedicated tmpfs filesystem at dir with transparent hugepages enabled when supported.
func mountGoldenSlotTmpfs(dir string, size int64, resize bool) (mounted, huge string, err error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", "", err
	}
	const flags = unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC
	var remount uintptr
	if isTmpfsMountPoint(dir) {
		if !resize {
			return dir, "existing", nil
		}
		remount = unix.MS_REMOUNT
	}
	opts := fmt.Sprintf("size=%d,mode=0700", size)
	huge = "within_size"
	err = unix.Mount("ateworker-golden-slots", dir, "tmpfs", flags|remount, opts+",huge="+huge)
	if errors.Is(err, unix.EINVAL) {
		huge = "never"
		err = unix.Mount("ateworker-golden-slots", dir, "tmpfs", flags|remount, opts)
	}
	if err != nil {
		return "", "", fmt.Errorf("mount tmpfs on %s: %w", dir, err)
	}
	return dir, huge, nil
}

// memTotalBytes reads total host memory in bytes from /proc/meminfo.
func memTotalBytes() (int64, error) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	return parseMemTotal(string(b))
}

// parseMemTotal extracts MemTotal in bytes from the contents of /proc/meminfo.
func parseMemTotal(meminfo string) (int64, error) {
	for _, line := range strings.Split(meminfo, "\n") {
		f := strings.Fields(line)
		if len(f) >= 2 && f[0] == "MemTotal:" {
			kb, err := strconv.ParseInt(f[1], 10, 64)
			if err != nil || kb <= 0 || (len(f) > 2 && f[2] != "kB") {
				return 0, fmt.Errorf("bad MemTotal line %q", line)
			}
			return kb << 10, nil
		}
	}
	return 0, errors.New("no MemTotal in /proc/meminfo")
}

// isTmpfsMountPoint reports whether dir is a distinct tmpfs mount point.
func isTmpfsMountPoint(dir string) bool {
	var fs unix.Statfs_t
	var st, parent unix.Stat_t
	return unix.Statfs(dir, &fs) == nil && fs.Type == unix.TMPFS_MAGIC &&
		unix.Stat(dir, &st) == nil && unix.Stat(filepath.Dir(dir), &parent) == nil && st.Dev != parent.Dev
}

// tmpfsFreeBytes returns the available bytes on dir if dir resides on a tmpfs filesystem.
func tmpfsFreeBytes(dir string) (int64, bool) {
	var fs unix.Statfs_t
	if unix.Statfs(dir, &fs) != nil || fs.Type != unix.TMPFS_MAGIC {
		return 0, false
	}
	return int64(fs.Bavail) * fs.Bsize, true
}

// sharesInode reports whether path has more than one hard link.
func sharesInode(path string) bool {
	var st unix.Stat_t
	return unix.Stat(path, &st) == nil && st.Nlink > 1
}

// goldenSlot represents a single pre-staged guest memory slot file in a baseSlotPool.
type goldenSlot struct{ path string }

// baseSlotPool manages a pool of pre-populated golden base memory-ranges slot files for one golden base snapshot.
type baseSlotPool struct {
	srcFile, baseFile, slotDir string
	inTmpfs                    bool
	prewarm                    int
	idle                       chan *goldenSlot
	nextID                     atomic.Uint64
	stageOnce                  sync.Once
	stageErr                   error
	staged                     atomic.Bool
}

func poolKey(baseFile, tmpfsRoot string) string { return tmpfsRoot + "\x00" + baseFile }

// newBaseSlotPool constructs a baseSlotPool for baseFile, placing slots under tmpfsRoot when non-empty.
func newBaseSlotPool(baseFile, tmpfsRoot string) *baseSlotPool {
	pid := os.Getpid()
	p := &baseSlotPool{
		srcFile: baseFile, baseFile: baseFile,
		slotDir: filepath.Join(filepath.Dir(baseFile), fmt.Sprintf("slots-%d", pid)),
		prewarm: goldenSlotPrewarmCount,
	}
	capacity := goldenSlotPoolCap
	if tmpfsRoot != "" {
		sum := sha256.Sum256([]byte(baseFile))
		dir := filepath.Join(tmpfsRoot, fmt.Sprintf("%s-%x", filepath.Base(filepath.Dir(baseFile)), sum[:4]))
		p.baseFile = filepath.Join(dir, "base-memory-ranges")
		p.slotDir = filepath.Join(dir, fmt.Sprintf("slots-%d", pid))
		p.inTmpfs, p.prewarm, capacity = true, goldenTmpfsSlotPrewarmCount, goldenTmpfsSlotPoolCap
	}
	p.idle = make(chan *goldenSlot, capacity)
	return p
}

// getOrCreateBaseSlotPool returns the existing baseSlotPool for (baseFile, tmpfsRoot) or initializes a new one.
func getOrCreateBaseSlotPool(baseFile, tmpfsRoot string) *baseSlotPool {
	key := poolKey(baseFile, tmpfsRoot)
	if v, ok := goldenPools.Load(key); ok {
		return v.(*baseSlotPool)
	}
	pool := newBaseSlotPool(baseFile, tmpfsRoot)
	if actual, loaded := goldenPools.LoadOrStore(key, pool); loaded {
		return actual.(*baseSlotPool)
	}
	_ = os.MkdirAll(pool.slotDir, 0o700)
	return pool
}

// ready ensures the golden base memory-ranges file is mirrored into tmpfs for tmpfs-backed pools.
func (p *baseSlotPool) ready(ctx context.Context) bool {
	if !p.inTmpfs {
		return true
	}
	p.stageOnce.Do(func() {
		if err := mirrorGoldenBase(context.WithoutCancel(ctx), p.srcFile, p.baseFile); err != nil {
			p.stageErr = err
			_ = os.RemoveAll(filepath.Dir(p.baseFile))
			return
		}
		p.staged.Store(true)
	})
	return p.stageErr == nil
}

// mirrorGoldenBase copies src to dst as a sparse file if dst does not already exist with matching size.
func mirrorGoldenBase(ctx context.Context, src, dst string) error {
	si, err := os.Stat(src)
	if err != nil {
		return err
	}
	if di, err := os.Stat(dst); err == nil && di.Mode().IsRegular() && di.Size() == si.Size() {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	tmp := dst + ".tmp"
	_ = os.Remove(tmp)
	if err := ch.CopySparseFileFast(ctx, src, tmp); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// goldenSlotPool returns the active tmpfs or disk-backed slot pool for baseFile.
func goldenSlotPool(ctx context.Context, baseFile string) *baseSlotPool {
	if root := goldenTmpfsRoot; root != "" {
		if p := getOrCreateBaseSlotPool(baseFile, root); p.ready(ctx) {
			return p
		}
	}
	return getOrCreateBaseSlotPool(baseFile, "")
}

// goldenBaseReadPath returns the tmpfs-mirrored golden base path when staged, or baseFile otherwise.
func goldenBaseReadPath(baseFile string) string {
	if root := goldenTmpfsRoot; root != "" {
		if v, ok := goldenPools.Load(poolKey(baseFile, root)); ok && v.(*baseSlotPool).staged.Load() {
			return v.(*baseSlotPool).baseFile
		}
	}
	return baseFile
}

// maybePrewarmGoldenSlots asynchronously populates the idle slot pool for baseFile once per process.
func maybePrewarmGoldenSlots(baseFile string) {
	if goldenTmpfsRoot == "" {
		return
	}
	if _, err := os.Stat(baseFile); err != nil {
		return
	}
	if _, started := goldenPrewarmStarted.LoadOrStore(baseFile, struct{}{}); started {
		return
	}
	go func() {
		pool := goldenSlotPool(context.Background(), baseFile)
		for i := 0; i < pool.prewarm; i++ {
			pool.replenishOneSlot()
		}
	}()
}

// replenishOneSlot creates one sparse copy of p.baseFile and enqueues it into p.idle if capacity permits.
func (p *baseSlotPool) replenishOneSlot() {
	if os.MkdirAll(p.slotDir, 0o700) != nil {
		return
	}
	slotPath := filepath.Join(p.slotDir, fmt.Sprintf("slot-%d", p.nextID.Add(1)))
	if ch.CopySparseFileFast(context.Background(), p.baseFile, slotPath) != nil {
		_ = os.Remove(slotPath)
		return
	}
	select {
	case p.idle <- &goldenSlot{path: slotPath}:
	default:
		_ = os.Remove(slotPath)
	}
}

// resetAndReturnSlot restores dirtyRanges in slot back to p.baseFile and returns slot to p.idle.
func (p *baseSlotPool) resetAndReturnSlot(slot *goldenSlot, dirtyRanges []ch.SparseExtent) {
	if slot == nil {
		return
	}
	if ch.RestoreSparseRegions(context.Background(), p.baseFile, slot.path, dirtyRanges) != nil {
		_ = os.Remove(slot.path)
		return
	}
	select {
	case p.idle <- slot:
	default:
		_ = os.Remove(slot.path)
	}
}

// linkSlot links slotPath at outFile using a hard link when on the same filesystem or a symlink across mounts.
func linkSlot(slotPath, outFile string) (symlinked bool, err error) {
	if err = os.Link(slotPath, outFile); err == nil {
		return false, nil
	}
	if !errors.Is(err, unix.EXDEV) {
		return false, err
	}
	return true, os.Symlink(slotPath, outFile)
}

// materializeSlot replaces a symlinked outFile with a standalone sparse copy of slotPath.
func materializeSlot(ctx context.Context, slotPath, outFile string) error {
	tmp := outFile + ".materialize"
	_ = os.Remove(tmp)
	if err := ch.CopySparseFileFast(ctx, slotPath, tmp); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, outFile)
}

// recoverInterruptedOverlay restores outFile from its delta backup if a previous overlay left a dangling symlink.
func recoverInterruptedOverlay(outFile string) {
	deltaBackup := outFile + ".delta.bak"
	if fi, err := os.Lstat(outFile); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		if _, err := os.Lstat(deltaBackup); err == nil {
			_ = os.Rename(deltaBackup, outFile)
		}
	}
}

// goldenSlotLease tracks an acquired golden slot and its commit/abort lifecycle callbacks during restore.
type goldenSlotLease struct {
	commit   func(eager, preserve bool) (keepDelta bool)
	abort    func()
	slot     *goldenSlot
	pool     *baseSlotPool
	fromIdle bool
	claimed  *atomic.Bool
}

// claimForVM transfers exclusive ownership of the leased slot to a file-backed VM restore and triggers background replenishment.
func (l *goldenSlotLease) claimForVM() string {
	if l == nil || l.slot == nil || l.claimed == nil || !l.claimed.CompareAndSwap(false, true) {
		return ""
	}
	go l.pool.replenishOneSlot()
	return l.slot.path
}

// acquireGoldenSlotOverlay acquires a golden slot, overlays deltaFile in place, binds it to outFile, and returns commit and abort callbacks.
func acquireGoldenSlotOverlay(ctx context.Context, baseFile, deltaFile, outFile string) (commit func(eager, preserve bool) (keepDelta bool), abort func(), err error) {
	l, err := acquireGoldenSlot(ctx, baseFile, deltaFile, outFile)
	if err != nil {
		return nil, nil, err
	}
	return l.commit, l.abort, nil
}

// acquireGoldenSlot acquires a golden slot from the pool for baseFile, overlays sparse deltaFile onto it, and binds it to outFile.
func acquireGoldenSlot(ctx context.Context, baseFile, deltaFile, outFile string) (*goldenSlotLease, error) {
	noopLease := &goldenSlotLease{commit: func(bool, bool) bool { return false }, abort: func() {}}
	recoverInterruptedOverlay(outFile)
	maybePrewarmGoldenSlots(baseFile)
	bi, err := os.Stat(baseFile)
	if err != nil {
		return nil, err
	}
	di, err := os.Stat(deltaFile)
	if err != nil {
		return nil, err
	}
	if bi.Size() != di.Size() {
		return nil, fmt.Errorf("acquireGoldenSlotOverlay: size mismatch base=%d delta=%d", bi.Size(), di.Size())
	}
	pool := goldenSlotPool(ctx, baseFile)
	slot, fromIdle, err := acquireRawSlotFromPool(ctx, pool, bi.Size())
	if err != nil {
		if !pool.inTmpfs {
			return nil, fmt.Errorf("staging golden slot: %w", err)
		}
		return noopLease, ch.MergeSparseOverlay(ctx, baseFile, deltaFile, outFile)
	}
	dirtyRanges, _, oerr := ch.OverlaySparseRegionsRecorded(ctx, deltaFile, slot.path)
	if oerr != nil {
		_ = os.Remove(slot.path)
		return nil, oerr
	}
	return bindSlotToOutFile(ctx, pool, slot, fromIdle, dirtyRanges, deltaFile, outFile, func() error {
		return ch.MergeSparseOverlay(ctx, baseFile, deltaFile, outFile)
	})
}

// acquireGoldenSlotCompact acquires a golden slot from the pool for baseFile, overlays compact ADIF v1 metaFile and dataFile onto it, and binds it to outFile.
func acquireGoldenSlotCompact(ctx context.Context, baseFile, metaFile, dataFile, outFile string) (*goldenSlotLease, error) {
	noopLease := &goldenSlotLease{commit: func(bool, bool) bool { return false }, abort: func() {}}
	maybePrewarmGoldenSlots(baseFile)
	bi, err := os.Stat(baseFile)
	if err != nil {
		return nil, err
	}
	pool := goldenSlotPool(ctx, baseFile)
	slot, fromIdle, err := acquireRawSlotFromPool(ctx, pool, bi.Size())
	if err != nil {
		if !pool.inTmpfs {
			return nil, fmt.Errorf("staging golden slot: %w", err)
		}
		return noopLease, ch.RestoreFromDelta(ctx, baseFile, metaFile, dataFile, outFile)
	}
	dirtyRanges, _, oerr := ch.OverlayDeltaFilesRecorded(ctx, metaFile, dataFile, slot.path)
	if oerr != nil {
		_ = os.Remove(slot.path)
		return nil, oerr
	}
	return bindSlotToOutFile(ctx, pool, slot, fromIdle, dirtyRanges, "", outFile, func() error {
		return ch.RestoreFromDelta(ctx, baseFile, metaFile, dataFile, outFile)
	})
}

// acquireRawSlotFromPool pops a valid pre-warmed slot from pool.idle or creates a fresh sparse copy of pool.baseFile.
func acquireRawSlotFromPool(ctx context.Context, pool *baseSlotPool, expectedSize int64) (slot *goldenSlot, fromIdle bool, err error) {
	for {
		select {
		case s := <-pool.idle:
			if st, serr := os.Stat(s.path); serr == nil && st.Size() == expectedSize && !sharesInode(s.path) {
				return s, true, nil
			}
			_ = os.Remove(s.path)
			continue
		default:
		}
		break
	}
	slotPath := filepath.Join(pool.slotDir, fmt.Sprintf("slot-%d", pool.nextID.Add(1)))
	if err := os.MkdirAll(pool.slotDir, 0o700); err != nil {
		return nil, false, err
	}
	if err := ch.CopySparseFileFast(ctx, pool.baseFile, slotPath); err != nil {
		_ = os.Remove(slotPath)
		return nil, false, err
	}
	return &goldenSlot{path: slotPath}, false, nil
}

// bindSlotToOutFile links an overlaid golden slot to outFile and constructs the goldenSlotLease managing its lifecycle.
func bindSlotToOutFile(
	ctx context.Context, pool *baseSlotPool, slot *goldenSlot, fromIdle bool,
	dirtyRanges []ch.SparseExtent, deltaFile, outFile string, diskFallback func() error,
) (*goldenSlotLease, error) {
	noopLease := &goldenSlotLease{commit: func(bool, bool) bool { return false }, abort: func() {}}
	hasBackup := deltaFile != "" && deltaFile == outFile
	deltaBackup := outFile + ".delta.bak"
	if hasBackup {
		_ = os.Remove(deltaBackup)
		if err := os.Rename(outFile, deltaBackup); err != nil {
			go pool.resetAndReturnSlot(slot, dirtyRanges)
			return nil, fmt.Errorf("backup delta %q: %w", outFile, err)
		}
	} else {
		_ = os.Remove(outFile)
	}
	symlinked, err := linkSlot(slot.path, outFile)
	if err != nil {
		if hasBackup {
			_ = os.Rename(deltaBackup, outFile)
		}
		go pool.resetAndReturnSlot(slot, dirtyRanges)
		return noopLease, diskFallback()
	}

	bg := context.WithoutCancel(ctx)
	detach := func() {
		if hasBackup {
			_ = os.Remove(deltaBackup)
		}
		if symlinked && materializeSlot(bg, slot.path, outFile) != nil {
			return
		}
		_ = os.Remove(slot.path)
		go pool.replenishOneSlot()
	}

	claimed := new(atomic.Bool)
	var once sync.Once
	commitFn := func(eager, preserve bool) (keepDelta bool) {
		once.Do(func() {
			if claimed.Load() {
				keepDelta = commitClaimedSlot(outFile, deltaBackup, hasBackup, preserve)
				return
			}
			switch {
			case eager && !preserve:
				if hasBackup {
					_ = os.Remove(deltaBackup)
				}
				_ = os.Remove(outFile)
				go pool.resetAndReturnSlot(slot, dirtyRanges)
			case eager && pool.inTmpfs:
				if hasBackup {
					if os.Rename(deltaBackup, outFile) != nil {
						detach()
						return
					}
				} else {
					_ = os.Remove(outFile)
				}
				keepDelta = true
				go pool.resetAndReturnSlot(slot, dirtyRanges)
			default:
				detach()
			}
		})
		return keepDelta
	}
	abortFn := func() {
		once.Do(func() {
			_ = os.Remove(outFile)
			if hasBackup {
				_ = os.Rename(deltaBackup, outFile)
			}
			if !claimed.Load() {
				go pool.resetAndReturnSlot(slot, dirtyRanges)
			}
		})
	}
	return &goldenSlotLease{commit: commitFn, abort: abortFn, slot: slot, pool: pool, fromIdle: fromIdle, claimed: claimed}, nil
}

// commitClaimedSlot cleans up or restores the delta file in restoreDir after a slot has been claimed for zero-copy VM restore.
func commitClaimedSlot(outFile, deltaBackup string, hasBackup, preserve bool) (keepDelta bool) {
	if !preserve {
		_ = os.Remove(deltaBackup)
		_ = os.Remove(outFile)
		return false
	}
	if !hasBackup {
		_ = os.Remove(outFile)
		return true
	}
	_ = os.Rename(deltaBackup, outFile)
	return true
}

// zeroCopyRestore tracks the state of an active zero-copy file-backed guest RAM restore.
type zeroCopyRestore struct {
	restoreDir, slot, slotBase string
	slotDev, slotIno           uint64
	origMemory                 json.RawMessage
	reverted                   bool
}

// startZeroCopyRestore rewrites restoreDir to map lease's tmpfs slot directly as guest RAM and claims the slot for the VM.
func startZeroCopyRestore(ctx context.Context, actorUID, restoreDir string, lease *goldenSlotLease) (zc *zeroCopyRestore, fallback string, err error) {
	switch {
	case lease == nil:
		return nil, zeroCopyNoGoldenDelta, nil
	case lease.slot == nil:
		return nil, zeroCopyMergedOnDisk, nil
	case !lease.pool.inTmpfs || goldenZeroCopyDisabled:
		return nil, zeroCopyDiskSlot, nil
	case !lease.fromIdle:
		return nil, zeroCopyNoIdleSlot, nil
	}
	if _, failed := zeroCopyBackoff.LoadAndDelete(actorUID); failed {
		return nil, zeroCopyPreviousFailure, nil
	}
	slot := lease.slot.path
	var st unix.Stat_t
	if unix.Stat(slot, &st) != nil {
		return nil, zeroCopyRewriteFailed, nil
	}
	orig, err := ch.UseFileBackedMemory(restoreDir, slot)
	if err != nil {
		if errors.Is(err, ch.ErrUnsupportedLayout) {
			return nil, zeroCopyLayout, nil
		}
		if cerr := ch.CanonicalizeFileBackedSnapshot(restoreDir, nil); cerr != nil {
			return nil, "", fmt.Errorf("while mapping golden slot as guest RAM: %w (%v)", err, cerr)
		}
		return nil, zeroCopyRewriteFailed, nil
	}
	if lease.claimForVM() == "" {
		if err := ch.CanonicalizeFileBackedSnapshot(restoreDir, orig); err != nil {
			return nil, "", fmt.Errorf("while putting snapshot back: %w", err)
		}
		return nil, zeroCopyRewriteFailed, nil
	}
	return &zeroCopyRestore{
		restoreDir: restoreDir, slot: slot, slotDev: uint64(st.Dev), slotIno: st.Ino,
		slotBase: lease.pool.baseFile, origMemory: orig,
	}, "", nil
}

// revert restores restoreDir's config.json and state.json to canonical memfd format.
func (z *zeroCopyRestore) revert() error {
	if z.reverted {
		return nil
	}
	if err := ch.CanonicalizeFileBackedSnapshot(z.restoreDir, z.origMemory); err != nil {
		return err
	}
	z.reverted = true
	return nil
}

// abandon reverts restoreDir, deletes the claimed slot file, and records a one-shot zero-copy backoff for actorUID.
func (z *zeroCopyRestore) abandon(ctx context.Context, actorUID string) {
	_ = z.revert()
	removeMemSlot(ctx, actorUID, z.slot)
	zeroCopyBackoff.Store(actorUID, struct{}{})
}

func removeMemSlot(_ context.Context, _, slot string) { _ = os.Remove(slot) }

// discardMemSlot removes the guest RAM slot file owned by ra during actor teardown.
func (s *AteWorkerService) discardMemSlot(ctx context.Context, actorUID string, ra *runningActor) {
	s.actorsMu.Lock()
	slot := ra.memSlot
	ra.memSlot = ""
	s.actorsMu.Unlock()
	if slot != "" {
		removeMemSlot(ctx, actorUID, slot)
	}
}

// checkSlotSnapshot verifies that Cloud Hypervisor did not write a redundant memory-ranges file and that ra.memSlot still matches its original inode.
func checkSlotSnapshot(checkpointDir string, ra *runningActor) error {
	if _, err := os.Lstat(filepath.Join(checkpointDir, "memory-ranges")); err == nil {
		return fmt.Errorf("cloud-hypervisor wrote memory-ranges for a guest whose RAM is golden slot %s", ra.memSlot)
	} else if !os.IsNotExist(err) {
		return err
	}
	var st unix.Stat_t
	if err := unix.Stat(ra.memSlot, &st); err != nil {
		return fmt.Errorf("while checking golden slot holding guest RAM: %w", err)
	}
	if uint64(st.Dev) != ra.memSlotDev || st.Ino != ra.memSlotIno {
		return fmt.Errorf("golden slot %s is no longer the file the guest RAM is mapped from", ra.memSlot)
	}
	return nil
}

// copySlotMemory copies the full sparse guest RAM image from ra.memSlot to checkpointMem.
func copySlotMemory(ctx context.Context, actorUID string, ra *runningActor, checkpointMem string) error {
	if err := ch.CopySparseFileFast(ctx, ra.memSlot, checkpointMem); err != nil {
		_ = os.Remove(checkpointMem)
		return fmt.Errorf("while copying guest memory from golden slot: %w", err)
	}
	return nil
}
