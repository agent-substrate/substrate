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

// Package trash renames directories aside into per-mount .trash directories
// and removes them on a bounded background worker pool.
package trash

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.ate.dev/substrate/internal/imagecache"
)

const (
	defaultWorkers         = 2
	defaultMaxPendingItems = 64
	defaultMaxPendingBytes = int64(4 << 30) // 4 GiB per filesystem
	defaultMaxRetries      = 5
	defaultBaseRetryDelay  = 500 * time.Millisecond
)

// Kind identifies what a retired directory held, for naming and logs.
type Kind string

const (
	KindActorDir      Kind = "actor_dir"
	KindLocalSnapshot Kind = "local_snapshot"
	kindStartupSweep  Kind = "startup_sweep"
)

// RetireOptions configures a Retire call.
type RetireOptions struct {
	ActorUID       string
	Kind           Kind
	EstimatedBytes int64
	// NonBlocking skips backpressure waits and does not fall back to
	// synchronous removal on rename failure, for callers holding a lock.
	NonBlocking bool
}

type item struct {
	path           string
	actorUID       string
	kind           Kind
	dev            uint64
	estimatedBytes int64
	attempts       int
	actorTracked   bool
}

type devAccounting struct {
	pendingItems int
	pendingBytes int64
}

// Reaper renames directories into per-mount .trash directories and unlinks
// them asynchronously with per-actor and per-filesystem backpressure.
type Reaper struct {
	ctx             context.Context
	instancePrefix  string
	workers         int
	maxPendingItems int
	maxPendingBytes int64
	maxRetries      int
	baseRetryDelay  time.Duration
	removeFunc      func(string) error

	seq atomic.Uint64
	wg  sync.WaitGroup

	mu              sync.Mutex
	started         bool
	closing         bool
	closeCh         chan struct{}
	queue           []*item
	byDev           map[uint64]*devAccounting
	actorPending    map[string]int
	headroomWaiters int
	workCh          chan struct{}
	notifyCh        chan struct{}
}

// New creates a Reaper bound to ctx. Call Start to launch its workers and
// startup sweep.
func New(ctx context.Context) *Reaper {
	return &Reaper{
		ctx:             ctx,
		instancePrefix:  rand.Text()[:8] + "-",
		workers:         defaultWorkers,
		maxPendingItems: defaultMaxPendingItems,
		maxPendingBytes: defaultMaxPendingBytes,
		maxRetries:      defaultMaxRetries,
		baseRetryDelay:  defaultBaseRetryDelay,
		removeFunc:      removeAll,
		closeCh:         make(chan struct{}),
		byDev:           map[uint64]*devAccounting{},
		actorPending:    map[string]int{},
		workCh:          make(chan struct{}, 1),
		notifyCh:        make(chan struct{}),
	}
}

// Start launches the worker pool and sweeps any entries left in trashDirs from
// a previous run asynchronously. Calling Start more than once is a no-op.
func (r *Reaper) Start(trashDirs ...string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.started || r.closing {
		r.mu.Unlock()
		return
	}
	r.started = true
	r.mu.Unlock()

	for range r.workers {
		r.wg.Go(r.runWorker)
	}
	if len(trashDirs) > 0 {
		dirs := append([]string(nil), trashDirs...)
		r.wg.Go(func() {
			r.sweepStartup(dirs)
		})
	}
}

// Wait stops dequeuing new items and waits for in-flight removals to finish
// until ctx ends. Unprocessed items remain in .trash for the next startup sweep.
func (r *Reaper) Wait(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if !r.closing {
		r.closing = true
		close(r.closeCh)
		r.broadcastLocked()
	}
	r.mu.Unlock()
	r.signalWork()

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Retire moves path into trashDir and enqueues it for background removal.
// An empty directory is removed in place without entering trashDir.
// A nil Reaper removes path synchronously.
func (r *Reaper) Retire(ctx context.Context, trashDir, path string, opts RetireOptions) error {
	if r == nil {
		return removeAll(path)
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if info.IsDir() {
		// Remove empty directories in one syscall so ResetActorDirs cycles do
		// not fill the pending-item ceiling with empty directories.
		if err := os.Remove(path); err == nil || errors.Is(err, os.ErrNotExist) {
			return nil
		}
	}

	if err := os.MkdirAll(trashDir, 0o700); err != nil {
		if opts.NonBlocking {
			return err
		}
		return r.removeFunc(path)
	}

	dev, err := pathDev(trashDir)
	if err != nil {
		if opts.NonBlocking {
			return err
		}
		return r.removeFunc(path)
	}

	if !opts.NonBlocking {
		if err := r.waitForHeadroom(ctx, dev, opts.EstimatedBytes); err != nil {
			return err
		}
	}

	kind := opts.Kind
	if kind == "" {
		kind = KindActorDir
	}
	name := fmt.Sprintf("%s%s-%s-%d-%d", r.instancePrefix, safeNamePart(opts.ActorUID), kind, time.Now().UnixNano(), r.seq.Add(1))
	dst := filepath.Join(trashDir, name)
	if err := os.Rename(path, dst); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Only treat ENOENT as already-gone if path itself vanished;
			// otherwise trashDir may have been removed concurrently.
			if _, statErr := os.Lstat(path); errors.Is(statErr, os.ErrNotExist) {
				return nil
			}
		}
		if opts.NonBlocking {
			return err
		}
		slog.WarnContext(ctx, "Failed to move directory to trash; removing synchronously",
			slog.String("path", path), slog.String("trashDir", trashDir), slog.Any("err", err))
		return r.removeFunc(path)
	}

	r.enqueue(&item{
		path:           dst,
		actorUID:       opts.ActorUID,
		kind:           kind,
		dev:            dev,
		estimatedBytes: max(opts.EstimatedBytes, 0),
		actorTracked:   opts.ActorUID != "",
	})
	return nil
}

// WaitForActor blocks until all currently queued trash entries for actorUID
// have finished their first removal attempt, or ctx (or the reaper's context)
// ends.
func (r *Reaper) WaitForActor(ctx context.Context, actorUID string) error {
	if r == nil || actorUID == "" {
		return nil
	}
	for {
		r.mu.Lock()
		if r.actorPending[actorUID] == 0 {
			r.mu.Unlock()
			return nil
		}
		if r.closing {
			r.mu.Unlock()
			return context.Canceled
		}
		if err := r.ctx.Err(); err != nil {
			r.mu.Unlock()
			return err
		}
		ch := r.notifyCh
		r.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	}
}

func (r *Reaper) waitForHeadroom(ctx context.Context, dev uint64, incomingBytes int64) error {
	for {
		r.mu.Lock()
		if r.closing {
			r.mu.Unlock()
			return context.Canceled
		}
		acc := r.byDev[dev]
		overItems := acc != nil && acc.pendingItems >= r.maxPendingItems
		overBytes := acc != nil && acc.pendingItems > 0 && (acc.pendingBytes >= r.maxPendingBytes || (incomingBytes > 0 && acc.pendingBytes+incomingBytes > r.maxPendingBytes))
		if !overItems && !overBytes {
			r.mu.Unlock()
			return nil
		}
		r.headroomWaiters++
		ch := r.notifyCh
		r.mu.Unlock()

		var waitErr error
		select {
		case <-ch:
		case <-ctx.Done():
			waitErr = ctx.Err()
		case <-r.ctx.Done():
			waitErr = r.ctx.Err()
		}

		r.mu.Lock()
		r.headroomWaiters--
		r.mu.Unlock()
		if waitErr != nil {
			return waitErr
		}
	}
}

func (r *Reaper) enqueue(it *item) {
	r.mu.Lock()
	acc := r.devAccountLocked(it.dev)
	acc.pendingItems++
	acc.pendingBytes += it.estimatedBytes
	if it.actorTracked {
		r.actorPending[it.actorUID]++
	}
	r.queue = append(r.queue, it)
	r.mu.Unlock()
	r.signalWork()
}

func (r *Reaper) devAccountLocked(dev uint64) *devAccounting {
	acc, ok := r.byDev[dev]
	if !ok {
		acc = &devAccounting{}
		r.byDev[dev] = acc
	}
	return acc
}

func (r *Reaper) signalWork() {
	select {
	case r.workCh <- struct{}{}:
	default:
	}
}

func (r *Reaper) broadcastLocked() {
	close(r.notifyCh)
	r.notifyCh = make(chan struct{})
}

func (r *Reaper) pop() *item {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing || len(r.queue) == 0 {
		return nil
	}
	it := r.queue[0]
	r.queue[0] = nil
	r.queue = r.queue[1:]
	if len(r.queue) == 0 {
		r.queue = r.queue[:0]
	} else {
		r.signalWork()
	}
	return it
}

func (r *Reaper) runWorker() {
	for {
		if it := r.pop(); it != nil {
			r.processItem(it)
			continue
		}
		r.mu.Lock()
		closing := r.closing
		r.mu.Unlock()
		if closing || r.ctx.Err() != nil {
			return
		}
		select {
		case <-r.workCh:
		case <-r.closeCh:
			return
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Reaper) processItem(it *item) {
	start := time.Now()
	err := r.removeFunc(it.path)
	elapsed := time.Since(start)
	it.attempts++

	if err == nil {
		r.finishItem(it)
		slog.DebugContext(r.ctx, "Removed retired directory",
			slog.String("actorUID", it.actorUID),
			slog.String("kind", string(it.kind)),
			slog.String("path", it.path),
			slog.Duration("took", elapsed))
		return
	}

	// Detach from WaitForActor after the first attempt so an undeletable
	// directory cannot block future checkpoints for the actor indefinitely.
	r.detachActorWaiter(it)

	slog.WarnContext(r.ctx, "Failed to remove retired directory",
		slog.String("actorUID", it.actorUID),
		slog.String("kind", string(it.kind)),
		slog.String("path", it.path),
		slog.Int("attempt", it.attempts),
		slog.Any("err", err))

	r.mu.Lock()
	canRetry := !r.closing && r.ctx.Err() == nil && it.attempts < r.maxRetries
	r.mu.Unlock()
	if !canRetry {
		r.finishItem(it)
		return
	}

	delay := r.baseRetryDelay << min(it.attempts-1, 6)
	r.wg.Go(func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			r.mu.Lock()
			if r.closing || r.ctx.Err() != nil {
				r.mu.Unlock()
				r.finishItem(it)
				return
			}
			r.queue = append(r.queue, it)
			r.mu.Unlock()
			r.signalWork()
		case <-r.closeCh:
			r.finishItem(it)
		case <-r.ctx.Done():
			r.finishItem(it)
		}
	})
}

func (r *Reaper) detachActorWaiter(it *item) {
	if !it.actorTracked {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if it.actorTracked {
		it.actorTracked = false
		r.decrementActorLocked(it.actorUID)
		r.broadcastLocked()
	}
}

func (r *Reaper) finishItem(it *item) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if acc, ok := r.byDev[it.dev]; ok {
		acc.pendingItems = max(acc.pendingItems-1, 0)
		acc.pendingBytes = max(acc.pendingBytes-it.estimatedBytes, 0)
	}
	if it.actorTracked {
		it.actorTracked = false
		r.decrementActorLocked(it.actorUID)
	}
	r.broadcastLocked()
}

func (r *Reaper) decrementActorLocked(actorUID string) {
	if n := r.actorPending[actorUID] - 1; n <= 0 {
		delete(r.actorPending, actorUID)
	} else {
		r.actorPending[actorUID] = n
	}
}

func (r *Reaper) sweepStartup(trashDirs []string) {
	for _, dir := range trashDirs {
		if r.ctx.Err() != nil {
			return
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				slog.WarnContext(r.ctx, "Failed to sweep trash directory at startup",
					slog.String("trashDir", dir), slog.Any("err", err))
			}
			continue
		}
		if len(entries) == 0 {
			continue
		}
		dev, err := pathDev(dir)
		if err != nil {
			slog.WarnContext(r.ctx, "Failed to stat trash directory at startup",
				slog.String("trashDir", dir), slog.Any("err", err))
			continue
		}
		for _, entry := range entries {
			// Skip entries retired by this Reaper instance so startup sweep
			// never double-enqueues live items from concurrent Retire calls.
			if strings.HasPrefix(entry.Name(), r.instancePrefix) {
				continue
			}
			r.mu.Lock()
			closing := r.closing
			r.mu.Unlock()
			if closing || r.ctx.Err() != nil {
				return
			}
			r.enqueue(&item{
				path: filepath.Join(dir, entry.Name()),
				kind: kindStartupSweep,
				dev:  dev,
			})
		}
	}
}

// removeAll removes path, falling back to imagecache.RemoveAllWritable on
// permission errors (such as read-only directories in unpacked images or
// guest-chmodded workspaces).
func removeAll(path string) error {
	err := os.RemoveAll(path)
	if err == nil || !errors.Is(err, os.ErrPermission) {
		return err
	}
	return imagecache.RemoveAllWritable(path)
}

func pathDev(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Dev), nil
	}
	return 0, nil
}

func safeNamePart(s string) string {
	if s == "" {
		return "unknown"
	}
	return filepath.Base(s)
}
