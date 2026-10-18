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
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/util/workqueue"

	"go.ate.dev/substrate/internal/imagecache"
)

// defaultPrewarmAttemptTimeout bounds one prewarm attempt. The queue is
// drained by one worker, so an attempt that never returned would block
// every other runtime's prewarm. The asset downloads are bounded by the
// asset cache's own fetch timeout; this covers the pause image pull beside
// them.
const defaultPrewarmAttemptTimeout = 10 * time.Minute

// prewarmMaxRetries bounds how often one failed prewarm is retried before
// it is dropped until the worker is planned again (or first use, which
// stays the correctness path). The backoff below caps at 5 minutes, so
// this covers transient bucket or registry outages of several minutes
// without hammering a permanently broken runtime forever.
const prewarmMaxRetries = 8

// sandboxAssetFetcher is the slice of Atelet the prewarmer and the syncer's
// probe need. Staging through the same methods as the Run/Restore path
// keeps the fetches from ever diverging on cache layout or validation.
type sandboxAssetFetcher interface {
	ensureSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord, dstDir string) (map[string]string, error)
	stageSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord, dstDir string) (map[string]string, error)
}

// prewarmItem is one sync of a worker: the directory the worker sandbox
// syncer made for it under the worker's own, and the bundle hashes of the
// sync's default runtimes, one per sandbox class. Each runtime is linked
// into its own subdirectory of dir (see defaultRuntimeSubdir).
type prewarmItem struct {
	dir string
	// hashes holds the bundle hashes joined by hashSep rather than as a
	// slice: the item is the workqueue's key, which must be comparable so
	// the queue can dedupe a waiting item and count its retries, and a
	// struct with a slice is not. hashList splits it back.
	hashes string
}

const hashSep = ","

func (it prewarmItem) hashList() []string {
	return strings.Split(it.hashes, hashSep)
}

// runtimePrewarmer fetches the default runtime of each worker on this node
// into the asset and image caches before any actor asks for it, so the
// fetch inside the first Run/Restore on the node is a cache hit instead of
// a download+extract on the critical path. The worker sandbox syncer hands
// it the default runtimes of every sync of a worker, with a directory to
// stage them into; the index holds the records to fetch.
//
// The assets are linked into that directory and left there: the cache
// never evicts a linked entry, so a default runtime stays cached while no
// actor runs it. The directory belongs to the worker's directory and goes
// with it. The syncer numbers the directories in sync order, so a prewarm
// can tell from its siblings alone where it stands: one whose directory
// is gone, or has a later one beside it, was superseded by a later sync or
// the worker's removal and does nothing; one that staged every runtime of
// its sync removes the directories of the syncs before it, and only those,
// so a sync that arrives while it works is never undone.
//
// Prewarming is purely a latency optimization: every failure is logged and
// left to the on-demand fetch in ensureSandboxAssets and the pull inside
// prepareOCIBundles, which remain the correctness path. Only default
// runtimes are prewarmed; the first restore of another version a worker
// advertises pays its download.
type runtimePrewarmer struct {
	index  *runtimeIndex
	assets sandboxAssetFetcher
	images *imagecache.Store
	// queue carries items. It dedupes while an item waits, and it
	// rate-limits retries after failures. A single worker drains it, so
	// the prewarms of different workers never compete for node bandwidth;
	// the runtimes of one sync are staged side by side.
	queue workqueue.TypedRateLimitingInterface[prewarmItem]
	// done is closed when the worker goroutine exits; wait blocks on it.
	done chan struct{}

	// attemptTimeout is defaultPrewarmAttemptTimeout. Shortened in tests.
	attemptTimeout time.Duration
	// onFinished, if set, observes the result of each prewarm attempt once
	// the worker's call has returned. Tests set it to wait for that call
	// before tearing down what it writes into.
	onFinished func(item prewarmItem, err error)
}

func newRuntimePrewarmer(index *runtimeIndex, assets sandboxAssetFetcher, images *imagecache.Store) *runtimePrewarmer {
	return &runtimePrewarmer{
		index:          index,
		assets:         assets,
		images:         images,
		attemptTimeout: defaultPrewarmAttemptTimeout,
		// Downloads fail on the scale of network timeouts, not API conflicts,
		// so back off in seconds and cap in minutes rather than the
		// millisecond-based controller default.
		queue: workqueue.NewTypedRateLimitingQueue(
			workqueue.NewTypedItemExponentialFailureRateLimiter[prewarmItem](time.Second, 5*time.Minute)),
		done: make(chan struct{}),
	}
}

// enqueue asks for the runtimes advertised under hashes to be prewarmed
// into dir. The syncer calls it as it plans a worker, after registering
// the hashes in the index and creating dir.
func (p *runtimePrewarmer) enqueue(dir string, hashes []string) {
	p.queue.Add(prewarmItem{dir: dir, hashes: strings.Join(hashes, hashSep)})
}

// run drains the queue until ctx ends.
func (p *runtimePrewarmer) run(ctx context.Context) {
	defer close(p.done)
	go func() {
		<-ctx.Done()
		p.queue.ShutDown()
	}()
	for {
		item, shutdown := p.queue.Get()
		if shutdown {
			return
		}
		p.process(ctx, item)
	}
}

// wait blocks until the worker has exited, which happens after the context
// given to run is canceled and the in-flight item finishes. Tests wait here
// before tearing down what the worker writes into.
func (p *runtimePrewarmer) wait() {
	<-p.done
}

// process prewarms the item, requeueing with backoff on failure.
func (p *runtimePrewarmer) process(ctx context.Context, item prewarmItem) {
	defer p.queue.Done(item)
	runtimes := make(map[string]*indexedRuntime, len(item.hashList()))
	for _, hash := range item.hashList() {
		rt, ok := p.index.lookup(hash)
		if !ok {
			// The syncer registers before it enqueues, and the index never
			// forgets, so this is a programming error rather than a race.
			slog.ErrorContext(ctx, "Sandbox runtime to prewarm is not in the index", slog.String("runtime_hash", hash))
			p.queue.Forget(item)
			return
		}
		runtimes[hash] = rt
	}
	err := p.prewarm(ctx, item, runtimes)
	if p.onFinished != nil {
		p.onFinished(item, err)
	}
	if err == nil {
		p.queue.Forget(item)
		return
	}
	if ctx.Err() != nil {
		// Shutting down, not a prewarm failure; drop without retry noise.
		return
	}
	if retries := p.queue.NumRequeues(item); retries < prewarmMaxRetries {
		slog.WarnContext(ctx, "Sandbox runtime prewarm failed; will retry",
			slog.String("runtime_hashes", item.hashes), slog.String("dir", item.dir),
			slog.Int("retries", retries), slog.Any("err", err))
		p.queue.AddRateLimited(item)
		return
	}
	// Best-effort: give up until the worker is synced again or first use.
	slog.WarnContext(ctx, "Sandbox runtime prewarm failed; giving up",
		slog.String("runtime_hashes", item.hashes), slog.String("dir", item.dir),
		slog.Int("retries", prewarmMaxRetries), slog.Any("err", err))
	p.queue.Forget(item)
}

// prewarm fetches the runtimes' assets into the asset cache, linked into
// each runtime's subdirectory of the item's directory, and their pause
// images into the image cache, the runtimes side by side, then removes the
// directories of the worker's earlier syncs. Racing an on-demand
// ensureSandboxAssets for the same assets is safe: the asset cache runs one
// download per asset and serves every caller from it, and the image cache
// collapses concurrent pulls of one digest.
//
// An item superseded before or while it works, by a later sync or the
// worker's removal, ends without an error: nothing of it is wanted any
// more, and nothing is recreated, since no step below creates a parent.
func (p *runtimePrewarmer) prewarm(ctx context.Context, item prewarmItem, runtimes map[string]*indexedRuntime) error {
	ctx, cancel := context.WithTimeout(ctx, p.attemptTimeout)
	defer cancel()
	t := time.Now()
	if superseded, err := p.superseded(ctx, item); err != nil || superseded {
		return err
	}
	// The runtimes of one sync are staged side by side: each goes into its
	// own subdirectory, and the cache runs concurrent fetches of different
	// assets on their own. A failure of one fails the item; the retry finds
	// the others staged and fetches the one again.
	var g errgroup.Group
	for _, hash := range item.hashList() {
		g.Go(func() error {
			if err := p.stage(ctx, runtimes[hash], defaultRuntimeSubdir(item.dir, hash)); err != nil {
				return fmt.Errorf("runtime %s: %w", hash, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		if superseded, serr := p.superseded(ctx, item); serr == nil && superseded {
			return nil // the failure is the directory's removal, expected
		}
		return err
	}
	if err := removeEarlierSiblings(item.dir); err != nil {
		return fmt.Errorf("while removing the directories of earlier syncs: %w", err)
	}
	slog.InfoContext(ctx, "Sandbox runtimes prewarmed",
		slog.String("runtime_hashes", item.hashes),
		slog.String("dir", item.dir),
		slog.Duration("duration", time.Since(t)))
	return nil
}

// superseded reports whether a later sync of the worker, or the worker's
// removal, made the item moot: its directory is gone, or a sibling sorts
// after it. Sync directories are numbered in order (see
// newDefaultRuntimeDir), so the names decide.
func (p *runtimePrewarmer) superseded(ctx context.Context, item prewarmItem) (bool, error) {
	siblings, err := os.ReadDir(filepath.Dir(item.dir))
	if errors.Is(err, fs.ErrNotExist) {
		slog.DebugContext(ctx, "Sandbox runtime prewarm superseded; the worker is gone", slog.String("dir", item.dir))
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("while listing the worker's sync directories: %w", err)
	}
	own := filepath.Base(item.dir)
	found := false
	for _, e := range siblings {
		switch {
		case e.Name() == own:
			found = true
		case e.Name() > own:
			slog.DebugContext(ctx, "Sandbox runtime prewarm superseded by a later sync", slog.String("dir", item.dir), slog.String("later", e.Name()))
			return true, nil
		}
	}
	if !found {
		slog.DebugContext(ctx, "Sandbox runtime prewarm superseded; its directory is gone", slog.String("dir", item.dir))
		return true, nil
	}
	return false, nil
}

// stage links rt's assets into dir, fetching what the cache lacks, and
// pulls its pause image, the two side by side. dir is made here, without
// parents, and the staging creates none either: the worker's directory
// removed under the item makes every step fail instead of coming back.
func (p *runtimePrewarmer) stage(ctx context.Context, rt *indexedRuntime, dir string) error {
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("while creating the runtime's directory: %w", err)
	}
	var assetErr, imageErr error
	var wg sync.WaitGroup
	wg.Go(func() {
		_, assetErr = p.assets.stageSandboxAssets(ctx, rt.rec, dir)
	})
	if rt.rec.PauseImage != "" {
		wg.Go(func() {
			if _, err := p.images.EnsureImage(ctx, rt.rec.PauseImage); err != nil {
				imageErr = fmt.Errorf("while prewarming pause image %q: %w", rt.rec.PauseImage, err)
			}
		})
	}
	wg.Wait()
	return errors.Join(assetErr, imageErr)
}

// removeEarlierSiblings removes every entry beside dir in its parent that
// sorts before it. A later one is a sync that arrived meanwhile and stays.
// The parent gone with the worker is not an error: nothing is left to
// remove.
func removeEarlierSiblings(dir string) error {
	parent := filepath.Dir(dir)
	entries, err := os.ReadDir(parent)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	own := filepath.Base(dir)
	var errs []error
	for _, e := range entries {
		if e.Name() >= own {
			continue
		}
		if err := os.RemoveAll(filepath.Join(parent, e.Name())); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
