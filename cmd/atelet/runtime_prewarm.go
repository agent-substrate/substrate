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
	"time"

	"k8s.io/client-go/util/workqueue"
)

// defaultPrewarmAttemptTimeout bounds one prewarm attempt. The queue is
// drained by one worker, so an attempt that never returned would block
// every other runtime's prewarm.
const defaultPrewarmAttemptTimeout = 10 * time.Minute

// prewarmMaxRetries bounds how often one failed prewarm is retried before
// it is dropped until the worker is planned again (or first use, which
// stays the correctness path). The backoff below caps at 5 minutes, so
// this covers transient bucket or registry outages of several minutes
// without hammering a permanently broken runtime forever.
const prewarmMaxRetries = 8

// sandboxAssetFetcher is the slice of Atelet the prewarmer and the syncer
// need. Staging through the same methods as the Run/Restore path keeps the
// fetches from ever diverging on cache layout or validation.
type sandboxAssetFetcher interface {
	ensureSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord, dstDir string) (map[string]string, error)
	stageSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord, dstDir string) (map[string]string, error)
}

// prewarmItem is one default runtime of a worker to prewarm: the hash to
// fetch, and the directory the worker sandbox syncer made for it under the
// worker's own, to link the assets into.
type prewarmItem struct {
	hash string
	dir  string
}

// runtimePrewarmer links the default runtime of each worker on this node
// from the asset cache into the worker's default-runtime directory (fetching
// on a cache miss after eviction), so the fetch inside the first Run/Restore
// on the node is a cache hit instead of a download+extract on the critical
// path. The worker sandbox syncer hands it each default runtime a worker
// advertises, once the worker's file says so, with a directory to stage it
// into; the index holds the record.
//
// The assets are linked into that directory and left there: the cache
// never evicts a linked entry, so a default runtime stays cached while no
// actor runs it. The directory is the syncer's to make and to remove: it
// exists while the worker advertises the runtime as a default, and goes
// with the worker's directory when the pod does. The prewarmer only fills
// what exists. A directory gone before or while an item is staged means a
// later sync or the worker's removal no longer wants it, and the item ends
// without fetching, retrying, or recreating anything.
//
// Prewarming is purely a latency optimization: every failure is logged and
// left to the on-demand fetch in ensureSandboxAssets, which remains the
// correctness path. Only default runtimes are linked here; non-default
// versions stay unlinked while no actor runs them.
type runtimePrewarmer struct {
	index  *runtimeIndex
	assets sandboxAssetFetcher
	// queue carries items. It dedupes while an item waits, and it
	// rate-limits retries after failures. A single worker drains it.
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

func newRuntimePrewarmer(index *runtimeIndex, assets sandboxAssetFetcher) *runtimePrewarmer {
	return &runtimePrewarmer{
		index:          index,
		assets:         assets,
		attemptTimeout: defaultPrewarmAttemptTimeout,
		// Downloads fail on the scale of network timeouts, not API conflicts,
		// so back off in seconds and cap in minutes rather than the
		// millisecond-based controller default.
		queue: workqueue.NewTypedRateLimitingQueue(
			workqueue.NewTypedItemExponentialFailureRateLimiter[prewarmItem](time.Second, 5*time.Minute)),
		done: make(chan struct{}),
	}
}

// enqueue asks for the runtime advertised under hash to be prewarmed into
// dir, which the syncer created. The syncer calls it once the worker's
// file advertises the runtime as a default.
func (p *runtimePrewarmer) enqueue(hash, dir string) {
	p.queue.Add(prewarmItem{hash: hash, dir: dir})
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
	rt, ok := p.index.lookup(item.hash)
	if !ok {
		// The syncer registers before it enqueues, and the index never
		// forgets, so this is a programming error rather than a race.
		slog.ErrorContext(ctx, "Sandbox runtime to prewarm is not in the index", slog.String("runtime_hash", item.hash))
		p.queue.Forget(item)
		return
	}
	err := p.prewarm(ctx, item, rt)
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
			slog.String("runtime_hash", item.hash), slog.String("config", rt.config), slog.String("version", rt.version),
			slog.Int("retries", retries), slog.Any("err", err))
		p.queue.AddRateLimited(item)
		return
	}
	// Best-effort: give up until the worker is synced again or first use.
	slog.WarnContext(ctx, "Sandbox runtime prewarm failed; giving up",
		slog.String("runtime_hash", item.hash), slog.String("config", rt.config), slog.String("version", rt.version),
		slog.Int("retries", prewarmMaxRetries), slog.Any("err", err))
	p.queue.Forget(item)
}

// prewarm links the runtime's assets from the asset cache into the item's
// directory, fetching on a cache miss after eviction. Racing an on-demand
// ensureSandboxAssets for the same assets is safe: the asset cache runs one
// download per asset and serves every caller from it. A directory the syncer
// already filled costs a stat per asset.
//
// Nothing here creates a directory: the staging goes into the item's
// directory as it is, so a directory removed before or during the work, by
// a later sync or the worker's removal, makes the next step fail, and the
// item ends without an error and without recreating anything.
func (p *runtimePrewarmer) prewarm(ctx context.Context, item prewarmItem, rt *indexedRuntime) error {
	ctx, cancel := context.WithTimeout(ctx, p.attemptTimeout)
	defer cancel()
	t := time.Now()
	if gone, err := dirGone(item.dir); err != nil || gone {
		if gone {
			slog.DebugContext(ctx, "Sandbox runtime prewarm superseded; its directory is gone", slog.String("runtime_hash", item.hash), slog.String("dir", item.dir))
		}
		return err
	}

	if _, err := p.assets.stageSandboxAssets(ctx, rt.rec, item.dir); err != nil {
		if gone, gerr := dirGone(item.dir); gerr == nil && gone {
			slog.DebugContext(ctx, "Sandbox runtime prewarm superseded while staging", slog.String("runtime_hash", item.hash), slog.String("dir", item.dir))
			return nil
		}
		return err
	}
	slog.InfoContext(ctx, "Sandbox runtime prewarmed",
		slog.String("runtime_hash", item.hash),
		slog.String("config", rt.config),
		slog.String("version", rt.version),
		slog.Int("assets", len(rt.rec.Assets)),
		slog.String("dir", item.dir),
		slog.Duration("duration", time.Since(t)))
	return nil
}

// dirGone reports whether dir does not exist.
func dirGone(dir string) (bool, error) {
	_, err := os.Stat(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("while checking the prewarm directory: %w", err)
	}
	return false, nil
}
