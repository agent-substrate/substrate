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
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"k8s.io/client-go/util/workqueue"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/imagecache"
)

// defaultPrewarmAttemptTimeout bounds one prewarm attempt. The queue is
// drained by one worker, so an attempt that never returned would block
// every other runtime's prewarm. The asset downloads are bounded by the
// asset cache's own fetch timeout; this covers the pause image pull beside
// them.
const defaultPrewarmAttemptTimeout = 10 * time.Minute

// prewarmMaxRetries bounds how often one runtime's failed prewarm is retried
// before it is dropped until a worker advertises it again (or first use,
// which stays the correctness path). The backoff below caps at 5 minutes,
// so this covers transient bucket or registry outages of several minutes
// without hammering a permanently broken runtime forever.
const prewarmMaxRetries = 8

// sandboxAssetFetcher is the one slice of Atelet the prewarmer needs.
// Prewarming through the same method as the Run/Restore path keeps the two
// fetches from ever diverging on cache layout or validation.
type sandboxAssetFetcher interface {
	ensureSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord, dstDir string) (map[string]string, error)
}

// pruneItem is the queue item that asks the worker to remove the pins of
// runtimes no local worker advertises as its default any more. It shares
// the queue with the hashes so pinning and unpinning never interleave.
const pruneItem = "prune-pins"

// runtimePrewarmer fetches the default runtime of each worker on this node
// into the asset and image caches before any actor asks for it, so the
// fetch inside the first Run/Restore on the node is a cache hit instead of
// a download+extract on the critical path. The worker sandbox syncer hands
// it the default runtime hash of every worker it plans; the index holds
// the record to fetch.
//
// The assets are linked into a pin directory per runtime and left there
// (see ateletpath.SandboxAssetPinsDir): the cache never evicts a linked
// entry, so a default runtime stays cached while no actor runs it. The
// pin goes when no worker advertises the runtime as its default any more.
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
	// queue carries runtime hashes. It dedupes while an item waits, and it
	// rate-limits retries after failures. A single worker drains it, which
	// also serializes downloads so concurrent prewarms never compete for
	// node bandwidth.
	queue workqueue.TypedRateLimitingInterface[string]
	// prewarmed holds the hashes whose prewarm succeeded since atelet
	// started. The queue forgets an item once it is done, and every pod of
	// a pool, every re-plan and every retry of a sync hands the same default
	// over again; enqueue skips what is here, so a runtime is prewarmed
	// once rather than re-staged and logged once per pod. Content-addressed
	// hashes make that safe: the bytes a hash names never change.
	mu        sync.Mutex
	prewarmed map[string]bool
	// done is closed when the worker goroutine exits; wait blocks on it.
	done chan struct{}

	// attemptTimeout is defaultPrewarmAttemptTimeout. Shortened in tests.
	attemptTimeout time.Duration
	// onFinished, if set, observes the result of each prewarm attempt, by
	// runtime hash, once the worker's call has returned. Tests set it to
	// wait for that call before tearing down what it writes into.
	onFinished func(hash string, err error)
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
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](time.Second, 5*time.Minute)),
		prewarmed: map[string]bool{},
		done:      make(chan struct{}),
	}
}

// enqueue asks for the runtime advertised under hash to be prewarmed,
// unless it already was. The syncer calls it as it plans a worker, after
// registering the hash in the index and planning it as the worker's
// default, which keeps the pin from a prune until the worker's file says.
func (p *runtimePrewarmer) enqueue(hash string) {
	p.mu.Lock()
	already := p.prewarmed[hash]
	p.mu.Unlock()
	if already {
		return
	}
	p.queue.Add(hash)
}

// markPrewarmed records that hash's prewarm succeeded, so enqueue skips it
// from now on.
func (p *runtimePrewarmer) markPrewarmed(hash string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.prewarmed[hash] = true
}

// prune asks for the pins of runtimes no worker advertises as its default
// any more to be removed. The syncer calls it when a worker's advertised
// runtimes change or a worker goes, and once its initial pass is complete,
// which also clears pins a previous atelet left.
func (p *runtimePrewarmer) prune() {
	p.queue.Add(pruneItem)
}

// run drains the queue until ctx ends.
func (p *runtimePrewarmer) run(ctx context.Context) {
	defer close(p.done)
	go func() {
		<-ctx.Done()
		p.queue.ShutDown()
	}()
	for {
		hash, shutdown := p.queue.Get()
		if shutdown {
			return
		}
		p.process(ctx, hash)
	}
}

// wait blocks until the worker has exited, which happens after the context
// given to run is canceled and the in-flight item finishes. Tests wait here
// before tearing down what the worker writes into.
func (p *runtimePrewarmer) wait() {
	<-p.done
}

// process prewarms the runtime under hash, requeueing with backoff on
// failure, or prunes the pins when asked to.
func (p *runtimePrewarmer) process(ctx context.Context, hash string) {
	defer p.queue.Done(hash)
	if hash == pruneItem {
		if err := p.prunePins(ctx); err != nil {
			slog.WarnContext(ctx, "Could not prune sandbox asset pins; will retry on the next change", slog.Any("err", err))
		}
		p.queue.Forget(hash)
		return
	}
	rt, ok := p.index.lookup(hash)
	if !ok {
		// The syncer registers before it enqueues, and the index never
		// forgets, so this is a programming error rather than a race.
		slog.ErrorContext(ctx, "Sandbox runtime to prewarm is not in the index", slog.String("runtime_hash", hash))
		p.queue.Forget(hash)
		return
	}
	err := p.prewarm(ctx, hash, rt)
	if p.onFinished != nil {
		p.onFinished(hash, err)
	}
	if err == nil {
		p.markPrewarmed(hash)
		p.queue.Forget(hash)
		return
	}
	if ctx.Err() != nil {
		// Shutting down, not a prewarm failure; drop without retry noise.
		return
	}
	if retries := p.queue.NumRequeues(hash); retries < prewarmMaxRetries {
		slog.WarnContext(ctx, "Sandbox runtime prewarm failed; will retry",
			slog.String("runtime_hash", hash), slog.String("config", rt.config), slog.String("version", rt.version),
			slog.Int("retries", retries), slog.Any("err", err))
		p.queue.AddRateLimited(hash)
		return
	}
	// Best-effort: give up until a worker advertises it again or first use.
	slog.WarnContext(ctx, "Sandbox runtime prewarm failed; giving up",
		slog.String("runtime_hash", hash), slog.String("config", rt.config), slog.String("version", rt.version),
		slog.Int("retries", prewarmMaxRetries), slog.Any("err", err))
	p.queue.Forget(hash)
}

// prewarm fetches the runtime's assets into the asset cache, linked into
// the runtime's pin directory, and its pause image into the image cache.
// Racing an on-demand ensureSandboxAssets for the same assets is safe: the
// asset cache runs one download per asset and serves every caller from it,
// and the image cache collapses concurrent pulls of one digest. A pin left
// half-filled by a crash is completed, not rebuilt: what is staged stays.
func (p *runtimePrewarmer) prewarm(ctx context.Context, hash string, rt *indexedRuntime) error {
	ctx, cancel := context.WithTimeout(ctx, p.attemptTimeout)
	defer cancel()
	t := time.Now()

	var assetErr, imageErr error
	var wg sync.WaitGroup
	wg.Go(func() {
		_, assetErr = p.assets.ensureSandboxAssets(ctx, rt.rec, ateletpath.SandboxAssetPinDir(hash))
	})
	if rt.rec.PauseImage != "" {
		wg.Go(func() {
			if _, err := p.images.EnsureImage(ctx, rt.rec.PauseImage); err != nil {
				imageErr = fmt.Errorf("while prewarming pause image %q: %w", rt.rec.PauseImage, err)
			}
		})
	}
	wg.Wait()
	if err := errors.Join(assetErr, imageErr); err != nil {
		return err
	}
	slog.InfoContext(ctx, "Sandbox runtime prewarmed",
		slog.String("runtime_hash", hash),
		slog.String("config", rt.config),
		slog.String("version", rt.version),
		slog.Int("assets", len(rt.rec.Assets)),
		slog.String("pause_image", rt.rec.PauseImage),
		slog.Duration("duration", time.Since(t)))
	return nil
}

// prunePins removes the pin of every runtime no worker advertises as its
// default any more, and none is about to, so its assets become ordinary
// eviction candidates. A pruned runtime is prewarmed again if a worker
// advertises it later. It runs on the queue's worker, so it never races a
// prewarm filling a pin.
func (p *runtimePrewarmer) prunePins(ctx context.Context) error {
	keep := map[string]string{} // pin dir name -> hash
	for hash := range p.index.defaultHashes() {
		keep[filepath.Base(ateletpath.SandboxAssetPinDir(hash))] = hash
	}
	p.mu.Lock()
	pinned := map[string]string{} // pin dir name -> hash, for the ones to forget
	for hash := range p.prewarmed {
		pinned[filepath.Base(ateletpath.SandboxAssetPinDir(hash))] = hash
	}
	p.mu.Unlock()
	entries, err := os.ReadDir(ateletpath.SandboxAssetPinsDir)
	if err != nil {
		return fmt.Errorf("while listing sandbox asset pins: %w", err)
	}
	var errs []error
	for _, e := range entries {
		if _, ok := keep[e.Name()]; ok {
			continue
		}
		if err := os.RemoveAll(filepath.Join(ateletpath.SandboxAssetPinsDir, e.Name())); err != nil {
			errs = append(errs, err)
			continue
		}
		if hash, ok := pinned[e.Name()]; ok {
			p.mu.Lock()
			delete(p.prewarmed, hash)
			p.mu.Unlock()
		}
		slog.InfoContext(ctx, "Unpinned a sandbox runtime no local worker advertises as its default", slog.String("pin", e.Name()))
	}
	return errors.Join(errs...)
}
