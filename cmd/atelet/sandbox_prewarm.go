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
	"math/rand/v2"
	"runtime"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
)

// prewarmMaxJitter spreads the fleet's asset downloads after a SandboxConfig
// change. Every atelet observes a create/update within about a second, and
// without jitter they would all open the same bucket objects at once. The
// jitter is applied as the enqueue delay, so duplicate events arriving inside
// the window collapse into one queue entry instead of stacking waits in the
// worker. A var so tests can zero it.
var prewarmMaxJitter = 30 * time.Second

// prewarmTimeout bounds a single prewarm attempt. The queue is drained by one
// worker, so without a deadline a download that hangs without failing (a
// registry that accepts the connection and stalls, a wedged bucket read)
// would block every other config forever — prewarm runs on the daemon's
// never-cancelled context. Generous enough for multi-hundred-MiB micro-VM
// guest images on a busy node; a timed-out attempt requeues with backoff like
// any other failure. A var so tests can shorten it.
var prewarmTimeout = 5 * time.Minute

// prewarmMaxRetries bounds how often one config's failed prewarm is retried
// before it is dropped until the next config event (or first use, which stays
// the correctness path). The backoff below caps at 5 minutes, so this covers
// transient bucket or registry outages of several minutes without hammering a
// permanently broken config forever.
const prewarmMaxRetries = 8

// sandboxAssetFetcher is the one slice of AteomHerder the prewarmer needs.
// Prewarming through the same method as the Run/Restore path keeps the two
// fetches from ever diverging on cache layout or validation.
type sandboxAssetFetcher interface {
	ensureSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord) (map[string]string, error)
}

// sandboxPrewarmer downloads SandboxConfig assets into the node's
// content-addressed static-files cache before any actor asks for them, so the
// fetch inside the first Run/Restore on the node is a cache hit instead of a
// download+extract on the critical path.
type sandboxPrewarmer struct {
	assets sandboxAssetFetcher
	images *imagecache.Store
	// lister resolves a queued config name to its latest revision at
	// processing time, so coalesced events never prewarm a stale spec.
	lister listersv1alpha1.SandboxConfigLister
	// queue decouples informer event handlers (which must not block) from the
	// downloads, dedupes by config name so relists cannot stack duplicate
	// work, and rate-limits retries after failures. A single worker drains
	// it, which also serializes downloads so concurrent prewarms never
	// compete for node bandwidth.
	queue workqueue.TypedRateLimitingInterface[string]
	// microvmCapable gates micro-VM configs: their guest images run to
	// hundreds of MiB, and a node without /dev/kvm can never run that class
	// (workers request the ate.dev/kvm extended resource, so they only
	// schedule where the device exists). See microvmNodeCapable.
	microvmCapable bool
}

func newSandboxPrewarmer(assets sandboxAssetFetcher, images *imagecache.Store, lister listersv1alpha1.SandboxConfigLister, microvmCapable bool) *sandboxPrewarmer {
	return &sandboxPrewarmer{
		assets: assets,
		images: images,
		lister: lister,
		// Downloads fail on the scale of network timeouts, not API conflicts,
		// so back off in seconds and cap in minutes rather than the
		// millisecond-based controller default.
		queue: workqueue.NewTypedRateLimitingQueue(
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](time.Second, 5*time.Minute)),
		microvmCapable: microvmCapable,
	}
}

// startSandboxAssetPrewarm registers an event handler on the SandboxConfig
// informer and starts a background worker that pre-downloads each config's
// sandbox assets for this node's architecture, and pulls its pause image into
// the image cache. Prewarming is purely a latency optimization: every failure
// is logged and left to the on-demand fetch in ensureSandboxAssets and the
// pull inside prepareOCIBundles, which remain the correctness path.
//
// TODO: the static-files cache is never pruned, and prewarming every config
// revision makes stale releases accumulate faster. Add a GC that removes
// assets referenced by no current SandboxConfig and no on-node actor record.
func startSandboxAssetPrewarm(ctx context.Context, informer cache.SharedIndexInformer, assets sandboxAssetFetcher, images *imagecache.Store, microvmCapable bool) error {
	p := newSandboxPrewarmer(assets, images, listersv1alpha1.NewSandboxConfigLister(informer.GetIndexer()), microvmCapable)
	// Atelet startup never waits for this informer to sync: prewarm is
	// best-effort, so a failing list/watch (e.g. Forbidden while an RBAC
	// rollout lags the binary) must degrade prewarm, not hang the node. The
	// handler makes that degradation visible in the log; the reflector keeps
	// retrying and prewarm recovers with it. Setting it fails only on an
	// already-started informer, where the default reflector logging applies.
	if err := informer.SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
		slog.WarnContext(ctx, "SandboxConfig list/watch failed; sandbox asset prewarm degraded until it recovers", slog.Any("err", err))
	}); err != nil {
		slog.InfoContext(ctx, "Could not set sandbox config watch error handler", slog.Any("err", err))
	}
	// The initial List replays every existing SandboxConfig into the handler
	// as an Add: a freshly booted node prewarms the current configs, not only
	// future changes.
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { p.enqueue(ctx, obj) },
		UpdateFunc: func(_, obj any) { p.enqueue(ctx, obj) },
	}); err != nil {
		return fmt.Errorf("while registering sandbox config prewarm handler: %w", err)
	}
	go p.run(ctx)
	slog.InfoContext(ctx, "Sandbox asset prewarm started", slog.Bool("microvmCapable", microvmCapable))
	return nil
}

// skipConfig reports whether this node has nothing to prewarm for cfg,
// logging why. Both enqueue and process apply it: sandboxClass is mutable,
// so the class observed at enqueue time can be stale by the time the worker
// resolves the name after jitter or backoff, and the gate must hold for the
// revision actually prewarmed.
func (p *sandboxPrewarmer) skipConfig(ctx context.Context, cfg *v1alpha1.SandboxConfig) bool {
	switch cfg.Spec.SandboxClass {
	case v1alpha1.SandboxClassGvisor:
		// Every node runs gVisor workers; always prewarm.
		return false
	case v1alpha1.SandboxClassMicroVM:
		if !p.microvmCapable {
			slog.DebugContext(ctx, "Skipping sandbox asset prewarm: node has no /dev/kvm, cannot run micro-VM workers",
				slog.String("config", cfg.Name))
			return true
		}
		return false
	default:
		// An unknown class has no backend in this atelet (likely version skew
		// with a newer control plane); nothing to prewarm.
		slog.InfoContext(ctx, "Skipping sandbox asset prewarm: unknown sandbox class",
			slog.String("config", cfg.Name),
			slog.String("sandboxClass", string(cfg.Spec.SandboxClass)))
		return true
	}
}

func (p *sandboxPrewarmer) enqueue(ctx context.Context, obj any) {
	cfg, ok := obj.(*v1alpha1.SandboxConfig)
	if !ok {
		return
	}
	if p.skipConfig(ctx, cfg) {
		return
	}
	if prewarmMaxJitter > 0 {
		p.queue.AddAfter(cfg.Name, rand.N(prewarmMaxJitter))
		return
	}
	p.queue.Add(cfg.Name)
}

func (p *sandboxPrewarmer) run(ctx context.Context) {
	go func() {
		<-ctx.Done()
		p.queue.ShutDown()
	}()
	for {
		name, shutdown := p.queue.Get()
		if shutdown {
			return
		}
		p.process(ctx, name)
	}
}

// process prewarms the named config's current revision, requeueing with
// backoff on failure. The queue holds names, not objects, so an event that
// arrives while its config is being processed is simply requeued by the
// workqueue and prewarms the newer revision afterwards.
func (p *sandboxPrewarmer) process(ctx context.Context, name string) {
	defer p.queue.Done(name)
	cfg, err := p.lister.Get(name)
	if apierrors.IsNotFound(err) {
		// Deleted since it was enqueued; nothing to prewarm anymore.
		p.queue.Forget(name)
		return
	}
	if err == nil && p.skipConfig(ctx, cfg) {
		// The class gate re-applies to the revision resolved now, which may
		// differ from the one that passed enqueue's filter.
		p.queue.Forget(name)
		return
	}
	if err == nil {
		err = p.prewarm(ctx, cfg)
	}
	if err == nil {
		p.queue.Forget(name)
		return
	}
	if ctx.Err() != nil {
		// Shutting down, not a prewarm failure; drop without retry noise.
		return
	}
	if retries := p.queue.NumRequeues(name); retries < prewarmMaxRetries {
		slog.WarnContext(ctx, "Sandbox asset prewarm failed; will retry",
			slog.String("config", name), slog.Int("retries", retries), slog.Any("err", err))
		p.queue.AddRateLimited(name)
		return
	}
	// Best-effort: give up until the next config event or first use.
	slog.WarnContext(ctx, "Sandbox asset prewarm failed; giving up",
		slog.String("config", name), slog.Int("retries", prewarmMaxRetries), slog.Any("err", err))
	p.queue.Forget(name)
}

// prewarm fetches every asset of one SandboxConfig into the static-files
// cache and its pause image into the image cache. Racing an on-demand
// ensureSandboxAssets for the same assets is safe: both paths install
// content-addressed files via atomic rename, and the image cache collapses
// concurrent pulls of one digest.
func (p *sandboxPrewarmer) prewarm(ctx context.Context, cfg *v1alpha1.SandboxConfig) error {
	ctx, cancel := context.WithTimeout(ctx, prewarmTimeout)
	defer cancel()
	t := time.Now()

	var imageErr error
	var wg sync.WaitGroup
	// schedule prewarm pause image if provided
	if cfg.Spec.PauseImage != "" {
		wg.Go(func() {
			if _, err := p.images.EnsureImage(ctx, cfg.Spec.PauseImage); err != nil {
				imageErr = fmt.Errorf("while prewarming pause image %q: %w", cfg.Spec.PauseImage, err)
			}
		})
	}
	// schedule prewarm sandbox assets if provided
	rec, assetErr := recordFromSandboxConfig(cfg)
	switch {
	case errors.Is(assetErr, errNoAssetsForArch):
		// Permanent until the config changes, and a change re-enqueues:
		// retrying would burn every attempt on a non-failure and redo the
		// pause-image pull with it. Nothing to do for this node.
		slog.DebugContext(ctx, "No sandbox assets to prewarm for this node's architecture",
			slog.String("config", cfg.Name), slog.String("arch", runtime.GOARCH))
		assetErr = nil
	case assetErr == nil:
		wg.Go(func() {
			_, assetErr = p.assets.ensureSandboxAssets(ctx, rec)
		})
	}
	wg.Wait()
	if err := errors.Join(assetErr, imageErr); err != nil {
		return err
	}
	assets := 0
	if rec != nil {
		assets = len(rec.Assets)
	}
	slog.InfoContext(ctx, "Sandbox assets prewarmed",
		slog.String("config", cfg.Name),
		slog.Int("assets", assets),
		slog.String("pauseImage", cfg.Spec.PauseImage),
		slog.Duration("duration", time.Since(t)))
	return nil
}

// errNoAssetsForArch reports that a SandboxConfig lists no assets for this
// node's architecture. Unlike a failed download it is permanent until the
// config changes, so prewarm treats it as nothing-to-do rather than retrying.
var errNoAssetsForArch = errors.New("no sandbox assets for this architecture")

// recordFromSandboxConfig projects a SandboxConfig's per-architecture assets
// onto the local node's architecture, mirroring recordFromRequest.
func recordFromSandboxConfig(cfg *v1alpha1.SandboxConfig) (*sandboxAssetsRecord, error) {
	arch := runtime.GOARCH
	files := cfg.Spec.Assets[arch]
	if len(files) == 0 {
		return nil, fmt.Errorf("sandbox config %q, architecture %q: %w", cfg.Name, arch, errNoAssetsForArch)
	}
	rec := &sandboxAssetsRecord{
		SandboxClass: string(cfg.Spec.SandboxClass),
		PauseImage:   cfg.Spec.PauseImage,
		Assets:       make(map[string]assetEntry, len(files)),
	}
	for name, f := range files {
		rec.Assets[name] = assetEntry{URL: f.URL, SHA256: f.SHA256}
	}
	return rec, nil
}
