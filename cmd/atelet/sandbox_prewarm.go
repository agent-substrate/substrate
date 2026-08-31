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
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime"
	"time"

	"k8s.io/client-go/tools/cache"

	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

// prewarmMaxJitter spreads the fleet's asset downloads after a SandboxConfig
// change. Every atelet observes a create/update within about a second, and
// without jitter they would all open the same bucket objects at once. A var so
// tests can zero it.
var prewarmMaxJitter = 30 * time.Second

// sandboxPrewarmer downloads SandboxConfig assets into the node's
// content-addressed static-files cache before any actor asks for them, so the
// fetch inside the first Run/Restore on the node is a cache hit instead of a
// download+extract on the critical path.
type sandboxPrewarmer struct {
	herder *AteomHerder
	// queue decouples informer event handlers (which must not block) from the
	// downloads. A single worker drains it, which also serializes downloads so
	// concurrent prewarms never compete for node bandwidth.
	queue chan *v1alpha1.SandboxConfig
	// microvmCapable gates micro-VM configs: their guest images run to
	// hundreds of MiB, and a node without /dev/kvm can never run that class
	// (workers request the ate.dev/kvm extended resource, so they only
	// schedule where the device exists). See microvmNodeCapable.
	microvmCapable bool
}

// startSandboxAssetPrewarm registers an event handler on the SandboxConfig
// informer and starts a background worker that pre-downloads each config's
// sandbox assets for this node's architecture. Prewarming is purely a latency
// optimization: every failure is logged and left to the on-demand fetch in
// ensureSandboxAssets, which remains the correctness path.
//
// TODO: the static-files cache is never pruned, and prewarming every config
// revision makes stale releases accumulate faster. Add a GC that removes
// assets referenced by no current SandboxConfig and no on-node actor record.
func startSandboxAssetPrewarm(ctx context.Context, informer cache.SharedIndexInformer, herder *AteomHerder, microvmCapable bool) error {
	p := &sandboxPrewarmer{
		herder: herder,
		// SandboxConfigs are cluster-scoped and number a handful; 64 buffered
		// events is far beyond any realistic burst.
		queue:          make(chan *v1alpha1.SandboxConfig, 64),
		microvmCapable: microvmCapable,
	}
	// The handler is registered after the informer cache has synced, so it
	// replays every existing SandboxConfig as a synthetic Add: a freshly booted
	// node prewarms the current configs, not only future changes.
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

func (p *sandboxPrewarmer) enqueue(ctx context.Context, obj any) {
	cfg, ok := obj.(*v1alpha1.SandboxConfig)
	if !ok {
		return
	}
	switch cfg.Spec.SandboxClass {
	case v1alpha1.SandboxClassGvisor:
		// Every node runs gVisor workers; always prewarm.
	case v1alpha1.SandboxClassMicroVM:
		if !p.microvmCapable {
			slog.DebugContext(ctx, "Skipping sandbox asset prewarm: node has no /dev/kvm, cannot run micro-VM workers",
				slog.String("config", cfg.Name))
			return
		}
	default:
		// An unknown class has no backend in this atelet (likely version skew
		// with a newer control plane); nothing to prewarm.
		slog.InfoContext(ctx, "Skipping sandbox asset prewarm: unknown sandbox class",
			slog.String("config", cfg.Name),
			slog.String("sandboxClass", string(cfg.Spec.SandboxClass)))
		return
	}

	select {
	case p.queue <- cfg:
	default:
		// Best-effort: dropping an event only costs a download at first use.
		slog.WarnContext(ctx, "Sandbox asset prewarm queue full; skipping config", slog.String("config", cfg.Name))
	}
}

func (p *sandboxPrewarmer) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case cfg := <-p.queue:
			if prewarmMaxJitter > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(rand.N(prewarmMaxJitter)):
				}
			}
			if err := p.prewarm(ctx, cfg); err != nil {
				// TODO: retry with backoff (e.g. a rate-limited workqueue).
				// Until then a transient failure leaves the asset cold until
				// the next config event or first use.
				slog.WarnContext(ctx, "Sandbox asset prewarm failed", slog.String("config", cfg.Name), slog.Any("err", err))
			}
		}
	}
}

// prewarm fetches every asset of one SandboxConfig into the static-files
// cache. Racing an on-demand ensureSandboxAssets for the same assets is safe:
// both paths install content-addressed files via atomic rename.
func (p *sandboxPrewarmer) prewarm(ctx context.Context, cfg *v1alpha1.SandboxConfig) error {
	rec, err := recordFromSandboxConfig(cfg)
	if err != nil {
		return err
	}
	t := time.Now()
	if _, err := p.herder.ensureSandboxAssets(ctx, rec); err != nil {
		return err
	}
	slog.InfoContext(ctx, "Sandbox assets prewarmed",
		slog.String("config", cfg.Name),
		slog.Int("assets", len(rec.Assets)),
		slog.Duration("duration", time.Since(t)))
	return nil
}

// recordFromSandboxConfig projects a SandboxConfig's per-architecture assets
// onto the local node's architecture, mirroring recordFromRequest.
func recordFromSandboxConfig(cfg *v1alpha1.SandboxConfig) (*sandboxAssetsRecord, error) {
	arch := runtime.GOARCH
	files := cfg.Spec.Assets[arch]
	if len(files) == 0 {
		return nil, fmt.Errorf("sandbox config %q has no assets for architecture %q", cfg.Name, arch)
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
