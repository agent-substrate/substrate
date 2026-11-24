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

package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	providerclient "github.com/ai-on-gke/image-provider-plugin/client"

	"go.ate.dev/substrate/internal/imagecache"
)

// imageProviderMonitorPeriod is how often atelet checks the image provider
// plugin's socket.
const imageProviderMonitorPeriod = 2 * time.Second

// forEachOverlaySpec calls fn with each actor UID under actorsDir and the
// overlay spec of each of its bundles. It returns the errors of the bundles
// it could not read, so a caller that needs every spec can fail closed.
func forEachOverlaySpec(actorsDir string, fn func(actorUID string, spec *imagecache.OverlaySpec)) error {
	actors, err := os.ReadDir(actorsDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("while listing actors dir %q: %w", actorsDir, err)
	}
	var errs []error
	for _, actor := range actors {
		if !actor.IsDir() {
			continue
		}
		bundlesDir := filepath.Join(actorsDir, actor.Name(), "bundles")
		bundles, err := os.ReadDir(bundlesDir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("while listing bundles dir %q: %w", bundlesDir, err))
			continue
		}
		for _, bundle := range bundles {
			if !bundle.IsDir() {
				continue
			}
			spec, err := imagecache.ReadSpec(filepath.Join(bundlesDir, bundle.Name()))
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if spec != nil {
				fn(actor.Name(), spec)
			}
		}
	}
	return errors.Join(errs...)
}

// activeProviderLeases returns the provider leases that the overlay specs
// under actorsDir record.
func activeProviderLeases(actorsDir string) ([]*providerclient.ActiveLease, error) {
	var leases []*providerclient.ActiveLease
	add := func(leaseID, ref, digest string) {
		if leaseID != "" {
			leases = append(leases, &providerclient.ActiveLease{LeaseID: leaseID, ImageRef: ref, ImageDigest: digest})
		}
	}
	err := forEachOverlaySpec(actorsDir, func(_ string, spec *imagecache.OverlaySpec) {
		add(spec.LeaseID, spec.ImageRef, spec.ImageDigest)
		for _, v := range spec.ImageVolumes {
			add(v.LeaseID, v.ImageRef, v.ImageDigest)
		}
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(leases, func(a, b *providerclient.ActiveLease) int { return cmp.Compare(a.LeaseID, b.LeaseID) })
	return leases, nil
}

const (
	// providerReconcileTimeout bounds one ReconcileImages call, which runs
	// with providerLeaseTracker.mu held.
	providerReconcileTimeout = 30 * time.Second
	// providerReconcilePeriod is how often atelet reconciles the plugin's
	// leases while the socket stays the same, which drops the leases whose
	// release failed.
	providerReconcilePeriod = 5 * time.Minute
)

// providerLeaseTracker orders the provider leases that bundles take and return
// against reconciles, so the plugin never learns a lease set that misses a
// lease atelet holds. mu is held only for bookkeeping, except that a reconcile
// holds it across its RPC: the plugin would drop a lease taken after the
// reconcile read the specs if its prepare finished first.
type providerLeaseTracker struct {
	mu sync.Mutex
	// inflight maps the leases taken, or being taken, and not yet recorded in
	// an overlay spec, to their image refs.
	inflight map[string]string
}

// take records that a bundle is about to take leaseID on ref.
func (t *providerLeaseTracker) take(leaseID, ref string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inflight == nil {
		t.inflight = make(map[string]string)
	}
	t.inflight[leaseID] = ref
}

// forget drops leaseIDs once a spec records them or they are released.
func (t *providerLeaseTracker) forget(leaseIDs ...string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, id := range leaseIDs {
		delete(t.inflight, id)
	}
}

// removeSpec removes bundle's overlay spec, so that a reconcile sees its
// leases either all held or all gone.
func (t *providerLeaseTracker) removeSpec(bundle string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return os.Remove(filepath.Join(bundle, imagecache.OverlaySpecFileName))
}

// reconcileProviderLeases replaces the plugin's leases with those the overlay
// specs on the node record and those bundles are taking.
func (s *Atelet) reconcileProviderLeases(ctx context.Context, actorsDir string) error {
	t := &s.providerLeases
	t.mu.Lock()
	defer t.mu.Unlock()
	leases, err := activeProviderLeases(actorsDir)
	if err != nil {
		return err
	}
	for id, ref := range t.inflight {
		if !slices.ContainsFunc(leases, func(l *providerclient.ActiveLease) bool { return l.LeaseID == id }) {
			leases = append(leases, &providerclient.ActiveLease{LeaseID: id, ImageRef: ref})
		}
	}
	slices.SortFunc(leases, func(a, b *providerclient.ActiveLease) int { return cmp.Compare(a.LeaseID, b.LeaseID) })
	rpcCtx, cancel := context.WithTimeout(ctx, providerReconcileTimeout)
	defer cancel()
	if err := s.imageProvider.ReconcileImages(rpcCtx, leases); err != nil {
		return fmt.Errorf("while reconciling leases with the image provider plugin: %w", err)
	}
	slog.InfoContext(ctx, "Reconciled image provider leases",
		slog.String("provider", s.imageProvider.Name()), slog.Int("leases", len(leases)))
	return nil
}

// monitorImageProvider watches the image provider plugin until ctx ends. Each
// time the plugin's socket appears or is replaced, at startup or after the
// plugin restarts, it reconciles the plugin's leases, and retries a failed
// reconcile on the next check. It also reconciles every
// providerReconcilePeriod. A goroutine refreshes the registry credentials of
// the leases that need it every providerAuthRefreshPeriod, and right after
// each reconcile that follows a socket change.
func (s *Atelet) monitorImageProvider(ctx context.Context, socket, actorsDir string) {
	refreshNow := make(chan struct{}, 1)
	go s.runProviderAuthRefresh(ctx, socket, actorsDir, refreshNow)
	var last os.FileInfo
	var lastReconcile time.Time
	ticker := time.NewTicker(imageProviderMonitorPeriod)
	defer ticker.Stop()
	for {
		info := providerSocketInfo(socket)
		if newProviderSocket(last, info) {
			if err := s.reconcileProviderLeases(ctx, actorsDir); err != nil {
				slog.WarnContext(ctx, "Failed to reconcile image provider leases", slog.Any("err", err))
				info = nil
			} else {
				// The plugin may have restarted and lost the credentials.
				s.providerAuth.reset()
				select {
				case refreshNow <- struct{}{}:
				default:
					// A refresh is already due.
				}
			}
			lastReconcile = time.Now()
		} else if info != nil && time.Since(lastReconcile) >= providerReconcilePeriod {
			if err := s.reconcileProviderLeases(ctx, actorsDir); err != nil {
				slog.WarnContext(ctx, "Failed to reconcile image provider leases", slog.Any("err", err))
			}
			lastReconcile = time.Now()
		}
		last = info
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// providerSocketInfo returns the plugin socket's file info, or nil when the
// socket does not exist.
func providerSocketInfo(socket string) os.FileInfo {
	info, err := os.Stat(strings.TrimPrefix(socket, "unix://"))
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		return nil
	}
	return info
}

// newProviderSocket reports whether the plugin socket appeared or was
// replaced since last. A restarted plugin listens on a new socket file, often
// within one monitor period.
func newProviderSocket(last, cur os.FileInfo) bool {
	return cur != nil && (last == nil || !os.SameFile(last, cur))
}
