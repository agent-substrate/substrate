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

// providerReconcileTimeout bounds one ReconcileImages call, which runs with
// providerLeaseMu held for writing.
const providerReconcileTimeout = 30 * time.Second

// reconcileProviderLeases replaces the plugin's leases with those the overlay
// specs on the node record. It holds providerLeaseMu, so no bundle is between
// taking a lease and recording it in its spec.
func (s *Atelet) reconcileProviderLeases(ctx context.Context, actorsDir string) error {
	s.providerLeaseMu.Lock()
	defer s.providerLeaseMu.Unlock()
	leases, err := activeProviderLeases(actorsDir)
	if err != nil {
		return err
	}
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
// reconcile on the next check. Right after a reconcile, and every
// providerAuthRefreshPeriod, it refreshes the registry credentials of the
// leases that need it.
func (s *Atelet) monitorImageProvider(ctx context.Context, socket, actorsDir string) {
	var last os.FileInfo
	var lastRefresh time.Time
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
				lastRefresh = time.Time{}
			}
		}
		last = info
		if info != nil && time.Since(lastRefresh) >= providerAuthRefreshPeriod {
			s.refreshProviderAuth(ctx, actorsDir)
			lastRefresh = time.Now()
		}
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
