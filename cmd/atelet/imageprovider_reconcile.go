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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	imageprovider "github.com/ai-on-gke/image-provider-plugin/api/imageprovider/v1alpha1"

	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

const (
	// imageProviderMonitorPeriod is how often atelet checks the image
	// provider plugin's socket and state.
	imageProviderMonitorPeriod = 2 * time.Second
	// imageProviderStateTimeout bounds one State call to the plugin.
	imageProviderStateTimeout = 2 * time.Second
	// drainSuspendTimeout bounds one suspend of an actor that holds image
	// provider leases, checkpoint included.
	drainSuspendTimeout = 10 * time.Minute
)

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

// providerLease is a provider lease that an overlay spec records.
type providerLease struct {
	leaseID  string
	imageRef string
	actorUID string
}

// activeProviderLeases returns the provider leases that the overlay specs
// under actorsDir record, sorted by lease ID.
func activeProviderLeases(actorsDir string) ([]providerLease, error) {
	var leases []providerLease
	err := forEachOverlaySpec(actorsDir, func(actorUID string, spec *imagecache.OverlaySpec) {
		if spec.LeaseID != "" {
			leases = append(leases, providerLease{spec.LeaseID, spec.ImageRef, actorUID})
		}
		for _, v := range spec.ImageVolumes {
			if v.LeaseID != "" {
				leases = append(leases, providerLease{v.LeaseID, v.ImageRef, actorUID})
			}
		}
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(leases, func(a, b providerLease) int { return strings.Compare(a.leaseID, b.leaseID) })
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
	// inflight holds the leases taken, or being taken, and not yet recorded
	// in an overlay spec.
	inflight map[string]bool
	// lost holds the leases that the last reconcile reported lost, that a
	// spec recorded then, and that were not released or taken again since.
	lost map[string]bool
}

// take records that a bundle is about to take leaseID.
func (t *providerLeaseTracker) take(leaseID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inflight == nil {
		t.inflight = make(map[string]bool)
	}
	t.inflight[leaseID] = true
}

// forget drops leaseIDs once a spec records them or they are released.
func (t *providerLeaseTracker) forget(leaseIDs ...string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, id := range leaseIDs {
		delete(t.inflight, id)
		delete(t.lost, id)
	}
}

// lostLeases returns a copy of the lost leases.
func (t *providerLeaseTracker) lostLeases() map[string]bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return maps.Clone(t.lost)
}

// removeSpec removes bundle's overlay spec, so that a reconcile sees its
// leases either all held or all gone.
func (t *providerLeaseTracker) removeSpec(bundle string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return os.Remove(filepath.Join(bundle, imagecache.OverlaySpecFileName))
}

// reconcileProviderLeases replaces the plugin's leases with those the overlay
// specs on the node record and those bundles are taking. It records the
// leases the plugin lost, so the monitor suspends the actors that hold them,
// and marks the auth keys the plugin needs credentials for. It returns
// whether any auth key needs a refresh now.
func (s *Atelet) reconcileProviderLeases(ctx context.Context, actorsDir string) (bool, error) {
	t := &s.providerLeases
	t.mu.Lock()
	defer t.mu.Unlock()
	leases, err := activeProviderLeases(actorsDir)
	if err != nil {
		return false, err
	}
	// holders maps each lease a spec records to its actor.
	holders := make(map[string]string, len(leases))
	ids := make([]string, 0, len(leases)+len(t.inflight))
	for _, l := range leases {
		holders[l.leaseID] = l.actorUID
		ids = append(ids, l.leaseID)
	}
	for id := range t.inflight {
		if _, ok := holders[id]; !ok {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	rpcCtx, cancel := context.WithTimeout(ctx, providerReconcileTimeout)
	defer cancel()
	res, err := s.imageProvider.ReconcileImages(rpcCtx, ids)
	if err != nil {
		return false, fmt.Errorf("while reconciling leases with the image provider plugin: %w", err)
	}
	t.lost = map[string]bool{}
	lostHolders := map[string]bool{}
	for _, l := range res.LostLeases {
		slog.WarnContext(ctx, "Image provider plugin lost a lease",
			slog.String("provider", s.imageProvider.Name()), slog.String("lease", l.LeaseID), slog.String("message", l.Message))
		// A lease whose PrepareImage has not reached the plugin yet may come
		// back lost, so only a lease a spec records counts.
		if uid, ok := holders[l.LeaseID]; ok {
			t.lost[l.LeaseID] = true
			lostHolders[uid] = true
		}
	}
	if len(lostHolders) > 0 {
		// An actor asked to suspend before still holds the lease, so ask
		// again.
		s.drain.reask(lostHolders)
	}
	s.providerAuth.require(res.AuthRefreshRequired)
	slog.InfoContext(ctx, "Reconciled image provider leases",
		slog.String("provider", s.imageProvider.Name()), slog.Int("leases", len(ids)),
		slog.Int("lost", len(res.LostLeases)), slog.Int("auth_refresh_required", len(res.AuthRefreshRequired)))
	return len(res.AuthRefreshRequired) > 0, nil
}

// suspendHolders returns the actors under actorsDir to suspend: those that
// hold a lost lease, and, if draining, every actor that holds a provider
// lease.
func (s *Atelet) suspendHolders(actorsDir string, draining bool) (map[string]bool, error) {
	lost := s.providerLeases.lostLeases()
	holders := map[string]bool{}
	if !draining && len(lost) == 0 {
		return holders, nil
	}
	leases, err := activeProviderLeases(actorsDir)
	if err != nil {
		return nil, err
	}
	for _, l := range leases {
		if draining || lost[l.leaseID] {
			holders[l.actorUID] = true
		}
	}
	return holders, nil
}

// startProviderSuspends finds the actors running on the node that hold lost
// provider leases, or any provider lease while the plugin drains, and has
// s.drain suspend those that are due. It returns how many suspends it
// started, and a func that waits for them.
func (s *Atelet) startProviderSuspends(ctx context.Context, actorsDir, workersDir string, draining bool) (int, func()) {
	holders, err := s.suspendHolders(actorsDir, draining)
	if err != nil {
		slog.WarnContext(ctx, "Failed to find the actors that hold image provider leases", slog.Any("err", err))
		return 0, func() {}
	}
	if !draining && len(holders) == 0 {
		return 0, func() {}
	}
	var candidates []drainCandidate
	if len(holders) > 0 {
		if candidates, err = s.runningLeaseHolders(ctx, workersDir, holders); err != nil {
			slog.WarnContext(ctx, "Failed to list workers to suspend actors that hold image provider leases", slog.Any("err", err))
			return 0, func() {}
		}
	}
	return s.drain.start(ctx, holders, candidates)
}

// runningLeaseHolders returns the actors in holders that the workers under
// workersDir are running. It skips a worker that does not answer within
// statsRPCTimeout.
func (s *Atelet) runningLeaseHolders(ctx context.Context, workersDir string, holders map[string]bool) ([]drainCandidate, error) {
	workers, err := os.ReadDir(workersDir)
	if err != nil {
		return nil, err
	}
	var candidates []drainCandidate
	// TODO: share this worker walk with statsPoller.collect. See #4105.
	for _, w := range workers {
		podUID := w.Name()
		if !w.IsDir() {
			continue
		}
		if _, err := os.Stat(workerSocketPath(podUID)); err != nil {
			continue
		}
		// One deadline over the dial and the call, as in statsPoller.collect.
		callCtx, cancel := context.WithTimeout(ctx, statsRPCTimeout)
		resp, err := s.activeWorkloads(callCtx, podUID)
		cancel()
		if err != nil {
			slog.DebugContext(ctx, "Skipping a worker that did not list its actors", slog.String("pod_uid", podUID), slog.Any("err", err))
			continue
		}
		for _, sample := range resp.GetSamples() {
			if holders[sample.GetActorUid()] {
				candidates = append(candidates, drainCandidate{
					podUID:   podUID,
					atespace: sample.GetAtespace(),
					name:     sample.GetActorName(),
					uid:      sample.GetActorUid(),
				})
			}
		}
	}
	return candidates, nil
}

// activeWorkloads dials the worker podUID and lists the workloads it runs.
func (s *Atelet) activeWorkloads(ctx context.Context, podUID string) (*ateworkerpb.GetActiveWorkloadStatsResponse, error) {
	worker, err := s.dialWorker(ctx, podUID)
	if err != nil {
		return nil, err
	}
	return worker.GetActiveWorkloadStats(ctx, &ateworkerpb.GetActiveWorkloadStatsRequest{})
}

// monitorImageProvider watches the image provider plugin until ctx ends. Each
// time the plugin's socket appears or is replaced, at startup or after the
// plugin restarts, it reconciles the plugin's leases, and retries a failed
// reconcile on the next check. It also reconciles every
// providerReconcilePeriod. A goroutine refreshes the registry credentials of
// the auth keys that need it every providerAuthRefreshPeriod, and right after
// a reconcile that reports auth keys without credentials. It suspends the
// actors that hold the leases the last reconcile reported lost, and, while
// the plugin reports PLUGIN_STATE_DRAINING, the actors that hold any of its
// leases.
func (s *Atelet) monitorImageProvider(ctx context.Context, socket, actorsDir, workersDir string) {
	refreshNow := make(chan struct{}, 1)
	go s.runProviderAuthRefresh(ctx, socket, actorsDir, refreshNow)
	reconcile := func() bool {
		refresh, err := s.reconcileProviderLeases(ctx, actorsDir)
		if err != nil {
			slog.WarnContext(ctx, "Failed to reconcile image provider leases", slog.Any("err", err))
			return false
		}
		if refresh {
			select {
			case refreshNow <- struct{}{}:
			default:
				// A refresh is already due.
			}
		}
		return true
	}
	var last os.FileInfo
	var lastReconcile time.Time
	ticker := time.NewTicker(imageProviderMonitorPeriod)
	defer ticker.Stop()
	for {
		info := providerSocketInfo(socket)
		if newProviderSocket(last, info) {
			// A new plugin may implement RefreshAuth. Clear the flag before
			// the reconcile kicks a refresh.
			s.providerAuth.clearUnsupported()
			if reconcile() {
				s.drain.forget()
			} else {
				info = nil
			}
			lastReconcile = time.Now()
		} else if info != nil && time.Since(lastReconcile) >= providerReconcilePeriod {
			reconcile()
			lastReconcile = time.Now()
		}
		last = info
		// Suspends run in the background, drainSuspendConcurrency at a
		// time. Each tick starts those that are due.
		s.startProviderSuspends(ctx, actorsDir, workersDir, info != nil && s.imageProviderDraining(ctx))
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

// imageProviderDraining reports whether the plugin says it is draining. Any
// other state, or a failed call, is not draining.
func (s *Atelet) imageProviderDraining(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, imageProviderStateTimeout)
	defer cancel()
	state, err := s.imageProvider.State(ctx)
	return err == nil && state == imageprovider.PluginState_PLUGIN_STATE_DRAINING
}
