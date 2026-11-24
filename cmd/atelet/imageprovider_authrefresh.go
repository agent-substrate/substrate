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
	"log/slog"
	"sync"
	"time"

	imageprovider "github.com/ai-on-gke/image-provider-plugin/api/imageprovider/v1alpha1"
	providerclient "github.com/ai-on-gke/image-provider-plugin/client"
)

const (
	// providerAuthRefreshPeriod is how often atelet sends fresh registry
	// credentials for the provider leases that need them. The credential
	// provider keychain keeps an answer for at least a minute, so the plugin
	// is never more than one cache period behind atelet.
	providerAuthRefreshPeriod = time.Minute

	// providerAuthRefreshTimeout bounds one RefreshAuth call, credential
	// lookup included.
	providerAuthRefreshTimeout = 10 * time.Second
)

// providerAuthEntry is one answer the plugin gave for a lease. Entries are
// compared by identity, so a refresh pass can tell whether the entry it read
// was replaced since.
type providerAuthEntry struct {
	mode imageprovider.AuthRefresh
}

// providerAuthModes remembers what the plugin reported in auth_refresh for
// each provider lease. atelet refreshes a lease it has no answer for, for
// example after it restarts. The zero value is ready to use.
type providerAuthModes struct {
	mu    sync.Mutex
	modes map[string]*providerAuthEntry
	// resets counts reset calls, so a refresh pass can tell that the plugin
	// may have restarted while it waited for an answer.
	resets uint64
	// unsupported is set when the plugin does not implement RefreshAuth.
	unsupported bool
}

// providerAuthSeen is what a refresh pass read about a lease before it called
// the plugin.
type providerAuthSeen struct {
	entry  *providerAuthEntry
	resets uint64
}

func (m *providerAuthModes) set(leaseID string, r imageprovider.AuthRefresh) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.modes == nil {
		m.modes = make(map[string]*providerAuthEntry)
	}
	m.modes[leaseID] = &providerAuthEntry{mode: r}
}

func (m *providerAuthModes) forget(leaseID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.modes, leaseID)
}

// reset forgets every answer, so the next refresh asks the plugin about
// every lease again. atelet calls it after a reconcile, since the plugin may
// have restarted.
func (m *providerAuthModes) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.modes = nil
	m.unsupported = false
	m.resets++
}

// needsRefresh reports whether atelet must refresh leaseID, and what it read
// about the lease. It refreshes any lease not marked AUTH_REFRESH_NOT_NEEDED.
func (m *providerAuthModes) needsRefresh(leaseID string) (bool, providerAuthSeen) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := providerAuthSeen{entry: m.modes[leaseID], resets: m.resets}
	if m.unsupported {
		return false, seen
	}
	return seen.entry == nil || seen.entry.mode != imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED, seen
}

// setIfUnchanged records r for leaseID unless a prepare, release or reset
// changed what needsRefresh read. It reports whether it recorded r.
func (m *providerAuthModes) setIfUnchanged(leaseID string, seen providerAuthSeen, r imageprovider.AuthRefresh) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.resets != seen.resets || m.modes[leaseID] != seen.entry {
		return false
	}
	if m.modes == nil {
		m.modes = make(map[string]*providerAuthEntry)
	}
	m.modes[leaseID] = &providerAuthEntry{mode: r}
	return true
}

// setUnsupported records that the plugin does not implement RefreshAuth,
// unless a reset ran since seen.
func (m *providerAuthModes) setUnsupported(seen providerAuthSeen) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.resets == seen.resets {
		m.unsupported = true
	}
}

// retain forgets the leases not in leaseIDs, such as one whose answer came
// back after it was released.
func (m *providerAuthModes) retain(leaseIDs map[string]bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id := range m.modes {
		if !leaseIDs[id] {
			delete(m.modes, id)
		}
	}
}

// runProviderAuthRefresh refreshes the registry credentials of the provider
// leases that need it every providerAuthRefreshPeriod, and right away on
// each value from kick, until ctx ends. It runs apart from the monitor, so a
// slow plugin does not hold up reconciles.
func (s *Atelet) runProviderAuthRefresh(ctx context.Context, socket, actorsDir string, kick <-chan struct{}) {
	ticker := time.NewTicker(providerAuthRefreshPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-kick:
		}
		if providerSocketInfo(socket) != nil {
			s.refreshProviderAuth(ctx, actorsDir)
		}
	}
}

// refreshProviderAuth sends fresh registry credentials for each provider
// lease the overlay specs under actorsDir record and that needs them. A
// lease the plugin says it does not hold is not refreshed again until a
// reconcile. A plugin without RefreshAuth gets no more refreshes until the
// next reconcile.
func (s *Atelet) refreshProviderAuth(ctx context.Context, actorsDir string) {
	leases, err := activeProviderLeases(actorsDir)
	if err != nil {
		slog.WarnContext(ctx, "Failed to list image provider leases to refresh", slog.Any("err", err))
		return
	}
	leaseIDs := make(map[string]bool, len(leases))
	for _, l := range leases {
		leaseIDs[l.LeaseID] = true
	}
	s.providerAuth.retain(leaseIDs)
	if s.imageKeychain != nil {
		ctx = providerclient.WithKeychainContext(ctx, s.imageKeychain)
	}
	for _, l := range leases {
		need, seen := s.providerAuth.needsRefresh(l.LeaseID)
		if !need {
			continue
		}
		rpcCtx, cancel := context.WithTimeout(ctx, providerAuthRefreshTimeout)
		mode, err := s.imageProvider.RefreshAuth(rpcCtx, &providerclient.RefreshRequest{ImageRef: l.ImageRef, LeaseID: l.LeaseID})
		cancel()
		switch {
		case errors.Is(err, providerclient.ErrNotSupported):
			slog.WarnContext(ctx, "Image provider plugin cannot refresh registry credentials",
				slog.String("provider", s.imageProvider.Name()), slog.Any("err", err))
			s.providerAuth.setUnsupported(seen)
			return
		case errors.Is(err, providerclient.ErrLeaseNotHeld):
			// A running plugin keeps a lease until it is released, so this
			// is a plugin bug. Only the plugin can restore the lease.
			if s.providerAuth.setIfUnchanged(l.LeaseID, seen, imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED) {
				slog.WarnContext(ctx, "Image provider plugin does not hold a lease that atelet records; not refreshing it",
					slog.String("provider", s.imageProvider.Name()), slog.String("lease", l.LeaseID))
			}
		case err != nil:
			slog.WarnContext(ctx, "Failed to refresh registry credentials of an image provider lease",
				slog.String("provider", s.imageProvider.Name()), slog.String("lease", l.LeaseID), slog.Any("err", err))
			s.instruments.recordImageProviderAuthRefreshFailure(ctx, s.imageProvider.Name())
		default:
			s.providerAuth.setIfUnchanged(l.LeaseID, seen, mode)
		}
	}
}
