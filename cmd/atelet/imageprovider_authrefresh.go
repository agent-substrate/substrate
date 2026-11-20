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

// providerAuthModes remembers what the plugin reported in auth_refresh for
// each provider lease. atelet refreshes a lease it has no answer for, for
// example after it restarts. The zero value is ready to use.
type providerAuthModes struct {
	mu    sync.Mutex
	modes map[string]imageprovider.AuthRefresh
	// unsupported is set when the plugin does not implement RefreshAuth.
	unsupported bool
}

func (m *providerAuthModes) set(leaseID string, r imageprovider.AuthRefresh) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.modes == nil {
		m.modes = make(map[string]imageprovider.AuthRefresh)
	}
	m.modes[leaseID] = r
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
}

func (m *providerAuthModes) setUnsupported() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unsupported = true
}

// needsRefresh reports whether atelet must refresh leaseID. It treats
// AUTH_REFRESH_UNSPECIFIED, from an older plugin, as AUTH_REFRESH_REQUIRED.
func (m *providerAuthModes) needsRefresh(leaseID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.unsupported {
		return false
	}
	r, ok := m.modes[leaseID]
	return !ok || r != imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED
}

// refreshProviderAuth sends fresh registry credentials for each provider
// lease the overlay specs under actorsDir record and that needs them. A
// lease the plugin does not hold is skipped until the next reconcile, which
// restores it. A plugin without RefreshAuth gets no more refreshes until the
// next reconcile.
func (s *Atelet) refreshProviderAuth(ctx context.Context, actorsDir string) {
	leases, err := activeProviderLeases(actorsDir)
	if err != nil {
		slog.WarnContext(ctx, "Failed to list image provider leases to refresh", slog.Any("err", err))
		return
	}
	if s.imageKeychain != nil {
		ctx = providerclient.WithKeychainContext(ctx, s.imageKeychain)
	}
	for _, l := range leases {
		if !s.providerAuth.needsRefresh(l.LeaseID) {
			continue
		}
		rpcCtx, cancel := context.WithTimeout(ctx, providerAuthRefreshTimeout)
		mode, err := s.imageProvider.RefreshAuth(rpcCtx, &providerclient.RefreshRequest{ImageRef: l.ImageRef, LeaseID: l.LeaseID})
		cancel()
		switch {
		case errors.Is(err, providerclient.ErrNotSupported):
			slog.WarnContext(ctx, "Image provider plugin cannot refresh registry credentials",
				slog.String("provider", s.imageProvider.Name()), slog.Any("err", err))
			s.providerAuth.setUnsupported()
			return
		case errors.Is(err, providerclient.ErrLeaseNotHeld):
			s.providerAuth.set(l.LeaseID, imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED)
		case err != nil:
			slog.WarnContext(ctx, "Failed to refresh registry credentials of an image provider lease",
				slog.String("provider", s.imageProvider.Name()), slog.String("lease", l.LeaseID), slog.Any("err", err))
			s.instruments.recordImageProviderAuthRefreshFailure(ctx, s.imageProvider.Name())
		default:
			s.providerAuth.set(l.LeaseID, mode)
		}
	}
}
