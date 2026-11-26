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
	"maps"
	"slices"
	"sync"
	"time"

	imageprovider "github.com/ai-on-gke/image-provider-plugin/api/imageprovider/v1alpha1"
	providerclient "github.com/ai-on-gke/image-provider-plugin/client"
)

const (
	// providerAuthRefreshPeriod is how often atelet sends fresh registry
	// credentials for the auth keys that need them. The credential
	// provider keychain keeps an answer for at least a minute, so the plugin
	// is never more than one cache period behind atelet.
	providerAuthRefreshPeriod = time.Minute

	// providerAuthRefreshTimeout bounds one RefreshAuth call, credential
	// lookup included.
	providerAuthRefreshTimeout = 10 * time.Second
)

// providerAuthEntry is one answer the plugin gave for an auth key. Entries
// are compared by identity, so a refresh pass can tell whether the entry it
// read was replaced since.
type providerAuthEntry struct {
	mode imageprovider.AuthRefresh
}

// providerAuthModes remembers what the plugin reported in auth_refresh for
// each auth key. Leases with the same auth key share credentials, and one
// RefreshAuth refreshes them all. atelet refreshes an auth key it has no
// answer for, for example after it restarts. The zero value is ready to use.
type providerAuthModes struct {
	mu    sync.Mutex
	modes map[string]*providerAuthEntry
	// resets counts clearUnsupported calls, so a refresh pass can tell that
	// the plugin may have changed while it waited for an answer.
	resets uint64
	// unsupported is set when the plugin does not implement RefreshAuth.
	unsupported bool
}

// providerAuthSeen is what a refresh pass read about an auth key before it
// called the plugin.
type providerAuthSeen struct {
	entry  *providerAuthEntry
	resets uint64
}

// prepared records the auth_refresh that PrepareImage returned for a lease
// with authKey. The auth key keeps needing refreshes while any of its leases
// does, so AUTH_REFRESH_NOT_NEEDED does not replace an earlier answer.
func (m *providerAuthModes) prepared(authKey string, r imageprovider.AuthRefresh) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.modes == nil {
		m.modes = make(map[string]*providerAuthEntry)
	}
	if m.modes[authKey] == nil || r != imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED {
		m.modes[authKey] = &providerAuthEntry{mode: r}
	}
}

// require marks authKeys as needing a refresh, for example because the
// plugin restarted and lost their credentials.
func (m *providerAuthModes) require(authKeys []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.modes == nil {
		m.modes = make(map[string]*providerAuthEntry)
	}
	for _, k := range authKeys {
		m.modes[k] = &providerAuthEntry{mode: imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED}
	}
}

// clearUnsupported forgets that the plugin does not implement RefreshAuth.
// atelet calls it when the plugin socket changes, since a new plugin may
// implement it.
func (m *providerAuthModes) clearUnsupported() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unsupported = false
	m.resets++
}

// needsRefresh reports whether atelet must refresh authKey, and what it read
// about the auth key. It refreshes any auth key not marked
// AUTH_REFRESH_NOT_NEEDED.
func (m *providerAuthModes) needsRefresh(authKey string) (bool, providerAuthSeen) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := providerAuthSeen{entry: m.modes[authKey], resets: m.resets}
	if m.unsupported {
		return false, seen
	}
	return seen.entry == nil || seen.entry.mode != imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED, seen
}

// setIfUnchanged records r for authKey unless a prepare or reconcile replaced
// what needsRefresh read. It reports whether it recorded r.
func (m *providerAuthModes) setIfUnchanged(authKey string, seen providerAuthSeen, r imageprovider.AuthRefresh) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.modes[authKey] != seen.entry {
		return false
	}
	if m.modes == nil {
		m.modes = make(map[string]*providerAuthEntry)
	}
	m.modes[authKey] = &providerAuthEntry{mode: r}
	return true
}

// setUnsupported records that the plugin does not implement RefreshAuth,
// unless clearUnsupported ran since seen.
func (m *providerAuthModes) setUnsupported(seen providerAuthSeen) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.resets == seen.resets {
		m.unsupported = true
	}
}

// retain forgets the auth keys not in authKeys, whose leases were all
// released.
func (m *providerAuthModes) retain(authKeys map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.modes {
		if _, ok := authKeys[k]; !ok {
			delete(m.modes, k)
		}
	}
}

// runProviderAuthRefresh refreshes the registry credentials of the auth keys
// that need it every providerAuthRefreshPeriod, and right away on each value
// from kick, until ctx ends. It runs apart from the monitor, so a slow plugin
// does not hold up reconciles.
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

// refreshProviderAuth sends fresh registry credentials once for each auth key
// of the provider leases the overlay specs under actorsDir record, if the
// auth key needs them. An auth key the plugin says no lease has is not
// refreshed again until a prepare or reconcile asks for it. A plugin without
// RefreshAuth gets no more refreshes until its socket changes.
func (s *Atelet) refreshProviderAuth(ctx context.Context, actorsDir string) {
	leases, err := activeProviderLeases(actorsDir)
	if err != nil {
		slog.WarnContext(ctx, "Failed to list image provider leases to refresh", slog.Any("err", err))
		return
	}
	// refs maps each auth key to the image of one of its leases, which the
	// keychain resolves credentials for.
	refs := make(map[string]string)
	for _, l := range leases {
		key, err := providerclient.AuthKey(l.imageRef)
		if err != nil {
			slog.WarnContext(ctx, "Failed to find the auth key of an image provider lease",
				slog.String("lease", l.leaseID), slog.Any("err", err))
			continue
		}
		if _, ok := refs[key]; !ok {
			refs[key] = l.imageRef
		}
	}
	s.providerAuth.retain(refs)
	if s.imageKeychain != nil {
		ctx = providerclient.WithKeychainContext(ctx, s.imageKeychain)
	}
	for _, key := range slices.Sorted(maps.Keys(refs)) {
		need, seen := s.providerAuth.needsRefresh(key)
		if !need {
			continue
		}
		rpcCtx, cancel := context.WithTimeout(ctx, providerAuthRefreshTimeout)
		mode, err := s.imageProvider.RefreshAuth(rpcCtx, &providerclient.RefreshRequest{AuthKey: key, ImageRef: refs[key]})
		cancel()
		switch {
		case errors.Is(err, providerclient.ErrNotSupported):
			slog.WarnContext(ctx, "Image provider plugin cannot refresh registry credentials",
				slog.String("provider", s.imageProvider.Name()), slog.Any("err", err))
			s.providerAuth.setUnsupported(seen)
			return
		case errors.Is(err, providerclient.ErrLeaseNotHeld):
			// A running plugin keeps a lease until it is released, so this
			// is a plugin bug. Only the plugin can restore the leases.
			if s.providerAuth.setIfUnchanged(key, seen, imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED) {
				slog.WarnContext(ctx, "Image provider plugin holds no lease with an auth key that atelet records; not refreshing it",
					slog.String("provider", s.imageProvider.Name()), slog.String("auth_key", key))
			}
		case err != nil:
			slog.WarnContext(ctx, "Failed to refresh registry credentials of image provider leases",
				slog.String("provider", s.imageProvider.Name()), slog.String("auth_key", key), slog.Any("err", err))
			s.instruments.recordImageProviderAuthRefreshFailure(ctx, s.imageProvider.Name())
		default:
			s.providerAuth.setIfUnchanged(key, seen, mode)
		}
	}
}
