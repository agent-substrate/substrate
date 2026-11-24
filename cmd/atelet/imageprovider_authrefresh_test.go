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
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"

	imageprovider "github.com/ai-on-gke/image-provider-plugin/api/imageprovider/v1alpha1"
	providerclient "github.com/ai-on-gke/image-provider-plugin/client"
	providermock "github.com/ai-on-gke/image-provider-plugin/client/mock"

	"go.ate.dev/substrate/internal/imagecache"
)

// refreshedLeases returns the lease IDs of the mock's RefreshAuth calls and
// clears them.
func refreshedLeases(m *providermock.Client) []string {
	var ids []string
	for _, r := range m.RefreshCalls {
		ids = append(ids, r.LeaseID)
	}
	m.RefreshCalls = nil
	slices.Sort(ids)
	return ids
}

func TestRefreshProviderAuth(t *testing.T) {
	actorsDir := t.TempDir()
	const app = "registry.example/app:1"
	for _, actor := range []string{"lazy", "eager", "unknown", "gone", "failing"} {
		writeTestBundle(t, actorsDir, actor, "main", &imagecache.OverlaySpec{
			ImageRef: app, ImageDigest: "sha256:aaa", LeaseID: actor + "/main", LowerDirs: []string{"/plugin/app/0"},
		})
	}
	// An image from the cache holds no lease and is never refreshed.
	writeTestBundle(t, actorsDir, "cached", "main", &imagecache.OverlaySpec{
		ImageRef: app, ImageDigest: "sha256:aaa", StoreLayers: []string{"/cache/aaa"},
	})

	provider := providermock.New("nydus")
	s := &Atelet{imageProvider: provider}
	s.providerAuth.set("lazy/main", imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED)
	s.providerAuth.set("eager/main", imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED)
	s.providerAuth.set("gone/main", imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED)
	s.providerAuth.set("failing/main", imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED)
	provider.RefreshAuthFunc = func(_ context.Context, req *providerclient.RefreshRequest) (imageprovider.AuthRefresh, error) {
		if req.ImageRef != app {
			t.Errorf("RefreshAuth image = %q, want %q", req.ImageRef, app)
		}
		switch req.LeaseID {
		case "gone/main":
			return imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED, providerclient.ErrLeaseNotHeld
		case "failing/main":
			return imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED, errors.New("credential provider failed")
		case "unknown/main":
			return imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED, nil
		}
		return imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED, nil
	}

	// The first pass skips leases that need no refresh and learns the mode
	// of the unknown one. UNSPECIFIED counts as REQUIRED.
	s.refreshProviderAuth(t.Context(), actorsDir)
	if got, want := refreshedLeases(provider), []string{"failing/main", "gone/main", "lazy/main", "unknown/main"}; !slices.Equal(got, want) {
		t.Errorf("first pass refreshed %v, want %v", got, want)
	}
	// The second pass skips the lease the plugin does not hold, and the one
	// that needs no refresh, and retries the failure.
	s.refreshProviderAuth(t.Context(), actorsDir)
	if got, want := refreshedLeases(provider), []string{"failing/main", "lazy/main"}; !slices.Equal(got, want) {
		t.Errorf("second pass refreshed %v, want %v", got, want)
	}

	// A plugin without RefreshAuth gets no more refreshes until a reconcile.
	provider.RefreshAuthFunc = nil
	s.refreshProviderAuth(t.Context(), actorsDir)
	if got := refreshedLeases(provider); len(got) != 1 {
		t.Errorf("pass against a plugin without RefreshAuth refreshed %v, want one attempt", got)
	}
	s.refreshProviderAuth(t.Context(), actorsDir)
	if got := refreshedLeases(provider); len(got) != 0 {
		t.Errorf("refreshed %v after the plugin lacked RefreshAuth, want nothing", got)
	}
	s.providerAuth.reset()
	s.refreshProviderAuth(t.Context(), actorsDir)
	if got := refreshedLeases(provider); len(got) != 1 {
		t.Errorf("refreshed %v after a reset, want one attempt", got)
	}
}

// authMode returns the recorded mode of leaseID, and whether there is one.
func authMode(m *providerAuthModes, leaseID string) (imageprovider.AuthRefresh, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.modes[leaseID]
	if !ok {
		return imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED, false
	}
	return e.mode, true
}

// A prepare, release or reset that runs while RefreshAuth is in flight wins
// over the answer.
func TestRefreshProviderAuthKeepsNewerAnswers(t *testing.T) {
	const lease = "actor/main"
	for _, tc := range []struct {
		name string
		// during runs while RefreshAuth is in flight.
		during          func(m *providerAuthModes)
		answer          imageprovider.AuthRefresh
		answerErr       error
		wantMode        imageprovider.AuthRefresh
		wantRecorded    bool
		wantUnsupported bool
	}{
		{
			name:         "unchanged lease takes the answer",
			answer:       imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED,
			wantMode:     imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED,
			wantRecorded: true,
		},
		{
			name:         "prepare during the call wins",
			during:       func(m *providerAuthModes) { m.set(lease, imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED) },
			answerErr:    providerclient.ErrLeaseNotHeld,
			wantMode:     imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED,
			wantRecorded: true,
		},
		{
			name:   "release during the call wins",
			during: func(m *providerAuthModes) { m.forget(lease) },
			answer: imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED,
		},
		{
			name:   "reset during the call wins",
			during: func(m *providerAuthModes) { m.reset() },
			answer: imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED,
		},
		{
			name:      "reset during the call keeps RefreshAuth in use",
			during:    func(m *providerAuthModes) { m.reset() },
			answerErr: providerclient.ErrNotSupported,
		},
		{
			name:            "plugin without RefreshAuth",
			answerErr:       providerclient.ErrNotSupported,
			wantMode:        imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED,
			wantRecorded:    true,
			wantUnsupported: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actorsDir := t.TempDir()
			writeTestBundle(t, actorsDir, "actor", "main", &imagecache.OverlaySpec{
				ImageRef: "registry.example/app:1", ImageDigest: "sha256:aaa", LeaseID: lease, LowerDirs: []string{"/plugin/app/0"},
			})
			provider := providermock.New("nydus")
			s := &Atelet{imageProvider: provider}
			s.providerAuth.set(lease, imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED)
			provider.RefreshAuthFunc = func(context.Context, *providerclient.RefreshRequest) (imageprovider.AuthRefresh, error) {
				if tc.during != nil {
					tc.during(&s.providerAuth)
				}
				return tc.answer, tc.answerErr
			}

			s.refreshProviderAuth(t.Context(), actorsDir)
			mode, recorded := authMode(&s.providerAuth, lease)
			if recorded != tc.wantRecorded || mode != tc.wantMode {
				t.Errorf("recorded mode = %v, %v; want %v, %v", mode, recorded, tc.wantMode, tc.wantRecorded)
			}
			if got := s.providerAuth.unsupported; got != tc.wantUnsupported {
				t.Errorf("unsupported = %v, want %v", got, tc.wantUnsupported)
			}
		})
	}
}

// A refresh pass forgets the leases no overlay spec records anymore.
func TestRefreshProviderAuthForgetsReleasedLeases(t *testing.T) {
	actorsDir := t.TempDir()
	writeTestBundle(t, actorsDir, "actor", "main", &imagecache.OverlaySpec{
		ImageRef: "registry.example/app:1", ImageDigest: "sha256:aaa", LeaseID: "actor/main", LowerDirs: []string{"/plugin/app/0"},
	})
	s := &Atelet{imageProvider: providermock.New("nydus")}
	s.providerAuth.set("released/main", imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED)
	s.refreshProviderAuth(t.Context(), actorsDir)
	if _, ok := authMode(&s.providerAuth, "released/main"); ok {
		t.Errorf("released lease still recorded after a refresh pass")
	}
}

// A kick starts a refresh pass right away, without waiting for the ticker.
func TestRunProviderAuthRefreshKick(t *testing.T) {
	actorsDir := t.TempDir()
	writeTestBundle(t, actorsDir, "actor", "main", &imagecache.OverlaySpec{
		ImageRef: "registry.example/app:1", ImageDigest: "sha256:aaa", LeaseID: "actor/main", LowerDirs: []string{"/plugin/app/0"},
	})
	// Unix socket paths are short, so the socket goes in its own temp dir.
	sockDir, err := os.MkdirTemp("", "provider-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "p.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lis.Close() })

	provider := providermock.New("nydus")
	refreshed := make(chan string, 1)
	provider.RefreshAuthFunc = func(_ context.Context, req *providerclient.RefreshRequest) (imageprovider.AuthRefresh, error) {
		refreshed <- req.LeaseID
		return imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED, nil
	}
	s := &Atelet{imageProvider: provider}
	ctx, cancel := context.WithCancel(t.Context())
	kick := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runProviderAuthRefresh(ctx, sock, actorsDir, kick)
	}()

	kick <- struct{}{}
	select {
	case got := <-refreshed:
		if got != "actor/main" {
			t.Errorf("refreshed lease %q, want actor/main", got)
		}
	case <-t.Context().Done():
		t.Fatal("no refresh after a kick")
	}
	cancel()
	<-done
}
