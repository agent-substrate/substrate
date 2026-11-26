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

// refreshedKeys returns the auth keys of the mock's RefreshAuth calls and
// clears them.
func refreshedKeys(m *providermock.Client) []string {
	var keys []string
	for _, r := range m.RefreshCalls {
		keys = append(keys, r.AuthKey)
	}
	m.RefreshCalls = nil
	slices.Sort(keys)
	return keys
}

func TestRefreshProviderAuth(t *testing.T) {
	actorsDir := t.TempDir()
	// Each actor's images have their own auth key, registry.example/<actor>.
	for _, actor := range []string{"lazy", "eager", "unknown", "gone", "failing"} {
		writeTestBundle(t, actorsDir, actor, "main", &imagecache.OverlaySpec{
			ImageRef: "registry.example/" + actor + ":1", ImageDigest: "sha256:aaa", LeaseID: actor + "/main", LowerDirs: []string{"/plugin/0"},
		})
	}
	// A second lease with the auth key of lazy/main shares its refresh.
	writeTestBundle(t, actorsDir, "lazy", "sidecar", &imagecache.OverlaySpec{
		ImageRef: "registry.example/lazy:2", ImageDigest: "sha256:bbb", LeaseID: "lazy/sidecar", LowerDirs: []string{"/plugin/1"},
	})
	// An image from the cache holds no lease and is never refreshed.
	writeTestBundle(t, actorsDir, "cached", "main", &imagecache.OverlaySpec{
		ImageRef: "registry.example/cached:1", ImageDigest: "sha256:aaa", StoreLayers: []string{"/cache/aaa"},
	})
	key := func(actor string) string { return "registry.example/" + actor }

	provider := providermock.New("nydus")
	s := &Atelet{imageProvider: provider}
	s.providerAuth.prepared(key("lazy"), imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED)
	s.providerAuth.prepared(key("eager"), imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED)
	s.providerAuth.prepared(key("gone"), imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED)
	s.providerAuth.prepared(key("failing"), imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED)
	provider.RefreshAuthFunc = func(_ context.Context, req *providerclient.RefreshRequest) (imageprovider.AuthRefresh, error) {
		// The image of the lease with the lowest lease ID.
		if want := req.AuthKey + ":1"; req.ImageRef != want {
			t.Errorf("RefreshAuth image = %q, want %q", req.ImageRef, want)
		}
		switch req.AuthKey {
		case key("gone"):
			return imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED, providerclient.ErrLeaseNotHeld
		case key("failing"):
			return imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED, errors.New("credential provider failed")
		case key("unknown"):
			return imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED, nil
		}
		return imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED, nil
	}

	// The first pass refreshes each auth key once, skips those that need no
	// refresh and learns the mode of the unknown one. UNSPECIFIED counts as
	// REQUIRED.
	s.refreshProviderAuth(t.Context(), actorsDir)
	if got, want := refreshedKeys(provider), []string{key("failing"), key("gone"), key("lazy"), key("unknown")}; !slices.Equal(got, want) {
		t.Errorf("first pass refreshed %v, want %v", got, want)
	}
	// The second pass skips the auth key with no lease in the plugin, and
	// the one that needs no refresh, and retries the failure.
	s.refreshProviderAuth(t.Context(), actorsDir)
	if got, want := refreshedKeys(provider), []string{key("failing"), key("lazy")}; !slices.Equal(got, want) {
		t.Errorf("second pass refreshed %v, want %v", got, want)
	}
	// A reconcile that reports auth keys without credentials has the next
	// pass refresh them.
	provider.ReconcileFunc = func(context.Context, []string) (*providerclient.ReconcileResult, error) {
		return &providerclient.ReconcileResult{AuthRefreshRequired: []string{key("eager"), key("gone")}}, nil
	}
	if _, err := s.reconcileProviderLeases(t.Context(), actorsDir); err != nil {
		t.Fatalf("reconcileProviderLeases: %v", err)
	}
	s.refreshProviderAuth(t.Context(), actorsDir)
	if got, want := refreshedKeys(provider), []string{key("eager"), key("failing"), key("gone"), key("lazy")}; !slices.Equal(got, want) {
		t.Errorf("pass after a reconcile refreshed %v, want %v", got, want)
	}

	// A plugin without RefreshAuth gets no more refreshes until its socket
	// changes.
	provider.RefreshAuthFunc = nil
	s.refreshProviderAuth(t.Context(), actorsDir)
	if got := refreshedKeys(provider); len(got) != 1 {
		t.Errorf("pass against a plugin without RefreshAuth refreshed %v, want one attempt", got)
	}
	s.refreshProviderAuth(t.Context(), actorsDir)
	if got := refreshedKeys(provider); len(got) != 0 {
		t.Errorf("refreshed %v after the plugin lacked RefreshAuth, want nothing", got)
	}
	s.providerAuth.clearUnsupported()
	s.refreshProviderAuth(t.Context(), actorsDir)
	if got := refreshedKeys(provider); len(got) != 1 {
		t.Errorf("refreshed %v after clearUnsupported, want one attempt", got)
	}
}

// authMode returns the recorded mode of authKey, and whether there is one.
func authMode(m *providerAuthModes, authKey string) (imageprovider.AuthRefresh, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.modes[authKey]
	if !ok {
		return imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED, false
	}
	return e.mode, true
}

// A prepare, reconcile or socket change that runs while RefreshAuth is in
// flight wins over the answer.
func TestRefreshProviderAuthKeepsNewerAnswers(t *testing.T) {
	const authKey = "registry.example/app"
	for _, tc := range []struct {
		name string
		// during runs while RefreshAuth is in flight.
		during          func(m *providerAuthModes)
		answer          imageprovider.AuthRefresh
		answerErr       error
		wantMode        imageprovider.AuthRefresh
		wantUnsupported bool
	}{
		{
			name:     "unchanged auth key takes the answer",
			answer:   imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED,
			wantMode: imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED,
		},
		{
			name:      "prepare during the call wins",
			during:    func(m *providerAuthModes) { m.prepared(authKey, imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED) },
			answerErr: providerclient.ErrLeaseNotHeld,
			wantMode:  imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED,
		},
		{
			name:     "reconcile during the call wins",
			during:   func(m *providerAuthModes) { m.require([]string{authKey}) },
			answer:   imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED,
			wantMode: imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED,
		},
		{
			name:      "socket change during the call keeps RefreshAuth in use",
			during:    func(m *providerAuthModes) { m.clearUnsupported() },
			answerErr: providerclient.ErrNotSupported,
			wantMode:  imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED,
		},
		{
			name:            "plugin without RefreshAuth",
			answerErr:       providerclient.ErrNotSupported,
			wantMode:        imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED,
			wantUnsupported: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actorsDir := t.TempDir()
			writeTestBundle(t, actorsDir, "actor", "main", &imagecache.OverlaySpec{
				ImageRef: "registry.example/app:1", ImageDigest: "sha256:aaa", LeaseID: "actor/main", LowerDirs: []string{"/plugin/app/0"},
			})
			provider := providermock.New("nydus")
			s := &Atelet{imageProvider: provider}
			s.providerAuth.prepared(authKey, imageprovider.AuthRefresh_AUTH_REFRESH_REQUIRED)
			provider.RefreshAuthFunc = func(context.Context, *providerclient.RefreshRequest) (imageprovider.AuthRefresh, error) {
				if tc.during != nil {
					tc.during(&s.providerAuth)
				}
				return tc.answer, tc.answerErr
			}

			s.refreshProviderAuth(t.Context(), actorsDir)
			mode, recorded := authMode(&s.providerAuth, authKey)
			if !recorded || mode != tc.wantMode {
				t.Errorf("recorded mode = %v, %v; want %v, true", mode, recorded, tc.wantMode)
			}
			if got := s.providerAuth.unsupported; got != tc.wantUnsupported {
				t.Errorf("unsupported = %v, want %v", got, tc.wantUnsupported)
			}
		})
	}
}

// A refresh pass forgets the auth keys no overlay spec records a lease for.
func TestRefreshProviderAuthForgetsReleasedAuthKeys(t *testing.T) {
	actorsDir := t.TempDir()
	writeTestBundle(t, actorsDir, "actor", "main", &imagecache.OverlaySpec{
		ImageRef: "registry.example/app:1", ImageDigest: "sha256:aaa", LeaseID: "actor/main", LowerDirs: []string{"/plugin/app/0"},
	})
	s := &Atelet{imageProvider: providermock.New("nydus")}
	s.providerAuth.prepared("registry.example/released", imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED)
	s.refreshProviderAuth(t.Context(), actorsDir)
	if _, ok := authMode(&s.providerAuth, "registry.example/released"); ok {
		t.Errorf("released auth key still recorded after a refresh pass")
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
		refreshed <- req.AuthKey
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
		if got != "registry.example/app" {
			t.Errorf("refreshed auth key %q, want registry.example/app", got)
		}
	case <-t.Context().Done():
		t.Fatal("no refresh after a kick")
	}
	cancel()
	<-done
}
