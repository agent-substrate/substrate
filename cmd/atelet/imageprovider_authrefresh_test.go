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
		t.Errorf("pass against an older plugin refreshed %v, want one attempt", got)
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
