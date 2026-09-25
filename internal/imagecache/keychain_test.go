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

package imagecache

import (
	"archive/tar"
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// newAuthedRegistry starts an in-memory registry that is open until
// requireAuth is called, after which every request needs the given basic
// credentials. Tests push their fixtures while it is open, then close it, so
// any later success is attributable to the credentials under test.
func newAuthedRegistry(t *testing.T, username, password string) (host string, requireAuth func()) {
	t.Helper()
	var authRequired atomic.Bool
	inner := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if authRequired.Load() {
			if u, p, ok := r.BasicAuth(); !ok || u != username || p != password {
				w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parsing registry URL: %v", err)
	}
	return u.Host, func() { authRequired.Store(true) }
}

// staticKeychain hands out the same credentials for every resource, recording
// what it was asked about.
type staticKeychain struct {
	auth     authn.Authenticator
	resolved []string
}

func (k *staticKeychain) Resolve(target authn.Resource) (authn.Authenticator, error) {
	k.resolved = append(k.resolved, target.String())
	return k.auth, nil
}

func TestEnsureImage_KeychainAuthenticatesPull(t *testing.T) {
	host, requireAuth := newAuthedRegistry(t, "robot", "s3cret")
	ref := host + "/test/app:latest"
	layer := layerFromEntries(t, []tarEntry{
		{name: "app/", typeflag: tar.TypeDir},
		{name: "app/main", typeflag: tar.TypeReg, mode: 0o755, body: "main"},
	})
	pushImage(t, ref, v1.Config{}, layer)
	requireAuth()

	// Without credentials the registry rejects the pull, so a success below is
	// attributable to the keychain and nothing else.
	if _, err := newTestStore(t).EnsureImage(context.Background(), ref); err == nil {
		t.Fatal("EnsureImage succeeded against an authenticated registry with no keychain, want an error")
	}

	kc := &staticKeychain{auth: authn.FromConfig(authn.AuthConfig{Username: "robot", Password: "s3cret"})}
	if _, err := newTestStore(t, WithKeychain(kc)).EnsureImage(context.Background(), ref); err != nil {
		t.Fatalf("EnsureImage with a keychain: %v", err)
	}
	if len(kc.resolved) == 0 {
		t.Error("keychain was never consulted")
	}
	for _, got := range kc.resolved {
		if want := host + "/test/app"; got != want {
			t.Errorf("keychain resolved %q, want %q", got, want)
		}
	}
}

func TestRemoteOptsAttachesKeychain(t *testing.T) {
	kc := &staticKeychain{auth: authn.Anonymous}

	// remote.Option values are opaque, so assert on how many the store
	// attaches: two unconditional ones (context, platform) plus the keychain
	// when one is set. The keychain is registry-agnostic -- it decides for
	// itself which registries it can authenticate.
	const base = 2
	for _, tc := range []struct {
		name string
		opts []Option
		ref  string
		want int
	}{
		{name: "no keychain", ref: "gcr.io/proj/img", want: base},
		{name: "keychain, gcp registry", opts: []Option{WithKeychain(kc)}, ref: "gcr.io/proj/img", want: base + 1},
		{name: "keychain, other registry", opts: []Option{WithKeychain(kc)}, ref: "quay.io/proj/img", want: base + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t, tc.opts...)
			parsed, err := s.parseRef(tc.ref)
			if err != nil {
				t.Fatalf("parseRef(%q): %v", tc.ref, err)
			}
			if got := len(s.remoteOpts(context.Background(), parsed)); got != tc.want {
				t.Errorf("remoteOpts returned %d options, want %d", got, tc.want)
			}
		})
	}
}
