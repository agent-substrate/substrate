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
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// TestEnsureImage_KeychainCredentials pins that a store with a keychain pulls
// from a registry that requires basic auth using the Docker config the
// keychain resolves, and that without a keychain the same pull is anonymous
// and rejected.
func TestEnsureImage_KeychainCredentials(t *testing.T) {
	const user, pass = "puller", "s3cret"

	// requireAuth stays false until the test image is pushed, so the
	// unauthenticated push helper still works.
	var requireAuth atomic.Bool
	inner := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requireAuth.Load() {
			if u, p, ok := r.BasicAuth(); !ok || u != user || p != pass {
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

	ref := u.Host + "/test/private:1"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "app", typeflag: tar.TypeReg, body: "app"},
	}))
	requireAuth.Store(true)

	// Point the default keychain at a Docker config that holds the
	// registry's credentials, and away from any config in the real home.
	t.Setenv("HOME", t.TempDir())
	dockerConfig := t.TempDir()
	t.Setenv("DOCKER_CONFIG", dockerConfig)
	auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
	cfg := fmt.Sprintf(`{"auths":{%q:{"auth":%q}}}`, u.Host, auth)
	if err := os.WriteFile(filepath.Join(dockerConfig, "config.json"), []byte(cfg), 0o600); err != nil {
		t.Fatalf("writing docker config: %v", err)
	}

	if _, err := newTestStore(t).EnsureImage(context.Background(), ref); err == nil {
		t.Fatalf("EnsureImage without a keychain succeeded against a registry requiring auth")
	}
	if _, err := newTestStore(t, WithKeychain(authn.DefaultKeychain)).EnsureImage(context.Background(), ref); err != nil {
		t.Fatalf("EnsureImage with the default keychain: %v", err)
	}
}
