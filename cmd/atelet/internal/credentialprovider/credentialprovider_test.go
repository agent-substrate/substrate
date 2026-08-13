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

package credentialprovider

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
)

// fakePlugin writes an executable named name into dir that records each
// request it is handed (one JSON object per line in <dir>/<name>.requests) and
// replies with response. It returns the requests file's path.
func fakePlugin(t *testing.T, dir, pluginName, response string) string {
	t.Helper()
	requests := filepath.Join(dir, pluginName+".requests")
	script := fmt.Sprintf(`#!/bin/sh
cat >> %q
echo >> %q
cat <<'RESPONSE'
%s
RESPONSE
`, requests, requests, response)
	if err := os.WriteFile(filepath.Join(dir, pluginName), []byte(script), 0o700); err != nil {
		t.Fatalf("Failed to write fake plugin: %v", err)
	}
	return requests
}

// failingPlugin writes an executable that prints message to stderr and exits
// non-zero.
func failingPlugin(t *testing.T, dir, pluginName, message string) {
	t.Helper()
	script := fmt.Sprintf("#!/bin/sh\necho %q >&2\nexit 7\n", message)
	if err := os.WriteFile(filepath.Join(dir, pluginName), []byte(script), 0o700); err != nil {
		t.Fatalf("Failed to write failing plugin: %v", err)
	}
}

// writeConfig writes a CredentialProviderConfig into dir and returns its path.
func writeConfig(t *testing.T, dir, contents string) string {
	t.Helper()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("Failed to write credential provider config: %v", err)
	}
	return path
}

// gcpLikeConfig mirrors the CredentialProviderConfig a GKE node ships.
const gcpLikeConfig = `kind: CredentialProviderConfig
apiVersion: kubelet.config.k8s.io/v1
providers:
  - name: fake-provider
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages:
    - "gcr.io"
    - "*.gcr.io"
    - "*.pkg.dev"
    args:
    - get-credentials
    defaultCacheDuration: 1m
`

// repo turns an image reference into the authn.Resource a pull would resolve
// against.
func repo(t *testing.T, ref string) authn.Resource {
	t.Helper()
	parsed, err := name.ParseReference(ref)
	if err != nil {
		t.Fatalf("Failed to parse reference %q: %v", ref, err)
	}
	return parsed.Context()
}

// resolvedAuth resolves ref through kc and returns the resulting basic auth.
func resolvedAuth(t *testing.T, kc *Keychain, ref string) *authn.AuthConfig {
	t.Helper()
	authenticator, err := kc.Resolve(repo(t, ref))
	if err != nil {
		t.Fatalf("Resolve(%q) returned unexpected error: %v", ref, err)
	}
	cfg, err := authenticator.Authorization()
	if err != nil {
		t.Fatalf("Authorization() returned unexpected error: %v", err)
	}
	return cfg
}

func TestKeychainResolvesMatchingImage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	requests := fakePlugin(t, dir, "fake-provider", `{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": "Registry",
  "auth": {"*.pkg.dev": {"username": "_token", "password": "ya29.fake"}}
}`)

	kc, err := New(writeConfig(t, dir, gcpLikeConfig), dir)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	got := resolvedAuth(t, kc, "us-central1-docker.pkg.dev/proj/repo/img:latest")
	if got.Username != "_token" || got.Password != "ya29.fake" {
		t.Errorf("Resolve returned username %q password %q, want %q / %q", got.Username, got.Password, "_token", "ya29.fake")
	}

	// The plugin must be handed a well-formed request naming the repository
	// without its tag, which is the granularity the protocol's cache keys use.
	raw, err := os.ReadFile(requests)
	if err != nil {
		t.Fatalf("Failed to read recorded requests: %v", err)
	}
	var req struct {
		Kind       string `json:"kind"`
		APIVersion string `json:"apiVersion"`
		Image      string `json:"image"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(raw))), &req); err != nil {
		t.Fatalf("Failed to parse recorded request %q: %v", raw, err)
	}
	if req.Kind != "CredentialProviderRequest" || req.APIVersion != supportedAPIVersion {
		t.Errorf("Plugin received kind %q apiVersion %q, want %q / %q", req.Kind, req.APIVersion, "CredentialProviderRequest", supportedAPIVersion)
	}
	if want := "us-central1-docker.pkg.dev/proj/repo/img"; req.Image != want {
		t.Errorf("Plugin received image %q, want %q", req.Image, want)
	}
}

func TestKeychainSkipsUnmatchedImage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// An image no provider claims must not exec anything, so point the config
	// at a plugin that would fail loudly if it ever ran.
	failingPlugin(t, dir, "fake-provider", "plugin must not be invoked")

	kc, err := New(writeConfig(t, dir, gcpLikeConfig), dir)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	authenticator, err := kc.Resolve(repo(t, "docker.io/library/busybox:latest"))
	if err != nil {
		t.Fatalf("Resolve returned unexpected error: %v", err)
	}
	if authenticator != authn.Anonymous {
		t.Errorf("Resolve returned %v for an unclaimed image, want authn.Anonymous", authenticator)
	}
}

func TestKeychainAnonymousWhenPluginReturnsNoAuth(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fakePlugin(t, dir, "fake-provider", `{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": "Registry"
}`)

	kc, err := New(writeConfig(t, dir, gcpLikeConfig), dir)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	authenticator, err := kc.Resolve(repo(t, "gcr.io/proj/img:latest"))
	if err != nil {
		t.Fatalf("Resolve returned unexpected error: %v", err)
	}
	if authenticator != authn.Anonymous {
		t.Errorf("Resolve returned %v when the plugin gave no credentials, want authn.Anonymous", authenticator)
	}
}

func TestKeychainSurfacesPluginFailure(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	failingPlugin(t, dir, "fake-provider", "metadata server unreachable")

	kc, err := New(writeConfig(t, dir, gcpLikeConfig), dir)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	_, err = kc.Resolve(repo(t, "gcr.io/proj/img:latest"))
	if err == nil {
		t.Fatal("Resolve returned no error for a failing plugin, want one")
	}
	// The plugin's own diagnostics are the only clue to why a pull lost its
	// credentials, so they must reach the error.
	if !strings.Contains(err.Error(), "metadata server unreachable") {
		t.Errorf("Resolve error %q does not carry the plugin's stderr", err)
	}
}

func TestKeychainRejectsMismatchedResponseVersion(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fakePlugin(t, dir, "fake-provider", `{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1beta1",
  "cacheKeyType": "Registry",
  "auth": {"gcr.io": {"username": "u", "password": "p"}}
}`)

	kc, err := New(writeConfig(t, dir, gcpLikeConfig), dir)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	if _, err := kc.Resolve(repo(t, "gcr.io/proj/img:latest")); err == nil {
		t.Fatal("Resolve accepted a response encoded at an unrequested apiVersion, want an error")
	}
}

func TestKeychainFallsThroughToSecondProvider(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// first-provider claims the image but has nothing for it; the keychain
	// must go on to ask second-provider rather than give up.
	fakePlugin(t, dir, "first-provider", `{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": "Registry",
  "auth": {}
}`)
	fakePlugin(t, dir, "second-provider", `{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": "Registry",
  "auth": {"gcr.io": {"username": "second", "password": "creds"}}
}`)

	kc, err := New(writeConfig(t, dir, `kind: CredentialProviderConfig
apiVersion: kubelet.config.k8s.io/v1
providers:
  - name: first-provider
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["gcr.io"]
    defaultCacheDuration: 1m
  - name: second-provider
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["gcr.io"]
    defaultCacheDuration: 1m
`), dir)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	got := resolvedAuth(t, kc, "gcr.io/proj/img:latest")
	if got.Username != "second" {
		t.Errorf("Resolve returned username %q, want %q from the second provider", got.Username, "second")
	}
}

// countRequests returns how many requests the fake plugin has recorded.
func countRequests(t *testing.T, requests string) int {
	t.Helper()
	raw, err := os.ReadFile(requests)
	if err != nil {
		t.Fatalf("Failed to read recorded requests: %v", err)
	}
	return len(strings.Fields(strings.TrimSpace(string(raw))))
}

func TestKeychainCaching(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// cacheKeyType and cacheDuration as the plugin reports them.
		cacheKeyType  string
		cacheDuration string
		// second is resolved after the first image; wantExecs is how many
		// times the plugin should have run in total by then.
		first, second string
		wantExecs     int
	}{
		{
			name:         "registry key reuses across repositories",
			cacheKeyType: "Registry",
			first:        "gcr.io/proj/one:latest",
			second:       "gcr.io/proj/two:latest",
			wantExecs:    1,
		},
		{
			name:         "registry key does not span registries",
			cacheKeyType: "Registry",
			first:        "gcr.io/proj/one:latest",
			second:       "us.gcr.io/proj/one:latest",
			wantExecs:    2,
		},
		{
			name:         "image key does not span repositories",
			cacheKeyType: "Image",
			first:        "gcr.io/proj/one:latest",
			second:       "gcr.io/proj/two:latest",
			wantExecs:    2,
		},
		{
			name:         "image key reuses for the same repository",
			cacheKeyType: "Image",
			first:        "gcr.io/proj/one:latest",
			second:       "gcr.io/proj/one:other",
			wantExecs:    1,
		},
		{
			name:         "global key spans registries",
			cacheKeyType: "Global",
			first:        "gcr.io/proj/one:latest",
			second:       "us.gcr.io/other/two:latest",
			wantExecs:    1,
		},
		{
			// Zero is the providers' back-compat default, not a considered
			// answer, so it is raised to minCacheDuration instead of forking a
			// subprocess per pull. See minCacheDuration.
			name:          "zero duration is cached anyway",
			cacheKeyType:  "Registry",
			cacheDuration: `"cacheDuration": "0s",`,
			first:         "gcr.io/proj/one:latest",
			second:        "gcr.io/proj/one:latest",
			wantExecs:     1,
		},
		{
			name:         "unknown key type disables caching",
			cacheKeyType: "Nonsense",
			first:        "gcr.io/proj/one:latest",
			second:       "gcr.io/proj/one:latest",
			wantExecs:    2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			requests := fakePlugin(t, dir, "fake-provider", fmt.Sprintf(`{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": %q,
  %s
  "auth": {"*.gcr.io": {"username": "u", "password": "p"}, "gcr.io": {"username": "u", "password": "p"}}
}`, tc.cacheKeyType, tc.cacheDuration))

			kc, err := New(writeConfig(t, dir, gcpLikeConfig), dir)
			if err != nil {
				t.Fatalf("New returned unexpected error: %v", err)
			}

			for _, ref := range []string{tc.first, tc.second} {
				if _, err := kc.Resolve(repo(t, ref)); err != nil {
					t.Fatalf("Resolve(%q) returned unexpected error: %v", ref, err)
				}
			}
			if got := countRequests(t, requests); got != tc.wantExecs {
				t.Errorf("Plugin ran %d times, want %d", got, tc.wantExecs)
			}
		})
	}
}

func TestKeychainCacheExpires(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	requests := fakePlugin(t, dir, "fake-provider", `{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": "Registry",
  "auth": {"gcr.io": {"username": "u", "password": "p"}}
}`)

	kc, err := New(writeConfig(t, dir, gcpLikeConfig), dir)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	clock := time.Now()
	kc.plugins[0].now = func() time.Time { return clock }

	ref := repo(t, "gcr.io/proj/img:latest")
	if _, err := kc.Resolve(ref); err != nil {
		t.Fatalf("Resolve returned unexpected error: %v", err)
	}
	// The config's defaultCacheDuration is 1m; just short of it still hits.
	clock = clock.Add(59 * time.Second)
	if _, err := kc.Resolve(ref); err != nil {
		t.Fatalf("Resolve returned unexpected error: %v", err)
	}
	if got := countRequests(t, requests); got != 1 {
		t.Fatalf("Plugin ran %d times before the cache expired, want 1", got)
	}

	clock = clock.Add(2 * time.Second)
	if _, err := kc.Resolve(ref); err != nil {
		t.Fatalf("Resolve returned unexpected error: %v", err)
	}
	if got := countRequests(t, requests); got != 2 {
		t.Errorf("Plugin ran %d times after the cache expired, want 2", got)
	}
}

// Not parallel: t.Setenv mutates the process environment the plugin inherits.
func TestKeychainPassesConfiguredEnv(t *testing.T) {
	dir := t.TempDir()
	// The plugin echoes an env var back as the username, so the assertion
	// covers both the configured env and the inherited process env.
	script := `#!/bin/sh
cat > /dev/null
cat <<RESPONSE
{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","auth":{"gcr.io":{"username":"$FROM_CONFIG","password":"$FROM_PROCESS"}}}
RESPONSE
`
	if err := os.WriteFile(filepath.Join(dir, "fake-provider"), []byte(script), 0o700); err != nil {
		t.Fatalf("Failed to write fake plugin: %v", err)
	}
	t.Setenv("FROM_PROCESS", "inherited")

	kc, err := New(writeConfig(t, dir, `kind: CredentialProviderConfig
apiVersion: kubelet.config.k8s.io/v1
providers:
  - name: fake-provider
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["gcr.io"]
    defaultCacheDuration: 1m
    env:
    - name: FROM_CONFIG
      value: configured
`), dir)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	got := resolvedAuth(t, kc, "gcr.io/proj/img:latest")
	if got.Username != "configured" {
		t.Errorf("Plugin saw FROM_CONFIG=%q, want %q", got.Username, "configured")
	}
	if got.Password != "inherited" {
		t.Errorf("Plugin saw FROM_PROCESS=%q, want %q", got.Password, "inherited")
	}
}

func TestKeychainPassesConfiguredArgs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	script := `#!/bin/sh
cat > /dev/null
cat <<RESPONSE
{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","auth":{"gcr.io":{"username":"$1","password":"$2"}}}
RESPONSE
`
	if err := os.WriteFile(filepath.Join(dir, "fake-provider"), []byte(script), 0o700); err != nil {
		t.Fatalf("Failed to write fake plugin: %v", err)
	}

	kc, err := New(writeConfig(t, dir, `kind: CredentialProviderConfig
apiVersion: kubelet.config.k8s.io/v1
providers:
  - name: fake-provider
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["gcr.io"]
    defaultCacheDuration: 1m
    args:
    - get-credentials
    - --v=3
`), dir)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	got := resolvedAuth(t, kc, "gcr.io/proj/img:latest")
	if got.Username != "get-credentials" || got.Password != "--v=3" {
		t.Errorf("Plugin got args [%q %q], want [%q %q]", got.Username, got.Password, "get-credentials", "--v=3")
	}
}

func TestKeychainRespectsContextCancellation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "fake-provider"), []byte("#!/bin/sh\nsleep 60\n"), 0o700); err != nil {
		t.Fatalf("Failed to write fake plugin: %v", err)
	}

	kc, err := New(writeConfig(t, dir, gcpLikeConfig), dir)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := kc.ResolveContext(ctx, repo(t, "gcr.io/proj/img:latest")); err == nil {
		t.Fatal("ResolveContext returned no error for a cancelled context, want one")
	}
}

// A provider atelet cannot run must not disable the ones it can: the node's
// config is shared with its kubelet and may name providers we do not implement.
func TestKeychainSkipsUnusableProviderAndKeepsRest(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fakePlugin(t, dir, "usable-provider", `{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": "Registry",
  "auth": {"gcr.io": {"username": "u", "password": "p"}}
}`)

	// unusable-provider sets tokenAttributes, which atelet does not implement.
	kc, err := New(writeConfig(t, dir, `kind: CredentialProviderConfig
apiVersion: kubelet.config.k8s.io/v1
providers:
  - name: unusable-provider
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["gcr.io"]
    defaultCacheDuration: 1m
    tokenAttributes:
      serviceAccountTokenAudience: aud
      cacheType: Token
      requireServiceAccount: true
  - name: usable-provider
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["gcr.io"]
    defaultCacheDuration: 1m
`), dir)
	if err != nil {
		t.Fatalf("New returned an error for a config with one unusable provider: %v", err)
	}

	got := resolvedAuth(t, kc, "gcr.io/proj/img:latest")
	if got.Username != "u" || got.Password != "p" {
		t.Errorf("Resolve returned %q/%q, want the usable provider's %q/%q", got.Username, got.Password, "u", "p")
	}
}

// A provider naming a lifetime shorter than minCacheDuration has opted into
// caching and may be describing a short-lived credential, so its value is used
// as given rather than stretched.
func TestKeychainHonorsShortCacheDuration(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	requests := fakePlugin(t, dir, "fake-provider", `{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": "Registry",
  "cacheDuration": "30s",
  "auth": {"gcr.io": {"username": "u", "password": "p"}}
}`)

	kc, err := New(writeConfig(t, dir, gcpLikeConfig), dir)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	clock := time.Now()
	kc.plugins[0].now = func() time.Time { return clock }

	ref := repo(t, "gcr.io/proj/img:latest")
	if _, err := kc.Resolve(ref); err != nil {
		t.Fatalf("Resolve returned unexpected error: %v", err)
	}
	// Past the plugin's 30s but inside minCacheDuration: honoring the plugin
	// means re-execing here, stretching it to a minute would not.
	clock = clock.Add(45 * time.Second)
	if _, err := kc.Resolve(ref); err != nil {
		t.Fatalf("Resolve returned unexpected error: %v", err)
	}
	if got := countRequests(t, requests); got != 2 {
		t.Errorf("Plugin ran %d times after its 30s cacheDuration expired, want 2", got)
	}
}
