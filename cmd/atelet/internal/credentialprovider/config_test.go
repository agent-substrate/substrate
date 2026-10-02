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
	"path/filepath"
	"testing"
	"time"
)

// gkeConfig is verbatim /etc/srv/kubernetes/cri_auth_config.yaml from a GKE
// node, the config this package exists to consume.
const gkeConfig = `kind: CredentialProviderConfig
apiVersion: kubelet.config.k8s.io/v1
providers:
  - name: auth-provider-gcp
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages:
    - "container.cloud.google.com"
    - "gcr.io"
    - "*.gcr.io"
    - "*.pkg.dev"
    args:
    - get-credentials
    - --v=3
    defaultCacheDuration: 1m
`

func TestLoadConfigGKE(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	plugins, err := loadConfig(writeConfig(t, dir, gkeConfig), "/home/kubernetes/bin")
	if err != nil {
		t.Fatalf("loadConfig returned unexpected error: %v", err)
	}
	if len(plugins) != 1 {
		t.Fatalf("loadConfig returned %d plugins, want 1", len(plugins))
	}
	p := plugins[0]
	if want := filepath.Join("/home/kubernetes/bin", "auth-provider-gcp"); p.path != want {
		t.Errorf("plugin path = %q, want %q", p.path, want)
	}
	if p.defaultCacheDuration != time.Minute {
		t.Errorf("plugin defaultCacheDuration = %v, want 1m", p.defaultCacheDuration)
	}
	if len(p.args) != 2 || p.args[0] != "get-credentials" {
		t.Errorf("plugin args = %v, want [get-credentials --v=3]", p.args)
	}

	// The provider must claim Artifact Registry and GCR but nothing else.
	for _, tc := range []struct {
		image string
		want  bool
	}{
		{image: "gcr.io/proj/img", want: true},
		{image: "us.gcr.io/proj/img", want: true},
		{image: "us-central1-docker.pkg.dev/proj/repo/img", want: true},
		{image: "docker.io/library/busybox", want: false},
		{image: "quay.io/proj/img", want: false},
	} {
		got, err := p.claims(tc.image)
		if err != nil {
			t.Fatalf("claims(%q) returned unexpected error: %v", tc.image, err)
		}
		if got != tc.want {
			t.Errorf("claims(%q) = %v, want %v", tc.image, got, tc.want)
		}
	}
}

// Config-level problems mean atelet was pointed at the wrong file, and are
// the only thing loadConfig treats as an error.
func TestLoadConfigRejectsInvalidConfig(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		config string
	}{
		{
			name: "wrong kind",
			config: `kind: KubeletConfiguration
apiVersion: kubelet.config.k8s.io/v1
providers:
  - name: p
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["gcr.io"]
    defaultCacheDuration: 1m
`,
		},
		{
			name: "wrong config apiVersion",
			config: `kind: CredentialProviderConfig
apiVersion: kubelet.config.k8s.io/v1beta1
providers:
  - name: p
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["gcr.io"]
    defaultCacheDuration: 1m
`,
		},
		{
			name:   "not yaml",
			config: "this: is: not: valid: yaml:\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if _, err := loadConfig(writeConfig(t, dir, tc.config), dir); err == nil {
				t.Error("loadConfig accepted an invalid config, want an error")
			}
		})
	}
}

// A provider entry atelet cannot use is skipped, not fatal: the config belongs
// to the node and is shared with its kubelet, so one unsupported entry must not
// cost us the others (or the whole atelet).
func TestLoadConfigSkipsUnusableProviders(t *testing.T) {
	t.Parallel()
	// Each case pairs one unusable provider with one good "keeper", so the
	// assertion covers both that the bad one is dropped and that the rest of
	// the config still loads.
	for _, tc := range []struct {
		name string
		bad  string
	}{
		{
			name: "missing name",
			bad: `  - apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["quay.io"]
    defaultCacheDuration: 1m
`,
		},
		{
			name: "name escapes the bin dir",
			bad: `  - name: ../../bin/sh
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["quay.io"]
    defaultCacheDuration: 1m
`,
		},
		{
			name: "no matchImages",
			bad: `  - name: bad
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    defaultCacheDuration: 1m
`,
		},
		{
			name: "missing defaultCacheDuration",
			bad: `  - name: bad
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["quay.io"]
`,
		},
		{
			name: "unsupported exec apiVersion",
			bad: `  - name: bad
    apiVersion: credentialprovider.kubelet.k8s.io/v1beta1
    matchImages: ["quay.io"]
    defaultCacheDuration: 1m
`,
		},
		{
			name: "service account token attributes",
			bad: `  - name: bad
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["quay.io"]
    defaultCacheDuration: 1m
    tokenAttributes:
      serviceAccountTokenAudience: aud
      cacheType: Token
      requireServiceAccount: true
`,
		},
		{
			name: "duplicate of the keeper",
			bad: `  - name: keeper
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["quay.io"]
    defaultCacheDuration: 1m
`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			config := "kind: CredentialProviderConfig\napiVersion: kubelet.config.k8s.io/v1\nproviders:\n" +
				`  - name: keeper
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["gcr.io"]
    defaultCacheDuration: 1m
` + tc.bad

			plugins, err := loadConfig(writeConfig(t, dir, config), dir)
			if err != nil {
				t.Fatalf("loadConfig returned an error for an unusable provider, want it skipped: %v", err)
			}
			if len(plugins) != 1 {
				names := make([]string, 0, len(plugins))
				for _, p := range plugins {
					names = append(names, p.name)
				}
				t.Fatalf("loadConfig returned providers %v, want only [keeper]", names)
			}
			if plugins[0].name != "keeper" {
				t.Errorf("surviving provider is %q, want %q", plugins[0].name, "keeper")
			}
			// The keeper must be intact, not just present.
			if got, err := plugins[0].claims("gcr.io/proj/img"); err != nil || !got {
				t.Errorf("keeper.claims(gcr.io/proj/img) = %v, %v; want true, nil", got, err)
			}
		})
	}
}

// An unusable config is degraded to "everything pulls anonymously" rather than
// killing atelet, so a node whose providers we cannot run still serves actors
// whose images are public or already cached.
func TestLoadConfigEmptyWhenNoProviderUsable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		config string
	}{
		{
			name: "no providers declared",
			config: `kind: CredentialProviderConfig
apiVersion: kubelet.config.k8s.io/v1
providers: []
`,
		},
		{
			name: "only provider is unusable",
			config: `kind: CredentialProviderConfig
apiVersion: kubelet.config.k8s.io/v1
providers:
  - name: bad
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    matchImages: ["gcr.io"]
    defaultCacheDuration: 1m
    tokenAttributes:
      serviceAccountTokenAudience: aud
      cacheType: Token
      requireServiceAccount: true
`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			plugins, err := loadConfig(writeConfig(t, dir, tc.config), dir)
			if err != nil {
				t.Fatalf("loadConfig returned an error, want an empty provider set: %v", err)
			}
			if len(plugins) != 0 {
				t.Errorf("loadConfig returned %d providers, want 0", len(plugins))
			}
		})
	}
}

func TestLoadConfigMissingFile(t *testing.T) {
	t.Parallel()
	if _, err := loadConfig(filepath.Join(t.TempDir(), "absent.yaml"), "/bin"); err == nil {
		t.Error("loadConfig accepted a missing config file, want an error")
	}
}
