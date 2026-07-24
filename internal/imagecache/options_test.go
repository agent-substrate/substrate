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
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

func TestOptionsApply(t *testing.T) {
	auth := authn.FromConfig(authn.AuthConfig{Username: "u"})
	s, err := New(t.TempDir(),
		WithAuthenticator(auth),
		WithLocalhostRegistryReplacement("kind-registry:5000"),
		WithPlatform(v1.Platform{OS: "linux", Architecture: "amd64"}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.authenticator != auth {
		t.Errorf("WithAuthenticator not applied")
	}
	if s.localhostRegistryReplacement != "kind-registry:5000" {
		t.Errorf("WithLocalhostRegistryReplacement not applied: %q", s.localhostRegistryReplacement)
	}
	if s.platform == nil || s.platform.Architecture != "amd64" || s.platform.OS != "linux" {
		t.Errorf("WithPlatform not applied: %+v", s.platform)
	}
}

func TestRegistryUsesGCPAuth(t *testing.T) {
	tests := []struct {
		registry string
		want     bool
	}{
		{"gcr.io", true},
		{"us.gcr.io", true},
		{"eu.gcr.io", true},
		{"pkg.dev", true},
		{"us-docker.pkg.dev", true},
		{"docker.io", false},
		{"index.docker.io", false},
		{"quay.io", false},
		{"notgcr.io", false},
		{"gcr.io.evil.example", false},
		{"pkg.dev.evil.example", false},
		{"kind-registry:5000", false},
		{"", false},
	}
	for _, tc := range tests {
		if got := registryUsesGCPAuth(tc.registry); got != tc.want {
			t.Errorf("registryUsesGCPAuth(%q) = %v, want %v", tc.registry, got, tc.want)
		}
	}
}
