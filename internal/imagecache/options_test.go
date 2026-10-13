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
	kc := authn.NewKeychainFromHelper(nil)
	s, err := New(t.TempDir(),
		WithKeychain(kc),
		WithLocalhostRegistryReplacement("kind-registry:5000"),
		WithPlatform(v1.Platform{OS: "linux", Architecture: "amd64"}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.keychain != kc {
		t.Errorf("WithKeychain not applied")
	}
	if s.localhostRegistryReplacement != "kind-registry:5000" {
		t.Errorf("WithLocalhostRegistryReplacement not applied: %q", s.localhostRegistryReplacement)
	}
	if s.platform == nil || s.platform.Architecture != "amd64" || s.platform.OS != "linux" {
		t.Errorf("WithPlatform not applied: %+v", s.platform)
	}
}
