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

package images

import (
	"context"
	"fmt"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/google"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// keychain authenticates the digest lookups.
//
// The docker config file and the credential helpers it names, plus GCP. ko
// resolves credentials through a multi-keychain of its own, and matching the
// GCP half of it matters: a user whose Artifact Registry credentials are
// gcloud's rather than a helper wired into ~/.docker/config.json would
// otherwise get a working build from source and a 401 from --image-repo. ECR
// and ACR need credential-helper modules this repository does not depend on,
// so those registries need a docker login.
var keychain = authn.NewMultiKeychain(authn.DefaultKeychain, google.Keychain)

// Digester resolves a tagged image reference to the digest its tag currently
// names, in "sha256:..." form. Prebuilt takes one so tests can rewrite
// manifests without a registry.
type Digester func(ctx context.Context, ref string) (string, error)

// RemoteDigest asks ref's registry which manifest the tag names.
//
// One HEAD request per image, needing only read access. A localhost registry is
// contacted over plain HTTP, which is what the Kind local registry serves.
func RemoteDigest(ctx context.Context, ref string) (string, error) {
	parsed, err := name.ParseReference(ref)
	if err != nil {
		return "", fmt.Errorf("%s is not a valid image reference: %w", ref, err)
	}
	desc, err := remote.Head(parsed,
		remote.WithContext(ctx),
		remote.WithAuthFromKeychain(keychain),
	)
	if err != nil {
		return "", fmt.Errorf("resolving %s to a digest: %w", ref, err)
	}
	return desc.Digest.String(), nil
}
