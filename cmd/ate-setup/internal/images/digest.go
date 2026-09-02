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
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// Digester resolves a tagged image reference to the digest its tag currently
// names, in "sha256:..." form. Prebuilt takes one so tests can rewrite
// manifests without a registry.
type Digester func(ctx context.Context, ref string) (string, error)

// RemoteDigest asks ref's registry which manifest the tag names.
//
// One HEAD request per image, needing only read access, and authenticated the
// way docker and ko are: from the docker config file and the credential helpers
// it names. A localhost registry is contacted over plain HTTP, which is what
// the Kind local registry serves.
func RemoteDigest(ctx context.Context, ref string) (string, error) {
	parsed, err := name.ParseReference(ref)
	if err != nil {
		return "", fmt.Errorf("%s is not a valid image reference: %w", ref, err)
	}
	desc, err := remote.Head(parsed,
		remote.WithContext(ctx),
		remote.WithAuthFromKeychain(authn.DefaultKeychain),
	)
	if err != nil {
		return "", fmt.Errorf("resolving %s to a digest: %w", ref, err)
	}
	return desc.Digest.String(), nil
}
