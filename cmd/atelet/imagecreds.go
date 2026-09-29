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

package main

import (
	"fmt"
	"os"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/credentialprovider"
	"github.com/google/go-containerregistry/pkg/authn"
)

// newImagePullCredentials builds the keychain atelet authenticates image pulls
// with, combining an optional Docker config (e.g. a mounted Kubernetes
// imagePullSecret at $DOCKER_CONFIG/config.json or ~/.docker/config.json) with
// the node's kubelet credential provider plugins. A nil keychain means
// anonymous pulls, which is all an unconfigured node (kind, say) can do and all
// a public registry needs.
func newImagePullCredentials() (authn.Keychain, error) {
	if *imageCredentialProviderConfig == "" {
		if os.Getenv("DOCKER_CONFIG") != "" {
			return authn.DefaultKeychain, nil
		}
		return nil, nil
	}
	if *imageCredentialProviderBinDir == "" {
		return nil, fmt.Errorf("--image-credential-provider-bin-dir is required when --image-credential-provider-config is set")
	}
	cpKeychain, err := credentialprovider.New(*imageCredentialProviderConfig, *imageCredentialProviderBinDir)
	if err != nil {
		return nil, err
	}
	return authn.NewMultiKeychain(authn.DefaultKeychain, cpKeychain), nil
}
