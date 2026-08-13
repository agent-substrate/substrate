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
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/credentialprovider"
	"github.com/google/go-containerregistry/pkg/authn"
	googlecontainerauth "github.com/google/go-containerregistry/pkg/v1/google"
)

// newImagePullCredentials picks how atelet authenticates image pulls: the
// node's kubelet credential provider plugins (registry-agnostic, no
// cloud-specific code) or the legacy GCP application default credentials,
// which cover gcr.io and pkg.dev only. At most one is non-nil; neither means
// anonymous pulls. The provider path wins when configured, since running both
// would obscure which produced a credential.
func newImagePullCredentials(ctx context.Context) (keychain authn.Keychain, gcpAuth authn.Authenticator, err error) {
	if *imageCredentialProviderConfig != "" {
		if *imageCredentialProviderBinDir == "" {
			return nil, nil, fmt.Errorf("--image-credential-provider-bin-dir is required when --image-credential-provider-config is set")
		}
		kc, err := credentialprovider.New(*imageCredentialProviderConfig, *imageCredentialProviderBinDir)
		if err != nil {
			return nil, nil, err
		}
		return kc, nil, nil
	}

	if *gcpAuthForImagePulls {
		gcpAuth, err := googlecontainerauth.NewEnvAuthenticator(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("while creating GCP registry authenticator: %w", err)
		}
		return nil, gcpAuth, nil
	}

	return nil, nil, nil
}
