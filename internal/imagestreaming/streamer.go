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

// Package imagestreaming provides generalized interfaces and registries for
// container image acceleration and lazy-pull streaming providers (such as
// Google Riptide/GCFS and AWS SOCI).
package imagestreaming

import (
	"context"
	"errors"

	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// ErrNotStreamable reports that a provider declined to stream an image, for
// example because the image has no streaming index or its registry isn't
// supported. Callers should pull the image without streaming instead.
var ErrNotStreamable = errors.New("image not streamable")

type contextKeyKeychain struct{}

// WithKeychainContext returns a child context carrying the specified authn.Keychain.
func WithKeychainContext(ctx context.Context, k authn.Keychain) context.Context {
	return context.WithValue(ctx, contextKeyKeychain{}, k)
}

// KeychainFromContext extracts an authn.Keychain from the context if present.
func KeychainFromContext(ctx context.Context) authn.Keychain {
	if ctx == nil {
		return nil
	}
	if k, ok := ctx.Value(contextKeyKeychain{}).(authn.Keychain); ok {
		return k
	}
	return nil
}

// AuthConfig contains registry credentials for one image request. Drivers use
// them to resolve the image manifest and config, and providers that expose a
// credential side-channel (such as Google Riptide V2's gcfsd keychain service)
// also receive them for on-demand layer streaming.
type AuthConfig struct {
	Username      string `json:"username,omitempty"`
	Password      string `json:"password,omitempty"`
	Auth          string `json:"auth,omitempty"`
	ServerAddress string `json:"serveraddress,omitempty"`
	IdentityToken string `json:"identitytoken,omitempty"`
	RegistryToken string `json:"registrytoken,omitempty"`
}

// StreamRequest encapsulates the parameters needed to evaluate or prepare
// streamed image layers on the node.
type StreamRequest struct {
	// ImageRef is the fully-qualified OCI image reference (with digest).
	ImageRef string

	// AuthConfig holds optional credentials for resolving the image manifest
	// and config (and, on Riptide V2, registering credentials with gcfsd).
	// When nil, the driver uses its node-level keychain.
	AuthConfig *AuthConfig
}

// StreamResult holds the prepared layer information for an accelerated image.
type StreamResult struct {
	// ImageDigest is the manifest digest the image resolved to ("sha256:<hex>").
	ImageDigest string

	// Config is the OCI image config (entrypoint, cmd, env, etc.). Optional if
	// the caller fetches the config blob independently.
	Config *v1.Config

	// LayerDirs contains the host directory paths of the mounted layer trees,
	// ordered bottom-most layer first (matching OCI manifest order).
	// These paths are directly consumable by overlayfs lowerdir composition.
	LayerDirs []string
}

// ActiveLease describes an active image lease to restore during reconciliation.
type ActiveLease struct {
	// ImageRef is the fully-qualified OCI image reference.
	ImageRef string

	// ImageDigest is the manifest digest the image resolved to ("sha256:<hex>").
	ImageDigest string

	// LayerDirs contains the host directory paths of the mounted layer trees.
	LayerDirs []string

	// RefCount is the number of active actor containers referencing this image.
	RefCount int
}

// ImageStreamer is the generalized interface implemented by image acceleration
// and streaming backends.
type ImageStreamer interface {
	// Name returns the identifier of the streaming provider (e.g. "riptide", "soci").
	Name() string

	// CanStream is a cheap check that the provider is available, such as
	// whether its daemon answers. It need not inspect the image: PrepareLayers
	// returns ErrNotStreamable if the provider declines the image.
	CanStream(ctx context.Context, req *StreamRequest) (bool, error)

	// PrepareLayers prepares and mounts the virtual layer directories on the node
	// and returns their absolute paths. It returns an error wrapping
	// ErrNotStreamable if the provider declines the image.
	PrepareLayers(ctx context.Context, req *StreamRequest) (*StreamResult, error)

	// ReleaseLayers releases or decrements the reference count for the layers
	// associated with this image request when the actor is torn down.
	ReleaseLayers(ctx context.Context, req *StreamRequest) error

	// ReconcileLeases restores active lease tracking and reference counts for
	// surviving actor workloads on node or process startup.
	ReconcileLeases(ctx context.Context, active []*ActiveLease) error
}
