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

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// AuthConfig contains registry authentication credentials that may be required
// by streaming providers to fetch layer chunks on demand from private registries.
type AuthConfig struct {
	Username      string `json:"username,omitempty"`
	Password      string `json:"password,omitempty"`
	Auth          string `json:"auth,omitempty"`
	IdentityToken string `json:"identitytoken,omitempty"`
	RegistryToken string `json:"registrytoken,omitempty"`
}

// StreamRequest encapsulates the parameters needed to evaluate or prepare
// streamed image layers on the node.
type StreamRequest struct {
	// ImageRef is the fully-qualified OCI image reference (with digest).
	ImageRef string

	// AuthConfig holds optional credentials for pulling from private registries.
	// When nil, the provider may rely on ambient node credentials (such as GKE Workload Identity).
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

	// CanStream checks if the provider can stream the requested image
	// (e.g., streaming index exists and registry is supported).
	CanStream(ctx context.Context, req *StreamRequest) (bool, error)

	// PrepareLayers prepares and mounts the virtual layer directories on the node
	// and returns their absolute paths.
	PrepareLayers(ctx context.Context, req *StreamRequest) (*StreamResult, error)

	// ReleaseLayers releases or decrements the reference count for the layers
	// associated with this image request when the actor is torn down.
	ReleaseLayers(ctx context.Context, req *StreamRequest) error

	// ReconcileLeases restores active lease tracking and reference counts for
	// surviving actor workloads on node or process startup.
	ReconcileLeases(ctx context.Context, active []*ActiveLease) error
}
