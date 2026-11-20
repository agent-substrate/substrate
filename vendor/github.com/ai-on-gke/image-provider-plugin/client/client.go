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

// Package client is a client for an ImageProviderPlugin served on a Unix
// domain socket. It is independent of the provider behind the plugin.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	imageprovider "github.com/ai-on-gke/image-provider-plugin/api/imageprovider/v1alpha1"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// DefaultSocketPath is the Unix domain socket an image provider plugin
// listens on.
const DefaultSocketPath = "/run/ate/image-provider.sock"

// ErrNotSupported reports that the plugin declined to serve an image. Callers
// should pull the image themselves.
var ErrNotSupported = errors.New("image not supported by the image provider plugin")

// ErrLeaseNotHeld reports that the plugin does not hold a lease.
var ErrLeaseNotHeld = errors.New("lease not held by the image provider plugin")

type contextKeyKeychain struct{}

// WithKeychainContext returns a child context carrying k.
func WithKeychainContext(ctx context.Context, k authn.Keychain) context.Context {
	return context.WithValue(ctx, contextKeyKeychain{}, k)
}

// KeychainFromContext returns the authn.Keychain in ctx, or nil.
func KeychainFromContext(ctx context.Context) authn.Keychain {
	if ctx == nil {
		return nil
	}
	if k, ok := ctx.Value(contextKeyKeychain{}).(authn.Keychain); ok {
		return k
	}
	return nil
}

// AuthConfig contains resolved registry credentials for one image request.
type AuthConfig struct {
	Username      string `json:"username,omitempty"`
	Password      string `json:"password,omitempty"`
	Auth          string `json:"auth,omitempty"`
	ServerAddress string `json:"serveraddress,omitempty"`
	IdentityToken string `json:"identitytoken,omitempty"`
	RegistryToken string `json:"registrytoken,omitempty"`
}

// PrepareRequest holds the parameters for CanProvide and PrepareImage.
type PrepareRequest struct {
	// ImageRef is the OCI image reference.
	ImageRef string

	// AuthConfig holds explicit workload credentials, for example from
	// imagePullSecrets. When nil, the client resolves credentials with the
	// keychain in the request context, if any.
	AuthConfig *AuthConfig

	// LeaseID names the lease PrepareImage takes. PrepareImage requires it,
	// and CanProvide ignores it.
	LeaseID string
}

// PrepareResult holds the prepared read-only host layer directories for an
// OCI image.
type PrepareResult struct {
	// ImageDigest is the resolved manifest digest ("sha256:<hex>").
	ImageDigest string

	// Config is the image configuration (entrypoint, cmd, env and so on).
	Config *v1.Config

	// LayerDirs contains the complete ordered list of read-only host layer
	// directories (bottom-most base layer first, matching OCI manifest order).
	LayerDirs []string

	// ProviderName is the provider name the plugin reported.
	ProviderName string

	// AuthRefresh says whether the caller must call RefreshAuth on the
	// lease.
	AuthRefresh imageprovider.AuthRefresh
}

// RefreshRequest holds the parameters for RefreshAuth.
type RefreshRequest struct {
	// ImageRef is the lease's image. The client resolves credentials for it
	// with the keychain in the request context when AuthConfig is nil.
	ImageRef string

	// AuthConfig holds explicit workload credentials.
	AuthConfig *AuthConfig

	// LeaseID is the LeaseID passed to PrepareImage.
	LeaseID string
}

// ReleaseRequest holds the parameters for ReleaseImage.
type ReleaseRequest struct {
	// LeaseID is the LeaseID passed to PrepareImage.
	LeaseID string

	// ImageDigest is the ImageDigest PrepareImage returned, or empty if
	// PrepareImage failed.
	ImageDigest string
}

// ActiveLease is a lease that a surviving workload still holds, sent in
// ReconcileImages.
type ActiveLease struct {
	LeaseID     string
	ImageRef    string
	ImageDigest string
}

// Client talks to an image provider plugin.
type Client interface {
	Name() string
	CanProvide(ctx context.Context, req *PrepareRequest) (bool, error)
	State(ctx context.Context) (imageprovider.PluginState, error)
	PrepareImage(ctx context.Context, req *PrepareRequest) (*PrepareResult, error)
	ReleaseImage(ctx context.Context, req *ReleaseRequest) error
	ReconcileImages(ctx context.Context, active []*ActiveLease) error
	RefreshAuth(ctx context.Context, req *RefreshRequest) (imageprovider.AuthRefresh, error)
	Close() error
}

// GRPCClient implements Client over a Unix domain socket. It always asks for
// LAYER_FORM_DIRS.
type GRPCClient struct {
	mu           sync.RWMutex
	socketPath   string
	conn         *grpc.ClientConn
	client       imageprovider.ImageProviderPluginClient
	providerName string
}

var _ Client = (*GRPCClient)(nil)

// NewGRPCClient creates a client for the plugin at socketPath. It does not
// connect until the first call.
func NewGRPCClient(socketPath string) (*GRPCClient, error) {
	if socketPath == "" {
		return nil, errors.New("image provider socket path cannot be empty")
	}
	target := socketPath
	if !strings.HasPrefix(target, "unix://") {
		target = "unix://" + target
	}
	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("creating client for image provider socket %s: %w", socketPath, err)
	}
	return &GRPCClient{
		socketPath:   socketPath,
		conn:         conn,
		client:       imageprovider.NewImageProviderPluginClient(conn),
		providerName: "image-provider",
	}, nil
}

// Name returns the provider name the plugin last reported.
func (c *GRPCClient) Name() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.providerName
}

func (c *GRPCClient) setName(name string) {
	if name == "" {
		return
	}
	c.mu.Lock()
	c.providerName = name
	c.mu.Unlock()
}

// resolveAuth returns req.AuthConfig, or credentials from the keychain in
// ctx. It returns nil for anonymous access, and when the keychain fails.
func resolveAuth(ctx context.Context, req *PrepareRequest) *imageprovider.RegistryAuth {
	if req.AuthConfig != nil {
		return registryAuth(req.AuthConfig)
	}
	auth, err := keychainAuth(ctx, req.ImageRef)
	if err != nil {
		return nil
	}
	return auth
}

// registryAuth converts a to its wire form.
func registryAuth(a *AuthConfig) *imageprovider.RegistryAuth {
	return &imageprovider.RegistryAuth{
		Username:      a.Username,
		Password:      a.Password,
		Auth:          a.Auth,
		ServerAddress: a.ServerAddress,
		IdentityToken: a.IdentityToken,
		RegistryToken: a.RegistryToken,
	}
}

// keychainAuth returns the credentials for imageRef from the keychain in
// ctx. It returns nil for anonymous access or when ctx has no keychain.
func keychainAuth(ctx context.Context, imageRef string) (*imageprovider.RegistryAuth, error) {
	k := KeychainFromContext(ctx)
	if k == nil {
		return nil, nil
	}
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return nil, fmt.Errorf("parsing image reference %q: %w", imageRef, err)
	}
	auth, err := k.Resolve(ref.Context())
	if err != nil {
		return nil, fmt.Errorf("resolving credentials for %s: %w", ref.Context(), err)
	}
	cfg, err := auth.Authorization()
	if err != nil {
		return nil, fmt.Errorf("reading credentials for %s: %w", ref.Context(), err)
	}
	if cfg.Username == "" && cfg.Password == "" && cfg.Auth == "" && cfg.IdentityToken == "" && cfg.RegistryToken == "" {
		return nil, nil
	}
	return &imageprovider.RegistryAuth{
		Username:      cfg.Username,
		Password:      cfg.Password,
		Auth:          cfg.Auth,
		ServerAddress: ref.Context().RegistryStr(),
		IdentityToken: cfg.IdentityToken,
		RegistryToken: cfg.RegistryToken,
	}, nil
}

// CanProvide reports whether the plugin serves the image. It returns
// (false, nil) when the plugin socket does not exist, so callers notice when
// a plugin is installed or removed. A plugin that is draining, or in a state
// this client does not know, is not used for new images.
func (c *GRPCClient) CanProvide(ctx context.Context, req *PrepareRequest) (bool, error) {
	if req == nil {
		return false, errors.New("prepare request cannot be nil")
	}
	if !IsSocketAvailable(c.socketPath) {
		return false, nil
	}
	res, err := c.client.CanProvide(ctx, &imageprovider.CanProvideRequest{
		ImageRef: req.ImageRef,
		Auth:     resolveAuth(ctx, req),
		Form:     imageprovider.LayerForm_LAYER_FORM_DIRS,
	})
	if err != nil {
		return false, fmt.Errorf("calling CanProvide on image provider: %w", err)
	}
	c.setName(res.GetProviderName())
	return res.GetCanProvide() && res.GetState() == imageprovider.PluginState_PLUGIN_STATE_SERVING, nil
}

// State returns the plugin's lifecycle state. It returns
// PLUGIN_STATE_UNSPECIFIED when the plugin socket does not exist.
func (c *GRPCClient) State(ctx context.Context) (imageprovider.PluginState, error) {
	if !IsSocketAvailable(c.socketPath) {
		return imageprovider.PluginState_PLUGIN_STATE_UNSPECIFIED, nil
	}
	res, err := c.client.CanProvide(ctx, &imageprovider.CanProvideRequest{})
	if err != nil {
		return imageprovider.PluginState_PLUGIN_STATE_UNSPECIFIED, fmt.Errorf("calling CanProvide on image provider: %w", err)
	}
	c.setName(res.GetProviderName())
	return res.GetState(), nil
}

// PrepareImage prepares the image's layers, takes the lease req.LeaseID, and
// returns the layer directories. It returns an error wrapping
// ErrNotSupported when the plugin declines the image.
func (c *GRPCClient) PrepareImage(ctx context.Context, req *PrepareRequest) (*PrepareResult, error) {
	if req == nil {
		return nil, errors.New("prepare request cannot be nil")
	}
	res, err := c.client.PrepareImage(ctx, &imageprovider.PrepareImageRequest{
		ImageRef: req.ImageRef,
		Auth:     resolveAuth(ctx, req),
		Form:     imageprovider.LayerForm_LAYER_FORM_DIRS,
		LeaseId:  req.LeaseID,
	})
	if err != nil {
		if st, ok := status.FromError(err); ok && (st.Code() == codes.FailedPrecondition || st.Code() == codes.Unimplemented) {
			return nil, fmt.Errorf("%w: %s", ErrNotSupported, st.Message())
		}
		return nil, fmt.Errorf("calling PrepareImage on image provider: %w", err)
	}
	c.setName(res.GetProviderName())

	var cfg *v1.Config
	if len(res.GetConfigJson()) > 0 {
		cfg = &v1.Config{}
		if err := json.Unmarshal(res.GetConfigJson(), cfg); err != nil {
			return nil, fmt.Errorf("decoding image config for %s: %w", req.ImageRef, err)
		}
	}
	dirs := res.GetLayerDirs()
	if dirs == nil {
		return nil, fmt.Errorf("image provider returned %T for %s, want layer dirs", res.GetLayers(), req.ImageRef)
	}
	return &PrepareResult{
		ImageDigest:  res.GetImageDigest(),
		Config:       cfg,
		LayerDirs:    dirs.GetDirs(),
		ProviderName: c.Name(),
		AuthRefresh:  res.GetAuthRefresh(),
	}, nil
}

// ReleaseImage releases the lease req.LeaseID.
func (c *GRPCClient) ReleaseImage(ctx context.Context, req *ReleaseRequest) error {
	if req == nil {
		return errors.New("release request cannot be nil")
	}
	if _, err := c.client.ReleaseImage(ctx, &imageprovider.ReleaseImageRequest{
		LeaseId:     req.LeaseID,
		ImageDigest: req.ImageDigest,
		Form:        imageprovider.LayerForm_LAYER_FORM_DIRS,
	}); err != nil {
		return fmt.Errorf("calling ReleaseImage on image provider: %w", err)
	}
	return nil
}

// ReconcileImages replaces the plugin's leases with active.
func (c *GRPCClient) ReconcileImages(ctx context.Context, active []*ActiveLease) error {
	req := &imageprovider.ReconcileImagesRequest{
		ActiveLeases: make([]*imageprovider.ActiveLease, 0, len(active)),
	}
	for _, a := range active {
		if a == nil {
			continue
		}
		req.ActiveLeases = append(req.ActiveLeases, &imageprovider.ActiveLease{
			LeaseId:     a.LeaseID,
			ImageRef:    a.ImageRef,
			ImageDigest: a.ImageDigest,
			Form:        imageprovider.LayerForm_LAYER_FORM_DIRS,
		})
	}
	if _, err := c.client.ReconcileImages(ctx, req); err != nil {
		return fmt.Errorf("calling ReconcileImages on image provider: %w", err)
	}
	return nil
}

// RefreshAuth sends fresh credentials for the lease req.LeaseID and returns
// whether the plugin needs more refreshes. It resolves req.AuthConfig, or
// credentials for req.ImageRef from the keychain in ctx, and fails if the
// keychain fails. It returns an error wrapping ErrLeaseNotHeld when the
// plugin does not hold the lease, and ErrNotSupported when the plugin does
// not implement RefreshAuth.
func (c *GRPCClient) RefreshAuth(ctx context.Context, req *RefreshRequest) (imageprovider.AuthRefresh, error) {
	if req == nil {
		return imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED, errors.New("refresh request cannot be nil")
	}
	var auth *imageprovider.RegistryAuth
	if req.AuthConfig != nil {
		auth = registryAuth(req.AuthConfig)
	} else {
		var err error
		if auth, err = keychainAuth(ctx, req.ImageRef); err != nil {
			return imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED, err
		}
	}
	res, err := c.client.RefreshAuth(ctx, &imageprovider.RefreshAuthRequest{
		LeaseId: req.LeaseID,
		Auth:    auth,
	})
	if err != nil {
		switch status.Code(err) {
		case codes.NotFound:
			return imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED, fmt.Errorf("%w: %s", ErrLeaseNotHeld, status.Convert(err).Message())
		case codes.Unimplemented:
			return imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED, fmt.Errorf("%w: %s", ErrNotSupported, status.Convert(err).Message())
		}
		return imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED, fmt.Errorf("calling RefreshAuth on image provider: %w", err)
	}
	return res.GetAuthRefresh(), nil
}

// Close closes the gRPC connection.
func (c *GRPCClient) Close() error {
	return c.conn.Close()
}

// IsSocketAvailable reports whether socketPath, with an optional unix://
// prefix, is a Unix socket.
func IsSocketAvailable(socketPath string) bool {
	if socketPath == "" {
		return false
	}
	info, err := os.Stat(strings.TrimPrefix(socketPath, "unix://"))
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeSocket != 0
}
