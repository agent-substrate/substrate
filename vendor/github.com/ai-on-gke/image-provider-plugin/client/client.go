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
	"path/filepath"
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
// listens on. It has a directory of its own, so a client can mount only that
// directory, read-only.
const DefaultSocketPath = "/run/ate/image-provider/plugin.sock"

// ErrNotSupported reports that the plugin declined to serve an image. Callers
// should pull the image themselves.
var ErrNotSupported = errors.New("image not supported by the image provider plugin")

// ErrLeaseNotHeld reports that the plugin does not hold a lease.
var ErrLeaseNotHeld = errors.New("lease not held by the image provider plugin")

// ErrNoPlugin reports that no plugin socket exists. Callers pull the image
// themselves, and do not count it as a request to a plugin.
var ErrNoPlugin = errors.New("no image provider plugin socket")

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

// PrepareResult holds the prepared read-only root filesystem of an OCI
// image.
type PrepareResult struct {
	// ImageDigest is the resolved manifest digest ("sha256:<hex>").
	ImageDigest string

	// Config is the image configuration (entrypoint, cmd, env and so on).
	Config *v1.Config

	// Rootfs is the host directory of the image's read-only root
	// filesystem, with all layers applied. It is under the mount root.
	Rootfs string

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
// LAYER_FORM_ROOTFS, and sends its mount root in every request.
type GRPCClient struct {
	mu           sync.RWMutex
	socketPath   string
	mountRoot    string
	conn         *grpc.ClientConn
	client       imageprovider.ImageProviderPluginClient
	providerName string
}

var _ Client = (*GRPCClient)(nil)

// NewGRPCClient creates a client for the plugin at socketPath. mountRoot is
// the host directory the plugin mounts this client's images under. It must
// be a clean absolute path that no other client uses. The client does not
// connect until the first call.
func NewGRPCClient(socketPath, mountRoot string) (*GRPCClient, error) {
	if socketPath == "" {
		return nil, errors.New("image provider socket path cannot be empty")
	}
	if !filepath.IsAbs(mountRoot) || filepath.Clean(mountRoot) != mountRoot {
		return nil, fmt.Errorf("mount root %q must be a clean absolute path", mountRoot)
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
		mountRoot:    mountRoot,
		conn:         conn,
		client:       imageprovider.NewImageProviderPluginClient(conn),
		providerName: "unknown",
	}, nil
}

// Name returns the provider name the plugin last reported, or "unknown" before
// the plugin first answers.
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
// ErrNoPlugin when the plugin socket does not exist, so callers notice when a
// plugin is installed or removed. A plugin that is draining, or in a state
// this client does not know, is not used for new images. CanProvide sends no
// registry credentials.
func (c *GRPCClient) CanProvide(ctx context.Context, req *PrepareRequest) (bool, error) {
	if req == nil {
		return false, errors.New("prepare request cannot be nil")
	}
	if !IsSocketAvailable(c.socketPath) {
		return false, ErrNoPlugin
	}
	res, err := c.client.CanProvide(ctx, &imageprovider.CanProvideRequest{
		ImageRef:  req.ImageRef,
		Form:      imageprovider.LayerForm_LAYER_FORM_ROOTFS,
		MountRoot: c.mountRoot,
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
	res, err := c.client.CanProvide(ctx, &imageprovider.CanProvideRequest{
		Form:      imageprovider.LayerForm_LAYER_FORM_ROOTFS,
		MountRoot: c.mountRoot,
	})
	if err != nil {
		return imageprovider.PluginState_PLUGIN_STATE_UNSPECIFIED, fmt.Errorf("calling CanProvide on image provider: %w", err)
	}
	c.setName(res.GetProviderName())
	return res.GetState(), nil
}

// PrepareImage prepares the image, takes the lease req.LeaseID, and returns
// its root filesystem. It returns an error wrapping ErrNotSupported when the
// plugin declines the image, and fails when the root filesystem is not under
// the mount root.
func (c *GRPCClient) PrepareImage(ctx context.Context, req *PrepareRequest) (*PrepareResult, error) {
	if req == nil {
		return nil, errors.New("prepare request cannot be nil")
	}
	res, err := c.client.PrepareImage(ctx, &imageprovider.PrepareImageRequest{
		ImageRef:  req.ImageRef,
		Auth:      resolveAuth(ctx, req),
		Form:      imageprovider.LayerForm_LAYER_FORM_ROOTFS,
		LeaseId:   req.LeaseID,
		MountRoot: c.mountRoot,
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
	rootfs := res.GetRootfs()
	if rootfs == nil {
		return nil, fmt.Errorf("image provider returned %T for %s, want a rootfs", res.GetLayers(), req.ImageRef)
	}
	dir := filepath.Clean(rootfs.GetDir())
	if !isUnder(c.mountRoot, dir) {
		return nil, fmt.Errorf("image provider returned rootfs %q for %s, want a directory under %s", rootfs.GetDir(), req.ImageRef, c.mountRoot)
	}
	return &PrepareResult{
		ImageDigest:  res.GetImageDigest(),
		Config:       cfg,
		Rootfs:       dir,
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
		Form:        imageprovider.LayerForm_LAYER_FORM_ROOTFS,
		MountRoot:   c.mountRoot,
	}); err != nil {
		return fmt.Errorf("calling ReleaseImage on image provider: %w", err)
	}
	return nil
}

// ReconcileImages replaces the plugin's leases under the mount root with
// active.
func (c *GRPCClient) ReconcileImages(ctx context.Context, active []*ActiveLease) error {
	req := &imageprovider.ReconcileImagesRequest{
		ActiveLeases: make([]*imageprovider.ActiveLease, 0, len(active)),
		MountRoot:    c.mountRoot,
	}
	for _, a := range active {
		if a == nil {
			continue
		}
		req.ActiveLeases = append(req.ActiveLeases, &imageprovider.ActiveLease{
			LeaseId:     a.LeaseID,
			ImageRef:    a.ImageRef,
			ImageDigest: a.ImageDigest,
			Form:        imageprovider.LayerForm_LAYER_FORM_ROOTFS,
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
		LeaseId:   req.LeaseID,
		Auth:      auth,
		MountRoot: c.mountRoot,
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

// isUnder reports whether the clean path dir is strictly under root.
func isUnder(root, dir string) bool {
	if !filepath.IsAbs(dir) {
		return false
	}
	rel, err := filepath.Rel(root, dir)
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, "../")
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
