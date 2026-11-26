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

// ErrLeaseNotHeld reports that the plugin holds no lease with an auth key.
var ErrLeaseNotHeld = errors.New("no lease with the auth key held by the image provider plugin")

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

	// AuthKey names the credentials. Leases with the same AuthKey share
	// credentials, and one RefreshAuth refreshes them all. Empty means
	// AuthKey(ImageRef), which fits keychain credentials. PrepareImage
	// requires it when AuthConfig is set, and CanProvide ignores it.
	AuthKey string
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
	// AuthKey is the AuthKey of the leases to refresh.
	AuthKey string

	// ImageRef is the image of a lease with AuthKey. The client resolves
	// credentials for it with the keychain in the request context when
	// AuthConfig is nil.
	ImageRef string

	// AuthConfig holds explicit workload credentials.
	AuthConfig *AuthConfig
}

// ReleaseRequest holds the parameters for ReleaseImage.
type ReleaseRequest struct {
	// LeaseID is the LeaseID passed to PrepareImage.
	LeaseID string
}

// LostLease is an active lease that ReconcileImages could not restore.
type LostLease struct {
	LeaseID string

	// Message says why the plugin could not restore the lease.
	Message string
}

// ReconcileResult is what ReconcileImages returns.
type ReconcileResult struct {
	// LostLeases are the active leases the plugin could not restore. The
	// caller stops the workloads that hold them.
	LostLeases []LostLease

	// AuthRefreshRequired are the auth keys of restored leases that need
	// RefreshAuth now.
	AuthRefreshRequired []string
}

// AuthKey returns the default auth key of imageRef: its repository, for
// example "us-docker.pkg.dev/project/repo/image".
func AuthKey(imageRef string) (string, error) {
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return "", fmt.Errorf("parsing image reference %q: %w", imageRef, err)
	}
	return ref.Context().Name(), nil
}

// Client talks to an image provider plugin.
type Client interface {
	Name() string
	CanProvide(ctx context.Context, req *PrepareRequest) (bool, error)
	State(ctx context.Context) (imageprovider.PluginState, error)
	PrepareImage(ctx context.Context, req *PrepareRequest) (*PrepareResult, error)
	ReleaseImage(ctx context.Context, req *ReleaseRequest) error
	ReconcileImages(ctx context.Context, leaseIDs []string) (*ReconcileResult, error)
	RefreshAuth(ctx context.Context, req *RefreshRequest) (imageprovider.AuthRefresh, error)
	Close() error
}

// GRPCClient implements Client over a Unix domain socket. It always asks for
// IMAGE_FORM_ROOTFS, and sends its mount root in every request.
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
// plugin is installed or removed. A draining plugin returns false.
// CanProvide sends no registry credentials.
func (c *GRPCClient) CanProvide(ctx context.Context, req *PrepareRequest) (bool, error) {
	if req == nil {
		return false, errors.New("prepare request cannot be nil")
	}
	if !IsSocketAvailable(c.socketPath) {
		return false, ErrNoPlugin
	}
	res, err := c.client.CanProvide(ctx, &imageprovider.CanProvideRequest{
		ImageRef:  req.ImageRef,
		Form:      imageprovider.ImageForm_IMAGE_FORM_ROOTFS,
		MountRoot: c.mountRoot,
	})
	if err != nil {
		return false, fmt.Errorf("calling CanProvide on image provider: %w", err)
	}
	return res.GetCanProvide(), nil
}

// State returns the plugin's lifecycle state from GetPluginInfo, the
// plugin's heartbeat. It returns PLUGIN_STATE_UNSPECIFIED when the plugin
// socket does not exist.
func (c *GRPCClient) State(ctx context.Context) (imageprovider.PluginState, error) {
	if !IsSocketAvailable(c.socketPath) {
		return imageprovider.PluginState_PLUGIN_STATE_UNSPECIFIED, nil
	}
	res, err := c.client.GetPluginInfo(ctx, &imageprovider.GetPluginInfoRequest{})
	if err != nil {
		return imageprovider.PluginState_PLUGIN_STATE_UNSPECIFIED, fmt.Errorf("calling GetPluginInfo on image provider: %w", err)
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
	authKey := req.AuthKey
	if authKey == "" {
		if req.AuthConfig != nil {
			return nil, errors.New("prepare request with AuthConfig needs an AuthKey")
		}
		var err error
		if authKey, err = AuthKey(req.ImageRef); err != nil {
			return nil, err
		}
	}
	res, err := c.client.PrepareImage(ctx, &imageprovider.PrepareImageRequest{
		ImageRef:  req.ImageRef,
		Auth:      resolveAuth(ctx, req),
		Form:      imageprovider.ImageForm_IMAGE_FORM_ROOTFS,
		LeaseId:   req.LeaseID,
		MountRoot: c.mountRoot,
		AuthKey:   authKey,
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
		return nil, fmt.Errorf("image provider returned %T for %s, want a rootfs", res.GetImage(), req.ImageRef)
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
		LeaseId:   req.LeaseID,
		MountRoot: c.mountRoot,
	}); err != nil {
		return fmt.Errorf("calling ReleaseImage on image provider: %w", err)
	}
	return nil
}

// ReconcileImages replaces the plugin's leases under the mount root with
// leaseIDs.
func (c *GRPCClient) ReconcileImages(ctx context.Context, leaseIDs []string) (*ReconcileResult, error) {
	req := &imageprovider.ReconcileImagesRequest{
		ActiveLeases: make([]*imageprovider.ActiveLease, 0, len(leaseIDs)),
		MountRoot:    c.mountRoot,
	}
	for _, id := range leaseIDs {
		req.ActiveLeases = append(req.ActiveLeases, &imageprovider.ActiveLease{LeaseId: id})
	}
	res, err := c.client.ReconcileImages(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("calling ReconcileImages on image provider: %w", err)
	}
	out := &ReconcileResult{AuthRefreshRequired: res.GetAuthRefreshRequired()}
	for _, l := range res.GetLostLeases() {
		out.LostLeases = append(out.LostLeases, LostLease{LeaseID: l.GetLeaseId(), Message: l.GetMessage()})
	}
	return out, nil
}

// RefreshAuth sends fresh credentials for the leases with req.AuthKey and
// returns whether the plugin needs more refreshes. It resolves
// req.AuthConfig, or credentials for req.ImageRef from the keychain in ctx,
// and fails if the keychain fails. It returns an error wrapping
// ErrLeaseNotHeld when the plugin holds no lease with the auth key, and
// ErrNotSupported when the plugin does not implement RefreshAuth.
func (c *GRPCClient) RefreshAuth(ctx context.Context, req *RefreshRequest) (imageprovider.AuthRefresh, error) {
	if req == nil {
		return imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED, errors.New("refresh request cannot be nil")
	}
	if req.AuthKey == "" {
		return imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED, errors.New("refresh request needs an AuthKey")
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
		AuthKey:   req.AuthKey,
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
