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

// Package remotesnapshotter implements a unified ImageStreamer driver for CNCF
// Remote Snapshotters (containerd.services.snapshots.v1.Snapshots), supporting
// both Google Riptide (containerd-gcfs-grpc) and AWS SOCI (soci-snapshotter-grpc).
package remotesnapshotter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/imagestreaming"
	"github.com/agent-substrate/substrate/internal/proto/snapshots"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/google"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// ProviderRemoteSnapshotter identifies the generic remote snapshotter provider.
	ProviderRemoteSnapshotter = "remotesnapshotter"

	// ProviderRiptide identifies the Google Riptide remote snapshotter provider.
	ProviderRiptide = "riptide"

	// ProviderSOCI identifies the AWS SOCI remote snapshotter provider.
	ProviderSOCI = "soci"

	// DefaultRiptideSocket is the default UNIX socket path for containerd-gcfs-grpc.
	DefaultRiptideSocket = "/run/containerd-gcfs-grpc"

	// DefaultSOCISocket is the default UNIX socket path for soci-snapshotter.
	DefaultSOCISocket = "/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock"

	// DefaultBaseWorkDir is the default root path where streamed layer mounts are wrapped.
	DefaultBaseWorkDir = "/run/ate/streaming"

	// DefaultNamespace is the default containerd namespace passed to the snapshotter.
	DefaultNamespace = "default"

	// ContainerdNamespaceHeader is the gRPC metadata key for containerd namespace.
	ContainerdNamespaceHeader = "containerd-namespace"

	// DefaultListableTimeout is the default maximum duration to wait for a layer view to become listable.
	DefaultListableTimeout = 10 * time.Second

	// DefaultListableInterval is the poll interval between readdir probe attempts.
	DefaultListableInterval = 50 * time.Millisecond
)

func init() {
	imagestreaming.Register(ProviderRemoteSnapshotter, func(ctx context.Context, cfg imagestreaming.Config) (imagestreaming.ImageStreamer, error) {
		return NewFromConfig(ProviderRemoteSnapshotter, cfg)
	})
}

// Option configures a Driver.
type Option func(*Driver)

// WithName sets the provider name reported by Name().
func WithName(name string) Option {
	return func(d *Driver) {
		d.name = name
	}
}

// WithSocketPath sets the daemon UNIX socket path.
func WithSocketPath(p string) Option {
	return func(d *Driver) {
		d.socket = p
	}
}

// WithWorkDir sets the base directory for layer wrapper directories.
func WithWorkDir(dir string) Option {
	return func(d *Driver) {
		d.workDir = dir
	}
}

// WithNamespace sets the containerd namespace passed in gRPC requests.
func WithNamespace(ns string) Option {
	return func(d *Driver) {
		d.namespace = ns
	}
}

// WithSnapshotterName sets the snapshotter plugin name passed in Prepare/View requests.
func WithSnapshotterName(name string) Option {
	return func(d *Driver) {
		d.snapshotterName = name
	}
}

// WithListableTimeout sets the maximum duration to wait for a mount to become listable.
func WithListableTimeout(timeout time.Duration) Option {
	return func(d *Driver) {
		d.listableTimeout = timeout
	}
}

// WithListableInterval sets the polling interval between listability probe attempts.
func WithListableInterval(interval time.Duration) Option {
	return func(d *Driver) {
		d.listableInterval = interval
	}
}

// WithSnapshotsClient injects a SnapshotsClient (useful in tests).
func WithSnapshotsClient(c snapshots.SnapshotsClient) Option {
	return func(d *Driver) {
		d.snapshotsClient = c
	}
}

// ImageResolverFunc resolves an image reference to its manifest digest, config, diffIDs, and layer digests.
type ImageResolverFunc func(ctx context.Context, ref string, auth *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error)

// WithImageResolver sets the resolver function used to obtain image diffIDs and config.
func WithImageResolver(fn ImageResolverFunc) Option {
	return func(d *Driver) {
		d.imageResolver = fn
	}
}

type imageLease struct {
	digest       string
	config       *v1.Config
	snapshotKeys []string
	layers       []string
	workDir      string
	refCount     int
}

// Driver implements imagestreaming.ImageStreamer for CNCF remote snapshotters.
type Driver struct {
	name             string
	socket           string
	workDir          string
	namespace        string
	snapshotterName  string
	listableTimeout  time.Duration
	listableInterval time.Duration

	snapshotsClient snapshots.SnapshotsClient
	imageResolver   ImageResolverFunc

	mu     sync.Mutex
	conn   *grpc.ClientConn
	leases map[string]*imageLease
}

// New creates a generic remote snapshotter driver.
func New(opts ...Option) (*Driver, error) {
	d := &Driver{
		name:             ProviderRemoteSnapshotter,
		socket:           DefaultRiptideSocket,
		workDir:          filepath.Join(DefaultBaseWorkDir, ProviderRemoteSnapshotter),
		namespace:        DefaultNamespace,
		listableTimeout:  DefaultListableTimeout,
		listableInterval: DefaultListableInterval,
		leases:           make(map[string]*imageLease),
	}
	for _, opt := range opts {
		opt(d)
	}
	if d.imageResolver == nil {
		d.imageResolver = defaultImageResolver
	}
	return d, nil
}

// NewRiptide creates a remote snapshotter driver configured for Google Riptide.
func NewRiptide(opts ...Option) (*Driver, error) {
	d := &Driver{
		name:             ProviderRiptide,
		socket:           DefaultRiptideSocket,
		workDir:          filepath.Join(DefaultBaseWorkDir, ProviderRiptide),
		namespace:        DefaultNamespace,
		snapshotterName:  "gcfs",
		listableTimeout:  DefaultListableTimeout,
		listableInterval: DefaultListableInterval,
		leases:           make(map[string]*imageLease),
	}
	for _, opt := range opts {
		opt(d)
	}
	if d.imageResolver == nil {
		d.imageResolver = defaultImageResolver
	}
	return d, nil
}

// NewSOCI creates a remote snapshotter driver configured for AWS SOCI.
func NewSOCI(opts ...Option) (*Driver, error) {
	d := &Driver{
		name:             ProviderSOCI,
		socket:           DefaultSOCISocket,
		workDir:          filepath.Join(DefaultBaseWorkDir, ProviderSOCI),
		namespace:        DefaultNamespace,
		snapshotterName:  "soci",
		listableTimeout:  DefaultListableTimeout,
		listableInterval: DefaultListableInterval,
		leases:           make(map[string]*imageLease),
	}
	for _, opt := range opts {
		opt(d)
	}
	if d.imageResolver == nil {
		d.imageResolver = defaultImageResolver
	}
	return d, nil
}

// NewFromConfig builds a driver from a configuration map.
func NewFromConfig(provider string, cfg imagestreaming.Config) (*Driver, error) {
	opts := []Option{}
	if sock := cfg[imagestreaming.SocketPathKey]; sock != "" {
		opts = append(opts, WithSocketPath(sock))
	}
	if workDir := cfg["work_dir"]; workDir != "" {
		opts = append(opts, WithWorkDir(workDir))
	}
	if ns := cfg["namespace"]; ns != "" {
		opts = append(opts, WithNamespace(ns))
	}
	if sn := cfg["snapshotter"]; sn != "" {
		opts = append(opts, WithSnapshotterName(sn))
	}
	if to := cfg["listable_timeout"]; to != "" {
		if dur, err := time.ParseDuration(to); err == nil {
			opts = append(opts, WithListableTimeout(dur))
		}
	}
	if inv := cfg["listable_interval"]; inv != "" {
		if dur, err := time.ParseDuration(inv); err == nil {
			opts = append(opts, WithListableInterval(dur))
		}
	}

	switch provider {
	case ProviderRiptide:
		return NewRiptide(opts...)
	case ProviderSOCI:
		return NewSOCI(opts...)
	default:
		return New(opts...)
	}
}

func (d *Driver) withNamespace(ctx context.Context) context.Context {
	ns := d.namespace
	if ns == "" {
		ns = DefaultNamespace
	}
	return metadata.AppendToOutgoingContext(ctx, ContainerdNamespaceHeader, ns)
}

// Name returns the provider identifier.
func (d *Driver) Name() string {
	return d.name
}

// CanStream checks whether the remote snapshotter daemon socket is available.
func (d *Driver) CanStream(ctx context.Context, req *imagestreaming.StreamRequest) (bool, error) {
	if d.snapshotsClient != nil {
		return true, nil
	}
	if _, err := os.Stat(d.socket); err != nil {
		return false, nil
	}
	dialer := net.Dialer{Timeout: 500 * time.Millisecond}
	conn, err := dialer.DialContext(ctx, "unix", d.socket)
	if err != nil {
		slog.Debug("remote snapshotter socket present but unreachable", "provider", d.name, "socket", d.socket, "error", err)
		return false, nil
	}
	_ = conn.Close()
	return true, nil
}

// PrepareLayers asks the remote snapshotter to prepare snapshots for each layer in the image.
func (d *Driver) PrepareLayers(ctx context.Context, req *imagestreaming.StreamRequest) (*imagestreaming.StreamResult, error) {
	if req == nil || req.ImageRef == "" {
		return nil, errors.New("image reference is required")
	}

	d.mu.Lock()
	if lease, ok := d.leases[req.ImageRef]; ok && lease.refCount > 0 {
		if lease.config != nil {
			lease.refCount++
			res := &imagestreaming.StreamResult{
				ImageDigest: lease.digest,
				Config:      lease.config,
				LayerDirs:   append([]string(nil), lease.layers...),
			}
			d.mu.Unlock()
			return res, nil
		}
	}
	d.mu.Unlock()

	client, err := d.getClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to snapshotter %s: %w", d.name, err)
	}

	digest, cfg, diffIDs, layerDigests, err := d.imageResolver(ctx, req.ImageRef, req.AuthConfig)
	if err != nil {
		return nil, fmt.Errorf("resolving image %s: %w", req.ImageRef, err)
	}

	// Fast path for reconciled leases whose layers are already mounted on host:
	d.mu.Lock()
	if lease, ok := d.leases[req.ImageRef]; ok && len(lease.layers) > 0 {
		lease.config = cfg
		if lease.digest == "" {
			lease.digest = digest
		}
		lease.refCount++
		res := &imagestreaming.StreamResult{
			ImageDigest: lease.digest,
			Config:      lease.config,
			LayerDirs:   append([]string(nil), lease.layers...),
		}
		d.mu.Unlock()
		return res, nil
	}
	d.mu.Unlock()

	if len(diffIDs) == 0 {
		return nil, fmt.Errorf("image %s has 0 layers", req.ImageRef)
	}

	chainInfos := computeChainIDs(diffIDs)
	imgKey := sanitizePathKey(req.ImageRef)
	imageWorkDir := filepath.Join(d.workDir, imgKey)
	var snapshotKeys []string
	var layerDirs []string

	cleanupOnErr := func(failedKeys ...string) {
		for _, k := range failedKeys {
			if k != "" {
				_, _ = client.Remove(d.withNamespace(context.Background()), &snapshots.RemoveSnapshotRequest{Key: k})
			}
		}
		for _, key := range snapshotKeys {
			_, _ = client.Remove(d.withNamespace(context.Background()), &snapshots.RemoveSnapshotRequest{Key: key})
		}
		_ = os.RemoveAll(imageWorkDir)
	}

	allLayersStr := strings.Join(layerDigests, ",")
	runID := fmt.Sprintf("%x", time.Now().UnixNano())
	snapshotter := d.snapshotterName
	if snapshotter == "" {
		snapshotter = d.name
	}

	for i, c := range chainInfos {
		key := fmt.Sprintf("%s-%s-l%d", imgKey, runID, i)
		labels := map[string]string{
			"containerd.io/snapshot.ref":                 c.ChainID,
			"containerd.io/snapshot/cri.image-ref":       req.ImageRef,
			"containerd.io/snapshot/cri.manifest-digest": digest,
		}
		if i < len(layerDigests) {
			labels["containerd.io/snapshot/cri.layer-digest"] = layerDigests[i]
		}
		if allLayersStr != "" {
			labels["containerd.io/snapshot/cri.image-layers"] = allLayersStr
		}

		resp, err := client.Prepare(d.withNamespace(ctx), &snapshots.PrepareSnapshotRequest{
			Snapshotter: snapshotter,
			Key:         key,
			Parent:      c.ParentChainID,
			Labels:      labels,
		})
		var mounts []*snapshots.Mount
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.AlreadyExists {
				// Snapshot was already prepared/committed; create a read-only view.
				viewKey := fmt.Sprintf("%s-%s-l%d-view", imgKey, runID, i)
				viewResp, viewErr := client.View(d.withNamespace(ctx), &snapshots.ViewSnapshotRequest{
					Snapshotter: snapshotter,
					Key:         viewKey,
					Parent:      c.ChainID,
				})
				if viewErr != nil {
					cleanupOnErr(key, viewKey)
					return nil, fmt.Errorf("creating view for existing snapshot %s: %w", c.ChainID, viewErr)
				}
				mounts = viewResp.GetMounts()
				snapshotKeys = append(snapshotKeys, viewKey)
			} else {
				cleanupOnErr(key)
				return nil, fmt.Errorf("PrepareSnapshot for layer %d (%s): %w", i, c.ChainID, err)
			}
		} else {
			mounts = resp.GetMounts()
			snapshotKeys = append(snapshotKeys, key)
		}

		if len(mounts) == 0 {
			cleanupOnErr()
			return nil, fmt.Errorf("no mounts returned for layer %d (%s)", i, c.ChainID)
		}

		mountDir := extractMountDir(mounts)
		if mountDir == "" {
			cleanupOnErr()
			return nil, fmt.Errorf("unable to determine mount directory from mounts for layer %d", i)
		}

		// Ensure the mount directory is listable to eliminate the async index loading race.
		if err := d.probeListable(ctx, mountDir); err != nil {
			cleanupOnErr()
			return nil, fmt.Errorf("layer %d mount not listable: %w", i, err)
		}

		// Create layer wrapper directory: layerDir/fs
		layerDir := filepath.Join(imageWorkDir, fmt.Sprintf("layer-%d", i))
		if err := os.MkdirAll(layerDir, 0o755); err != nil {
			cleanupOnErr()
			return nil, fmt.Errorf("creating layer wrapper directory %s: %w", layerDir, err)
		}

		fsLink := filepath.Join(layerDir, "fs")
		_ = os.Remove(fsLink)
		if err := os.Symlink(mountDir, fsLink); err != nil {
			cleanupOnErr()
			return nil, fmt.Errorf("symlinking layer view %s -> %s: %w", fsLink, mountDir, err)
		}

		// Write finalized marker so ateom avoids whiteout materialization.
		finalizedMarker := filepath.Join(layerDir, "finalized")
		if err := os.WriteFile(finalizedMarker, nil, 0o600); err != nil {
			cleanupOnErr()
			return nil, fmt.Errorf("writing finalized marker %s: %w", finalizedMarker, err)
		}

		// Asynchronously prefetch directory metadata on the host to warm the snapshotter/kernel cache,
		// eliminating gVisor Sentry/Gofer traversal stalls on actor startup.
		go warmLayerMetadata(mountDir)

		layerDirs = append(layerDirs, layerDir)
	}

	d.mu.Lock()
	d.leases[req.ImageRef] = &imageLease{
		digest:       digest,
		config:       cfg,
		snapshotKeys: snapshotKeys,
		layers:       layerDirs,
		workDir:      imageWorkDir,
		refCount:     1,
	}
	d.mu.Unlock()

	return &imagestreaming.StreamResult{
		ImageDigest: digest,
		Config:      cfg,
		LayerDirs:   layerDirs,
	}, nil
}

func (d *Driver) probeListable(ctx context.Context, mountDir string) error {
	if d.listableTimeout <= 0 {
		return nil
	}
	interval := d.listableInterval
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	start := time.Now()
	for {
		entries, err := os.ReadDir(mountDir)
		if err == nil {
			_ = entries
			return nil
		}
		if time.Since(start) > d.listableTimeout {
			return fmt.Errorf("mount %q not listable within %s: %w", mountDir, d.listableTimeout, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// ReleaseLayers decrements the reference count or cleans up the snapshots and wrapper directories.
func (d *Driver) ReleaseLayers(ctx context.Context, req *imagestreaming.StreamRequest) error {
	if req == nil || req.ImageRef == "" {
		return nil
	}

	d.mu.Lock()
	lease, ok := d.leases[req.ImageRef]
	if !ok {
		d.mu.Unlock()
		return nil
	}
	lease.refCount--
	if lease.refCount > 0 {
		d.mu.Unlock()
		return nil
	}
	delete(d.leases, req.ImageRef)
	d.mu.Unlock()

	client, err := d.getClient(ctx)
	if err == nil && client != nil {
		for _, key := range lease.snapshotKeys {
			_, delErr := client.Remove(d.withNamespace(ctx), &snapshots.RemoveSnapshotRequest{Key: key})
			if delErr != nil {
				slog.Debug("RemoveSnapshot cleanup notification", "key", key, "error", delErr)
			}
		}
	}

	_ = os.RemoveAll(lease.workDir)
	return nil
}

// ReconcileLeases restores active lease tracking and reference counts for
// surviving actor workloads on node or process startup.
func (d *Driver) ReconcileLeases(ctx context.Context, active []*imagestreaming.ActiveLease) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, al := range active {
		if al == nil || (al.ImageRef == "" && al.ImageDigest == "") {
			continue
		}
		key := al.ImageRef
		if key == "" {
			key = al.ImageDigest
		}
		existing, ok := d.leases[key]
		if ok {
			existing.refCount += al.RefCount
			if len(existing.layers) == 0 && len(al.LayerDirs) > 0 {
				existing.layers = append([]string(nil), al.LayerDirs...)
			}
			if existing.digest == "" {
				existing.digest = al.ImageDigest
			}
		} else {
			d.leases[key] = &imageLease{
				refCount: al.RefCount,
				digest:   al.ImageDigest,
				layers:   append([]string(nil), al.LayerDirs...),
			}
		}
		if al.ImageDigest != "" && al.ImageDigest != key {
			d.leases[al.ImageDigest] = d.leases[key]
		}
		slog.InfoContext(ctx, "Reconciled active streamed image lease",
			slog.String("provider", d.Name()),
			slog.String("image", key),
			slog.Int("refCount", d.leases[key].refCount))
	}
	return nil
}

// Close closes gRPC connection.
func (d *Driver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn != nil {
		_ = d.conn.Close()
		d.conn = nil
	}
	return nil
}

func (d *Driver) getClient(ctx context.Context) (snapshots.SnapshotsClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.snapshotsClient != nil {
		return d.snapshotsClient, nil
	}
	target := "unix://" + d.socket
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = d.withNamespace(ctx)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(interceptor),
	)
	if err != nil {
		return nil, err
	}
	d.conn = conn
	d.snapshotsClient = snapshots.NewSnapshotsClient(conn)
	return d.snapshotsClient, nil
}

func warmLayerMetadata(root string) {
	_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		_, _ = d.Info()
		return nil
	})
}

func extractMountDir(mounts []*snapshots.Mount) string {
	if len(mounts) == 0 {
		return ""
	}
	for _, m := range mounts {
		if m == nil {
			continue
		}
		if m.Source != "" && m.Source != "overlay" && m.Source != "none" {
			return m.Source
		}
		for _, opt := range m.Options {
			if strings.HasPrefix(opt, "lowerdir=") {
				parts := strings.Split(strings.TrimPrefix(opt, "lowerdir="), ":")
				if len(parts) > 0 && parts[0] != "" {
					return parts[0]
				}
			}
		}
	}
	return mounts[0].Source
}

type chainInfo struct {
	ChainID       string
	ParentChainID string
}

func computeChainIDs(diffIDs []string) []chainInfo {
	res := make([]chainInfo, len(diffIDs))
	parent := ""
	for i, diff := range diffIDs {
		var current string
		if parent == "" {
			current = diff
		} else {
			h := sha256.Sum256([]byte(parent + " " + diff))
			current = "sha256:" + hex.EncodeToString(h[:])
		}
		res[i] = chainInfo{
			ChainID:       current,
			ParentChainID: parent,
		}
		parent = current
	}
	return res
}

func sanitizePathKey(ref string) string {
	s := strings.ReplaceAll(ref, "/", "_")
	s = strings.ReplaceAll(s, ":", "_")
	s = strings.ReplaceAll(s, "@", "_")
	return s
}

func defaultImageResolver(ctx context.Context, refStr string, authConfig *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
	ref, err := name.ParseReference(refStr)
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("parsing reference %q: %w", refStr, err)
	}

	var opts []remote.Option
	opts = append(opts, remote.WithContext(ctx))

	if authConfig != nil && (authConfig.Username != "" || authConfig.Password != "" || authConfig.Auth != "" || authConfig.IdentityToken != "" || authConfig.RegistryToken != "") {
		var auth authn.Authenticator
		if authConfig.Username != "" || authConfig.Password != "" {
			auth = &authn.Basic{
				Username: authConfig.Username,
				Password: authConfig.Password,
			}
		} else if authConfig.Auth != "" {
			auth = authn.FromConfig(authn.AuthConfig{
				Auth: authConfig.Auth,
			})
		} else if authConfig.RegistryToken != "" {
			auth = &authn.Bearer{Token: authConfig.RegistryToken}
		} else if authConfig.IdentityToken != "" {
			auth = &authn.Bearer{Token: authConfig.IdentityToken}
		}
		if auth != nil {
			opts = append(opts, remote.WithAuth(auth))
		}
	} else {
		opts = append(opts, remote.WithAuthFromKeychain(authn.NewMultiKeychain(authn.DefaultKeychain, google.Keychain)))
	}

	img, err := remote.Image(ref, opts...)
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("remote.Image: %w", err)
	}

	digest, err := img.Digest()
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("reading image digest: %w", err)
	}

	cfg, err := img.ConfigFile()
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("reading image config: %w", err)
	}

	manifest, err := img.Manifest()
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("reading image manifest: %w", err)
	}

	layerDigests := make([]string, len(manifest.Layers))
	for i, l := range manifest.Layers {
		layerDigests[i] = l.Digest.String()
	}

	diffIDs := make([]string, len(cfg.RootFS.DiffIDs))
	for i, d := range cfg.RootFS.DiffIDs {
		diffIDs[i] = d.String()
	}
	return digest.String(), &cfg.Config, diffIDs, layerDigests, nil
}
