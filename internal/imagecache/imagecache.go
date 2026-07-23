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

// Package imagecache implements the node-local OCI image cache: a
// content-addressed pool of unpacked image layers shared by every actor on
// the node, plus the per-bundle overlay spec that tells the ateom runtimes
// how to compose an actor rootfs from cached layers.
//
// The work is split along the existing atelet/ateom privilege boundary:
//
//   - atelet (plain root, all capabilities dropped) pulls layers and unpacks
//     them into the pool (Store.EnsureImage), and writes a rootfs-overlay.json
//     next to each bundle's config.json (WriteSpec). Whiteout entries are
//     recorded in per-layer metadata rather than materialized, because
//     overlayfs whiteouts are char devices (CAP_MKNOD) with trusted.* xattrs
//     for opaque dirs (CAP_SYS_ADMIN).
//   - ateom (privileged; it already owns every mount on the node) finalizes
//     layers — materializing the recorded whiteout state, once per layer —
//     and mounts the overlay rootfs (SetupBundleRootfs) just before
//     `runsc create` / staging the micro-VM virtio-fs lower.
//
// On-disk layout under the cache root (a directory on the BasePath hostPath,
// so the same absolute paths resolve in atelet and every ateom pod):
//
//	version                          layout version marker
//	layers/sha256/<diffid-hex>/
//	    fs/                          the unpacked layer tree (overlay lowerdir)
//	    whiteouts.json               whiteout state recorded at unpack time
//	    finalized                    marker written by FinalizeLayer (ateom)
//	manifests/sha256/<digest-hex>.json
//	                                 image config + ordered diffID list
//
// Layers land in the pool via unpack-into-tempdir + atomic rename, so a
// layer directory that exists is always complete; startup recovery only has
// to sweep orphaned temp dirs.
package imagecache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

const (
	layoutVersion   = "1"
	versionFileName = "version"

	layerFSDirName           = "fs"
	layerWhiteoutsFileName   = "whiteouts.json"
	layerFinalizedMarkerName = "finalized"

	// layerPullConcurrency bounds concurrent layer download+unpack streams per
	// image pull. Memory use is O(stream buffers) per slot, independent of
	// layer size.
	layerPullConcurrency = 4
)

// Store is atelet's handle to the on-disk layer pool. It is safe for
// concurrent use; concurrent pulls of the same image or layer are collapsed.
// The store assumes it is the only writer on the node (one atelet per node).
type Store struct {
	root string

	// authenticator, when set, is attached to pulls from registries that use
	// GCP credentials (gcr.io / pkg.dev). See remoteOpts.
	authenticator authn.Authenticator

	localhostRegistryReplacement string

	// platform overrides the default pull platform (linux/GOARCH). Used by
	// validation tooling that runs on a different architecture than the
	// nodes it validates for.
	platform *v1.Platform

	imageSF singleflight.Group
	layerSF singleflight.Group
}

// Option configures a Store.
type Option func(*Store)

// WithAuthenticator attaches an authenticator used for gcr.io / pkg.dev
// registries. A nil authenticator is ignored.
func WithAuthenticator(a authn.Authenticator) Option {
	return func(s *Store) { s.authenticator = a }
}

// WithLocalhostRegistryReplacement rewrites localhost/loopback registry refs
// to the given endpoint, mirroring the containerd mirror config used by kind
// local registries (https://kind.sigs.k8s.io/docs/user/local-registry/).
func WithLocalhostRegistryReplacement(replacement string) Option {
	return func(s *Store) { s.localhostRegistryReplacement = replacement }
}

// WithPlatform overrides the pull platform (default: linux/GOARCH).
func WithPlatform(p v1.Platform) Option {
	return func(s *Store) { s.platform = &p }
}

// Image describes one cached, ready-to-compose image.
type Image struct {
	// Digest is the manifest digest the caller's ref resolved to (for a
	// multi-arch ref, the index digest as requested, not the per-platform
	// child).
	Digest v1.Hash
	// Config is the OCI image config (entrypoint, env, ...).
	Config v1.Config
	// LayerDirs are the absolute cached layer directories, bottom-most layer
	// first. Each contains the unpacked tree under "fs/".
	LayerDirs []string
}

// imageRecord is the persisted form of a cached image, stored under
// manifests/<algorithm>/<hex>.json.
type imageRecord struct {
	Version int       `json:"version"`
	Config  v1.Config `json:"config"`
	DiffIDs []string  `json:"diffIDs"`
}

// New opens (creating if needed) the layer pool rooted at root and runs
// startup recovery: verifying the layout version and sweeping temp dirs left
// by unpacks that were in flight when a previous atelet died.
func New(root string, opts ...Option) (*Store, error) {
	s := &Store{root: root}
	for _, o := range opts {
		o(s)
	}

	for _, d := range []string{s.layersDir(), s.manifestsDir()} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("while creating image cache dir %q: %w", d, err)
		}
	}

	versionPath := filepath.Join(root, versionFileName)
	switch b, err := os.ReadFile(versionPath); {
	case err == nil:
		if got := strings.TrimSpace(string(b)); got != layoutVersion {
			// Fail loudly instead of silently mixing layouts; an operator can
			// delete the cache dir to rebuild it (it holds no unique state).
			return nil, fmt.Errorf("image cache at %q has layout version %q, this atelet supports %q", root, got, layoutVersion)
		}
	case errors.Is(err, os.ErrNotExist):
		if err := os.WriteFile(versionPath, []byte(layoutVersion+"\n"), 0o600); err != nil {
			return nil, fmt.Errorf("while writing image cache version marker: %w", err)
		}
	default:
		return nil, fmt.Errorf("while reading image cache version marker: %w", err)
	}

	if err := s.sweepTempDirs(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) layersDir() string    { return filepath.Join(s.root, "layers", "sha256") }
func (s *Store) manifestsDir() string { return filepath.Join(s.root, "manifests", "sha256") }

func (s *Store) layerDir(diffID v1.Hash) string {
	return filepath.Join(s.root, "layers", diffID.Algorithm, diffID.Hex)
}

func (s *Store) recordPath(digest v1.Hash) string {
	return filepath.Join(s.root, "manifests", digest.Algorithm, digest.Hex+".json")
}

// sweepTempDirs removes unpack temp dirs and manifest-record temp files
// orphaned by a crash. A layer dir without the temp prefix and a record
// without a leading dot are always complete (both are moved into place with
// a single rename), so this is the only recovery the pool needs.
func (s *Store) sweepTempDirs() error {
	entries, err := os.ReadDir(s.layersDir())
	if err != nil {
		return fmt.Errorf("while listing layer pool: %w", err)
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".tmp-") {
			continue
		}
		p := filepath.Join(s.layersDir(), e.Name())
		if err := RemoveAllWritable(p); err != nil {
			return fmt.Errorf("while sweeping orphaned layer temp dir %q: %w", p, err)
		}
	}

	// writeRecord's temp files are ".<hex>.json.tmp-<rand>"; finished records
	// are "<hex>.json", so a leading dot alone identifies an orphan.
	records, err := os.ReadDir(s.manifestsDir())
	if err != nil {
		return fmt.Errorf("while listing manifest records: %w", err)
	}
	for _, e := range records {
		if !strings.HasPrefix(e.Name(), ".") {
			continue
		}
		p := filepath.Join(s.manifestsDir(), e.Name())
		if err := os.Remove(p); err != nil {
			return fmt.Errorf("while sweeping orphaned manifest temp file %q: %w", p, err)
		}
	}
	return nil
}

// EnsureImage makes ref's image available in the pool and returns its config
// and ordered layer directories. Digest refs hit the cache with no network
// I/O; tag refs cost one HEAD request to resolve the tag to a manifest
// digest (so tag refs are cacheable, and a moved tag is picked up on the
// next call).
func (s *Store) EnsureImage(ctx context.Context, ref string) (*Image, error) {
	parsedRef, err := s.parseRef(ref)
	if err != nil {
		return nil, fmt.Errorf("while parsing reference: %w", err)
	}

	var digest v1.Hash
	if d, ok := parsedRef.(name.Digest); ok {
		digest, err = v1.NewHash(d.DigestStr())
		if err != nil {
			return nil, fmt.Errorf("while parsing digest of %q: %w", ref, err)
		}
	} else {
		// Tag ref: one small HEAD request pins it to an immutable manifest
		// digest, which is the only safe cache key for mutable tags.
		desc, err := remote.Head(parsedRef, s.remoteOpts(ctx, parsedRef)...)
		if err != nil {
			return nil, fmt.Errorf("while resolving tag %q to a digest: %w", ref, err)
		}
		digest = desc.Digest
	}

	if img, err := s.cachedImage(digest); err != nil {
		return nil, err
	} else if img != nil {
		slog.InfoContext(ctx, "Image cache hit", slog.String("ref", ref), slog.String("digest", digest.String()))
		return img, nil
	}
	slog.InfoContext(ctx, "Image cache miss", slog.String("ref", ref), slog.String("digest", digest.String()))

	// Collapse concurrent pulls of the same digest (e.g. several containers of
	// one actor, or several actors landing at once). The winning call's ctx
	// governs the pull; if it is cancelled the waiters fail too and retry at
	// the RPC level.
	v, err, _ := s.imageSF.Do(digest.String(), func() (any, error) {
		return s.pull(ctx, parsedRef, digest)
	})
	if err != nil {
		return nil, err
	}
	return v.(*Image), nil
}

// cachedImage returns the cached image for digest, or nil if the record or
// any of its layer dirs is missing (in which case the caller re-pulls; only
// the missing layers cost anything).
func (s *Store) cachedImage(digest v1.Hash) (*Image, error) {
	b, err := os.ReadFile(s.recordPath(digest))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("while reading image record for %s: %w", digest, err)
	}
	var rec imageRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, fmt.Errorf("while decoding image record for %s: %w", digest, err)
	}

	layerDirs := make([]string, len(rec.DiffIDs))
	for i, d := range rec.DiffIDs {
		diffID, err := v1.NewHash(d)
		if err != nil {
			return nil, fmt.Errorf("invalid diffID %q in image record for %s: %w", d, digest, err)
		}
		dir := s.layerDir(diffID)
		if _, err := os.Stat(filepath.Join(dir, layerFSDirName)); err != nil {
			return nil, nil
		}
		layerDirs[i] = dir
	}
	return &Image{Digest: digest, Config: rec.Config, LayerDirs: layerDirs}, nil
}

// pull fetches the image (by its resolved digest, so what is unpacked is
// exactly what was recorded), unpacks every missing layer into the pool, and
// writes the image record.
func (s *Store) pull(ctx context.Context, parsedRef name.Reference, digest v1.Hash) (*Image, error) {
	// Re-check under the flight lock: a racing EnsureImage may have completed
	// the pull between our cache miss and winning the singleflight slot.
	if img, err := s.cachedImage(digest); err != nil {
		return nil, err
	} else if img != nil {
		return img, nil
	}

	tStart := time.Now()
	digestRef := parsedRef.Context().Digest(digest.String())
	img, err := remote.Image(digestRef, s.remoteOpts(ctx, parsedRef)...)
	if err != nil {
		return nil, fmt.Errorf("in remote.Image: %w", err)
	}

	cfgFile, err := img.ConfigFile()
	if err != nil {
		return nil, fmt.Errorf("while reading image config: %w", err)
	}

	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("while listing image layers: %w", err)
	}

	layerDirs := make([]string, len(layers))
	diffIDs := make([]string, len(layers))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(layerPullConcurrency)
	for i, layer := range layers {
		g.Go(func() error {
			diffID, err := layer.DiffID()
			if err != nil {
				return fmt.Errorf("while reading layer diffID: %w", err)
			}
			dir, err := s.ensureLayer(gctx, diffID, layer)
			if err != nil {
				return fmt.Errorf("while unpacking layer %s: %w", diffID, err)
			}
			layerDirs[i], diffIDs[i] = dir, diffID.String()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	rec := imageRecord{Version: 1, Config: cfgFile.Config, DiffIDs: diffIDs}
	if err := s.writeRecord(digest, rec); err != nil {
		return nil, err
	}
	// For a multi-arch ref the requested digest is the index digest, but the
	// layers unpacked belong to the per-platform child manifest. Record the
	// image under the child digest too, so refs pinned either way hit.
	if actual, err := img.Digest(); err == nil && actual != digest {
		if err := s.writeRecord(actual, rec); err != nil {
			slog.WarnContext(ctx, "Failed to record image under platform manifest digest",
				slog.String("digest", actual.String()), slog.Any("err", err))
		}
	}

	slog.InfoContext(ctx, "Image pulled into layer cache",
		slog.String("digest", digest.String()),
		slog.Int("layers", len(layers)),
		slog.Duration("took", time.Since(tStart)))

	return &Image{Digest: digest, Config: cfgFile.Config, LayerDirs: layerDirs}, nil
}

// ensureLayer makes the unpacked tree for diffID present in the pool,
// collapsing concurrent requests for the same layer across images.
func (s *Store) ensureLayer(ctx context.Context, diffID v1.Hash, layer v1.Layer) (string, error) {
	dir := s.layerDir(diffID)
	_, err, _ := s.layerSF.Do(diffID.String(), func() (any, error) {
		if _, err := os.Stat(filepath.Join(dir, layerFSDirName)); err == nil {
			return nil, nil
		}
		return nil, s.unpackLayerToPool(ctx, diffID, layer)
	})
	if err != nil {
		return "", err
	}
	return dir, nil
}

// unpackLayerToPool streams the layer (download → decompress → untar) into a
// temp dir and renames it into place, so a layer dir either exists complete
// or not at all.
func (s *Store) unpackLayerToPool(ctx context.Context, diffID v1.Hash, layer v1.Layer) (retErr error) {
	tmp, err := os.MkdirTemp(filepath.Dir(s.layerDir(diffID)), ".tmp-"+diffID.Hex[:12]+"-")
	if err != nil {
		return fmt.Errorf("while creating layer temp dir: %w", err)
	}
	defer func() {
		// No-op once the rename has moved tmp into place.
		if _, err := os.Stat(tmp); err == nil {
			if rmErr := RemoveAllWritable(tmp); rmErr != nil && retErr == nil {
				retErr = fmt.Errorf("while cleaning up layer temp dir: %w", rmErr)
			}
		}
	}()

	fsDir := filepath.Join(tmp, layerFSDirName)
	if err := os.Mkdir(fsDir, 0o755); err != nil {
		return fmt.Errorf("while creating layer fs dir: %w", err)
	}

	rc, err := layer.Uncompressed()
	if err != nil {
		return fmt.Errorf("while opening layer stream: %w", err)
	}
	defer rc.Close()

	root, err := os.OpenRoot(fsDir)
	if err != nil {
		return fmt.Errorf("while opening layer fs dir as os.Root: %w", err)
	}
	defer root.Close()

	wh, err := unpackLayer(ctx, rc, root)
	if err != nil {
		return err
	}

	whBytes, err := json.Marshal(wh)
	if err != nil {
		return fmt.Errorf("while encoding layer whiteouts: %w", err)
	}
	if err := os.WriteFile(filepath.Join(tmp, layerWhiteoutsFileName), whBytes, 0o600); err != nil {
		return fmt.Errorf("while writing layer whiteouts: %w", err)
	}

	if err := os.Rename(tmp, s.layerDir(diffID)); err != nil {
		// A concurrent unpack (another process sharing the pool) may have won;
		// its layer is as good as ours.
		if _, statErr := os.Stat(filepath.Join(s.layerDir(diffID), layerFSDirName)); statErr == nil {
			return nil
		}
		return fmt.Errorf("while moving layer into pool: %w", err)
	}
	return nil
}

func (s *Store) writeRecord(digest v1.Hash, rec imageRecord) error {
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("while encoding image record: %w", err)
	}
	path := s.recordPath(digest)
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("while creating image record temp file: %w", err)
	}
	defer os.Remove(tmp.Name()) // no-op once the rename succeeds
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return fmt.Errorf("while writing image record: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("while closing image record: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("while moving image record into place: %w", err)
	}
	return nil
}

// remoteOpts assembles the go-containerregistry options for pulls from
// parsedRef's registry.
func (s *Store) remoteOpts(ctx context.Context, parsedRef name.Reference) []remote.Option {
	platform := v1.Platform{
		Architecture: runtime.GOARCH,
		OS:           "linux",
	}
	if s.platform != nil {
		platform = *s.platform
	}
	opts := []remote.Option{
		// Propagate caller ctx into go-containerregistry so cancellation tears
		// down in-flight layer-blob HTTP requests instead of letting them run
		// to completion in background goroutines.
		remote.WithContext(ctx),
		remote.WithPlatform(platform),
	}
	registry := parsedRef.Context().Registry.RegistryStr()
	if s.authenticator != nil && registryUsesGCPAuth(registry) {
		opts = append(opts, remote.WithAuth(s.authenticator))
	}
	return opts
}

func registryUsesGCPAuth(registry string) bool {
	return registry == "gcr.io" || strings.HasSuffix(registry, ".gcr.io") ||
		registry == "pkg.dev" || strings.HasSuffix(registry, ".pkg.dev")
}

// parseRef applies the localhost-registry rewrite (kind local registries) and
// permits plain-HTTP pulls for localhost/loopback registries, matching docker
// behavior so local development needs no TLS certs.
func (s *Store) parseRef(ref string) (name.Reference, error) {
	rewritten := false
	if s.localhostRegistryReplacement != "" {
		if newRef := s.rewriteLocalRegistry(ref); newRef != ref {
			ref = newRef
			rewritten = true
		}
	}
	var nameOpts []name.Option
	if rewritten || isLocalRegistry(ref) {
		nameOpts = append(nameOpts, name.Insecure)
	}
	return name.ParseReference(ref, nameOpts...)
}

func registryHost(ref string) string {
	parts := strings.SplitN(ref, "/", 2)
	reg, err := name.NewRegistry(parts[0], name.Insecure)
	if err != nil {
		return ""
	}
	hostPart := reg.Name()
	if h, _, err := net.SplitHostPort(hostPart); err == nil {
		return h
	}
	return hostPart
}

func isLocalhostOrLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return true
	}
	return false
}

func isLocalRegistry(ref string) bool {
	// By default docker permits localhost and 127.0.0.0/8; we also permit the
	// IPv6 loopback here.
	return isLocalhostOrLoopback(registryHost(ref))
}

func (s *Store) rewriteLocalRegistry(ref string) string {
	if isLocalRegistry(ref) {
		parts := strings.SplitN(ref, "/", 2)
		return s.localhostRegistryReplacement + "/" + parts[1]
	}
	return ref
}
