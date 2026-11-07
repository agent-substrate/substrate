// Copyright 2026 The Agent Substrate Authors
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	providerclient "github.com/ai-on-gke/image-provider-plugin/client"
	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/moby/sys/user"
	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
)

const (
	// capabilityAll is the sentinel a template may put in drop to clear the
	// whole default set. It is rejected in add (see ateapi.Capabilities).
	capabilityAll = "ALL"

	// providerReleaseTimeout bounds one ReleaseImage call. Releases run on a
	// context detached from the caller, so a failed or cancelled request still
	// returns its leases.
	providerReleaseTimeout = 30 * time.Second
)

// errProviderDeclined reports that the image provider plugin does not serve
// an image, which is not a failure of the plugin.
var errProviderDeclined = errors.New("the image provider plugin declined the image")

// imageSource resolves container and volume images. It asks the image provider
// plugin first when one is configured, and pulls into the image cache when the
// plugin declines or fails.
type imageSource struct {
	cache *imagecache.Store
	// provider is nil when --image-provider-socket is empty.
	provider providerclient.Client
	// keychain resolves the registry credentials the provider gets.
	keychain    authn.Keychain
	instruments *Instruments
}

// ensure returns ref's image. A digest ref already in the cache is served from
// it. If the provider prepares the image, it does so under the lease leaseID,
// and the Image's LeaseID is set. The caller releases the lease with release.
func (s imageSource) ensure(ctx context.Context, ref, leaseID string) (*imagecache.Image, error) {
	if s.provider == nil {
		return s.cache.EnsureImage(ctx, ref)
	}
	if img, err := s.cache.CachedImage(ctx, ref); err != nil || img != nil {
		return img, err
	}
	start := time.Now()
	img, err := s.prepare(ctx, ref, leaseID)
	if err == nil {
		s.instruments.recordImageProvider(ctx, s.provider.Name(), ateattr.ImageProviderOutcomeSuccess, time.Since(start))
		return img, nil
	}
	if !errors.Is(err, errProviderDeclined) {
		slog.WarnContext(ctx, "Image provider plugin failed; pulling into the image cache",
			slog.String("image", ref), slog.String("provider", s.provider.Name()), slog.Any("err", err))
	}
	img, err = s.cache.EnsureImage(ctx, ref)
	outcome := ateattr.ImageProviderOutcomeFallback
	if err != nil {
		outcome = ateattr.ImageProviderOutcomeError
	}
	s.instruments.recordImageProvider(ctx, s.provider.Name(), outcome, time.Since(start))
	return img, err
}

// prepare has the provider prepare ref's layers under the lease leaseID. It
// returns an error wrapping errProviderDeclined if the provider does not
// serve ref.
func (s imageSource) prepare(ctx context.Context, ref, leaseID string) (*imagecache.Image, error) {
	if s.keychain != nil {
		ctx = providerclient.WithKeychainContext(ctx, s.keychain)
	}
	req := &providerclient.PrepareRequest{ImageRef: ref, LeaseID: leaseID}
	ok, err := s.provider.CanProvide(ctx, req)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errProviderDeclined
	}
	res, err := s.provider.PrepareImage(ctx, req)
	if err != nil {
		// The plugin may have taken the lease before the call failed, for
		// example on a timeout. A release without a digest matches it.
		s.release(ctx, leaseID, "")
	}
	if errors.Is(err, providerclient.ErrNotSupported) {
		return nil, fmt.Errorf("%w: %w", errProviderDeclined, err)
	}
	if err != nil {
		return nil, err
	}
	img, err := streamedImage(res, leaseID)
	if err != nil {
		s.release(ctx, leaseID, res.ImageDigest)
		return nil, err
	}
	return img, nil
}

// streamedImage converts the provider's result for one image, held under the
// lease leaseID.
func streamedImage(res *providerclient.PrepareResult, leaseID string) (*imagecache.Image, error) {
	if len(res.LayerDirs) == 0 {
		return nil, errors.New("the image provider plugin returned no layer directories")
	}
	if res.Config == nil {
		return nil, errors.New("the image provider plugin returned no image config")
	}
	digest, err := v1.NewHash(res.ImageDigest)
	if err != nil {
		return nil, fmt.Errorf("the image provider plugin returned image digest %q: %w", res.ImageDigest, err)
	}
	return &imagecache.Image{Digest: digest, Config: *res.Config, ProviderDirs: res.LayerDirs, LeaseID: leaseID}, nil
}

// release drops the provider lease leaseID on the image with digest. A failed
// release is logged.
func (s imageSource) release(ctx context.Context, leaseID, digest string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), providerReleaseTimeout)
	defer cancel()
	if err := s.provider.ReleaseImage(ctx, &providerclient.ReleaseRequest{LeaseID: leaseID, ImageDigest: digest}); err != nil {
		slog.WarnContext(ctx, "Failed to release an image provider lease",
			slog.String("lease", leaseID), slog.String("provider", s.provider.Name()), slog.Any("err", err))
	}
}

// providerLeaseID names the provider lease on a container's image, or on the
// image of its volume when volume is set. The name stays the same across runs
// of the actor, so a retried PrepareImage takes no second lease.
func providerLeaseID(actorUID, containerName, volume string) string {
	id := actorUID + "/" + containerName
	if volume != "" {
		id += "/" + volume
	}
	return id
}

// defaultCapabilities is what an actor container gets when its template asks
// for no adjustment. Names are unprefixed; resolveCapabilities adds the OCI
// "CAP_" prefix.
var defaultCapabilities = []string{
	"AUDIT_WRITE",
	"KILL",
	"NET_BIND_SERVICE",
}

// resolveCapabilities computes a container's effective capability set as
// default - drop + add. Drop applies first, so a capability named in both is
// granted. The result is CAP_-prefixed and sorted for a stable OCI spec: the
// spec is written on every run and a reordered set would churn the bundle.
func resolveCapabilities(caps *ateletpb.Capabilities) []string {
	effective := make(map[string]struct{}, len(defaultCapabilities))
	for _, c := range defaultCapabilities {
		effective[c] = struct{}{}
	}
	for _, d := range caps.GetDrop() {
		if d == capabilityAll {
			clear(effective)
			break
		}
		delete(effective, d)
	}
	for _, a := range caps.GetAdd() {
		effective[a] = struct{}{}
	}

	out := make([]string, 0, len(effective))
	for c := range effective {
		out = append(out, "CAP_"+c)
	}
	sort.Strings(out)
	return out
}

// prepareOCIDirectory resolves the container's image and writes the overlay
// spec the worker composes its rootfs from. It returns the image and the
// process's working directory, which the worker creates through the overlay.
// The spec records which images hold provider leases; on failure, the leases
// this call took are released.
func prepareOCIDirectory(ctx context.Context, images imageSource, actorUID, containerName, ref string, volumes []*ateletpb.Volume, volumeMounts []*ateletpb.VolumeMount) (_ *imagecache.Image, _ string, err error) {
	tracer := otel.Tracer("prepareOCIDirectory")

	ctx, span := tracer.Start(ctx, "prepareOCIDirectory")
	span.SetAttributes(attribute.String("image", ref))
	defer span.End()

	bundlePath := ateletpath.OCIBundlePath(actorUID, containerName)

	// Clear any previous bundle contents (belt and suspenders:
	// actordirs.ResetActorDirs already wiped the bundle dir on the Run/Restore
	// path).
	if err := imagecache.RemoveAllWritable(bundlePath); err != nil {
		return nil, "", fmt.Errorf("while clearing bundle %q: %w", bundlePath, err)
	}

	// The bundle's rootfs is composed by worker as an overlay mount just before
	// the workload runs: the cached image layers are the read-only lowerdirs,
	// and the bundle-local upper/work hold this actor's private writes (wiped
	// between runs, preserving the pristine-rootfs-per-run contract the old
	// full re-untar provided). atelet only prepares the (empty) directories:
	// it has no CAP_SYS_ADMIN, so it cannot mount.
	for _, d := range []string{"rootfs", "upper", "work"} {
		if err := os.MkdirAll(path.Join(bundlePath, d), 0o700); err != nil {
			return nil, "", fmt.Errorf("in os.MkdirAll for container bundle dir: %w", err)
		}
	}

	var (
		img          *imagecache.Image
		imageVolumes []imagecache.ImageVolumeOverlay
	)
	defer func() {
		if err == nil {
			return
		}
		if img != nil && img.LeaseID != "" {
			images.release(ctx, img.LeaseID, img.Digest.String())
		}
		for _, v := range imageVolumes {
			if v.LeaseID != "" {
				images.release(ctx, v.LeaseID, v.ImageDigest)
			}
		}
	}()
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		if img, err = images.ensure(gctx, ref, providerLeaseID(actorUID, containerName, "")); err != nil {
			return fmt.Errorf("while resolving image %q: %w", ref, err)
		}
		return nil
	})
	g.Go(func() error {
		var err error
		imageVolumes, err = resolveImageVolumes(gctx, images, actorUID, containerName, volumes, volumeMounts)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, "", err
	}
	cwd, err := resolveCwd(&img.Config)
	if err != nil {
		return nil, "", fmt.Errorf("while resolving the working directory of container %q: %w", containerName, err)
	}

	// Every bind target must exist in the rootfs for the mount to attach, and
	// the working directory for the process to start; worker creates them
	// through the mounted overlay (they land in the actor's upper).
	var extraDirs []string
	for _, vm := range volumeMounts {
		extraDirs = append(extraDirs, vm.GetMountPath())
	}
	if cwd != "/" {
		extraDirs = append(extraDirs, cwd)
	}
	if err := imagecache.WriteSpec(bundlePath, &imagecache.OverlaySpec{
		ImageDigest:  img.Digest.String(),
		ImageRef:     ref,
		LeaseID:      img.LeaseID,
		LowerDirs:    img.LowerDirs(),
		StoreLayers:  img.LayerDirs,
		ExtraDirs:    extraDirs,
		ImageVolumes: imageVolumes,
	}); err != nil {
		return nil, "", fmt.Errorf("while writing overlay spec: %w", err)
	}

	return img, cwd, nil
}

// resolveCwd reads the image's WORKDIR; none means "/". The OCI spec requires
// an absolute path.
func resolveCwd(imageCfg *v1.Config) (string, error) {
	if imageCfg == nil || imageCfg.WorkingDir == "" {
		return "/", nil
	}
	if !path.IsAbs(imageCfg.WorkingDir) {
		return "", fmt.Errorf("image WorkingDir %q is not absolute", imageCfg.WorkingDir)
	}
	return path.Clean(imageCfg.WorkingDir), nil
}

// resolveImageVolumes resolves the image behind every image-typed volume this
// container mounts and returns what the overlay spec needs to compose each. On
// failure, the provider leases it took are released.
func resolveImageVolumes(ctx context.Context, images imageSource, actorUID, containerName string, volumes []*ateletpb.Volume, volumeMounts []*ateletpb.VolumeMount) ([]imagecache.ImageVolumeOverlay, error) {
	mounted := make(map[string]bool, len(volumeMounts))
	for _, vm := range volumeMounts {
		mounted[vm.GetName()] = true
	}

	var wanted []*ateletpb.Volume
	for _, vol := range volumes {
		if vol.GetImage() == nil || !mounted[vol.GetName()] {
			continue
		}
		wanted = append(wanted, vol)
	}

	// Resolve the volumes concurrently; each entry lands at its own index so
	// the spec order stays the template order.
	out := make([]imagecache.ImageVolumeOverlay, len(wanted))
	g, gctx := errgroup.WithContext(ctx)
	for i, vol := range wanted {
		g.Go(func() error {
			volRef := vol.GetImage().GetReference()
			img, err := images.ensure(gctx, volRef, providerLeaseID(actorUID, containerName, vol.GetName()))
			if err != nil {
				return fmt.Errorf("while resolving image %q for volume %q: %w", volRef, vol.GetName(), err)
			}
			out[i] = imagecache.ImageVolumeOverlay{
				Name:        vol.GetName(),
				ImageDigest: img.Digest.String(),
				ImageRef:    volRef,
				LeaseID:     img.LeaseID,
				LowerDirs:   img.LowerDirs(),
				StoreLayers: img.LayerDirs,
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		for _, v := range out {
			if v.LeaseID != "" {
				images.release(ctx, v.LeaseID, v.ImageDigest)
			}
		}
		return nil, err
	}
	return out, nil
}

// resolveActorEnv computes the final container environment from the image's ENV
// and the ActorTemplate env, with the template taking precedence. Duplicate keys
// are removed in favor of template env > image env, and a default PATH stands in
// when neither source sets one.
func resolveActorEnv(imageCfg *v1.Config, templateEnv []string) []string {
	var imageEnv []string
	if imageCfg != nil {
		imageEnv = imageCfg.Env
	}

	seen := make(map[string]struct{})
	var out []string
	add := func(entries ...string) {
		for _, e := range entries {
			key, _, _ := strings.Cut(e, "=")
			if key == "" {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, e)
		}
	}

	add(templateEnv...)
	add(imageEnv...)
	add("PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	return out
}

// resolveProcessArgs computes the final process argv for a container,
// following Kubernetes Pod semantics: setting command overrides both the
// image's ENTRYPOINT and its CMD (CMD is dropped, not appended), while
// setting only args overrides just the image's CMD.
func resolveProcessArgs(imageCfg *v1.Config, command, args []string) ([]string, error) {
	var entrypoint, cmd []string
	if imageCfg != nil {
		entrypoint = imageCfg.Entrypoint
		cmd = imageCfg.Cmd
	}
	if len(command) > 0 {
		entrypoint = command
		cmd = nil
	}
	if len(args) > 0 {
		cmd = args
	}

	argv := make([]string, 0, len(entrypoint)+len(cmd))
	argv = append(argv, entrypoint...)
	argv = append(argv, cmd...)
	if len(argv) == 0 {
		return nil, fmt.Errorf("no command specified: image defines neither ENTRYPOINT nor CMD and the container sets neither command nor args")
	}
	return argv, nil
}

// resolveImageUser resolves the image's USER against the image's own
// /etc/passwd and /etc/group, either of which may be absent. No USER is root,
// reported as nil.
func resolveImageUser(img *imagecache.Image) (*ateworkerpb.ProcessUser, error) {
	if img.Config.User == "" {
		return nil, nil
	}
	passwd, err := readOptionalImageFile(img, "etc/passwd")
	if err != nil {
		return nil, err
	}
	group, err := readOptionalImageFile(img, "etc/group")
	if err != nil {
		return nil, err
	}
	return resolveUser(img.Config.User, passwd, group)
}

// readOptionalImageFile returns a nil reader for a file the image lacks.
func readOptionalImageFile(img *imagecache.Image, name string) (io.Reader, error) {
	b, err := img.ReadFile(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

// resolveUser follows Docker's rules for USER "user[:group]": numeric ids as
// is, a bare uid with its login group from passwd (gid 0 without an entry),
// names looked up or an error, group memberships as supplementary groups.
// Ids are bounded to int32. A nil reader means the image lacks the file.
func resolveUser(userSpec string, passwd, group io.Reader) (*ateworkerpb.ProcessUser, error) {
	if strings.Count(userSpec, ":") > 1 {
		return nil, fmt.Errorf("image User %q: want user[:group]", userSpec)
	}
	u, err := user.GetExecUser(userSpec, nil, passwd, group)
	if err != nil {
		return nil, fmt.Errorf("image User %q: %w", userSpec, err)
	}
	identity := &ateworkerpb.ProcessUser{Uid: uint32(u.Uid), Gid: uint32(u.Gid)}
	for _, gid := range u.Sgids {
		identity.AdditionalGids = append(identity.AdditionalGids, uint32(gid))
	}
	return identity, nil
}
