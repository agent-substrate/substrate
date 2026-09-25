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
	"log/slog"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/imagestreaming"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/google"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
)

const (
	// capabilityAll is the sentinel a template may put in drop to clear the
	// whole default set. It is rejected in add (see v1alpha1.Capabilities).
	capabilityAll = "ALL"
)

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

func prepareOCIDirectory(ctx context.Context, imageCache *imagecache.Store, streamer imagestreaming.ImageStreamer, keychain authn.Keychain, instruments *Instruments, actorUID, containerName, ref string, command, args []string, env []string, netns string, volumes []*ateletpb.Volume, volumeMounts []*ateletpb.VolumeMount, capabilities []string, resources *ateletpb.ResourceLimits) error {
	tracer := otel.Tracer("prepareOCIDirectory")

	ctx, span := tracer.Start(ctx, "prepareOCIDirectory")
	span.SetAttributes(attribute.String("image", ref))
	defer span.End()

	bundlePath := ateompath.OCIBundlePath(actorUID, containerName)

	// Clear any previous bundle contents (belt and suspenders: resetActorDirs
	// already wiped the bundle dir on the Run/Restore path).
	if err := imagecache.RemoveAllWritable(bundlePath); err != nil {
		return fmt.Errorf("while clearing bundle %q: %w", bundlePath, err)
	}

	// The bundle's rootfs is composed by ateom as an overlay mount just before
	// the workload runs: the cached image layers are the read-only lowerdirs,
	// and the bundle-local upper/work hold this actor's private writes (wiped
	// between runs, preserving the pristine-rootfs-per-run contract the old
	// full re-untar provided). atelet only prepares the (empty) directories —
	// it deliberately runs with no capabilities, so it cannot mount.
	for _, d := range []string{"rootfs", "upper", "work"} {
		if err := os.MkdirAll(path.Join(bundlePath, d), 0o700); err != nil {
			return fmt.Errorf("in os.MkdirAll for container bundle dir: %w", err)
		}
	}

	var (
		img          *imagecache.Image
		imageVolumes []imagecache.ImageVolumeOverlay
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		if img, err = ensureContainerImage(gctx, imageCache, streamer, keychain, instruments, ref); err != nil {
			return fmt.Errorf("in ensureContainerImage: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		var err error
		imageVolumes, err = resolveImageVolumes(gctx, imageCache, volumes, volumeMounts)
		return err
	})
	if err := g.Wait(); err != nil {
		return err
	}

	// Argv and env need only the image config; resolve them before writing
	// any spec so an invalid container config fails fast.
	resolvedArgs, err := resolveProcessArgs(&img.Config, command, args)
	if err != nil {
		return fmt.Errorf("while resolving process args for container %q: %w", containerName, err)
	}
	resolvedEnv := resolveActorEnv(&img.Config, env)

	// Every bind target must exist in the rootfs for the mount to attach;
	// ateom creates them through the mounted overlay (they land in the
	// actor's upper).
	var extraDirs []string
	for _, vm := range volumeMounts {
		extraDirs = append(extraDirs, vm.GetMountPath())
	}
	if err := imagecache.WriteSpec(bundlePath, &imagecache.OverlaySpec{
		ImageDigest:  img.Digest.String(),
		ImageRef:     ref,
		Layers:       img.LayerDirs,
		ExtraDirs:    extraDirs,
		ImageVolumes: imageVolumes,
	}); err != nil {
		return fmt.Errorf("while writing overlay spec: %w", err)
	}

	// Write the runtime-neutral OCI spec to config.json.
	if err := ocispec.Save(bundlePath, ocispec.Build(ocispec.Options{
		ActorUID:      actorUID,
		ContainerName: containerName,
		Args:          resolvedArgs,
		Env:           resolvedEnv,
		NetNSPath:     netns,
		Volumes:       volumes,
		VolumeMounts:  volumeMounts,
		Capabilities:  capabilities,
		Resources:     resources,
	})); err != nil {
		return fmt.Errorf("while writing OCI spec: %w", err)
	}

	return nil
}

// resolveImageVolumes pulls the image behind every image-typed volume this
// container mounts and returns what the overlay spec needs to compose each.
func resolveImageVolumes(ctx context.Context, imageCache *imagecache.Store, volumes []*ateletpb.Volume, volumeMounts []*ateletpb.VolumeMount) ([]imagecache.ImageVolumeOverlay, error) {
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

	// Pull the volumes concurrently; each entry lands at its own index so the
	// spec order stays the template order.
	out := make([]imagecache.ImageVolumeOverlay, len(wanted))
	g, gctx := errgroup.WithContext(ctx)
	for i, vol := range wanted {
		g.Go(func() error {
			img, err := imageCache.EnsureImage(gctx, vol.GetImage().GetReference())
			if err != nil {
				return fmt.Errorf("in imageCache.EnsureImage for volume %q: %w", vol.GetName(), err)
			}
			out[i] = imagecache.ImageVolumeOverlay{
				Name:        vol.GetName(),
				ImageDigest: img.Digest.String(),
				Layers:      img.LayerDirs,
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
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
		return nil, fmt.Errorf("%w: no command specified: image defines neither ENTRYPOINT nor CMD and the container sets neither command nor args", ateerrors.ReasonInvalidContainerConfig)
	}
	return argv, nil
}

// ensureContainerImage resolves and prepares layers for an image.
// If an ImageStreamer is provided and supports streaming the image, it mounts
// the virtual layer directories via the streaming provider without full layer download/untar.
// If streaming fails or is unsupported, it falls back to imageCache.EnsureImage.
func ensureContainerImage(ctx context.Context, imageCache *imagecache.Store, streamer imagestreaming.ImageStreamer, keychain authn.Keychain, instruments *Instruments, ref string) (*imagecache.Image, error) {
	t0 := time.Now()
	if streamer != nil {
		req := &imagestreaming.StreamRequest{ImageRef: ref}
		canStream, err := streamer.CanStream(ctx, req)
		if err != nil {
			instruments.RecordImageStreaming(ctx, streamer.Name(), ateattr.ImageStreamingOutcomeFallback, time.Since(t0))
			slog.WarnContext(ctx, "Error evaluating image streaming eligibility; falling back to cache",
				slog.String("image", ref),
				slog.String("streamer", streamer.Name()),
				slog.Any("err", err))
		} else if !canStream {
			instruments.RecordImageStreaming(ctx, streamer.Name(), ateattr.ImageStreamingOutcomeFallback, time.Since(t0))
		} else {
			res, err := streamer.PrepareLayers(ctx, req)
			if err != nil {
				instruments.RecordImageStreaming(ctx, streamer.Name(), ateattr.ImageStreamingOutcomeFallback, time.Since(t0))
				slog.WarnContext(ctx, "Image streaming layer preparation failed; falling back to cache",
					slog.String("image", ref),
					slog.String("streamer", streamer.Name()),
					slog.Any("err", err))
			} else if res != nil && len(res.LayerDirs) > 0 {
				var (
					digest v1.Hash
					cfg    v1.Config
				)
				if res.ImageDigest != "" {
					digest, _ = v1.NewHash(res.ImageDigest)
				}
				if res.Config != nil {
					cfg = *res.Config
				} else {
					d, c, err := fetchImageConfig(ctx, ref, keychain)
					if err != nil {
						instruments.RecordImageStreaming(ctx, streamer.Name(), ateattr.ImageStreamingOutcomeFallback, time.Since(t0))
						slog.WarnContext(ctx, "Failed to resolve image config for streamed image; falling back to cache",
							slog.String("image", ref),
							slog.Any("err", err))
						goto fallback
					}
					if digest.String() == "" {
						digest = d
					}
					cfg = c
				}
				instruments.RecordImageStreaming(ctx, streamer.Name(), ateattr.ImageStreamingOutcomeSuccess, time.Since(t0))
				return &imagecache.Image{
					Digest:    digest,
					Config:    cfg,
					LayerDirs: res.LayerDirs,
				}, nil
			} else {
				instruments.RecordImageStreaming(ctx, streamer.Name(), ateattr.ImageStreamingOutcomeFallback, time.Since(t0))
			}
		}
	}

fallback:
	if imageCache == nil {
		if streamer != nil {
			instruments.RecordImageStreaming(ctx, streamer.Name(), ateattr.ImageStreamingOutcomeError, time.Since(t0))
		}
		return nil, fmt.Errorf("imageCache is nil and image streaming is unavailable for %q", ref)
	}
	img, err := imageCache.EnsureImage(ctx, ref)
	if err != nil && streamer != nil {
		instruments.RecordImageStreaming(ctx, streamer.Name(), ateattr.ImageStreamingOutcomeError, time.Since(t0))
	}
	return img, err
}

func fetchImageConfig(ctx context.Context, ref string, keychain authn.Keychain) (v1.Hash, v1.Config, error) {
	opts := []name.Option{name.Insecure}
	parsedRef, err := name.ParseReference(ref, opts...)
	if err != nil {
		return v1.Hash{}, v1.Config{}, fmt.Errorf("while parsing reference %q: %w", ref, err)
	}
	remoteOpts := []remote.Option{remote.WithContext(ctx)}
	if keychain != nil {
		remoteOpts = append(remoteOpts, remote.WithAuthFromKeychain(keychain))
	} else {
		remoteOpts = append(remoteOpts, remote.WithAuthFromKeychain(authn.NewMultiKeychain(authn.DefaultKeychain, google.Keychain)))
	}
	img, err := remote.Image(parsedRef, remoteOpts...)
	if err != nil {
		return v1.Hash{}, v1.Config{}, fmt.Errorf("while fetching image metadata %q: %w", ref, err)
	}
	digest, err := img.Digest()
	if err != nil {
		return v1.Hash{}, v1.Config{}, fmt.Errorf("while resolving image digest: %w", err)
	}
	cfgFile, err := img.ConfigFile()
	if err != nil {
		return v1.Hash{}, v1.Config{}, fmt.Errorf("while reading config file: %w", err)
	}
	return digest, cfgFile.Config, nil
}
