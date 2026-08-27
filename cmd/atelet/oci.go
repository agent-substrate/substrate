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
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/opencontainers/runtime-spec/specs-go"
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

func prepareOCIDirectory(ctx context.Context, imageCache *imagecache.Store, actorUID, containerName, ref string, command, args []string, env []string, annotations map[string]string, netns string, volumes []*ateletpb.Volume, volumeMounts []*ateletpb.VolumeMount, capabilities []string, resources *ateletpb.ResourceLimits) error {
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
		if img, err = imageCache.EnsureImage(gctx, ref); err != nil {
			return fmt.Errorf("in imageCache.EnsureImage: %w", err)
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
		Layers:       img.LayerDirs,
		ExtraDirs:    extraDirs,
		ImageVolumes: imageVolumes,
	}); err != nil {
		return fmt.Errorf("while writing overlay spec: %w", err)
	}

	ociSpec := buildActorOCISpec(actorUID, containerName, resolvedArgs, resolvedEnv, annotations, netns, volumes, volumeMounts, capabilities, resources)
	ociSpecBytes, err := json.MarshalIndent(ociSpec, "", "  ")
	if err != nil {
		return fmt.Errorf("while marshaling OCI spec: %w", err)
	}
	specPath := path.Join(bundlePath, "config.json")
	if err := os.WriteFile(specPath, ociSpecBytes, 0o600); err != nil {
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

const (
	// cpuQuotaPeriodUS is the CFS period the CPU quota is expressed against: a
	// container limited to N milli-cores may run for N/1000 of every period.
	cpuQuotaPeriodUS = 100000
	// cpuQuotaMinUS is the smallest quota the kernel accepts. tg_set_cfs_bandwidth
	// rejects a quota below 1ms, so a limit under
	// cpuQuotaMinUS*1000/cpuQuotaPeriodUS milli-cores (10m) is raised to this
	// floor rather than producing a spec the guest refuses.
	cpuQuotaMinUS = 1000
)

// ociResources maps the resolved limits onto the OCI spec's linux.resources.
// Returns nil when nothing is set, so the spec carries no linux.resources at
// all for a container that declares no limits.
func ociResources(r *ateletpb.ResourceLimits) *specs.LinuxResources {
	if r == nil || (r.GetMemoryBytes() <= 0 && r.GetCpuMillis() <= 0) {
		return nil
	}
	out := &specs.LinuxResources{}
	if b := r.GetMemoryBytes(); b > 0 {
		out.Memory = &specs.LinuxMemory{Limit: &b}
	}
	if m := r.GetCpuMillis(); m > 0 {
		quota := max(m*cpuQuotaPeriodUS/1000, cpuQuotaMinUS)
		period := uint64(cpuQuotaPeriodUS)
		out.CPU = &specs.LinuxCPU{Quota: &quota, Period: &period}
	}
	return out
}

// buildActorOCISpec assembles the OCI runtime spec for an actor container from
// already-resolved args, env and capabilities (see resolveProcessArgs,
// resolveActorEnv and resolveCapabilities). An empty capabilities set means the
// process runs with none, which is what the pause container gets.
func buildActorOCISpec(actorUID, containerName string, args []string, env []string, annotations map[string]string, netns string, volumes []*ateletpb.Volume, volumeMounts []*ateletpb.VolumeMount, capabilities []string, resources *ateletpb.ResourceLimits) *specs.Spec {
	mounts := []specs.Mount{
		{
			Destination: "/proc",
			Type:        "proc",
			Source:      "proc",
		},
		{
			Destination: "/dev",
			Type:        "tmpfs",
			Source:      "tmpfs",
		},
		{
			Destination: "/sys",
			Type:        "sysfs",
			Source:      "sysfs",
			Options: []string{
				"nosuid",
				"noexec",
				"nodev",
				"ro",
			},
		},
		{
			Destination: "/etc/resolv.conf",
			Type:        "bind",
			Source:      "/etc/resolv.conf",
			Options:     []string{"ro"},
		},
	}

	spec := &specs.Spec{
		Process: &specs.Process{
			User: specs.User{
				UID: 0,
				GID: 0,
			},
			Args: args,
			Env:  env,
			Cwd:  "/",
			Capabilities: &specs.LinuxCapabilities{
				Bounding:  capabilities,
				Effective: capabilities,
				Permitted: capabilities,
				// Inheritable stays empty, as in containerd/CRI-O/Docker: it only
				// applies on execve, ANDed with the file's own inheritable set, so
				// a non-empty one lets a container that drops to an unprivileged
				// uid regain a capability (CVE-2022-24769). Children inherit via
				// Bounding.
				//
				// TODO(gvisor.dev/issue/3166): support ambient capabilities
			},
			Rlimits: []specs.POSIXRlimit{
				{
					Type: "RLIMIT_NOFILE",
					Hard: 1024,
					Soft: 1024,
				},
			},
		},
		Root: &specs.Root{
			Path:     "rootfs",
			Readonly: false,
		},
		Hostname: "runsc",
		Mounts:   mounts,
		Linux: &specs.Linux{
			Namespaces: []specs.LinuxNamespace{
				{
					Type: "pid",
				},
				{
					Type: "network",
					Path: netns, // Will be created by ateom
				},
				{
					Type: "ipc",
				},
				{
					Type: "uts",
				},
				{
					Type: "mount",
				},
			},
			Resources: ociResources(resources),
		},
		Annotations: annotations,
	}

	// Prepare and mount all volumes.
	volumesByName := make(map[string]*ateletpb.Volume)
	for _, vol := range volumes {
		volumesByName[vol.GetName()] = vol
	}

	for _, vm := range volumeMounts {
		var srcPath string
		options := []string{"bind", "rw"}
		switch volumesByName[vm.GetName()].GetSource().(type) {
		case *ateletpb.Volume_DurableDir:
			srcPath = ateompath.DurableDirVolumeMountPoint(actorUID, vm.GetName())
		case *ateletpb.Volume_External:
			srcPath = ateompath.VolumeHostPath(actorUID, vm.GetName())
		case *ateletpb.Volume_SystemInfo:
			// System-info contents are generated by atelet; the workload only
			// reads them.
			srcPath = ateompath.SystemInfoVolumeRoot(actorUID, vm.GetName())
			options = []string{"bind", "ro"}
		case *ateletpb.Volume_Image:
			srcPath = ateompath.ImageVolumeMountPath(actorUID, containerName, vm.GetName())
			options = []string{"bind", "ro"}
		default:
			continue
		}
		spec.Mounts = append(spec.Mounts, specs.Mount{
			Destination: vm.GetMountPath(),
			Type:        "bind",
			Source:      srcPath,
			Options:     options,
		})
	}

	return spec
}
