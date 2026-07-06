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
	"io"
	"os"
	"path"
	"strings"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/memorypullcache"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/rootfscache"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/opencontainers/runtime-spec/specs-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

const (
	// IdentityMountPath is the in-actor directory at which atelet bind-mounts
	// the actor's identity data. Workloads read the files inside it (at
	// request time, not cached at startup) to learn about themselves. It is
	// delivered as a per-actor bind mount rather than environment variables
	// because env lives in the checkpointed process memory and would be
	// frozen at the golden snapshot's values after a restore; a bind mount is
	// re-attached per-actor on every resume. A directory (rather than a
	// single-file mount) so further identity data can be added without
	// changing the mount shape.
	IdentityMountPath = "/run/ate"

	// ActorIDFileName is the file inside IdentityMountPath holding the
	// actor's own ID, raw with no trailing newline.
	ActorIDFileName = "actor-id"
)

// prepareOCIDirectory assembles the OCI bundle for one container inside an
// actor.  When a rootfsCache is available and the image ref contains a digest,
// the rootfs is materialized via an overlayfs mount over a node-local cache
// instead of re-extracting the tarball — reducing per-restore latency from
// seconds to sub-millisecond on cache hits.
func prepareOCIDirectory(ctx context.Context, pullCache *memorypullcache.MemoryPullCache, rootfsCache *rootfscache.Cache, actorTemplateNamespace, actorTemplateName, actorID, containerName, ref string, args []string, env []string, annotations map[string]string, netns string, identityDir string, durableDirVolumeMounts []*ateletpb.VolumeMount) error {
	tracer := otel.Tracer("prepareOCIDirectory")

	ctx, span := tracer.Start(ctx, "prepareOCIDirectory")
	span.SetAttributes(attribute.String("image", ref))
	defer span.End()

	bundlePath := ateompath.OCIBundlePath(actorTemplateNamespace, actorTemplateName, actorID, containerName)
	rootPath := path.Join(bundlePath, "rootfs")

	// Try the overlayfs cache path first.  This succeeds when:
	//   1. rootfsCache is non-nil, AND
	//   2. the image ref includes a digest (@sha256:…).
	// On a cache hit, tarData is NOT consumed, so we can skip the untar
	// entirely.  On a miss, the cache extracts and caches for next time.
	digest := extractDigestFromRef(ref)
	if rootfsCache != nil && digest != "" {
		// Pass Fetch as a lazy provider: on a cache hit EnsureRootfs returns the
		// on-disk lowerDir without invoking it, so we do NOT pull or extract the
		// image (and never buffer it in the memory pull cache) on the hot path.
		// The pull only happens on a genuine cache miss.
		lowerDir, _, err := rootfsCache.EnsureRootfs(ctx, digest, func() (io.ReadCloser, error) {
			return pullCache.Fetch(ctx, ref)
		})
		if err != nil {
			return fmt.Errorf("in rootfsCache.EnsureRootfs: %w", err)
		}

		// Create the overlay mount target. The actual overlayfs mount is
		// performed by the privileged ateom worker just before `runsc create`,
		// because atelet runs with all capabilities dropped and cannot call
		// mount(2). We record the read-only lowerdir in a per-bundle marker file
		// that ateom reads; its presence is ateom's signal to mount an overlay
		// (upperdir/workdir/target are derived from the bundle path by
		// convention on both sides).
		if err := os.MkdirAll(rootPath, 0o700); err != nil {
			return fmt.Errorf("in os.MkdirAll for rootfs mount target: %w", err)
		}
		markerPath := ateompath.OverlayLowerMarkerFile(actorTemplateNamespace, actorTemplateName, actorID, containerName)
		if err := os.WriteFile(markerPath, []byte(lowerDir), 0o600); err != nil {
			return fmt.Errorf("while writing overlay lower marker: %w", err)
		}

		span.SetAttributes(attribute.String("rootfs_method", "overlay"))
	} else {
		// Fallback: no digest or no cache — extract directly (original path).
		if err := os.RemoveAll(rootPath); err != nil {
			return fmt.Errorf("while clearing rootfs %q: %w", rootPath, err)
		}
		if err := os.MkdirAll(rootPath, 0o700); err != nil {
			return fmt.Errorf("in os.MkdirAll for container bundle dir: %w", err)
		}

		tarData, err := pullCache.Fetch(ctx, ref)
		if err != nil {
			return fmt.Errorf("in pullCache.Fetch: %w", err)
		}
		defer tarData.Close()

		if err := rootfscache.Untar(ctx, tarData, rootPath); err != nil {
			return fmt.Errorf("in untar: %w", err)
		}

		span.SetAttributes(attribute.String("rootfs_method", "untar"))
	}

	// Bind-mount the per-actor identity directory so the workload can read its
	// own ID at IdentityMountPath/ActorIDFileName. The bind target must exist
	// in the rootfs for the mount to attach.
	if identityDir != "" {
		if err := createMountPoint(rootPath, IdentityMountPath); err != nil {
			return fmt.Errorf("while creating identity mount point: %w", err)
		}
	}

	ociSpec := buildActorOCISpec(actorTemplateNamespace, actorTemplateName, actorID, args, env, annotations, netns, identityDir, durableDirVolumeMounts)
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

// extractDigestFromRef extracts the sha256 digest from an image reference.
// Returns "" if the ref does not contain a digest.
//   - "registry/image@sha256:abc123" → "sha256:abc123"
//   - "registry/image:latest"        → ""
func extractDigestFromRef(ref string) string {
	const prefix = "@sha256:"
	idx := strings.LastIndex(ref, prefix)
	if idx < 0 {
		return ""
	}
	return strings.TrimPrefix(ref[idx:], "@")
}

// buildActorOCISpec assembles the OCI runtime spec for an actor container.
// When identityDir is non-empty it adds a read-only bind mount of that host
// directory at IdentityMountPath so the actor can read its own ID (see
// IdentityMountPath for why this is a bind mount rather than env vars).
func buildActorOCISpec(actorTemplateNamespace string, actorTemplateName string, actorID string, args []string, env []string, annotations map[string]string, netns string, identityDir string, durableDirVolumeMounts []*ateletpb.VolumeMount) *specs.Spec {
	envVars := []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	}
	envVars = append(envVars, env...)

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
	if identityDir != "" {
		mounts = append(mounts, specs.Mount{
			Destination: IdentityMountPath,
			Type:        "bind",
			Source:      identityDir,
			Options:     []string{"ro"},
		})
	}

	spec := &specs.Spec{
		Process: &specs.Process{
			User: specs.User{
				UID: 0,
				GID: 0,
			},
			Args: args,
			Env:  envVars,
			Cwd:  "/",
			Capabilities: &specs.LinuxCapabilities{
				Bounding: []string{
					"CAP_AUDIT_WRITE",
					"CAP_KILL",
					"CAP_NET_BIND_SERVICE",
				},
				Effective: []string{
					"CAP_AUDIT_WRITE",
					"CAP_KILL",
					"CAP_NET_BIND_SERVICE",
				},
				Inheritable: []string{
					"CAP_AUDIT_WRITE",
					"CAP_KILL",
					"CAP_NET_BIND_SERVICE",
				},
				Permitted: []string{
					"CAP_AUDIT_WRITE",
					"CAP_KILL",
					"CAP_NET_BIND_SERVICE",
				},
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
		},
		Annotations: annotations,
	}

	// Prepare and mount durable-dir volumes.
	for _, vm := range durableDirVolumeMounts {
		spec.Mounts = append(spec.Mounts, specs.Mount{
			Destination: vm.GetMountPath(),
			Type:        "bind",
			Source:      ateompath.DurableDirVolumeMountPoint(actorTemplateNamespace, actorTemplateName, actorID, vm.GetName()),
		})
	}

	return spec
}

// createMountPoint creates the directory mountPath (an absolute in-rootfs
// path) to serve as a bind-mount target. It uses os.Root so the operation is
// confined to rootPath: a symlink planted by the image cannot redirect the
// write outside the extracted rootfs (same protection untar relies on).
func createMountPoint(rootPath, mountPath string) error {
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return fmt.Errorf("opening rootfs %q: %w", rootPath, err)
	}
	defer root.Close()

	rel := strings.TrimPrefix(mountPath, "/")
	if err := root.MkdirAll(rel, 0o755); err != nil {
		return fmt.Errorf("creating mount dir %q: %w", rel, err)
	}
	return nil
}

// untar is a thin wrapper around rootfscache.Untar kept in this package so
// that existing tests (package main) continue to compile without importing
// the rootfscache package directly.
func untar(ctx context.Context, tarData io.Reader, rootPath string) error {
	return rootfscache.Untar(ctx, tarData, rootPath)
}

// validateTarName is re-exported here for the same reason as untar: tests in
// package main call it directly.
func validateTarName(name string) (cleaned string, skip bool, err error) {
	return rootfscache.ValidateTarName(name)
}
