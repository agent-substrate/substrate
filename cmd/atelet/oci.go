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
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/memorypullcache"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/opencontainers/runtime-spec/specs-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

func prepareOCIDirectory(ctx context.Context, pullCache *memorypullcache.MemoryPullCache, actorTemplateNamespace, actorTemplateName, actorID, containerName, ref string, args []string, env []string, annotations map[string]string, netns string, identityDir string) error {
	tracer := otel.Tracer("prepareOCIDirectory")

	ctx, span := tracer.Start(ctx, "prepareOCIDirectory")
	span.SetAttributes(attribute.String("image", ref))
	defer span.End()

	bundlePath := ateompath.OCIBundlePath(actorTemplateNamespace, actorTemplateName, actorID, containerName)
	rootPath := path.Join(bundlePath, "rootfs")

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

	if err := untar(ctx, tarData, rootPath); err != nil {
		return fmt.Errorf("in untar: %w", err)
	}

	// Bind-mount the per-actor identity directory so the workload can read its
	// own ID at IdentityMountPath/ActorIDFileName. The bind target must exist
	// in the rootfs for the mount to attach.
	if identityDir != "" {
		if err := createMountPoint(rootPath, IdentityMountPath); err != nil {
			return fmt.Errorf("while creating identity mount point: %w", err)
		}
	}

	ociSpec := buildActorOCISpec(args, env, annotations, netns, identityDir)
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

// buildActorOCISpec assembles the OCI runtime spec for an actor container.
// When identityDir is non-empty it adds a read-only bind mount of that host
// directory at IdentityMountPath so the actor can read its own ID (see
// IdentityMountPath for why this is a bind mount rather than env vars).
func buildActorOCISpec(args []string, env []string, annotations map[string]string, netns string, identityDir string) *specs.Spec {
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

	return &specs.Spec{
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

func validateTarName(name string) (cleaned string, skip bool, err error) {
	if name == "" {
		return "", true, nil
	}
	cleaned = filepath.Clean(name)
	if cleaned == "." {
		return "", true, nil
	}
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "" || cleaned == "." {
		return "", true, nil
	}
	if !filepath.IsLocal(cleaned) {
		return "", false, fmt.Errorf("not a local path: %q", name)
	}
	return cleaned, false, nil
}

func untar(ctx context.Context, tarData io.Reader, rootPath string) error {
	tracer := otel.Tracer("ateom-gvisor")
	ctx, span := tracer.Start(ctx, "untar")
	defer span.End()

	// os.Root confines file operations to rootPath: ".." components and
	// out-of-tree symlinks are refused by the kernel.
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return fmt.Errorf("while opening rootfs %q as os.Root: %w", rootPath, err)
	}
	defer root.Close()

	tarReader := tar.NewReader(tarData)
	for {
		hdr, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("in tarReader.Next: %w", err)
		}

		name, skip, err := validateTarName(hdr.Name)
		if err != nil {
			return fmt.Errorf("invalid tar entry: %w", err)
		}
		if skip {
			continue
		}

		mode := hdr.FileInfo().Mode().Perm()

		switch hdr.Typeflag {
		case tar.TypeReg: // Regular file
			// Same "later entry wins" handling: if any entry exists at the target path,
			// remove it first. This ensures that:
			// 1. If it's a symlink, we don't write through it (security vulnerability / incorrectness).
			// 2. If it's a hardlink, we unlink it instead of truncating the shared inode.
			// 3. If it's a directory, we recursively remove it so we can write the file.
			if _, err := root.Lstat(name); err == nil {
				if err := root.RemoveAll(name); err != nil {
					return fmt.Errorf("while replacing existing path at %q before regular file: %w", name, err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("while checking existing path at %q before regular file: %w", name, err)
			}

			// Stream directly from tarReader to target file to avoid buffering in memory.
			outFile, err := root.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, mode)
			if err != nil {
				return fmt.Errorf("while creating file %q: %w", name, err)
			}

			_, err = io.Copy(outFile, tarReader)
			closeErr := outFile.Close()

			if err != nil {
				return fmt.Errorf("while writing contents of %q from tar stream: %w", name, err)
			}
			if closeErr != nil {
				return fmt.Errorf("while closing file %q: %w", name, closeErr)
			}

		case tar.TypeDir:
			err := root.Mkdir(name, mode)
			if errors.Is(err, os.ErrExist) {
				// Ignore --- real images produced by ko seem to have directory entries placed multiple times?
			} else if err != nil {
				return fmt.Errorf("while creating directory=%q, mode=%v: %w", name, mode, err)
			}

		case tar.TypeSymlink:
			// OCI image layers may re-define the same path across layers (e.g.
			// an earlier layer creates /var/run as a directory and a later
			// layer re-declares it as a symlink to /run). Standard tar-extract
			// semantics are "later entry wins": replace any existing entry.
			if existing, err := root.Lstat(name); err == nil {
				// If it's already the same symlink, skip the unlink+symlink pair.
				if existing.Mode()&os.ModeSymlink != 0 {
					if cur, rerr := root.Readlink(name); rerr == nil && cur == hdr.Linkname {
						continue
					}
				}
				// Root.RemoveAll removes the symlink entry itself; it does NOT
				// traverse and remove the directory the symlink points to.
				// That's the desired semantic here — replace this path's
				// entry without touching whatever the prior symlink targeted.
				if err := root.RemoveAll(name); err != nil {
					return fmt.Errorf("while replacing existing path at %q before symlink: %w", name, err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("while checking existing path at %q before symlink: %w", name, err)
			}
			if err := root.Symlink(hdr.Linkname, name); err != nil {
				return fmt.Errorf("while creating symlink src=%q target=%q: %w", name, hdr.Linkname, err)
			}

		case tar.TypeLink:
			linkname, linkSkip, err := validateTarName(hdr.Linkname)
			if err != nil {
				return fmt.Errorf("invalid hardlink target for %q: %w", name, err)
			}
			if linkSkip {
				return fmt.Errorf("invalid hardlink target for %q: empty", name)
			}
			// Same "later entry wins" handling as TypeSymlink: replace existing entry.
			if _, err := root.Lstat(name); err == nil {
				if err := root.RemoveAll(name); err != nil {
					return fmt.Errorf("while replacing existing path at %q before hardlink: %w", name, err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("while checking existing path at %q before hardlink: %w", name, err)
			}
			if err := root.Link(linkname, name); err != nil {
				return fmt.Errorf("while creating hardlink src=%q target=%q: %w", name, linkname, err)
			}

		default:
			tfStr := string([]byte{hdr.Typeflag})
			slog.ErrorContext(ctx, "Unhandled tar entry typeflag", slog.String("typeflag", tfStr), slog.Any("hdr", hdr))
			return fmt.Errorf("unhandled tar entry typeflag %q", tfStr)
		}

	}

	return nil
}
