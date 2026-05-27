//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

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

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/memorypullcache"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/opencontainers/runtime-spec/specs-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sys/unix"
)

func prepareOCIDirectory(ctx context.Context, pullCache *memorypullcache.MemoryPullCache, actorTemplateNamespace, actorTemplateName, actorID, containerName, ref string, args []string, env []string, annotations map[string]string, netns string, gpu *ateletpb.GpuSpec) error {
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

	envVars := []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	}
	envVars = append(envVars, env...)

	ociSpec := &specs.Spec{
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
		Mounts: []specs.Mount{
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
		},
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

	if gpu != nil {
		if err := addGPUToOCISpec(ociSpec); err != nil {
			return fmt.Errorf("while adding GPU passthrough to OCI spec: %w", err)
		}
	}
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

func untar(ctx context.Context, tarData io.Reader, rootPath string) error {
	tracer := otel.Tracer("ateom-gvisor")
	ctx, span := tracer.Start(ctx, "untar")
	defer span.End()

	tarReader := tar.NewReader(tarData)
	for {
		hdr, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("in tarReader.Next: %w", err)
		}

		switch hdr.Typeflag {
		case tar.TypeReg: // Regular file
			target := filepath.Join(rootPath, hdr.Name)

			// Stream directly from tarReader to target file to avoid buffering in memory.
			outFile, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR|os.O_TRUNC, hdr.FileInfo().Mode())
			if err != nil {
				return fmt.Errorf("while creating file %q: %w", target, err)
			}

			// TODO: Use a constrained fs so that paths containing `..` cannot
			// end up outside the root, and symlinks / hardlinks cannot point
			// outside the root.
			_, err = io.Copy(outFile, tarReader)
			closeErr := outFile.Close()

			if err != nil {
				return fmt.Errorf("while writing contents of %q from tar stream: %w", hdr.Name, err)
			}
			if closeErr != nil {
				return fmt.Errorf("while closing file %q: %w", target, closeErr)
			}

		case tar.TypeDir:
			if hdr.Name == "." {
				// Huh?  I guess this is for setting mode, etc on the root
				// folder.  Ignore for now.
				continue
			}
			target := filepath.Join(rootPath, hdr.Name)
			err := os.Mkdir(target, hdr.FileInfo().Mode())
			if errors.Is(err, os.ErrExist) {
				// Ignore --- real images produced by ko seem to have directory entries placed multiple times?
			} else if err != nil {
				return fmt.Errorf("while creating directory=%q, mode=%v: %w", target, hdr.FileInfo().Mode(), err)
			}

		case tar.TypeSymlink:
			// TODO: Make sure no tricky people are trying to create a symlink pointing out of the rootfs.
			source := filepath.Join(rootPath, hdr.Name)
			// OCI image layers may re-define the same path across layers (e.g.
			// an earlier layer creates /var/run as a directory and a later
			// layer re-declares it as a symlink to /run). Standard tar-extract
			// semantics are "later entry wins": replace any existing entry.
			if existing, err := os.Lstat(source); err == nil {
				// If it's already the same symlink, skip the unlink+symlink pair.
				if existing.Mode()&os.ModeSymlink != 0 {
					if cur, rerr := os.Readlink(source); rerr == nil && cur == hdr.Linkname {
						continue
					}
				}
				// os.RemoveAll removes the symlink entry itself; it does NOT
				// traverse and remove the directory the symlink points to.
				// That's the desired semantic here — replace this path's
				// entry without touching whatever the prior symlink targeted.
				if err := os.RemoveAll(source); err != nil {
					return fmt.Errorf("while replacing existing path at %q before symlink: %w", source, err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("while checking existing path at %q before symlink: %w", source, err)
			}
			if err := os.Symlink(hdr.Linkname, source); err != nil {
				return fmt.Errorf("while creating symlink src=%q target=%q: %w", source, hdr.Linkname, err)
			}

		case tar.TypeLink:
			// TODO: Make sure no tricky people are trying to create a hardlink pointing out of the rootfs.
			source := filepath.Join(rootPath, hdr.Linkname)
			target := filepath.Join(rootPath, hdr.Name)
			// Same "later entry wins" handling as TypeSymlink: replace existing entry.
			if _, err := os.Lstat(target); err == nil {
				if err := os.RemoveAll(target); err != nil {
					return fmt.Errorf("while replacing existing path at %q before hardlink: %w", target, err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("while checking existing path at %q before hardlink: %w", target, err)
			}
			if err := os.Link(source, target); err != nil {
				return fmt.Errorf("while creating hardlink src=%q target=%q: %w", source, target, err)
			}

		default:
			tfStr := string([]byte{hdr.Typeflag})
			slog.ErrorContext(ctx, "Unhandled tar entry typeflag", slog.String("typeflag", tfStr), slog.Any("hdr", hdr))
			return fmt.Errorf("unhandled tar entry typeflag %q", tfStr)
		}

	}

	return nil
}

var gpuDevicePaths = []string{
	"/dev/nvidiactl",
	"/dev/nvidia-uvm",
	"/dev/nvidia-uvm-tools",
	"/dev/nvidia-modeset",
}

func addGPUToOCISpec(spec *specs.Spec) error {
	paths := append([]string{}, gpuDevicePaths...)
	entries, err := os.ReadDir("/dev")
	if err != nil {
		return fmt.Errorf("reading /dev: %w", err)
	}
	for _, e := range entries {
		n := e.Name()
		if len(n) > 6 && n[:6] == "nvidia" && n[6] >= '0' && n[6] <= '9' {
			paths = append(paths, "/dev/"+n)
		}
	}
	for _, p := range paths {
		var st unix.Stat_t
		if err := unix.Stat(p, &st); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("stat %s: %w", p, err)
		}
		major := int64(unix.Major(uint64(st.Rdev))) //nolint:gosec
		minor := int64(unix.Minor(uint64(st.Rdev))) //nolint:gosec
		mode := os.FileMode(st.Mode & 0o777)        //nolint:gosec
		uid := st.Uid
		gid := st.Gid
		spec.Linux.Devices = append(spec.Linux.Devices, specs.LinuxDevice{
			Path: p, Type: "c", Major: major, Minor: minor,
			FileMode: &mode, UID: &uid, GID: &gid,
		})
	}
	for _, src := range []string{
		"/usr/local/bin/cuda-checkpoint",
		"/usr/local/bin/cuda-checkpoint-wrapper.sh",
	} {
		if _, err := os.Stat(src); err != nil {
			continue
		}
		spec.Mounts = append(spec.Mounts, specs.Mount{
			Destination: src, Type: "bind", Source: src,
			Options: []string{"ro", "bind"},
		})
	}
	return nil
}
