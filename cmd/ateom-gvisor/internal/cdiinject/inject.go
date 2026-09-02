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

// Package cdiinject applies CDI container edits to an actor's OCI bundle.
//
// This is the gVisor shape and does not generalize: it writes a bundle on disk,
// and it resolves device numbers by stat'ing the host, which only means anything
// for a sandbox on the host kernel. A micro-VM ships its OCI spec to the guest
// agent and needs the device passed through by VFIO first, so it would read the
// same spec (see internal/cdi) and apply it its own way.
package cdiinject

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/internal/cdi"
)

// Options tune what a merge is allowed to do. Everything vendor-specific lives
// here rather than in the merge itself.
type Options struct {
	// Devices names the CDI devices to apply.
	Devices []string

	// AllowedHooks are the createContainer hooks that may run, by their first
	// argument. An allowlist because the generator is usually the cluster's own
	// toolkit, so its version is not ours to choose and a newer one can emit
	// hooks nobody has reviewed against this sandbox's posture.
	AllowedHooks map[string]bool

	// HookBinary replaces the hook path the spec carries, so hooks run from a
	// binary the caller trusts rather than wherever the generator pointed.
	HookBinary string

	// LibraryDirs are prepended to the container's LD_LIBRARY_PATH.
	LibraryDirs []string

	// DropEnv are environment variables to remove from the container.
	DropEnv []string
}

// IntoBundle merges a CDI spec into the OCI config.json in bundleDir: device
// nodes with their numbers resolved from the host, mounts, env, and the
// allowlisted hooks.
//
// Injecting twice would double every entry, so a bundle that already carries
// injected device nodes is left alone.
func IntoBundle(ctx context.Context, bundleDir string, spec *cdi.Spec, opts Options) error {
	edits, err := spec.EditsFor(opts.Devices)
	if err != nil {
		return err
	}
	if len(edits.DeviceNodes) == 0 {
		return fmt.Errorf("CDI spec resolved no devices")
	}

	cfgPath := filepath.Join(bundleDir, "config.json")
	specData, err := os.ReadFile(cfgPath)
	if err != nil {
		return fmt.Errorf("reading %s: %w", cfgPath, err)
	}
	var ociSpec specs.Spec
	if err := json.Unmarshal(specData, &ociSpec); err != nil {
		return fmt.Errorf("parsing OCI spec: %w", err)
	}
	if hasInjectedDevice(ociSpec.Linux, edits.DeviceNodes) {
		slog.InfoContext(ctx, "Bundle already has CDI devices; skipping injection",
			slog.String("bundle", bundleDir))
		return nil
	}

	if ociSpec.Linux == nil {
		ociSpec.Linux = &specs.Linux{}
	}
	if ociSpec.Linux.Resources == nil {
		ociSpec.Linux.Resources = &specs.LinuxResources{}
	}
	for _, dn := range edits.DeviceNodes {
		major, minor, err := resolveDevNumbers(dn.Path, dn.Major, dn.Minor)
		if err != nil {
			return err
		}
		devType := dn.Type
		if devType == "" {
			devType = "c" // Generators omit type for char devices; runsc needs it.
		}
		ociSpec.Linux.Devices = append(ociSpec.Linux.Devices, specs.LinuxDevice{
			Path: dn.Path, Type: devType, Major: major, Minor: minor,
		})
		ociSpec.Linux.Resources.Devices = append(ociSpec.Linux.Resources.Devices, specs.LinuxDeviceCgroup{
			Allow: true, Type: devType, Major: &major, Minor: &minor, Access: "rwm",
		})
	}

	for _, m := range edits.Mounts {
		mType := m.Type
		if mType == "" {
			mType = "bind" // CDI omits type for its bind mounts; runsc's gofer needs it.
		}
		ociSpec.Mounts = append(ociSpec.Mounts, specs.Mount{
			Source: m.HostPath, Destination: m.ContainerPath, Type: mType, Options: m.Options,
		})
	}

	if ociSpec.Process != nil {
		ociSpec.Process.Env = append(ociSpec.Process.Env, edits.Env...)
		for _, key := range opts.DropEnv {
			ociSpec.Process.Env = dropEnvVar(ociSpec.Process.Env, key)
		}
		ociSpec.Process.Env = prependLibraryPath(ociSpec.Process.Env, opts.LibraryDirs)
	}

	if ociSpec.Hooks == nil {
		ociSpec.Hooks = &specs.Hooks{}
	}
	for _, h := range edits.Hooks {
		if h.HookName != "createContainer" || len(h.Args) < 2 {
			continue
		}
		if !opts.AllowedHooks[h.Args[1]] {
			slog.InfoContext(ctx, "Skipping CDI hook outside the allowlist",
				slog.String("hook", h.Args[1]))
			continue
		}
		binary := h.Path
		if opts.HookBinary != "" {
			binary = opts.HookBinary
		}
		ociSpec.Hooks.CreateContainer = append(ociSpec.Hooks.CreateContainer, specs.Hook{
			Path: binary,
			Args: append([]string{binary}, h.Args[1:]...),
			Env:  h.Env,
		})
	}

	rootfs := "rootfs"
	if ociSpec.Root != nil && ociSpec.Root.Path != "" {
		rootfs = ociSpec.Root.Path
	}
	if !filepath.IsAbs(rootfs) {
		rootfs = filepath.Join(bundleDir, rootfs)
	}
	if err := StageSonameSymlinks(ctx, rootfs, ociSpec.Mounts); err != nil {
		return fmt.Errorf("staging SONAME symlinks: %w", err)
	}

	out, err := json.Marshal(&ociSpec)
	if err != nil {
		return fmt.Errorf("serializing OCI spec: %w", err)
	}
	return os.WriteFile(cfgPath, out, 0o644)
}

// hasInjectedDevice reports whether the OCI spec already carries any of the
// device nodes about to be injected, which is how an already-injected bundle is
// recognized.
func hasInjectedDevice(l *specs.Linux, nodes []cdi.Dev) bool {
	if l == nil {
		return false
	}
	for _, d := range l.Devices {
		for _, n := range nodes {
			if d.Path == n.Path {
				return true
			}
		}
	}
	return false
}

// resolveDevNumbers fills a device node's major/minor from the host when the CDI
// spec omitted them. CDI delegates that to the OCI runtime, which stats the
// host; the merge happens here instead, so the stat happens here too. Otherwise
// the container gets bogus 0,0 char devices that reach no driver.
func resolveDevNumbers(path string, major, minor int64) (int64, int64, error) {
	if major != 0 {
		return major, minor, nil
	}
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return 0, 0, fmt.Errorf("stat device %s: %w", path, err)
	}
	rdev := uint64(st.Rdev)
	return int64(unix.Major(rdev)), int64(unix.Minor(rdev)), nil
}

// dropEnvVar returns env with every "KEY=..." entry for the given key removed.
func dropEnvVar(env []string, key string) []string {
	prefix := key + "="
	out := make([]string, 0, len(env))
	for _, e := range env {
		if !strings.HasPrefix(e, prefix) {
			out = append(out, e)
		}
	}
	return out
}

// prependLibraryPath puts dirs at the front of LD_LIBRARY_PATH, keeping whatever
// the image already set. This is the fallback for not running the CDI cache-
// updating hook: without it an image that sets no LD_LIBRARY_PATH cannot find
// the injected driver libraries, and the runtime reports zero devices even
// though they are all present. Directories already on the path are left alone.
//
// It is weaker than a loader cache -- LD_LIBRARY_PATH is inherited by children
// and outranks an executable's own DT_RUNPATH -- so callers should pass only the
// directory holding the libraries, not every directory the mounts touch.
func prependLibraryPath(env, dirs []string) []string {
	if len(dirs) == 0 {
		return env
	}
	existing := ""
	for _, e := range env {
		if v, ok := strings.CutPrefix(e, "LD_LIBRARY_PATH="); ok {
			existing = v // OCI semantics: a later entry wins.
		}
	}
	have := map[string]bool{}
	for _, d := range strings.Split(existing, ":") {
		have[d] = true
	}
	var add []string
	for _, d := range dirs {
		if !have[d] {
			add = append(add, d)
		}
	}
	if len(add) == 0 {
		return env
	}
	val := strings.Join(add, ":")
	if existing != "" {
		val += ":" + existing
	}
	return append(dropEnvVar(env, "LD_LIBRARY_PATH"), "LD_LIBRARY_PATH="+val)
}
