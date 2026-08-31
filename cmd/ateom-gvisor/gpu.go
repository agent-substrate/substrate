//go:build linux

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
	"cmp"
	"context"
	"debug/elf"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

// toolkitDir is where the host's NVIDIA container toolkit (nvidia-ctk,
// nvidia-cdi-hook) is mounted into the worker pod — read-only from the node, so
// the binaries match whatever toolkit/driver the cluster installed. A var (not
// const) so tests can point it at a fixture directory.
var toolkitDir = "/opt/nvidia-toolkit"

// gpuDeviceGlob matches the per-GPU device nodes. The device plugin can assign any
// indices (a worker sharing a multi-GPU node may get /dev/nvidia2,3 with no
// /dev/nvidia0), so detection must not assume index 0. The [0-9] excludes the control
// nodes (/dev/nvidiactl, /dev/nvidia-uvm). A var so tests can point it at a fixture.
var gpuDeviceGlob = "/dev/nvidia[0-9]*"

// nvidiaDriverRoot is where the GPU device plugin mounts the driver into the pod.
// GKE and gpu-operator both use /usr/local/nvidia, but that is a convention rather
// than a contract, so it is overridable via ATE_NVIDIA_DRIVER_ROOT (propagated onto
// GPU worker pods by the controller).
var nvidiaDriverRoot = cmp.Or(os.Getenv("ATE_NVIDIA_DRIVER_ROOT"), "/usr/local/nvidia")

// Both directories are load-bearing for CDI generation, not just for completeness:
// without the library path nvidia-ctk cannot load libnvidia-ml.so.1 to enumerate the
// GPUs and generation fails outright, and without the bin path (which it discovers
// via PATH) the generated spec carries libraries but no nvidia-smi.
var (
	driverLibDir = filepath.Join(nvidiaDriverRoot, "lib64")
	driverBinDir = filepath.Join(nvidiaDriverRoot, "bin")
)

const cdiOutputDir = "/run/ate-cdi"

// enabledCDIHooks is the set of CDI createContainer hooks the actor runs. It is an
// allowlist because the toolkit is mounted from the host, so its version is the
// cluster's choice: a newer one can emit hooks that have never been reviewed
// against this worker's unprivileged posture, and those must not run by default.
// update-ldcache is absent deliberately — its ldconfig needs a private /proc
// mount, which the pod's masked /proc rejects, so the SONAME symlinks it would
// create are staged directly into the rootfs instead.
var enabledCDIHooks = map[string]bool{
	"create-symlinks":    true,
	"enable-cuda-compat": true,
}

// toolkitBinary resolves a toolkit command to an executable path, preferring the
// unwrapped ".real" binary the NVIDIA toolkit ships: the plain name is often a
// /bin/sh wrapper. The ateom image is glibc-based (debian), so the glibc-dynamic
// toolkit binaries run directly — no ld-linux loader shim is needed.
func toolkitBinary(name string) string {
	if real := filepath.Join(toolkitDir, name+".real"); fileExists(real) {
		return real
	}
	return filepath.Join(toolkitDir, name)
}

func fileExists(p string) bool { _, err := os.Stat(p); return err == nil }

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

// gpuPresent reports whether any GPU is assigned to this worker pod, matching any
// device index (not just /dev/nvidia0 — the device plugin can assign 2,3 etc.).
func gpuPresent() bool {
	matches, _ := filepath.Glob(gpuDeviceGlob)
	return len(matches) > 0
}

var (
	generateMu   sync.Mutex
	cdiGenerated bool
)

// generateCDISpec runs nvidia-ctk (from the host toolkit mounted into the pod) to
// produce a CDI spec scoped to this pod's assigned GPU. The glibc-based ateom image
// runs the glibc-dynamic toolkit binary directly. Runs through reaper like every
// other synchronous subprocess here.
func generateCDISpec(ctx context.Context, outDir string) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("creating CDI output dir %s: %w", outDir, err)
	}
	// No --nvidia-cdi-hook-path: we discard the CDI hooks (staging SONAME symlinks
	// ourselves), so the hook paths nvidia-ctk writes into the spec are never used.
	cmd := exec.CommandContext(ctx, toolkitBinary("nvidia-ctk"),
		"cdi", "generate",
		"--format=json",
		"--library-search-path="+driverLibDir,
		"--output="+filepath.Join(outDir, "nvidia.json"),
	)
	// nvidia-ctk finds the driver binaries (nvidia-smi, ...) via PATH.
	cmd.Env = append(os.Environ(), "PATH="+driverBinDir+":"+os.Getenv("PATH"))
	if out, err := reaper.CombinedOutput(cmd); err != nil {
		return fmt.Errorf("nvidia-ctk cdi generate failed: %w: %s", err, out)
	}
	return nil
}

// ensureCDISpec generates the per-pod CDI spec once, on the first actor. A failure is
// not memoized: a transient error (e.g. the toolkit mount not yet ready) is retried on
// the next actor rather than bricking GPU for the pod's lifetime.
func ensureCDISpec(ctx context.Context) error {
	generateMu.Lock()
	defer generateMu.Unlock()
	if cdiGenerated {
		return nil
	}
	if err := generateCDISpec(ctx, cdiOutputDir); err != nil {
		return err
	}
	cdiGenerated = true
	return nil
}

// maybeInjectGPU is a no-op unless the worker pod has a GPU. When it does, it
// generates the per-pod CDI spec once and injects the GPU into the actor
// container's OCI bundle before runsc create.
func maybeInjectGPU(ctx context.Context, actorUID, containerName string) error {
	if !gpuPresent() {
		return nil
	}
	slog.InfoContext(ctx, "Injecting GPU into actor container", slog.String("container", containerName))
	if err := ensureCDISpec(ctx); err != nil {
		return err
	}
	bundleDir := ateompath.OCIBundlePath(actorUID, containerName)
	if err := injectGPUIntoBundle(ctx, bundleDir, cdiOutputDir); err != nil {
		return fmt.Errorf("injecting GPU into %q bundle: %w", containerName, err)
	}
	return nil
}

// cdiSpec is the minimal shape of the JSON CDI spec (nvidia-ctk --format=json) that
// we consume: the device nodes, driver-library mounts, and env.
type cdiSpec struct {
	Devices []struct {
		Name           string   `json:"name"`
		ContainerEdits cdiEdits `json:"containerEdits"`
	} `json:"devices"`
	ContainerEdits cdiEdits `json:"containerEdits"`
}

// cdiAllDevice is the CDI device that carries every GPU assigned to the pod.
// nvidia-ctk also emits per-index ("0") and per-UUID devices that repeat the same
// nodes, so we apply only this one (plus the spec-level edits) to avoid injecting
// each device node several times.
const cdiAllDevice = "all"

type cdiEdits struct {
	Env         []string   `json:"env,omitempty"`
	DeviceNodes []cdiDev   `json:"deviceNodes,omitempty"`
	Mounts      []cdiMount `json:"mounts,omitempty"`
	Hooks       []cdiHook  `json:"hooks,omitempty"`
}

type cdiDev struct {
	Path  string `json:"path"`
	Type  string `json:"type,omitempty"`
	Major int64  `json:"major,omitempty"`
	Minor int64  `json:"minor,omitempty"`
}

type cdiMount struct {
	HostPath      string   `json:"hostPath"`
	ContainerPath string   `json:"containerPath"`
	Type          string   `json:"type,omitempty"`
	Options       []string `json:"options,omitempty"`
}

type cdiHook struct {
	HookName string   `json:"hookName"`
	Path     string   `json:"path"`
	Args     []string `json:"args,omitempty"`
	Env      []string `json:"env,omitempty"`
}

// resolveDevNumbers fills a device node's major/minor from the host when the CDI spec
// omitted them. nvidia-ctk emits deviceNodes carrying only a path (CDI delegates
// number resolution to the OCI runtime, which stats the host); we merge into runsc's
// spec ourselves, so we stat here too, otherwise the actor gets bogus 0,0 char devices
// and NVML can't reach the driver. An nvidia device major is never 0.
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

// injectGPUIntoBundle merges the CDI spec generated in cdiSpecDir into the actor's OCI
// config.json in bundleDir: device nodes (major/minor resolved from the host), the
// driver-library mounts, and env. It does NOT run the CDI hooks; instead it stages the
// SONAME symlinks (libcuda.so.1 -> libcuda.so.580.x) into the actor rootfs, which is
// what lets the GPU worker keep the plain unprivileged posture (no user namespace, no
// unmasked /proc). The CDI spec is plain JSON, so no CDI library is needed.
func injectGPUIntoBundle(ctx context.Context, bundleDir, cdiSpecDir string) error {
	cdiData, err := os.ReadFile(filepath.Join(cdiSpecDir, "nvidia.json"))
	if err != nil {
		return fmt.Errorf("reading CDI spec: %w", err)
	}
	var cdi cdiSpec
	if err := json.Unmarshal(cdiData, &cdi); err != nil {
		return fmt.Errorf("parsing CDI spec: %w", err)
	}
	// Spec-level edits (driver libs, common device nodes, env) plus only the "all"
	// device's edits — applying every device would inject each GPU node several times
	// (nvidia-ctk repeats nodes across its per-index, per-UUID, and "all" devices).
	edits := cdi.ContainerEdits
	var foundAll bool
	for _, d := range cdi.Devices {
		if d.Name != cdiAllDevice {
			continue
		}
		foundAll = true
		edits.Env = append(edits.Env, d.ContainerEdits.Env...)
		edits.DeviceNodes = append(edits.DeviceNodes, d.ContainerEdits.DeviceNodes...)
		edits.Mounts = append(edits.Mounts, d.ContainerEdits.Mounts...)
		edits.Hooks = append(edits.Hooks, d.ContainerEdits.Hooks...)
	}
	if !foundAll {
		return fmt.Errorf("CDI spec in %s has no %q device", cdiSpecDir, cdiAllDevice)
	}
	if len(edits.DeviceNodes) == 0 {
		return fmt.Errorf("CDI spec in %s resolved no devices", cdiSpecDir)
	}

	cfgPath := filepath.Join(bundleDir, "config.json")
	specData, err := os.ReadFile(cfgPath)
	if err != nil {
		return fmt.Errorf("reading %s: %w", cfgPath, err)
	}
	var spec specs.Spec
	if err := json.Unmarshal(specData, &spec); err != nil {
		return fmt.Errorf("parsing OCI spec: %w", err)
	}

	// The edits below append to the bundle's config.json, so injecting twice would
	// double every device, mount, env entry and hook. atelet re-unpacks the bundle
	// before each Run and Restore, so this normally runs once per bundle — but that
	// invariant lives in another component, so enforce it here rather than rely on it.
	if hasGPUDevice(spec.Linux) {
		slog.InfoContext(ctx, "Bundle already has GPU devices; skipping injection",
			slog.String("bundle", bundleDir))
		return nil
	}

	if spec.Linux == nil {
		spec.Linux = &specs.Linux{}
	}
	if spec.Linux.Resources == nil {
		spec.Linux.Resources = &specs.LinuxResources{}
	}
	for _, dn := range edits.DeviceNodes {
		major, minor, err := resolveDevNumbers(dn.Path, dn.Major, dn.Minor)
		if err != nil {
			return err
		}
		devType := dn.Type
		if devType == "" {
			devType = "c" // nvidia-ctk omits type for char devices; runsc needs it.
		}
		spec.Linux.Devices = append(spec.Linux.Devices, specs.LinuxDevice{
			Path: dn.Path, Type: devType, Major: major, Minor: minor,
		})
		spec.Linux.Resources.Devices = append(spec.Linux.Resources.Devices, specs.LinuxDeviceCgroup{
			Allow: true, Type: devType, Major: &major, Minor: &minor, Access: "rwm",
		})
	}

	for _, m := range edits.Mounts {
		mType := m.Type
		if mType == "" {
			mType = "bind" // CDI omits type for its bind mounts; runsc's gofer needs it.
		}
		spec.Mounts = append(spec.Mounts, specs.Mount{
			Source: m.HostPath, Destination: m.ContainerPath, Type: mType, Options: m.Options,
		})
	}

	if spec.Process != nil {
		spec.Process.Env = append(spec.Process.Env, edits.Env...)
		// runsc's nvproxy runs nvidia-container-cli when it sees NVIDIA_VISIBLE_DEVICES
		// (independent of --nvproxy); we set up the GPU via CDI, so strip it.
		spec.Process.Env = dropEnvVar(spec.Process.Env, "NVIDIA_VISIBLE_DEVICES")
		spec.Process.Env = prependLibraryPath(spec.Process.Env, []string{driverLibDir})
	}

	// Run the allowlisted CDI createContainer hooks (see enabledCDIHooks) from the
	// mounted host toolkit. Anything else the toolkit emits is skipped and logged,
	// so a toolkit upgrade that adds a hook is visible rather than silent.
	if spec.Hooks == nil {
		spec.Hooks = &specs.Hooks{}
	}
	for _, h := range edits.Hooks {
		if h.HookName != "createContainer" || len(h.Args) < 2 {
			continue
		}
		if !enabledCDIHooks[h.Args[1]] {
			slog.InfoContext(ctx, "Skipping CDI hook outside the allowlist",
				slog.String("hook", h.Args[1]))
			continue
		}
		binary := toolkitBinary("nvidia-cdi-hook")
		spec.Hooks.CreateContainer = append(spec.Hooks.CreateContainer, specs.Hook{
			Path: binary,
			Args: append([]string{binary}, h.Args[1:]...),
			Env:  h.Env,
		})
	}

	// Create the driver SONAME symlinks in the container's rootfs (spec.Root.Path,
	// relative to the bundle).
	rootfs := "rootfs"
	if spec.Root != nil && spec.Root.Path != "" {
		rootfs = spec.Root.Path
	}
	if !filepath.IsAbs(rootfs) {
		rootfs = filepath.Join(bundleDir, rootfs)
	}
	if err := stageSonameSymlinks(ctx, rootfs, spec.Mounts); err != nil {
		return fmt.Errorf("staging SONAME symlinks: %w", err)
	}

	out, err := json.Marshal(&spec)
	if err != nil {
		return fmt.Errorf("serializing OCI spec: %w", err)
	}
	return os.WriteFile(cfgPath, out, 0o644)
}

// hasGPUDevice reports whether the OCI spec already carries injected NVIDIA device
// nodes, which is how an already-injected bundle is recognized.
func hasGPUDevice(l *specs.Linux) bool {
	if l == nil {
		return false
	}
	for _, d := range l.Devices {
		if strings.HasPrefix(d.Path, "/dev/nvidia") {
			return true
		}
	}
	return false
}

// prependLibraryPath puts the driver library directories at the front of
// LD_LIBRARY_PATH, keeping whatever the image or ActorTemplate already set. The
// actor needs this to find libcuda.so.1: the update-ldcache hook that would
// normally add the directory to the loader cache is not run (see enabledCDIHooks),
// so an image that does not set LD_LIBRARY_PATH itself gets a CUDA runtime that
// reports zero devices even though the GPU is fully injected. Directories already
// on the path are left alone, so an NVIDIA base image keeps its own ordering.
//
// This is weaker than the ldcache the hook would have written — LD_LIBRARY_PATH is
// inherited by child processes and takes precedence over an executable's own
// DT_RUNPATH — so it carries only driverLibDir rather than every directory the CDI
// mounts touch, which would also sweep in the driver's X server modules.
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

// stageSonameSymlinks writes each driver library's SONAME symlink (e.g.
// libcuda.so.1 -> libcuda.so.580.65.06) into rootfs, so programs that link against
// the SONAME resolve it. For each CDI library mount it reads the library's ELF
// DT_SONAME and, when that differs from the mounted filename, writes a relative
// symlink alongside the mount destination; the real library file arrives at runtime
// via the CDI bind-mount into the same directory.
//
// A library whose SONAME cannot be read is skipped rather than failing the actor, so
// one odd file cannot cost us the whole GPU. That is logged: the missing symlink
// surfaces much later, as a loader error in the workload, with nothing pointing back
// here.
// Every path component under rootfs comes from the actor image, so the writes go
// through os.Root: an image that ships a driver-mount's parent directory as a
// symlink out of the rootfs would otherwise have the kernel resolve it in ateom's
// mount namespace, where the shared image cache and other actors' bundles are
// mounted. Same treatment as createExtraDirs in internal/imagecache.
func stageSonameSymlinks(ctx context.Context, rootfs string, mounts []specs.Mount) error {
	root, err := os.OpenRoot(rootfs)
	if err != nil {
		return fmt.Errorf("while opening rootfs %q: %w", rootfs, err)
	}
	defer root.Close()

	for _, m := range mounts {
		base := filepath.Base(m.Destination)
		// Only shared libraries (…/lib…/foo.so.<version>).
		if !strings.Contains(base, ".so.") || m.Source == "" {
			continue
		}
		soname, err := elfSonameFn(m.Source)
		if err != nil {
			slog.WarnContext(ctx, "Skipping SONAME symlink for driver library",
				slog.String("library", m.Source), slog.Any("err", err))
			continue
		}
		if soname == "" || soname == base {
			continue
		}
		// The SONAME is read out of the library and joined into a path, so require a
		// bare filename rather than trusting the file's contents.
		if soname != filepath.Base(soname) || !filepath.IsLocal(soname) {
			slog.WarnContext(ctx, "Skipping driver library whose SONAME is not a filename",
				slog.String("library", m.Source), slog.String("soname", soname))
			continue
		}
		dir := strings.TrimPrefix(filepath.Dir(m.Destination), "/")
		if dir != "" && !filepath.IsLocal(dir) {
			return fmt.Errorf("driver mount %q escapes the rootfs", m.Destination)
		}
		if err := root.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
		link := filepath.Join(dir, soname)
		_ = root.Remove(link)
		if err := root.Symlink(base, link); err != nil {
			return fmt.Errorf("symlink %s -> %s: %w", link, base, err)
		}
	}
	return nil
}

// elfSonameFn is elfSoname, as a var so tests can supply a SONAME without needing a
// real ELF file on disk.
var elfSonameFn = elfSoname

// elfSoname returns a shared library's DT_SONAME. It returns "" with a nil error for
// a library that simply carries no DT_SONAME entry, and an error when the file could
// not be opened or parsed as ELF.
func elfSoname(path string) (string, error) {
	f, err := elf.Open(path)
	if err != nil {
		return "", fmt.Errorf("opening ELF %s: %w", path, err)
	}
	defer f.Close()
	names, err := f.DynString(elf.DT_SONAME)
	if err != nil {
		return "", fmt.Errorf("reading DT_SONAME from %s: %w", path, err)
	}
	if len(names) == 0 {
		return "", nil
	}
	return names[0], nil
}
