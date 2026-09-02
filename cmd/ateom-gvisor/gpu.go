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
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/agent-substrate/substrate/cmd/ateom-gvisor/internal/cdiinject"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/cdi"
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

// cdiAllDevice is the CDI device carrying every GPU assigned to the pod.
// nvidia-ctk repeats the same nodes across per-index and per-UUID devices, so
// only one of them may be applied.
const cdiAllDevice = "all"

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

// injectGPUIntoBundle merges the CDI spec generated in cdiSpecDir into the actor's
// OCI bundle. It does NOT run the CDI cache-updating hook: that hook's ldconfig
// needs a private /proc, which the pod's masked /proc rejects, so the SONAME
// symlinks it would create are staged into the rootfs instead. That is what lets
// a GPU worker keep the plain unprivileged posture.
func injectGPUIntoBundle(ctx context.Context, bundleDir, cdiSpecDir string) error {
	data, err := os.ReadFile(filepath.Join(cdiSpecDir, "nvidia.json"))
	if err != nil {
		return fmt.Errorf("reading CDI spec: %w", err)
	}
	spec, err := cdi.Parse(data)
	if err != nil {
		return err
	}
	return cdiinject.IntoBundle(ctx, bundleDir, spec, cdiinject.Options{
		Devices:      []string{cdiAllDevice},
		AllowedHooks: enabledCDIHooks,
		HookBinary:   toolkitBinary("nvidia-cdi-hook"),
		LibraryDirs:  []string{driverLibDir},
		// runsc's nvproxy runs nvidia-container-cli when it sees
		// NVIDIA_VISIBLE_DEVICES (independent of --nvproxy); the GPU is set up via
		// CDI here, so strip it.
		DropEnv: []string{"NVIDIA_VISIBLE_DEVICES"},
	})
}
