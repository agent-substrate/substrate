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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	specs "github.com/opencontainers/runtime-spec/specs-go"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

type runsc struct {
	path     string
	actorUID string
}

// ensureContainerCgroupsPath sets the OCI spec's cgroupsPath so runsc creates a
// per-container cgroup leaf under the worker pod's own cgroup (see
// setupCgroupDelegation). atelet emits a runtime-agnostic spec with no
// cgroupsPath; the gVisor ateom fills in its own convention here, mirroring how
// the micro-VM ateom assigns /ateomchv/<id> in ensureKataCompatibleSpec. The
// path is colon-free (so runsc uses the cgroupfs driver, not systemd) and
// absolute, so it resolves under the pod scope in the worker's private cgroup
// namespace.
func (r *runsc) ensureContainerCgroupsPath(containerName string) error {
	specPath := filepath.Join(ateompath.OCIBundlePath(r.actorUID, containerName), "config.json")
	b, err := os.ReadFile(specPath)
	if err != nil {
		return fmt.Errorf("reading %q: %w", specPath, err)
	}
	var spec specs.Spec
	if err := json.Unmarshal(b, &spec); err != nil {
		return fmt.Errorf("parsing %q: %w", specPath, err)
	}
	if spec.Linux == nil {
		spec.Linux = &specs.Linux{}
	}
	if spec.Linux.CgroupsPath != "" {
		return nil
	}
	spec.Linux.CgroupsPath = "/" + containerName
	out, err := json.MarshalIndent(&spec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling %q: %w", specPath, err)
	}
	if err := os.WriteFile(specPath, out, 0o600); err != nil {
		return fmt.Errorf("writing %q: %w", specPath, err)
	}
	return nil
}

func (r *runsc) cmdCreate(ctx context.Context, out io.Writer, containerName string, additionalArgs []string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to run runsc create", slog.String("container", containerName))

	if err := r.ensureContainerCgroupsPath(containerName); err != nil {
		return fmt.Errorf("while setting cgroups path for %q: %w", containerName, err)
	}

	args := []string{
		"-log-format", "json",
		"--alsologtostderr",
		// "-debug",
		// "-debug-log", ateompath.RunscDebugLogDir(r.actorUID, containerName) + "/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-root", ateompath.RunSCStateDir(r.actorUID),
		"create",
		"-bundle", ateompath.OCIBundlePath(r.actorUID, containerName),
		"-pid-file", ateompath.PIDFilePath(r.actorUID, containerName),
	}

	args = append(args, additionalArgs...)
	args = append(args, containerName) // Name of the container
	cmd := exec.CommandContext(
		ctx,
		r.path,
		args...,
	)
	cmd.Stdout = out
	cmd.Stderr = out

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc create`: %w", err)
	}

	return nil
}

// allowConnectedOnSaveFlag lets runsc checkpoint a sandbox whose network
// connections are still established, so an actor keeps them across
// suspend/resume.
//
// It was introduced in runsc release-20260622.0. Builds older than that reject
// it on `runsc start` with "flag provided but not defined", which fails every
// actor start, so it is probed for below.
//
// There is no equivalent on those older builds. They have only
// -net-disconnect-ok, which defaults to true and *drops* open connections on
// save, and neither -allow-live-tcp-migration nor a working
// -save-restore-netstack (deprecated and inert on current builds, and about
// netstack save/restore generally rather than connection preservation). So
// dropping the flag is not a fallback path: it silently gives up connection
// preservation, which is why the probe below warns loudly when it has to.
const allowConnectedOnSaveFlag = "allow-connected-on-save"

// minRunscVersionForAllowConnectedOnSave is the first runsc release that
// defines allowConnectedOnSaveFlag. Named here for the operator-facing warning;
// the probe reads the binary rather than trusting a version string.
const minRunscVersionForAllowConnectedOnSave = "release-20260622.0"

// runscFlagSupport memoizes capability probes keyed by runsc binary path.
// Probing shells out, and cmdStart runs on every container start.
var runscFlagSupport sync.Map // map[string]bool

// supportsAllowConnectedOnSave reports whether the runsc binary at path defines
// -allow-connected-on-save, probing it once per path so that a build which does
// not define it starts actors instead of failing them.
//
// A false result is a degraded mode, not a supported configuration: actors will
// lose established connections on suspend. It is reported at Error once per
// binary rather than swallowed, and upgrading runsc is the actual fix.
func supportsAllowConnectedOnSave(ctx context.Context, path string) bool {
	if v, ok := runscFlagSupport.Load(path); ok {
		return v.(bool)
	}
	supported := probeAllowConnectedOnSave(ctx, path)
	if !supported {
		slog.ErrorContext(ctx, "runsc does not support -"+allowConnectedOnSaveFlag+
			"; actors on this build will lose established network connections across suspend/resume. Upgrade runsc to "+
			minRunscVersionForAllowConnectedOnSave+" or later.",
			slog.String("runsc", path),
			slog.String("minVersion", minRunscVersionForAllowConnectedOnSave))
	}
	runscFlagSupport.Store(path, supported)
	return supported
}

// probeAllowConnectedOnSave shells out to `runsc flags` and looks for the flag
// in the listing. A probe that cannot run at all assumes the flag is present:
// that reproduces the pre-probe behavior, so a build that does define the flag
// keeps it, and one that does not fails `runsc start` with runsc's own error
// rather than being silently downgraded on the strength of a failed probe.
//
// It must be `runsc flags`, not `runsc help start`: -allow-connected-on-save is
// a top-level flag, and the per-subcommand usage that `help start` prints lists
// only -h and -help even on builds that define it. Probing `help start` would
// therefore report "unsupported" on every build and silently stop passing the
// flag where it does work.
func probeAllowConnectedOnSave(ctx context.Context, path string) bool {
	cmd := exec.CommandContext(ctx, path, "flags")
	// Usage goes to stdout on some builds and stderr on others; capture both.
	var usage bytes.Buffer
	cmd.Stdout = &usage
	cmd.Stderr = &usage

	if err := cmd.Run(); err != nil {
		slog.WarnContext(ctx, "Could not probe runsc for -"+allowConnectedOnSaveFlag+"; assuming it is supported",
			slog.String("runsc", path),
			slog.Any("error", err))
		return true
	}

	supported := bytes.Contains(usage.Bytes(), []byte(allowConnectedOnSaveFlag))
	slog.InfoContext(ctx, "Probed runsc for -"+allowConnectedOnSaveFlag,
		slog.String("runsc", path),
		slog.Bool("supported", supported))
	return supported
}

func (r *runsc) cmdStart(ctx context.Context, out io.Writer, containerName string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to run runsc start", slog.String("container", containerName))

	args := []string{
		"-log-format", "json",
		"--alsologtostderr",
		// "-debug",
		// "-debug-log", ateompath.RunscDebugLogDir(r.actorUID, containerName)+"/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
	}
	if supportsAllowConnectedOnSave(ctx, r.path) {
		args = append(args, "-"+allowConnectedOnSaveFlag)
	}
	args = append(args,
		"-root", ateompath.RunSCStateDir(r.actorUID),
		"start",
		containerName, // Name of the container
	)
	cmd := exec.CommandContext(ctx, r.path, args...)
	cmd.Stdout = out
	cmd.Stderr = out

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc start`: %w", err)
	}

	return nil
}

func (r *runsc) cmdCheckpoint(ctx context.Context, containerName, checkpointPath string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to run runsc checkpoint", slog.String("container", containerName))

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		// "-debug",
		// "-debug-log", ateompath.RunscDebugLogDir(r.actorUID, containerName)+"/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-root", ateompath.RunSCStateDir(r.actorUID),
		"checkpoint",
		"-image-path", checkpointPath,
		containerName, // Name of the container
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc checkpoint`: %w", err)
	}
	return nil
}

func (r *runsc) cmdFsCheckpoint(ctx context.Context, containerName, checkpointPath string, durableDirMounts []string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to run runsc fscheckpoint", slog.String("container", containerName))

	args := []string{
		"-log-format", "json",
		"--alsologtostderr",
		// "-debug",
		// "-debug-log", ateompath.RunscDebugLogDir(r.actorUID, containerName)+"/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-root", ateompath.RunSCStateDir(r.actorUID),
		"fscheckpoint",
		"-image-path", checkpointPath,
	}
	for _, ddv := range durableDirMounts {
		args = append(args, "-path", ddv)
	}

	// name of the container must be the last parameter.
	args = append(args, containerName)

	cmd := exec.CommandContext(
		ctx,
		r.path,
		args...,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc fscheckpoint`: %w", err)
	}
	return nil
}

// We take a checkpoint only of the root container of the sandbox, but we need
// to call restore on each container, using the same checkpoint.
func (r *runsc) cmdRestore(ctx context.Context, out io.Writer, containerName, checkpointPath string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to run runsc restore", slog.String("container", containerName))

	if err := r.ensureContainerCgroupsPath(containerName); err != nil {
		return fmt.Errorf("while setting cgroups path for %q: %w", containerName, err)
	}

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		// "-debug",
		// "-debug-log", ateompath.RunscDebugLogDir(r.actorUID, containerName)+"/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-root", ateompath.RunSCStateDir(r.actorUID),
		"restore",
		"-bundle", ateompath.OCIBundlePath(r.actorUID, containerName),
		"-image-path", checkpointPath,
		"-pid-file", ateompath.PIDFilePath(r.actorUID, containerName),
		"-background",
		"-direct",
		"-detach",
		containerName,
	)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc restore`: %w", err)
	}
	return nil
}

func (r *runsc) cmdDelete(ctx context.Context, containerName string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	// token := rand.Text()
	// logFile := "/tmp/runsc.delete." + token + ".log"

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		// "-debug",
		"-root", ateompath.RunSCStateDir(r.actorUID),
		"delete",
		"-force",
		containerName,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc delete`: %w", err)
	}

	return nil
}

func (r *runsc) cmdState(ctx context.Context, containerName string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir(r.actorUID),
		"state",
		containerName,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc state`: %w", err)
	}
	return nil
}
