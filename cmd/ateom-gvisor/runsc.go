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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"

	specs "github.com/opencontainers/runtime-spec/specs-go"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

type runsc struct {
	path     string
	actorUID string
}

// nvproxyGlobalArgs returns the runsc global flags for GPU sandboxes, enabling
// gVisor's GPU ioctl proxy when the worker has a GPU. --nvproxy must be set when
// the sandbox is created (the pause/root container) so the sentry initializes GPU
// support up front — like enabling nvproxy in the containerd runtime config on
// normal Kubernetes. Otherwise nvproxy would try to initialize late, when the app
// subcontainer joins carrying the CDI /dev/nvidia* devices, and the running sentry
// crashes (StartSubcontainer: EOF).
//
// Only create and restore need it: both boot a sentry. `runsc start` acts on a
// sandbox that already exists, so the flag has no effect there.
func nvproxyGlobalArgs() []string {
	if gpuPresent() {
		return []string{"--nvproxy"}
	}
	return nil
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
	}
	args = append(args, nvproxyGlobalArgs()...)
	args = append(args,
		"create",
		"-bundle", ateompath.OCIBundlePath(r.actorUID, containerName),
		"-pid-file", ateompath.PIDFilePath(r.actorUID, containerName),
	)

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

func (r *runsc) cmdStart(ctx context.Context, out io.Writer, containerName string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to run runsc start", slog.String("container", containerName))

	startArgs := []string{
		"-log-format", "json",
		"--alsologtostderr",
		// "-debug",
		// "-debug-log", ateompath.RunscDebugLogDir(r.actorUID, containerName)+"/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-allow-connected-on-save",
		"-root", ateompath.RunSCStateDir(r.actorUID),
	}
	startArgs = append(startArgs, "start", containerName)
	cmd := exec.CommandContext(ctx, r.path, startArgs...)
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

	restoreArgs := []string{
		"-log-format", "json",
		"--alsologtostderr",
		// "-debug",
		// "-debug-log", ateompath.RunscDebugLogDir(r.actorUID, containerName)+"/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-root", ateompath.RunSCStateDir(r.actorUID),
	}
	restoreArgs = append(restoreArgs, nvproxyGlobalArgs()...)
	restoreArgs = append(restoreArgs,
		"restore",
		"-bundle", ateompath.OCIBundlePath(r.actorUID, containerName),
		"-image-path", checkpointPath,
		"-pid-file", ateompath.PIDFilePath(r.actorUID, containerName),
		"-background",
		"-direct",
		"-detach",
		containerName,
	)
	cmd := exec.CommandContext(ctx, r.path, restoreArgs...)
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

// killArgs builds the argv for `runsc kill <container> <signal>`. Factored out
// so the argument construction can be unit-tested without executing runsc.
func (r *runsc) killArgs(containerName, signal string) []string {
	return []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir(r.actorUID),
		"kill",
		containerName,
		signal,
	}
}

// cmdKill sends signal to the given container's process(es) inside the gVisor
// sandbox. Used during graceful shutdown to propagate SIGTERM to the actor.
func (r *runsc) cmdKill(ctx context.Context, containerName, signal string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to run runsc kill", slog.String("container", containerName), slog.String("signal", signal))

	cmd := exec.CommandContext(ctx, r.path, r.killArgs(containerName, signal)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc kill`: %w", err)
	}
	return nil
}

// waitArgs builds the argv for `runsc wait <container>`. Factored out so the
// argument construction can be unit-tested without executing runsc.
func (r *runsc) waitArgs(containerName string) []string {
	return []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir(r.actorUID),
		"wait",
		containerName,
	}
}

// cmdWait blocks until the given container's process exits. Used during
// graceful shutdown to confirm the actor has stopped before ateom exits.
//
// We deliberately DO NOT acquire reapLock here. If we held reapLock.RLock()
// during this long wait, a pending background reaper write lock (reapLock.Lock())
// would block, starving any subsequent read lock attempts (like CheckpointWorkload
// which needs to run runsc checkpoint).
func (r *runsc) cmdWait(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc wait", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.waitArgs(containerName)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		// TODO: If the background child reaper collects the runsc wait process before
		// cmd.Run's own wait finishes, it returns ECHILD. In these cases we return an error
		// when we shouldn't. We can fix this by forking the reap.ReapChildren() call in main.
		return fmt.Errorf("while running `runsc wait`: %w", err)
	}
	return nil
}
