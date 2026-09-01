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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"syscall"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/sizing"
)

type runsc struct {
	path     string
	actorUID string
	// size is the actor's declared limits.
	size sizing.SandboxSize
	// durableVolumes are the durable-dir volume names declared to the sandbox.
	durableVolumes []string
}

// durableVolumeNames returns the sorted, deduplicated durable-dir volume names
// mounted by workload containers.
func durableVolumeNames(spec *ateompb.WorkloadSpec) []string {
	var names []string
	for _, c := range spec.GetContainers() {
		for _, m := range c.GetDurableDirVolumeMounts() {
			names = append(names, m.GetVolumeName())
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// shapeSpec loads, shapes for gVisor, and saves the container's OCI spec.
func (r *runsc) shapeSpec(containerName string) error {
	bundle := ateompath.OCIBundlePath(r.actorUID, containerName)
	spec, err := ocispec.Load(bundle)
	if err != nil {
		return err
	}
	ocispec.ShapeGVisor(spec, ocispec.GVisorOptions{
		ActorUID:       r.actorUID,
		ContainerName:  containerName,
		DurableVolumes: r.durableVolumes,
		Size:           r.size,
	})
	return ocispec.Save(bundle, spec)
}

func (r *runsc) cmdCreate(ctx context.Context, out io.Writer, containerName string, additionalArgs []string) error {
	slog.InfoContext(ctx, "About to run runsc create", slog.String("container", containerName))

	if err := r.shapeSpec(containerName); err != nil {
		return fmt.Errorf("while shaping the OCI spec for %q: %w", containerName, err)
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
		// Provision the sentry's vCPU count from the cgroup CPU quota written by
		// sizing.ApplyToOCISpec, so the sandbox is sized to the pod's limit (runsc
		// otherwise sizes to all host CPUs). Global flag: before the subcommand.
		"--cpu-num-from-quota",
	}
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

	err := reaper.RunCommand(cmd)
	if err != nil {
		return fmt.Errorf("while running `runsc create`: %w", err)
	}

	return nil
}

func (r *runsc) cmdStart(ctx context.Context, out io.Writer, containerName string) error {
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

	err := reaper.RunCommand(cmd)
	if err != nil {
		return fmt.Errorf("while running `runsc start`: %w", err)
	}

	return nil
}

func (r *runsc) cmdCheckpoint(ctx context.Context, containerName, checkpointPath string) error {
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
	err := reaper.RunCommand(cmd)
	if err != nil {
		return fmt.Errorf("while running `runsc checkpoint`: %w", err)
	}
	return nil
}

//nolint:unused
func (r *runsc) cmdFsCheckpoint(ctx context.Context, containerName, checkpointPath string, durableDirMounts []string) error {
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
	err := reaper.RunCommand(cmd)
	if err != nil {
		return fmt.Errorf("while running `runsc fscheckpoint`: %w", err)
	}
	return nil
}

// pauseArgs builds the argv for `runsc pause <container>`. Factored out so the
// argument construction can be unit-tested without executing runsc.
func (r *runsc) pauseArgs(containerName string) []string {
	return []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir(r.actorUID),
		"pause",
		containerName,
	}
}

// cmdPause pauses all processes in the container (or sandbox, if pause).
func (r *runsc) cmdPause(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc pause", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.pauseArgs(containerName)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := reaper.RunCommand(cmd); err != nil {
		return fmt.Errorf("while running `runsc pause`: %w", err)
	}
	return nil
}

// resumeArgs builds the argv for `runsc resume <container>`. Factored out so the
// argument construction can be unit-tested without executing runsc.
func (r *runsc) resumeArgs(containerName string) []string {
	return []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir(r.actorUID),
		"resume",
		containerName,
	}
}

// cmdResume unpauses a paused container (or sandbox, if pause).
func (r *runsc) cmdResume(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc resume", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.resumeArgs(containerName)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := reaper.RunCommand(cmd); err != nil {
		return fmt.Errorf("while running `runsc resume`: %w", err)
	}
	return nil
}

// We take a checkpoint only of the root container of the sandbox, but we need
// to call restore on each container, using the same checkpoint.
func (r *runsc) cmdRestore(ctx context.Context, out io.Writer, containerName, checkpointPath string) error {
	slog.InfoContext(ctx, "About to run runsc restore", slog.String("container", containerName))

	if err := r.shapeSpec(containerName); err != nil {
		return fmt.Errorf("while shaping the OCI spec for %q: %w", containerName, err)
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
		// Match cmdCreate: size the restored sentry from the cgroup CPU quota.
		"--cpu-num-from-quota",
	}
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
	if err := reaper.RunCommand(cmd); err != nil {
		return fmt.Errorf("while running `runsc restore`: %w", err)
	}
	return nil
}

func (r *runsc) cmdDelete(ctx context.Context, containerName string) error {
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

	err := reaper.RunCommand(cmd)
	if err != nil {
		return fmt.Errorf("while running `runsc delete`: %w", err)
	}

	return nil
}

func (r *runsc) cmdState(ctx context.Context, containerName string) error {
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
	if err := reaper.RunCommand(cmd); err != nil {
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
	slog.InfoContext(ctx, "About to run runsc kill", slog.String("container", containerName), slog.String("signal", signal))

	cmd := exec.CommandContext(ctx, r.path, r.killArgs(containerName, signal)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := reaper.RunCommand(cmd); err != nil {
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
// Deliberately outside the reaper: this blocks for as long as the actor runs,
// and an entry held that long would hold off reaping and, past MaxDefer, every
// other runsc invocation with it.
func (r *runsc) cmdWait(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc wait", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.waitArgs(containerName)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		// Running outside the reaper means the reaper can collect this process
		// first, leaving os/exec nothing to wait for. `runsc wait` only exits
		// once the container has, so that is the answer we came for -- and
		// reporting it as a failure would stop the caller escalating to
		// SIGKILL.
		if errors.Is(err, syscall.ECHILD) {
			slog.DebugContext(ctx, "runsc wait was collected by the child reaper", slog.String("container", containerName))
			return nil
		}
		return fmt.Errorf("while running `runsc wait`: %w", err)
	}
	return nil
}
