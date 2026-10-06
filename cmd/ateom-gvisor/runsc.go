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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"

	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/sizing"
)

type runsc struct {
	path     string
	actorUID string
	// actorDirs are the actor's directories, from the request.
	actorDirs *ateompb.ActorDirs
	// size is the actor's declared limits.
	size sizing.SandboxSize
	// durableVolumes are the durable-dir volume names declared to the sandbox.
	durableVolumes []string
	// flags holds the configured flags; must not be nil.
	flags *flagStore
}

// buildArgs constructs the runsc argument vector for command, appending
// containerName at the end if non-empty:
//
//	<combined globalFlags> -root <stateDir> <command> <subcommandFlags> <extraArgs> [<containerName>]
func (r *runsc) buildArgs(command, containerName string, extraArgs ...string) []string {
	cfg := r.flags.get()
	globalFlags := cfg.globalFlags(command)
	subcommandFlags := cfg.subcommandFlags(command)

	args := make([]string, 0,
		len(globalFlags)+2+1+len(subcommandFlags)+len(extraArgs)+1)
	args = append(args, globalFlags...)
	args = append(args, "-root", runscStateDir(r.actorDirs))
	args = append(args, command)
	args = append(args, subcommandFlags...)
	args = append(args, extraArgs...)
	if containerName != "" {
		args = append(args, containerName)
	}
	return args
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
	bundle := ociBundlePath(r.actorDirs, containerName)
	spec, err := ocispec.Load(bundle)
	if err != nil {
		return err
	}
	ocispec.ShapeGVisor(spec, ocispec.GVisorOptions{
		ActorUID:       r.actorUID,
		ContainerName:  containerName,
		DurableVolumes: r.durableVolumes,
		Size:           r.size,
		ResolvConf:     resolvConfPath(r.actorDirs),
	})
	return ocispec.Save(bundle, spec)
}

func (r *runsc) cmdCreate(ctx context.Context, out io.Writer, containerName string, additionalArgs []string) error {
	slog.InfoContext(ctx, "About to run runsc create", slog.String("container", containerName))

	if err := r.shapeSpec(containerName); err != nil {
		return fmt.Errorf("while shaping the OCI spec for %q: %w", containerName, err)
	}

	args := make([]string, 0, 4+len(additionalArgs))
	args = append(args,
		"-bundle", ociBundlePath(r.actorDirs, containerName),
		"-pid-file", pidFilePath(r.actorDirs, containerName),
	)
	args = append(args, additionalArgs...)

	cmd := exec.CommandContext(
		ctx,
		r.path,
		r.buildArgs("create", containerName, args...)...,
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

	cmd := exec.CommandContext(ctx, r.path, r.buildArgs("start", containerName)...)
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
		r.buildArgs("checkpoint", containerName, "-image-path", checkpointPath)...,
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

	args := make([]string, 0, 2+2*len(durableDirMounts))
	args = append(args, "-image-path", checkpointPath)
	for _, ddv := range durableDirMounts {
		args = append(args, "-path", ddv)
	}

	cmd := exec.CommandContext(
		ctx,
		r.path,
		r.buildArgs("fscheckpoint", containerName, args...)...,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := reaper.RunCommand(cmd)
	if err != nil {
		return fmt.Errorf("while running `runsc fscheckpoint`: %w", err)
	}
	return nil
}

// cmdPause pauses all processes in the container (or sandbox, if pause).
func (r *runsc) cmdPause(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc pause", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.buildArgs("pause", containerName)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := reaper.RunCommand(cmd); err != nil {
		return fmt.Errorf("while running `runsc pause`: %w", err)
	}
	return nil
}

// cmdResume unpauses a paused container (or sandbox, if pause).
func (r *runsc) cmdResume(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc resume", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.buildArgs("resume", containerName)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := reaper.RunCommand(cmd); err != nil {
		return fmt.Errorf("while running `runsc resume`: %w", err)
	}
	return nil
}

// restoreArgs builds the argv for `runsc restore <container>`. Factored out so
// the argument construction can be unit-tested without executing runsc.
func (r *runsc) restoreArgs(containerName, checkpointPath string) []string {
	return r.buildArgs("restore", containerName,
		"-bundle", ociBundlePath(r.actorDirs, containerName),
		"-image-path", checkpointPath,
		"-pid-file", pidFilePath(r.actorDirs, containerName),
	)
}

// We take a checkpoint only of the root container of the sandbox, but we need
// to call restore on each container, using the same checkpoint.
func (r *runsc) cmdRestore(ctx context.Context, out io.Writer, containerName, checkpointPath string) error {
	slog.InfoContext(ctx, "About to run runsc restore", slog.String("container", containerName))

	if err := r.shapeSpec(containerName); err != nil {
		return fmt.Errorf("while shaping the OCI spec for %q: %w", containerName, err)
	}

	cmd := exec.CommandContext(ctx, r.path, r.restoreArgs(containerName, checkpointPath)...)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := reaper.RunCommand(cmd); err != nil {
		return fmt.Errorf("while running `runsc restore`: %w", err)
	}
	return nil
}

func (r *runsc) cmdDelete(ctx context.Context, containerName string) error {
	cmd := exec.CommandContext(
		ctx,
		r.path,
		r.buildArgs("delete", containerName)...,
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
		r.buildArgs("state", containerName)...,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := reaper.RunCommand(cmd); err != nil {
		return fmt.Errorf("while running `runsc state`: %w", err)
	}
	return nil
}

// cmdList returns the container IDs runsc has a record of.
func (r *runsc) cmdList(ctx context.Context) ([]string, error) {
	cmd := exec.CommandContext(
		ctx,
		r.path,
		r.buildArgs("list", "")...,
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := reaper.RunCommand(cmd); err != nil {
		return nil, fmt.Errorf("while running `runsc list`: %w", err)
	}
	return strings.Fields(out.String()), nil
}

// cmdKill sends signal to the given container's process(es) inside the gVisor
// sandbox. Used during graceful shutdown to propagate SIGTERM to the actor.
func (r *runsc) cmdKill(ctx context.Context, containerName, signal string) error {
	slog.InfoContext(ctx, "About to run runsc kill", slog.String("container", containerName), slog.String("signal", signal))

	cmd := exec.CommandContext(ctx, r.path, r.buildArgs("kill", "", containerName, signal)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := reaper.RunCommand(cmd); err != nil {
		return fmt.Errorf("while running `runsc kill`: %w", err)
	}
	return nil
}

// cmdWait blocks until the given container's process exits. Used during
// graceful shutdown to confirm the actor has stopped before ateom exits.
//
// Deliberately outside the reaper: this blocks for as long as the actor runs,
// and an entry held that long would hold off reaping and, past MaxDefer, every
// other runsc invocation with it.
func (r *runsc) cmdWait(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc wait", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.buildArgs("wait", containerName)...)
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
