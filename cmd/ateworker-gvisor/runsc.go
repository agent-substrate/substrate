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
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/sizing"
)

type runsc struct {
	path     string
	actorUID string
	// actorDirs are the actor's directories, from the request.
	actorDirs *ateworkerpb.ActorDirs
	// size is the actor's declared limits.
	size sizing.SandboxSize
	// durableVolumes are the durable-dir volume names declared to the sandbox.
	durableVolumes []string
	// containers are the workload's containers, from the request.
	containers []*ateworkerpb.Container
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
func durableVolumeNames(spec *ateworkerpb.WorkloadSpec) []string {
	var names []string
	for _, c := range spec.GetContainers() {
		for _, m := range c.GetDurableDirVolumeMounts() {
			names = append(names, m.GetName())
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// pauseContainer is the sandbox root container. It only reaps, so it gets no
// capabilities and mounts nothing.
func pauseContainer() *ateworkerpb.Container {
	return &ateworkerpb.Container{
		Name:          ocispec.PauseContainer,
		ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/pause"}},
	}
}

// writeSpec builds the container's OCI spec, shapes it for gVisor, and saves it
// into the container's bundle.
func (r *runsc) writeSpec(containerName string) error {
	container := pauseContainer()
	if containerName != ocispec.PauseContainer {
		i := slices.IndexFunc(r.containers, func(c *ateworkerpb.Container) bool { return c.GetName() == containerName })
		if i < 0 {
			return fmt.Errorf("container %q is not in the workload spec", containerName)
		}
		container = r.containers[i]
	}
	spec := ocispec.Build(ocispec.Options{ActorUID: r.actorUID, ActorDirs: r.actorDirs, Container: container})
	ocispec.ShapeGVisor(spec, ocispec.GVisorOptions{
		ActorUID:       r.actorUID,
		ActorDirs:      r.actorDirs,
		ContainerName:  containerName,
		DurableVolumes: r.durableVolumes,
		Size:           r.size,
		ResolvConf:     resolvConfPath(r.actorDirs),
	})
	return ocispec.Save(ociBundlePath(r.actorDirs, containerName), spec)
}

func (r *runsc) cmdCreate(ctx context.Context, out io.Writer, containerName string, additionalArgs []string) error {
	slog.InfoContext(ctx, "About to run runsc create", slog.String("container", containerName))

	if err := r.writeSpec(containerName); err != nil {
		return fmt.Errorf("while writing the OCI spec for %q: %w", containerName, err)
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

	err := cmd.Run()
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

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc start`: %w", err)
	}

	return nil
}

// checkpointArgs builds the argv for `runsc checkpoint <container>`. Factored
// out so the argument construction can be unit-tested without executing runsc.
func (r *runsc) checkpointArgs(containerName, checkpointPath string, fsCheckpointPaths []string) []string {
	extra := []string{"-image-path", checkpointPath}
	if len(fsCheckpointPaths) > 0 {
		extra = append(extra, "-fs-checkpoint-paths", strings.Join(fsCheckpointPaths, ","))
	}
	return r.buildArgs("checkpoint", containerName, extra...)
}

func (r *runsc) cmdCheckpoint(ctx context.Context, containerName, checkpointPath string, fsCheckpointPaths []string) error {
	slog.InfoContext(ctx, "About to run runsc checkpoint", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.checkpointArgs(containerName, checkpointPath, fsCheckpointPaths)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc checkpoint`: %w", err)
	}
	return nil
}

// fsCheckpointArgs builds the argv for `runsc fscheckpoint <container>`.
// Factored out so the argument construction can be unit-tested without
// executing runsc.
func (r *runsc) fsCheckpointArgs(containerName, checkpointPath string, fsCheckpointPaths []string) []string {
	extra := make([]string, 0, 2+2*len(fsCheckpointPaths))
	extra = append(extra, "-image-path", filepath.Join(checkpointPath, resources.FSCheckpointSubdir))
	for _, ddv := range fsCheckpointPaths {
		extra = append(extra, "-path", ddv)
	}
	return r.buildArgs("fscheckpoint", containerName, extra...)
}

func (r *runsc) cmdFsCheckpoint(ctx context.Context, containerName, checkpointPath string, fsCheckpointPaths []string) error {
	slog.InfoContext(ctx, "About to run runsc fscheckpoint", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.fsCheckpointArgs(containerName, checkpointPath, fsCheckpointPaths)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc fscheckpoint`: %w", err)
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

	if err := r.writeSpec(containerName); err != nil {
		return fmt.Errorf("while writing the OCI spec for %q: %w", containerName, err)
	}

	cmd := exec.CommandContext(ctx, r.path, r.restoreArgs(containerName, checkpointPath)...)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
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
	err := cmd.Run()
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
	if err := cmd.Run(); err != nil {
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
	if err := cmd.Run(); err != nil {
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
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc kill`: %w", err)
	}
	return nil
}

// runWait runs `runsc wait [extraArgs...] <container>` and blocks until the
// waited-on condition completes.
func (r *runsc) runWait(ctx context.Context, containerName string, extraArgs ...string) error {
	cmd := exec.CommandContext(ctx, r.path, r.buildArgs("wait", containerName, extraArgs...)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc wait`: %w", err)
	}
	return nil
}

// cmdWait blocks until the given container's process exits. Used during
// graceful shutdown to confirm the actor has stopped before ateworker exits.
func (r *runsc) cmdWait(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc wait", slog.String("container", containerName))
	return r.runWait(ctx, containerName)
}

// cmdWaitRestore blocks until background page loading from `runsc restore -background` completes.
func (r *runsc) cmdWaitRestore(ctx context.Context, containerName string) error {
	return r.runWait(ctx, containerName, "-restore")
}

// cmdWaitFSRestore blocks until background filesystem page loading from
// `--fs-restore-image-path` completes for the given container.
func (r *runsc) cmdWaitFSRestore(ctx context.Context, containerName string) error {
	return r.runWait(ctx, containerName, "-fsrestore")
}
