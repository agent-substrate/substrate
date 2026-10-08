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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
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

// runscLogTailBytes bounds how much of runsc's log, and of a sentry panic, an
// error quotes. The error ends up in the actor's crash message, which is
// capped at 4 KiB, so leave room for the context wrapped around it.
const runscLogTailBytes = 2048

// logMark is where the runsc log ended before a command ran, so a failure
// quotes what was logged during the command and nothing older.
type logMark int64

// logMark returns the current end of the actor's runsc log.
func (r *runsc) logMark() logMark {
	info, err := os.Stat(runscLogPath(r.actorDirs))
	if err != nil {
		return 0
	}
	return logMark(info.Size())
}

// loggedSince returns what runsc logged after mark, on one line, bounded to
// the last runscLogTailBytes. Every runsc command writes its log to the
// actor's runsc log file (see runscLogPath), and the sentry it starts logs
// there too, so this is what runsc had to say about the failure.
func (r *runsc) loggedSince(mark logMark) string {
	f, err := os.Open(runscLogPath(r.actorDirs))
	if err != nil {
		return ""
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() <= int64(mark) {
		return ""
	}
	offset, cut := int64(mark), false
	if info.Size()-offset > runscLogTailBytes {
		offset, cut = info.Size()-runscLogTailBytes, true
	}
	b := make([]byte, info.Size()-offset)
	if _, err := f.ReadAt(b, offset); err != nil && !errors.Is(err, io.EOF) {
		return ""
	}
	if cut {
		// Drop the line the cut landed in.
		if i := bytes.IndexByte(b, '\n'); i >= 0 {
			b = b[i+1:]
		}
	}
	msgs := logMessages(b)
	if msgs != "" && cut {
		msgs = "..." + msgs
	}
	return msgs
}

// logMessages joins the messages of runsc's JSON log lines with "; ". A line
// that is not one is kept as it is.
func logMessages(b []byte) string {
	var out []string
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var rec struct {
			Msg string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err == nil && rec.Msg != "" {
			line = strings.TrimSpace(rec.Msg)
		}
		out = append(out, line)
	}
	return strings.Join(out, "; ")
}

// oneLine joins the non-empty lines of b with "; " for an error message.
func oneLine(b []byte) string {
	var lines []string
	for _, line := range strings.Split(string(b), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "; ")
}

// sentryPanic returns the start of the sentry's panic log on one line, or ""
// when there is none. runsc gets -panic-log so a sentry panic outlives the
// sentry; the head is kept rather than the tail because the panic message and
// the goroutine that raised it come first.
func (r *runsc) sentryPanic() string {
	b, err := os.ReadFile(panicLogPath(r.actorDirs))
	if err != nil {
		return ""
	}
	cut := len(b) > runscLogTailBytes
	if cut {
		b = b[:runscLogTailBytes]
	}
	s := oneLine(b)
	if s != "" && cut {
		s += "..."
	}
	return s
}

// logSentryPanic reports the sentry's panic log, if there is one, with the
// actor's identity, and removes it so it is not reported again or attached
// to a later failure of a different activation. Terminate calls it as the
// last point this ateom looks at the actor's directories.
func (r *runsc) logSentryPanic(ctx context.Context, attribution resources.ActorAttribution) {
	p := r.sentryPanic()
	if p == "" {
		_ = os.Remove(panicLogPath(r.actorDirs))
		return
	}
	attrs := ateattr.ActorLogAttrs(attribution)
	attrs = append(attrs, slog.String("panic_log", p))
	slog.LogAttrs(ctx, slog.LevelWarn, "Sentry panic log found while terminating the actor", attrs...)
	if err := os.Remove(panicLogPath(r.actorDirs)); err != nil {
		slog.WarnContext(ctx, "Failed to remove the sentry panic log", slog.Any("err", err))
	}
}

// commandError wraps err from `runsc <verb>` with what runsc logged during
// the command and, when the sentry has panicked, the start of its panic log.
// The result flows through atelet into the actor's crash message, so it is
// what an operator sees.
func (r *runsc) commandError(verb string, err error, mark logMark) error {
	var detail string
	if logged := r.loggedSince(mark); logged != "" {
		detail += ": " + logged
	}
	if p := r.sentryPanic(); p != "" {
		detail += "; sentry panic log: " + p
	}
	return fmt.Errorf("while running `runsc %s`: %w%s", verb, err, detail)
}

// run executes runsc with args, its output going to out, or to this process's
// stdout and stderr when out is nil. out is handed to runsc as it is: for
// create, start and restore it is the container's stdio, which the sandbox
// inherits for its lifetime, so anything but a file there would hold the
// command's wait open for as long as the sandbox runs. What runsc has to say
// about a failure comes from its log file instead.
func (r *runsc) run(ctx context.Context, verb string, out io.Writer, args ...string) error {
	cmd := exec.CommandContext(ctx, r.path, args...)
	if out != nil {
		cmd.Stdout, cmd.Stderr = out, out
	} else {
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	}
	mark := r.logMark()
	if err := reaper.RunCommand(cmd); err != nil {
		return r.commandError(verb, err, mark)
	}
	return nil
}

// createArgs builds the argv for `runsc create <container>`. Factored out so
// the argument construction can be unit-tested without executing runsc.
func (r *runsc) createArgs(containerName string, additionalArgs []string) []string {
	args := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-log", runscLogPath(r.actorDirs),
		// "-debug",
		// "-debug-log", filepath.Join(r.actorDirs.GetRootDir(), "runsc-debug-logs", containerName) + "/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-root", runscStateDir(r.actorDirs),
		// Provision the sentry's vCPU count from the cgroup CPU quota written by
		// sizing.ApplyToOCISpec, so the sandbox is sized to the pod's limit (runsc
		// otherwise sizes to all host CPUs). Global flag: before the subcommand.
		"--cpu-num-from-quota",
		// Keep a sentry panic, which otherwise dies with the sentry; see sentryPanic.
		"-panic-log", panicLogPath(r.actorDirs),
	}
	args = append(args,
		"create",
		"-bundle", ociBundlePath(r.actorDirs, containerName),
		"-pid-file", pidFilePath(r.actorDirs, containerName),
	)

	args = append(args, additionalArgs...)
	args = append(args, containerName) // Name of the container
	return args
}

func (r *runsc) cmdCreate(ctx context.Context, out io.Writer, containerName string, additionalArgs []string) error {
	slog.InfoContext(ctx, "About to run runsc create", slog.String("container", containerName))

	if err := r.shapeSpec(containerName); err != nil {
		return fmt.Errorf("while shaping the OCI spec for %q: %w", containerName, err)
	}
	return r.run(ctx, "create", out, r.createArgs(containerName, additionalArgs)...)
}

func (r *runsc) cmdStart(ctx context.Context, out io.Writer, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc start", slog.String("container", containerName))

	startArgs := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-log", runscLogPath(r.actorDirs),
		// "-debug",
		// "-debug-log", filepath.Join(r.actorDirs.GetRootDir(), "runsc-debug-logs", containerName) + "/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-allow-connected-on-save",
		"-root", runscStateDir(r.actorDirs),
	}
	startArgs = append(startArgs, "start", containerName)
	return r.run(ctx, "start", out, startArgs...)
}

func (r *runsc) cmdCheckpoint(ctx context.Context, containerName, checkpointPath string) error {
	slog.InfoContext(ctx, "About to run runsc checkpoint", slog.String("container", containerName))

	return r.run(ctx, "checkpoint", nil,
		"-log-format", "json",
		"--alsologtostderr",
		"-log", runscLogPath(r.actorDirs),
		// "-debug",
		// "-debug-log", filepath.Join(r.actorDirs.GetRootDir(), "runsc-debug-logs", containerName) + "/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-root", runscStateDir(r.actorDirs),
		"checkpoint",
		"-image-path", checkpointPath,
		containerName, // Name of the container
	)
}

//nolint:unused
func (r *runsc) cmdFsCheckpoint(ctx context.Context, containerName, checkpointPath string, durableDirMounts []string) error {
	slog.InfoContext(ctx, "About to run runsc fscheckpoint", slog.String("container", containerName))

	args := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-log", runscLogPath(r.actorDirs),
		// "-debug",
		// "-debug-log", filepath.Join(r.actorDirs.GetRootDir(), "runsc-debug-logs", containerName) + "/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-root", runscStateDir(r.actorDirs),
		"fscheckpoint",
		"-image-path", checkpointPath,
	}
	for _, ddv := range durableDirMounts {
		args = append(args, "-path", ddv)
	}

	// name of the container must be the last parameter.
	args = append(args, containerName)
	return r.run(ctx, "fscheckpoint", nil, args...)
}

// pauseArgs builds the argv for `runsc pause <container>`. Factored out so the
// argument construction can be unit-tested without executing runsc.
func (r *runsc) pauseArgs(containerName string) []string {
	return []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-log", runscLogPath(r.actorDirs),
		"-root", runscStateDir(r.actorDirs),
		"pause",
		containerName,
	}
}

// cmdPause pauses all processes in the container (or sandbox, if pause).
func (r *runsc) cmdPause(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc pause", slog.String("container", containerName))

	return r.run(ctx, "pause", nil, r.pauseArgs(containerName)...)
}

// resumeArgs builds the argv for `runsc resume <container>`. Factored out so the
// argument construction can be unit-tested without executing runsc.
func (r *runsc) resumeArgs(containerName string) []string {
	return []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-log", runscLogPath(r.actorDirs),
		"-root", runscStateDir(r.actorDirs),
		"resume",
		containerName,
	}
}

// cmdResume unpauses a paused container (or sandbox, if pause).
func (r *runsc) cmdResume(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc resume", slog.String("container", containerName))

	return r.run(ctx, "resume", nil, r.resumeArgs(containerName)...)
}

// restoreArgs builds the argv for `runsc restore <container>`. Factored out so
// the argument construction can be unit-tested without executing runsc.
func (r *runsc) restoreArgs(containerName, checkpointPath string) []string {
	return []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-log", runscLogPath(r.actorDirs),
		// "-debug",
		// "-debug-log", filepath.Join(r.actorDirs.GetRootDir(), "runsc-debug-logs", containerName) + "/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-root", runscStateDir(r.actorDirs),
		// Match cmdCreate: size the restored sentry from the cgroup CPU quota.
		"--cpu-num-from-quota",
		// Match cmdCreate: keep a sentry panic; see sentryPanic.
		"-panic-log", panicLogPath(r.actorDirs),
		"restore",
		"-bundle", ociBundlePath(r.actorDirs, containerName),
		"-image-path", checkpointPath,
		"-pid-file", pidFilePath(r.actorDirs, containerName),
		"-background",
		"-detach",
		containerName,
	}
}

// We take a checkpoint only of the root container of the sandbox, but we need
// to call restore on each container, using the same checkpoint.
func (r *runsc) cmdRestore(ctx context.Context, out io.Writer, containerName, checkpointPath string) error {
	slog.InfoContext(ctx, "About to run runsc restore", slog.String("container", containerName))

	if err := r.shapeSpec(containerName); err != nil {
		return fmt.Errorf("while shaping the OCI spec for %q: %w", containerName, err)
	}

	return r.run(ctx, "restore", out, r.restoreArgs(containerName, checkpointPath)...)
}

func (r *runsc) cmdDelete(ctx context.Context, containerName string) error {
	return r.run(ctx, "delete", nil,
		"-log-format", "json",
		"--alsologtostderr",
		"-log", runscLogPath(r.actorDirs),
		// "-debug",
		"-root", runscStateDir(r.actorDirs),
		"delete",
		"-force",
		containerName,
	)
}

func (r *runsc) cmdState(ctx context.Context, containerName string) error {
	return r.run(ctx, "state", nil,
		"-log-format", "json",
		"--alsologtostderr",
		"-log", runscLogPath(r.actorDirs),
		"-root", runscStateDir(r.actorDirs),
		"state",
		containerName,
	)
}

// cmdList returns the container IDs runsc has a record of.
func (r *runsc) cmdList(ctx context.Context) ([]string, error) {
	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		"-log", runscLogPath(r.actorDirs),
		"-root", runscStateDir(r.actorDirs),
		"list",
		"-quiet",
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	mark := r.logMark()
	if err := reaper.RunCommand(cmd); err != nil {
		return nil, r.commandError("list", err, mark)
	}
	return strings.Fields(out.String()), nil
}

// killArgs builds the argv for `runsc kill <container> <signal>`. Factored out
// so the argument construction can be unit-tested without executing runsc.
func (r *runsc) killArgs(containerName, signal string) []string {
	return []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-log", runscLogPath(r.actorDirs),
		"-root", runscStateDir(r.actorDirs),
		"kill",
		containerName,
		signal,
	}
}

// cmdKill sends signal to the given container's process(es) inside the gVisor
// sandbox. Used during graceful shutdown to propagate SIGTERM to the actor.
func (r *runsc) cmdKill(ctx context.Context, containerName, signal string) error {
	slog.InfoContext(ctx, "About to run runsc kill", slog.String("container", containerName), slog.String("signal", signal))

	return r.run(ctx, "kill", nil, r.killArgs(containerName, signal)...)
}

// waitArgs builds the argv for `runsc wait <container>`. Factored out so the
// argument construction can be unit-tested without executing runsc.
func (r *runsc) waitArgs(containerName string) []string {
	return []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-log", runscLogPath(r.actorDirs),
		"-root", runscStateDir(r.actorDirs),
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
	mark := r.logMark()
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
		return r.commandError("wait", err, mark)
	}
	return nil
}
