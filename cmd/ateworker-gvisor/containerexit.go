//go:build linux

// Copyright 2026 The Agent Substrate Authors
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
	"log/slog"
	"os"
	"time"

	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/ateattr"
)

// containerExitTimeout bounds the runsc commands that read one container exit.
const containerExitTimeout = 10 * time.Second

// containerStopped is the status `runsc state` gives a container whose first
// process exited. It gives the same status when the sandbox is gone.
const containerStopped = "stopped"

// exitReader is the slice of *runsc that the container exit watch needs.
type exitReader interface {
	cmdState(ctx context.Context, containerName string) (string, error)
	cmdExitCode(ctx context.Context, containerName string) (int32, error)
}

// startContainerLogs forwards an application container's output as
// StartJSONLogPipe does, and checks for the container's exit when the output
// ends. A blocked `runsc wait` for each container would cost one runsc process
// each, so the end of the output is the signal.
func (s *AteWorkerService) startContainerLogs(hosted *hostedActor, rcmd exitReader, containerName string) (*os.File, error) {
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	go func() {
		s.actorLogger.WrapContainerLogs(pr, hosted.attribution, containerName)
		pr.Close()
		s.containerOutputEnded(hosted, rcmd, containerName)
	}()
	return pw, nil
}

// containerOutputEnded logs the container's exit if the container stopped while
// nothing was stopping the actor. The end of the output means only that each
// copy of the pipe's write end is closed: a container that closes its own
// stdout and stderr still runs, and `runsc state` tells the two apart.
func (s *AteWorkerService) containerOutputEnded(hosted *hostedActor, rcmd exitReader, containerName string) {
	if s.shuttingDown.Load() {
		return
	}
	attribution := hosted.attribution
	ctx := context.Background()
	// The lock keeps `runsc delete` away until the exit code is read. It also
	// makes an exit during a boot or a checkpoint wait for the result.
	if !s.locks.Lock(ctx, attribution.UID) {
		return
	}
	defer s.locks.Unlock(attribution.UID)
	if s.lookupActor(attribution.UID) != hosted || hosted.stopping.Load() || s.shuttingDown.Load() {
		return
	}
	// A dead sandbox closes each container's pipe. watchSandbox reports it.
	if hosted.sandbox != nil && hosted.sandbox.hasExited() {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, containerExitTimeout)
	defer cancel()
	logAttrs := append(ateattr.ActorLogAttrs(attribution), slog.String("container", containerName))
	status, err := rcmd.cmdState(ctx, containerName)
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelWarn, "Cannot tell if the container exited", append(logAttrs, slog.Any("err", err))...)
		return
	}
	if status != containerStopped {
		return
	}
	exit := actorlog.ContainerExit{Container: containerName}
	// The sandbox keeps the exit status until `runsc delete`, so this returns
	// at once.
	code, err := rcmd.cmdExitCode(ctx, containerName)
	if err != nil {
		// A dying sandbox closes the pipes before its own exit shows, so the
		// check above can miss it. With no pinned sandbox the failure cannot
		// be told from a sandbox death, so that case has no record.
		if hosted.sandbox == nil || hosted.sandbox.hasExited() {
			return
		}
		slog.LogAttrs(ctx, slog.LevelWarn, "Cannot read the container's exit code", append(logAttrs, slog.Any("err", err))...)
	} else {
		exit.ExitCode = &code
	}
	s.actorLogger.EmitContainerExit(attribution, exit)
}
