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
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.ate.dev/substrate/internal/ateworker/register"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
)

// crashReportTimeout bounds how long a crash report keeps retrying.
const crashReportTimeout = 5 * time.Minute

// crashReporter is the slice of register.CrashReporter the service uses.
type crashReporter interface {
	ReportCrashed(ctx context.Context, actor resources.ActorAttribution, message string) error
}

var _ crashReporter = (*register.CrashReporter)(nil)

// containerWaiter blocks until one workload container's process exits.
// *runsc implements it.
type containerWaiter interface {
	cmdWait(ctx context.Context, containerName string) error
}

// watchSandbox reports the actor crashed when its sandbox dies, or when every
// workload container has exited, and nothing is stopping it. The control plane
// will not reach this worker for the actor again, so the actor is torn down
// here. Only one report is sent. A nil wait leaves container exits unreported.
func (s *AteWorkerService) watchSandbox(ctx context.Context, hosted *hostedActor, runscPath string, wait containerWaiter, actorDirs *ateworkerpb.ActorDirs, containers []*ateworkerpb.Container) {
	attribution := hosted.attribution
	var sandboxExited <-chan struct{}
	var sandboxErr error
	pid := 0
	if hosted.sandbox == nil {
		sandboxErr = errors.New("sandbox process was never pinned")
	} else {
		pid = hosted.sandbox.pid
		exited, err := hosted.sandbox.exited()
		if err != nil {
			sandboxErr = err
		} else {
			sandboxExited = exited
		}
	}
	parent := hosted.ctx
	if parent == nil {
		parent = context.Background()
	}
	watchCtx, watchCancel := context.WithCancel(parent)
	containersExited, containersFailed := watchContainers(watchCtx, wait, containers)
	if sandboxExited == nil && containersExited == nil {
		watchCancel()
		slog.WarnContext(ctx, "Cannot watch the actor, a crash will go unreported", slog.String("actorUID", attribution.UID), slog.Any("err", sandboxErr))
		return
	}
	if sandboxExited == nil {
		slog.WarnContext(ctx, "Cannot watch the sandbox process", slog.String("actorUID", attribution.UID), slog.Any("err", sandboxErr))
	}
	go func() {
		defer watchCancel()
		var message string
		container := false
		select {
		case <-sandboxExited:
			message = "gVisor sandbox exited"
		case msg := <-containersExited:
			message = msg
			container = true
		case <-containersFailed:
			if sandboxExited == nil {
				return
			}
			select {
			case <-sandboxExited:
				message = "gVisor sandbox exited"
			case <-watchCtx.Done():
				return
			}
		case <-watchCtx.Done():
			return
		}
		// Release the other wait before teardown, and before the report, which
		// retries for minutes.
		watchCancel()
		s.reportObservedCrash(hosted, message, container, pid, runscPath, actorDirs, containers)
	}()
}

// reportObservedCrash tears the actor down and reports it crashed, unless this
// exit is one the worker caused: shutdown, a checkpoint or terminate in
// progress, or an activation that has already been replaced.
func (s *AteWorkerService) reportObservedCrash(hosted *hostedActor, message string, container bool, pid int, runscPath string, actorDirs *ateworkerpb.ActorDirs, containers []*ateworkerpb.Container) {
	if hosted.stopping.Load() || s.shuttingDown.Load() {
		return
	}
	if hosted.memory.Killed() {
		message = "killed for exceeding its memory limit"
	}
	ctx := context.Background()
	attribution := hosted.attribution
	// stopping is checked under the actor's lock, so an exit during a
	// checkpoint that then fails is still handled once it gives up.
	// Not reported under the lock: a suspend in flight holds the actor's
	// lease while it waits on that lock for its Checkpoint.
	if !s.locks.Lock(ctx, attribution.UID) {
		return
	}
	replaced := hosted.ctx != nil && hosted.ctx.Err() != nil
	ours := s.lookupActor(attribution.UID) == hosted && !hosted.stopping.Load() && !s.shuttingDown.Load() && !replaced
	if ours {
		if container {
			slog.WarnContext(ctx, "Workload container exited", slog.String("actorUID", attribution.UID), slog.String("reason", message))
		} else {
			slog.WarnContext(ctx, "Sandbox died", slog.String("actorUID", attribution.UID), slog.Int("pid", pid))
		}
		if err := s.terminateWorkload(ctx, attribution.Ref, attribution.UID, runscPath, actorDirs, containers); err != nil {
			slog.WarnContext(ctx, "Failed to tear down the actor", slog.String("actorUID", attribution.UID), slog.Any("err", err))
		}
		// not measured first: the sandbox, and its cgroup, are gone
		s.recordFinalIfEnded(ctx, hosted)
	}
	s.locks.Unlock(attribution.UID)
	if !ours || s.crashReporter == nil {
		return
	}
	reportCtx, cancel := context.WithTimeout(ctx, crashReportTimeout)
	defer cancel()
	if err := s.crashReporter.ReportCrashed(reportCtx, attribution, message); err != nil {
		slog.ErrorContext(ctx, "Failed to report the actor crashed", slog.String("actorUID", attribution.UID), slog.Any("err", err))
	}
}

// watchContainers returns a channel that receives one message once every
// workload container has exited, and a channel closed when that watch ends
// without an exit. A nil waiter or no containers yields nil channels. The
// caller cancels ctx to release the waits, including the runsc processes
// they start.
func watchContainers(ctx context.Context, wait containerWaiter, containers []*ateworkerpb.Container) (<-chan string, <-chan struct{}) {
	if wait == nil || len(containers) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(containers))
	for _, c := range containers {
		if name := c.GetName(); name != "" {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil, nil
	}
	out := make(chan string, 1)
	failedCh := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	exited := make([]string, 0, len(names))
	failed := false
	for _, name := range names {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			err := wait.cmdWait(ctx, name)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed = true
				if ctx.Err() == nil {
					slog.WarnContext(ctx, "Container wait failed", slog.String("container", name), slog.Any("err", err))
				}
				return
			}
			exited = append(exited, fmt.Sprintf("container %s exited", name))
		}(name)
	}
	go func() {
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		if failed || len(exited) != len(names) || ctx.Err() != nil {
			close(failedCh)
			return
		}
		out <- strings.Join(exited, ", ")
	}()
	return out, failedCh
}
