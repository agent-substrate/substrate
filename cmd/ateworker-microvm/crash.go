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

// watchVM reports the actor crashed when its VMM or virtiofsd dies, or when
// every workload container has exited, and nothing is tearing the VM down.
// The control plane will not reach this worker for the actor again, so the
// VM is torn down here. Only one report is sent.
func (s *AteWorkerService) watchVM(attribution resources.ActorAttribution, actorDirs *ateworkerpb.ActorDirs, ra *runningActor) {
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	// Copied under the lock: teardown clears guestAgent concurrently.
	s.actorsMu.RLock()
	agent := ra.guestAgent
	ids := append([]string(nil), ra.workloadIDs...)
	s.actorsMu.RUnlock()

	var message string
	container := false
	select {
	case <-ra.chProc.Done():
		message = fmt.Sprintf("cloud-hypervisor exited: %s", ra.chProc.Exit())
	case <-ra.vfsdProc.Done():
		message = fmt.Sprintf("virtiofsd exited: %s", ra.vfsdProc.Exit())
	case msg := <-watchWorkloads(watchCtx, agent, ids):
		message = msg
		container = true
	}
	// Unblock the workload waits before teardown closes the agent, and before
	// the report, which retries for minutes.
	watchCancel()
	if ra.stopping.Load() || s.shuttingDown.Load() {
		return
	}
	if ra.memory.Killed() {
		message = "killed for exceeding its memory limit"
	}
	ctx := context.Background()
	event := "Micro-VM died"
	if container {
		event = "Workload container exited"
	}
	slog.WarnContext(ctx, event, slog.String("actorUID", attribution.UID), slog.String("reason", message))

	// Not reported under the actor's lock: a suspend in flight holds the
	// actor's lease while it waits on that lock for its Checkpoint.
	if !s.locks.Lock(ctx, attribution.UID) {
		return
	}
	ours := s.runningVM(attribution.UID) == ra && !ra.stopping.Load() && !s.shuttingDown.Load()
	if ours {
		hosted := s.lookupActor(attribution.UID)
		if err := s.terminateWorkload(ctx, attribution, actorDirs); err != nil {
			slog.WarnContext(ctx, "Failed to tear down the actor", slog.String("actorUID", attribution.UID), slog.Any("err", err))
		}
		// not measured first: the guest is gone
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

// watchWorkloads returns a channel that receives one message once every
// workload process has exited. A nil agent, no workloads, a canceled ctx, or
// a failed wait (the agent connection dying because the VM is going away)
// yields a channel that never sends. The caller cancels ctx to release the
// waits. Shutdown waits on the same processes; the agent delivers the exit
// to each waiter.
func watchWorkloads(ctx context.Context, agent guestAgent, ids []string) <-chan string {
	if agent == nil || len(ids) == 0 {
		return nil
	}
	out := make(chan string, 1)
	var wg sync.WaitGroup
	var mu sync.Mutex
	exited := make([]string, 0, len(ids))
	failed := false
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			status, err := agent.WaitProcess(ctx, id, id)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed = true
				if ctx.Err() == nil {
					slog.WarnContext(ctx, "Workload wait failed", slog.String("container", id), slog.Any("err", err))
				}
				return
			}
			exited = append(exited, fmt.Sprintf("container %s exited: status %d", id, status))
		}(id)
	}
	go func() {
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		if failed || len(exited) != len(ids) || ctx.Err() != nil {
			return
		}
		out <- strings.Join(exited, ", ")
	}()
	return out
}
