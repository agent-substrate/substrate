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
	"log/slog"
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

// watchSandbox handles the actor's sandbox dying while nothing is stopping it:
// the actor is torn down here, since the control plane will not reach this
// worker for it again, and then reported crashed.
func (s *AteWorkerService) watchSandbox(ctx context.Context, hosted *hostedActor, runscPath string, actorDirs *ateworkerpb.ActorDirs, containers []*ateworkerpb.Container) {
	attribution := hosted.attribution
	if hosted.sandbox == nil {
		slog.WarnContext(ctx, "Cannot watch the sandbox, a crash will go unreported: it was never pinned", slog.String("actorUID", attribution.UID))
		return
	}
	pid := hosted.sandbox.pid
	exited, err := hosted.sandbox.exited()
	if err != nil {
		slog.WarnContext(ctx, "Cannot watch the sandbox, a crash will go unreported", slog.String("actorUID", attribution.UID), slog.Any("err", err))
		return
	}
	go func() {
		<-exited
		if s.shuttingDown.Load() {
			return
		}
		ctx := context.Background()
		const message = "gVisor sandbox exited"

		// stopping is checked under the actor's lock, so an exit during a
		// checkpoint that then fails is still handled once it gives up.
		// Not reported under the lock: a suspend in flight holds the actor's
		// lease while it waits on that lock for its Checkpoint.
		if !s.locks.Lock(ctx, attribution.UID) {
			return
		}
		ours := s.lookupActor(attribution.UID) == hosted && !hosted.stopping.Load()
		if ours {
			slog.WarnContext(ctx, "Sandbox died", slog.String("actorUID", attribution.UID), slog.Int("pid", pid))
			s.endDeadSandbox(ctx, hosted, func() error {
				return s.terminateWorkload(ctx, attribution.Ref, attribution.UID, runscPath, actorDirs, containers)
			})
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
	}()
}

// endDeadSandbox tears down the activation of a sandbox that died and writes
// its final record. The dead sandbox's cgroup keeps its counters until the
// teardown's runsc delete removes it, so the final usage is read first.
func (s *AteWorkerService) endDeadSandbox(ctx context.Context, hosted *hostedActor, teardown func() error) {
	s.readFinal(ctx, hosted)
	if err := teardown(); err != nil {
		slog.WarnContext(ctx, "Failed to tear down the dead sandbox", slog.String("actorUID", hosted.attribution.UID), slog.Any("err", err))
	}
	s.recordFinalIfEnded(ctx, hosted)
}
