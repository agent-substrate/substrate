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
	"fmt"
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

// watchVM handles the actor's VMM or virtiofsd dying while nothing is tearing
// them down: the actor is torn down here, since the control plane will not
// reach this worker for it again, and then reported crashed.
func (s *AteWorkerService) watchVM(attribution resources.ActorAttribution, actorDirs *ateworkerpb.ActorDirs, ra *runningActor) {
	what, exit := "cloud-hypervisor", ""
	select {
	case <-ra.chProc.Done():
		exit = ra.chProc.Exit()
	case <-ra.vfsdProc.Done():
		what, exit = "virtiofsd", ra.vfsdProc.Exit()
	}
	if ra.stopping.Load() || s.shuttingDown.Load() {
		return
	}
	ctx := context.Background()
	message := fmt.Sprintf("%s exited: %s", what, exit)
	slog.WarnContext(ctx, "Micro-VM died", slog.String("actorUID", attribution.UID), slog.String("reason", message))

	// Not reported under the actor's lock: a suspend in flight holds the
	// actor's lease while it waits on that lock for its Checkpoint.
	if !s.locks.Lock(ctx, attribution.UID) {
		return
	}
	ours := s.runningVM(attribution.UID) == ra && !ra.stopping.Load()
	if ours {
		hosted := s.lookupActor(attribution.UID)
		if err := s.terminateWorkload(ctx, attribution, actorDirs); err != nil {
			slog.WarnContext(ctx, "Failed to tear down the dead micro-VM", slog.String("actorUID", attribution.UID), slog.Any("err", err))
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
