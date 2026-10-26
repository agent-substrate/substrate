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

// watchVM handles the actor's VM ending while nothing is stopping it: the actor
// is torn down here, since the control plane will not reach this worker for it
// again, and then reported crashed. The returned channel is closed when the
// watcher is done.
func (s *AteWorkerService) watchVM(hosted *hostedActor, v *vm, actorDirs *ateworkerpb.ActorDirs) <-chan struct{} {
	attribution := hosted.attribution
	done := make(chan struct{})
	go func() {
		defer close(done)
		// hosted.ctx ends when the actor is replaced or unhosted, which is not
		// a crash.
		if err := v.waitGone(hosted.ctx); err != nil {
			return
		}
		if s.shuttingDown.Load() {
			return
		}
		ctx := context.Background()

		// stopping is checked under the actor's lock, so an exit during a
		// terminate that then fails is still handled once it gives up. Not
		// reported under the lock: a suspend in flight holds the actor's
		// lease while it waits on that lock.
		if !s.locks.Lock(ctx, attribution.UID) {
			return
		}
		ours := s.lookupActor(attribution.UID) == hosted && !hosted.stopping.Load()
		if ours {
			slog.WarnContext(ctx, "The Kata VM died", slog.String("actorUID", attribution.UID))
			cleanupCtx, cancel := context.WithTimeout(ctx, cleanupTimeout)
			if err := s.removeVM(cleanupCtx, v, actorDirs, hosted); err != nil {
				slog.WarnContext(ctx, "Failed to tear down the dead VM", slog.String("actorUID", attribution.UID), slog.Any("err", err))
			}
			cancel()
		}
		s.locks.Unlock(attribution.UID)
		if !ours || s.crashReporter == nil {
			return
		}
		reportCtx, cancel := context.WithTimeout(ctx, crashReportTimeout)
		defer cancel()
		if err := s.crashReporter.ReportCrashed(reportCtx, attribution, "Kata VM exited"); err != nil {
			slog.ErrorContext(ctx, "Failed to report the actor crashed", slog.String("actorUID", attribution.UID), slog.Any("err", err))
		}
	}()
	return done
}
