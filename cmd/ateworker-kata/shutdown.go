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
	"sync"
	"time"
)

// workloadGracePeriod is the whole budget for draining the worker on shutdown.
// It stays well below the Kubernetes termination grace period of the worker
// pod, so the escalation to SIGKILL happens here rather than as a kubelet
// SIGKILL of the worker itself.
const workloadGracePeriod = 30 * time.Minute

// gracefulShutdown sends SIGTERM to the containers of every hosted actor, and
// waits for their VMs to end. A container that has not exited when the budget
// runs out is killed.
func (s *AteWorkerService) gracefulShutdown(ctx context.Context) {
	s.shuttingDown.Store(true)
	// Cancel the boots in flight rather than waiting for them to finish and
	// then signaling them.
	for _, actorUID := range s.inFlight.CancelStartups() {
		slog.InfoContext(ctx, "Cancelling in-progress workload startup RPC due to graceful shutdown", slog.String("actorUID", actorUID))
	}

	deadline := time.Now().Add(workloadGracePeriod)
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if !s.inFlight.WaitIdle(waitCtx) {
		slog.ErrorContext(ctx, "Giving up waiting for in-flight RPCs during graceful shutdown", slog.Any("rpcs", s.inFlight.Names()))
	}

	var wg sync.WaitGroup
	for _, h := range s.hostedActors() {
		v := s.vmOf(h)
		if v == nil {
			continue
		}
		h.stopping.Store(true)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := v.stop(ctx, deadline, killWait); err != nil {
				slog.WarnContext(ctx, "Failed to stop a VM during shutdown", slog.String("actorUID", v.sandboxID), slog.Any("err", err))
			}
		}()
	}
	wg.Wait()
	slog.InfoContext(ctx, "Shutting down")
}
