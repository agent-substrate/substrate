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

	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/resources"
)

// watchOOMEvents reports the guest OOM killer's kills inside the actor's
// micro-VM, which are otherwise invisible: the VM keeps running, no worker log
// or crash report fires, and the actor stays RUNNING while its workload is
// dead (issue #3615).
//
// It runs one long-blocked kata-agent GetOOMEvent call at a time — how the
// kata shim consumes guest OOMs — re-issuing after every event. The call
// multiplexes over the agent connection the stdout/stderr forwarding
// goroutines already read, and ends when that connection does: teardownActor
// closes it (an expected teardown has ra.stopping set), and a VM death fails
// the in-flight call while watchVM reports the crash.
func (s *AteWorkerService) watchOOMEvents(attribution resources.ActorAttribution, ra *runningActor) {
	// Not bound to the RPC that started the watch, like the forwarding
	// goroutines: the watch spans the actor's lifetime.
	ctx := context.Background()
	for {
		if ra.stopping.Load() || s.shuttingDown.Load() {
			return
		}
		// Copy the agent under actorsMu: a concurrent teardown nils it there
		// before closing it (see teardownActor, like the graceful shutdown's
		// drain), and a watch that started before the teardown must not
		// dereference the nil.
		s.actorsMu.RLock()
		agent := ra.guestAgent
		s.actorsMu.RUnlock()
		if agent == nil {
			return
		}
		ev, err := agent.GetOOMEvent(ctx)
		if err != nil {
			// The watch does not outlive the connection. Nothing to report
			// here: teardown is expected (watchVM reports an unexpected VM
			// death), and a log per torn-down watch would be noise on every
			// actor shutdown.
			return
		}
		containerID := ev.GetContainerId()
		attrs := append(ateattr.ActorLogAttrs(attribution),
			slog.String("container_id", containerID),
			slog.String(string(ateattr.ActorContainerNameKey), ra.workloadName(containerID)))
		slog.LogAttrs(ctx, slog.LevelWarn, "Container killed for exceeding its memory limit inside the guest", attrs...)
	}
}
