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
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// workloadExit records the first guest workload exit the monitor observed
// outside of an intentional teardown.
type workloadExit struct {
	container  string
	observedAt time.Time
	// exitCode is nil when the exit was detected but the code is unknown
	// (the agent connection died and guestGone confirmed the VM dead).
	exitCode *int32
}

// activation is the state born at a Run/Restore and dropped by the
// Checkpoint/Terminate (or failed-boot cleanup) that ends it. Every
// activation is a fresh allocation, so pointer identity tells activations
// apart — the UID cannot, since a checkpoint and re-restore of the same
// actor keeps it.
type activation struct {
	attribution resources.ActorAttribution

	// exited is the first guest workload exit observed outside an intentional
	// teardown, or nil. First writer wins (CompareAndSwap from nil): the VM
	// dying trips every waiter at once. A stale waiter from an earlier
	// activation holds that activation's pointer and records there, so it can
	// never condemn this one. Atomic for the same reason as
	// AteomService.activeActor: GetWorkloadHealth reads it without taking lock.
	exited atomic.Pointer[workloadExit]
}

// exitMonitorRetryDelay spaces re-probes after a wait failure against a guest
// that does not yet look dead (ateom-gvisor's retry cadence).
const exitMonitorRetryDelay = time.Second

// startExitMonitor watches the actor's guest workloads via agent WaitProcess
// (the peer of ateom-gvisor's runsc-wait monitor) and records the first exit
// that is not part of an intentional teardown on act, for GetWorkloadHealth
// to report. Without an agent connection this activation gets no exit
// detection, like it gets no stats.
func (s *AteomService) startExitMonitor(ra *runningActor, act *activation) {
	// A freshly started monitor by definition expects no exits yet.
	// Expects all callers hold s.lock.
	ra.exitExpected.Store(false)
	if ra.guestAgent == nil {
		slog.Warn("No kata-agent connection; workload exit detection disabled for this activation",
			slog.String("actorUID", act.attribution.UID))
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	ra.exitMonitorCancel = cancel
	for _, wid := range ra.workloadIDs {
		// The agent is handed over rather than re-read in the goroutine:
		// teardownActor nils ra.guestAgent under s.lock, which the waiter
		// does not hold.
		go s.watchWorkloadExit(ctx, ra, act, ra.guestAgent, wid)
	}
}

func (s *AteomService) watchWorkloadExit(ctx context.Context, ra *runningActor, act *activation, agent *kata.AgentClient, wid string) {
	wait := s.waitWorkloadExit
	if wait == nil {
		wait = func(ctx context.Context, agent *kata.AgentClient, wid string) (int32, error) {
			return agent.WaitProcess(ctx, wid, wid)
		}
	}
	gone := s.guestGone
	if gone == nil {
		gone = guestGone
	}

	var exitCode *int32
	warned := false
	for {
		code, waitErr := wait(ctx, agent, wid)
		if ctx.Err() != nil || ra.exitExpected.Load() {
			return
		}
		if waitErr == nil {
			exitCode = &code
			break
		}
		// WaitProcess reports exit codes in-band, so an error is a dead or
		// wedged agent connection, not an observed exit. Record a crash only
		// when the guest is provably gone — and re-probe until then: a
		// SIGKILLed VMM stays a zombie (signalable, sockets still linked)
		// until the reaper collects it, so a single probe races the reaper.
		guestIsGone := gone(ra, act.attribution.UID)
		if ctx.Err() != nil || ra.exitExpected.Load() {
			return
		}
		if guestIsGone {
			break
		}
		if !warned {
			warned = true
			slog.Warn("Guest workload wait failed but the micro-VM appears alive; re-probing",
				slog.String("actorUID", act.attribution.UID), slog.String("workload", wid), slog.Any("err", waitErr))
		}
		select {
		case <-time.After(exitMonitorRetryDelay):
		case <-ctx.Done():
			return
		}
	}

	exit := &workloadExit{container: wid, observedAt: time.Now(), exitCode: exitCode}
	// First writer wins — the VM dying trips every waiter at once. The
	// record lands on this waiter's own activation, so a stale waiter from
	// an earlier one can never touch the current record.
	if !act.exited.CompareAndSwap(nil, exit) {
		return
	}
	attrs := []any{
		slog.String("actor", act.attribution.Ref.String()),
		slog.String("actorUID", act.attribution.UID),
		slog.String("workload", wid),
	}
	if exitCode != nil {
		attrs = append(attrs, slog.Int("exitCode", int(*exitCode)))
	}
	slog.ErrorContext(ctx, "Actor workload exited outside of a control-plane teardown", attrs...)
	s.actorLogger.EmitLifecycleLog(context.WithoutCancel(ctx), "Actor process exited", act.attribution)
}

// guestGone reports whether the micro-VM is definitely dead: a VMM process
// that cannot be signaled has exited, and cloud-hypervisor unlinks the vsock
// socket when the VM stops (the same evidence dialAgentRetry trusts).
func guestGone(ra *runningActor, actorUID string) bool {
	if ra.chCmd != nil && ra.chCmd.Process != nil {
		if err := ra.chCmd.Process.Signal(syscall.Signal(0)); err != nil {
			return true
		}
	}
	if _, err := os.Stat(kata.VsockSocketPath(actorUID)); err != nil {
		return true
	}
	return false
}

// GetWorkloadHealth implements ateompb.Ateom/GetWorkloadHealth from the exit
// monitor's record. Reads only the activation's atomics -- never the guest
// and never s.lock, pinned by TestGetWorkloadHealthDoesNotTakeLock.
func (s *AteomService) GetWorkloadHealth(ctx context.Context, req *ateompb.GetWorkloadHealthRequest) (*ateompb.GetWorkloadHealthResponse, error) {
	if req.GetActorUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor_uid is required")
	}

	// NOT_FOUND for both, same contract as GetWorkloadStats: the requested
	// actor is not here.
	active := s.activeActor.Load()
	if active == nil {
		return nil, status.Errorf(codes.NotFound, "ateom is available; it is not executing actor %q", req.GetActorUid())
	}
	if active.attribution.UID != req.GetActorUid() {
		return nil, status.Errorf(codes.NotFound, "ateom is executing actor %q, not the requested %q", active.attribution.UID, req.GetActorUid())
	}

	resp := &ateompb.GetWorkloadHealthResponse{
		Health:    ateompb.WorkloadHealth_WORKLOAD_HEALTH_EXECUTING,
		Atespace:  active.attribution.Ref.Atespace,
		ActorName: active.attribution.Ref.Name,
		ActorUid:  active.attribution.UID,
	}
	// The record lives on the activation itself, so a late write from a
	// previous activation's waiter cannot reach it — no fencing needed, even
	// for a re-activation of the same actor with the same UID.
	if exited := active.exited.Load(); exited != nil {
		resp.Health = ateompb.WorkloadHealth_WORKLOAD_HEALTH_EXITED
		resp.ExitedContainer = exited.container
		resp.ExitedAtUnixNano = exited.observedAt.UnixNano()
		if exited.exitCode != nil {
			code := *exited.exitCode
			resp.ExitCode = &code
		}
	}
	return resp, nil
}
