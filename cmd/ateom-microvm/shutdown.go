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

// Graceful termination. The kubelet sends SIGTERM at the start of the pod's
// termination grace period; main.go traps it and calls gracefulShutdown, which
// propagates the signal into the guest so the actor can save its state and exit
// on its own before the pod goes away. Stopping the workloads is all this does —
// the VM around them is left for the pod's own teardown to reap.

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
)

const (
	// workloadGracePeriod is how long a guest workload gets to handle SIGTERM and
	// exit on its own before ateom escalates to SIGKILL. Matches ateom-gvisor, and
	// is deliberately shorter than the pod's own termination grace period so the
	// escalation happens here rather than as a kubelet SIGKILL of ateom itself.
	workloadGracePeriod = 1 * time.Minute

	// workloadKillTimeout bounds the post-SIGKILL wait. The VM teardown that
	// follows is what ultimately guarantees the workload is gone, so a wedged
	// kata-agent must not hold shutdown open past this.
	workloadKillTimeout = 5 * time.Second

	// signalDeliveryTimeout bounds one SignalProcess round-trip. Delivering a signal
	// is a local ttrpc call that returns in microseconds; if it has not come back by
	// now the agent is not answering, and waiting longer will not change that.
	//
	// Separate from workloadGracePeriod on purpose. That is the allowance we owe the
	// actor to save its state, not a budget for a stalled transport to spend, and
	// sharing one number would both cheat the actor out of part of its grace period
	// and silently lengthen how long a wedged agent can stall shutdown whenever the
	// grace period is raised.
	signalDeliveryTimeout = 10 * time.Second
)

// gracefulShutdown propagates SIGTERM into every running actor's guest and waits
// for the workloads to exit, so the caller can exit cleanly. It holds lock only
// long enough to snapshot the running actors, and releases it before any blocking
// signaling or waiting, so it never holds it for the whole grace period and a
// suspend can still land mid-drain.
func (s *AteomService) gracefulShutdown(ctx context.Context) {
	// Set this first, before contending for lock, so an RPC that arrives while we
	// are still waiting is turned away rather than queued behind us.
	s.shuttingDown.Store(true)

	// Cancel an in-flight run or restore. Waiting for a cold boot to finish only to
	// SIGTERM the guest it just produced is strictly worse than aborting it.
	s.cancelActiveRestoreOrRunRPC()

	// Wait for whatever still holds lock — a suspend, a resume — to finish, but
	// only for the grace period. In the worst case that RPC burns nearly all of it
	// and then fails, and the stop below spends another grace period on top; that
	// is bounded well inside the pod's own termination grace period, and is the
	// price of not truncating an RPC that may be saving the actor's state.
	lockCtx, lockCancel := context.WithTimeout(ctx, workloadGracePeriod)
	defer lockCancel()
	if !s.lock.LockContext(lockCtx) {
		slog.ErrorContext(ctx, "Failed to acquire lock during graceful shutdown; another RPC is still running")
		return
	}
	// Snapshot by value rather than ranging over s.running directly: we drop the
	// lock immediately below, and a suspend landing mid-drain deletes from the live
	// map and writes through the *runningActor it finds there (teardownActor closes
	// guestAgent and nils the field). Copying the map alone would not help — its
	// values are pointers into that same mutable state.
	targets := make([]drainTarget, 0, len(s.running))
	for id, ra := range s.running {
		if ra == nil {
			continue
		}
		targets = append(targets, drainTarget{id: id, agent: ra.guestAgent, workloadIDs: ra.workloadIDs})
	}

	// Release lock so the service can answer new RPCs — notably a suspend arriving
	// mid-drain — while the stop below waits out the grace period.
	s.lock.Unlock()

	if len(targets) == 0 {
		slog.InfoContext(ctx, "No active actor sessions at shutdown; exiting cleanly")
		return
	}

	for _, t := range targets {
		gracefullyStopActor(ctx, t)
	}
	slog.InfoContext(ctx, "Shutting down")
}

// drainTarget is what gracefulShutdown needs from one runningActor, copied out
// under lock so the drain below never dereferences the shared struct. workloadIDs
// is set once before the actor is published to s.running and never mutated, so
// sharing the backing array is safe; guestAgent is the field a concurrent
// teardownActor writes, and is the reason this snapshot exists.
//
// The client the snapshot holds can still be closed under us by that teardown,
// which is not a problem here: every call on a closed AgentClient fails fast with
// ttrpc.ErrClosed instead of blocking or faulting, and an actor that has been torn
// down has no workload left for this path to stop.
type drainTarget struct {
	id          string
	agent       *kata.AgentClient
	workloadIDs []string
}

// gracefullyStopActor signals the actor's guest workloads with SIGTERM and waits
// out the grace period, escalating to SIGKILL.
func gracefullyStopActor(ctx context.Context, t drainTarget) {
	id := t.id

	// Obtain a kata-agent client to signal the guest: reuse the log-forwarding
	// connection if it's open, else dial a fresh one (best-effort). A dial we open
	// here is closed here; the snapshotted client belongs to the log forwarder.
	agent := t.agent
	var dialed *kata.AgentClient
	if agent == nil {
		a, err := dialAgentRetry(ctx, kata.VsockSocketPath(id), 15*time.Second)
		if err != nil {
			// Without an agent there is no way to reach the guest's processes. They
			// go down with the VM when the pod's containers are killed.
			slog.WarnContext(ctx, "Could not dial kata-agent for graceful stop; leaving the workload to pod teardown", slog.String("id", id), slog.Any("err", err))
			return
		}
		agent, dialed = a, a
	}

	// Stop the workloads concurrently. Each one is entitled to the full grace
	// period, so stopping them in series would multiply it by the container
	// count and overrun the pod's own termination grace period.
	var wg sync.WaitGroup
	for _, wid := range t.workloadIDs {
		wg.Add(1)
		go func(wid string) {
			defer wg.Done()
			if err := stopGuestWorkload(ctx, agent, id, wid); err != nil {
				slog.WarnContext(ctx, "Failed to stop guest workload during shutdown", slog.String("id", id), slog.String("workload", wid), slog.Any("err", err))
			}
		}(wid)
	}
	wg.Wait()
	if dialed != nil {
		_ = dialed.Close()
	}
}

// stopGuestWorkload stops one guest workload, wait out workloadGracePeriod, then
// escalate to SIGKILL and wait a bounded time for the kill to land.
func stopGuestWorkload(ctx context.Context, agent *kata.AgentClient, id, wid string) error {
	// Propagate SIGTERM so the actor can save state and close connections.
	// An actor that installed no handler terminates immediately.
	slog.InfoContext(ctx, "Sending SIGTERM to guest workload", slog.String("id", id), slog.String("workload", wid))
	if err := signalWorkload(ctx, agent, wid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("while propagating SIGTERM to workload %q: %w", wid, err)
	}

	// One WaitProcess (the guest's waitpid) feeds both waits below, so the SIGKILL
	// path picks up the exit the SIGTERM path timed out on rather than issuing a
	// second, competing wait. It runs on a context of its own so the grace-period
	// deadline bounds only our side of the wait; canceling it on return is what
	// unblocks the goroutine and stops the guest-side wait.
	waitCtx, waitCancel := context.WithCancel(ctx)
	defer waitCancel()
	done := make(chan error, 1)
	go func() {
		_, err := agent.WaitProcess(waitCtx, wid, wid)
		done <- err
	}()

	termCtx, termCancel := context.WithTimeout(ctx, workloadGracePeriod)
	defer termCancel()
	err := waitWorkloadStop(termCtx, done)
	if err == nil {
		slog.InfoContext(ctx, "Guest workload exited after SIGTERM", slog.String("id", id), slog.String("workload", wid))
		return nil
	}

	// The wait failed at the RPC layer rather than running out of time. That says
	// nothing about the workload: WaitProcess reports the exit code in its response
	// with a nil error, which the branch above already handled, so an error here is
	// a dead or wedged agent connection and not a process that exited badly.
	//
	// Liveness is therefore unknown, so report it rather than claiming the workload
	// exited. Do not escalate: without a working agent there is no way to reach the
	// process anyway, and ateom is already on its way out, so the container goes
	// down with the pod.
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("while waiting for workload %q to exit: %w", wid, err)
	}

	// The parent context, not our grace period, is what expired: stop here.
	if ctx.Err() != nil {
		return ctx.Err()
	}

	slog.WarnContext(ctx, "Grace period expired; killing guest workload", slog.String("id", id), slog.String("workload", wid), slog.Duration("grace", workloadGracePeriod))
	if err := signalWorkload(ctx, agent, wid, syscall.SIGKILL); err != nil {
		slog.WarnContext(ctx, "Failed to SIGKILL guest workload (it might have already exited)", slog.String("id", id), slog.String("workload", wid), slog.Any("err", err))
	}

	killCtx, killCancel := context.WithTimeout(ctx, workloadKillTimeout)
	defer killCancel()
	if err := waitWorkloadStop(killCtx, done); errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("workload %q failed to exit even after SIGKILL: %w", wid, err)
	} else if errors.Is(err, context.Canceled) {
		return err
	}

	slog.InfoContext(ctx, "Guest workload exited after SIGKILL", slog.String("id", id), slog.String("workload", wid))
	return nil
}

// signalWorkload delivers one signal to a guest workload's init process, bounded
// by signalDeliveryTimeout. ateom sets ExecId equal to ContainerId, so passing wid
// for both targets that init process.
//
// The bound is the point: the shutdown context has no deadline of its own, and
// DialAgent clears the socket deadline once the vsock handshake is done, so ttrpc
// has nothing but this ctx to give up on. A guest that is merely unresponsive
// rather than gone — a paused VM, most plausibly, since a suspend is allowed to
// land mid-drain — leaves the unix socket to CH perfectly healthy while the agent
// never answers, and an unbounded call there would hang until the kubelet's
// SIGKILL at the end of the pod's termination grace period.
func signalWorkload(ctx context.Context, agent *kata.AgentClient, wid string, sig syscall.Signal) error {
	sigCtx, cancel := context.WithTimeout(ctx, signalDeliveryTimeout)
	defer cancel()
	return agent.SignalProcess(sigCtx, wid, wid, uint32(sig))
}

// waitWorkloadStop waits for the workload's exit to land on done, or for ctx to
// terminate first.
func waitWorkloadStop(ctx context.Context, done <-chan error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}
