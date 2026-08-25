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
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

var healthTestActor = resources.ActorAttribution{
	Ref:               resources.ActorRef{Atespace: "space-a", Name: "actor-a"},
	UID:               "uid-a",
	TemplateNamespace: "ns-a",
	TemplateName:      "template-a",
}

// newHealthService builds a service whose agent wait and dead-guest check are
// stubbed through the seams by each test.
func newHealthService() *AteomService {
	return &AteomService{
		lock:        newCancelableMutex(),
		actorLogger: actorlog.NewActorLogger(actorlog.NewSyncedWriter(io.Discard), false),
	}
}

// monitoredActor is a runningActor the exit monitor will accept: the agent
// pointer is never dereferenced because tests stub waitWorkloadExit.
func monitoredActor() *runningActor {
	return &runningActor{guestAgent: &kata.AgentClient{}, workloadIDs: []string{"main"}}
}

func ptrInt32(v int32) *int32 { return &v }

func waitForExitRecord(t *testing.T, act *activation) *workloadExit {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if exit := act.exited.Load(); exit != nil {
			return exit
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("exit monitor recorded no exit")
	return nil
}

func assertNoExitRecord(t *testing.T, act *activation) {
	t.Helper()
	time.Sleep(50 * time.Millisecond)
	if exit := act.exited.Load(); exit != nil {
		t.Fatalf("exit monitor recorded %+v, want no record", exit)
	}
}

// TestExitMonitorRecordsExit: agent WaitProcess returning means the workload
// exited, and the in-band code is recorded.
func TestExitMonitorRecordsExit(t *testing.T) {
	s := newHealthService()
	s.waitWorkloadExit = func(ctx context.Context, agent *kata.AgentClient, wid string) (int32, error) {
		return 7, nil
	}

	act := &activation{attribution: healthTestActor}
	s.startExitMonitor(monitoredActor(), act)

	exit := waitForExitRecord(t, act)
	if exit.container != "main" {
		t.Errorf("recorded exit = %+v, want container main", exit)
	}
	if exit.exitCode == nil || *exit.exitCode != 7 {
		t.Errorf("recorded exitCode = %v, want 7", exit.exitCode)
	}
}

// TestExitMonitorIgnoresExpectedExit: an exit following stopExitMonitor
// (checkpoint, terminate, graceful shutdown) is not a crash.
func TestExitMonitorIgnoresExpectedExit(t *testing.T) {
	s := newHealthService()
	exited := make(chan struct{})
	s.waitWorkloadExit = func(ctx context.Context, agent *kata.AgentClient, wid string) (int32, error) {
		<-exited
		return 0, nil
	}

	act := &activation{attribution: healthTestActor}
	ra := monitoredActor()
	s.startExitMonitor(ra, act)
	ra.stopExitMonitor()
	close(exited)

	assertNoExitRecord(t, act)
}

// TestExitMonitorConfirmsBeforeRecording: a failed wait is a dead or wedged
// agent connection, not an observed exit; only a provably dead guest records.
func TestExitMonitorConfirmsBeforeRecording(t *testing.T) {
	for name, tc := range map[string]struct {
		guestGone  bool
		wantRecord bool
	}{
		"guest still alive": {guestGone: false, wantRecord: false},
		"guest gone":        {guestGone: true, wantRecord: true},
	} {
		t.Run(name, func(t *testing.T) {
			s := newHealthService()
			s.waitWorkloadExit = func(ctx context.Context, agent *kata.AgentClient, wid string) (int32, error) {
				return 0, errors.New("ttrpc: closed")
			}
			s.guestGone = func(ra *runningActor, actorUID string) bool { return tc.guestGone }

			act := &activation{attribution: healthTestActor}
			ra := monitoredActor()
			s.startExitMonitor(ra, act)

			if tc.wantRecord {
				exit := waitForExitRecord(t, act)
				// The wait errored, so no in-band code exists.
				if exit.exitCode != nil {
					t.Errorf("recorded exitCode = %d, want nil", *exit.exitCode)
				}
			} else {
				assertNoExitRecord(t, act)
			}
			ra.stopExitMonitor()
		})
	}
}

// TestExitMonitorReprobesUntilGuestGone pins the retry: a wait failure probed
// while the VMM is still a zombie (the reaper has not collected it, sockets
// still linked) looks alive, so a single probe would end detection right when
// the guest is dying. The monitor must re-probe until the guest is provably
// gone and then record.
func TestExitMonitorReprobesUntilGuestGone(t *testing.T) {
	s := newHealthService()
	s.waitWorkloadExit = func(ctx context.Context, agent *kata.AgentClient, wid string) (int32, error) {
		return 0, errors.New("ttrpc: closed")
	}
	var probes atomic.Int32
	s.guestGone = func(ra *runningActor, actorUID string) bool {
		return probes.Add(1) >= 3
	}

	act := &activation{attribution: healthTestActor}
	ra := monitoredActor()
	s.startExitMonitor(ra, act)

	exit := waitForExitRecord(t, act)
	// The wait errored, so no in-band code exists.
	if exit.exitCode != nil {
		t.Errorf("recorded exitCode = %d, want nil", *exit.exitCode)
	}
	if n := probes.Load(); n < 3 {
		t.Errorf("guestGone probes = %d, want >= 3", n)
	}
	ra.stopExitMonitor()
}

// TestExitMonitorUsesAgentCapturedAtStart pins the handover: the waiter keeps
// using the agent it was started with and never re-reads ra.guestAgent, which
// teardownActor nils out under s.lock the waiter does not hold.
func TestExitMonitorUsesAgentCapturedAtStart(t *testing.T) {
	s := newHealthService()
	agents := make(chan *kata.AgentClient, 2)
	firstCall := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	s.waitWorkloadExit = func(ctx context.Context, agent *kata.AgentClient, wid string) (int32, error) {
		agents <- agent
		if calls.Add(1) == 1 {
			close(firstCall)
			<-release
		}
		return 0, errors.New("ttrpc: closed")
	}
	s.guestGone = func(ra *runningActor, actorUID string) bool { return calls.Load() >= 2 }

	act := &activation{attribution: healthTestActor}
	ra := monitoredActor()
	s.startExitMonitor(ra, act)

	// A teardown drops the field while the waiter is mid-wait.
	<-firstCall
	ra.guestAgent = nil
	close(release)

	if a := <-agents; a == nil {
		t.Fatal("first wait call got a nil agent")
	}
	if a := <-agents; a == nil {
		t.Fatal("retry re-read ra.guestAgent after teardown dropped it")
	}
	waitForExitRecord(t, act)
	ra.stopExitMonitor()
}

// TestExitMonitorNoAgent: an activation without an agent connection gets no
// waiters and no false record.
func TestExitMonitorNoAgent(t *testing.T) {
	s := newHealthService()
	act := &activation{attribution: healthTestActor}
	s.startExitMonitor(&runningActor{workloadIDs: []string{"main"}}, act)
	assertNoExitRecord(t, act)
}

// TestExitMonitorScopesRecordToItsActivation pins the activation fence: a
// waiter left over from a previous activation records on that activation,
// never on the one that replaced it, so a late exit cannot condemn the new
// workload and GetWorkloadHealth keeps reporting it as EXECUTING.
func TestExitMonitorScopesRecordToItsActivation(t *testing.T) {
	s := newHealthService()
	exitNow := make(chan struct{})
	s.waitWorkloadExit = func(ctx context.Context, agent *kata.AgentClient, wid string) (int32, error) {
		<-exitNow
		return 1, nil
	}

	prev := &activation{attribution: healthTestActor}
	ra := monitoredActor()
	s.activeActor.Store(prev)
	s.startExitMonitor(ra, prev)

	// A checkpoint plus a re-restore of the very same actor: same UID, fresh
	// activation. The previous waiter has not observed its cancellation yet.
	next := &activation{attribution: healthTestActor}
	s.activeActor.Store(next)
	close(exitNow)

	waitForExitRecord(t, prev)
	if exit := next.exited.Load(); exit != nil {
		t.Errorf("next.exited = %+v, want nil: the stale waiter reached the new activation", exit)
	}
	got, err := s.GetWorkloadHealth(context.Background(), &ateompb.GetWorkloadHealthRequest{ActorUid: healthTestActor.UID})
	if err != nil {
		t.Fatalf("GetWorkloadHealth() error = %v, want nil", err)
	}
	if got.GetHealth() != ateompb.WorkloadHealth_WORKLOAD_HEALTH_EXECUTING {
		t.Errorf("GetWorkloadHealth() = %v, want EXECUTING", got.GetHealth())
	}
	ra.stopExitMonitor()
}

// TestGetWorkloadHealth covers the keyed contract's error codes and both
// health answers.
func TestGetWorkloadHealth(t *testing.T) {
	for name, tc := range map[string]struct {
		exited   *workloadExit // stored on the active activation when non-nil
		actor    bool
		actorUID string
		wantCode codes.Code // expected error code; OK means a healthy answer
		want     ateompb.WorkloadHealth
	}{
		"missing actor_uid": {actor: true, actorUID: "", wantCode: codes.InvalidArgument},
		// Not here at all. NOT_FOUND like GetWorkloadStats: what the caller
		// should do about it is re-resolve its mapping, not retry.
		"ateom is available": {actorUID: healthTestActor.UID, wantCode: codes.NotFound},
		// The worker was recycled between the caller's view of the world and
		// this call. Answering anyway could report another actor's exit as the
		// requested actor's crash.
		"actor_uid does not match the executing workload": {actor: true, actorUID: "uid-b", wantCode: codes.NotFound},
		"executing": {actor: true, actorUID: healthTestActor.UID, want: ateompb.WorkloadHealth_WORKLOAD_HEALTH_EXECUTING},
		"exited": {
			actor:    true,
			actorUID: healthTestActor.UID,
			exited:   &workloadExit{container: "main", observedAt: time.Unix(0, 1700000000000000000), exitCode: ptrInt32(7)},
			want:     ateompb.WorkloadHealth_WORKLOAD_HEALTH_EXITED,
		},
		"exited with unknown code": {
			actor:    true,
			actorUID: healthTestActor.UID,
			exited:   &workloadExit{container: "main", observedAt: time.Unix(0, 1700000000000000000)},
			want:     ateompb.WorkloadHealth_WORKLOAD_HEALTH_EXITED,
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newHealthService()
			if tc.actor {
				act := &activation{attribution: healthTestActor}
				if tc.exited != nil {
					act.exited.Store(tc.exited)
				}
				s.activeActor.Store(act)
			}

			got, err := s.GetWorkloadHealth(context.Background(), &ateompb.GetWorkloadHealthRequest{ActorUid: tc.actorUID})
			if tc.wantCode != codes.OK {
				if code := status.Code(err); code != tc.wantCode {
					t.Fatalf("GetWorkloadHealth() code = %v, want %v (err: %v)", code, tc.wantCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetWorkloadHealth() error = %v, want nil", err)
			}
			if got.GetHealth() != tc.want {
				t.Fatalf("GetWorkloadHealth() = %v, want %v", got.GetHealth(), tc.want)
			}
			if got.GetActorUid() != healthTestActor.UID {
				t.Errorf("GetWorkloadHealth() actor_uid = %q, want %q", got.GetActorUid(), healthTestActor.UID)
			}
			if tc.want == ateompb.WorkloadHealth_WORKLOAD_HEALTH_EXITED {
				if got.GetExitedContainer() != "main" || got.GetExitedAtUnixNano() != 1700000000000000000 {
					t.Errorf("GetWorkloadHealth() exit detail = (%q, %d), want (main, 1700000000000000000)", got.GetExitedContainer(), got.GetExitedAtUnixNano())
				}
				if tc.exited.exitCode != nil {
					if got.ExitCode == nil || got.GetExitCode() != *tc.exited.exitCode {
						t.Errorf("GetWorkloadHealth() exit_code = %v, want %d", got.ExitCode, *tc.exited.exitCode)
					}
				} else if got.ExitCode != nil {
					t.Errorf("GetWorkloadHealth() exit_code = %d, want unset", got.GetExitCode())
				}
			}
		})
	}
}

// TestGetWorkloadHealthDoesNotTakeLock is the twin of the stats pin: the
// health poll must not queue behind a lifecycle RPC.
func TestGetWorkloadHealthDoesNotTakeLock(t *testing.T) {
	s := newHealthService()
	s.activeActor.Store(&activation{attribution: healthTestActor})

	s.lock.Lock()
	defer s.lock.Unlock()

	if _, err := s.GetWorkloadHealth(context.Background(), &ateompb.GetWorkloadHealthRequest{ActorUid: healthTestActor.UID}); err != nil {
		t.Errorf("GetWorkloadHealth() error = %v, want nil", err)
	}
}
