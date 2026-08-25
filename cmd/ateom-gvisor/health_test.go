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
	"fmt"
	"io"
	"os/exec"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// newHealthService builds a service the exit monitor can run on: each test
// stubs the runsc invocations with a fakeRunsc on the session.
func newHealthService() *AteomService {
	return &AteomService{
		lock:        newCancelableMutex(),
		actorLogger: actorlog.NewActorLogger(actorlog.NewSyncedWriter(io.Discard), false),
	}
}

// fakeRunsc stubs the exit monitor's runsc wait/state invocations. A nil func,
// like any command outside the monitor's slice of runscCommands, panics via
// the nil embed: the test expects that command to never run.
type fakeRunsc struct {
	runscCommands
	wait  func(ctx context.Context, container string) (*int32, error)
	state func(ctx context.Context, container string) (string, error)
}

func (f fakeRunsc) cmdWait(ctx context.Context, container string) (*int32, error) {
	return f.wait(ctx, container)
}

func (f fakeRunsc) cmdStateStatus(ctx context.Context, container string) (string, error) {
	return f.state(ctx, container)
}

func ptrInt32(v int32) *int32 { return &v }

// waitForExitRecord polls until the monitor records an exit on act, or fails
// the test.
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

// assertNoExitRecord gives the monitor goroutine a moment to (wrongly) record
// an exit, then asserts it did not.
func assertNoExitRecord(t *testing.T, act *activation) {
	t.Helper()
	time.Sleep(50 * time.Millisecond)
	if exit := act.exited.Load(); exit != nil {
		t.Fatalf("exit monitor recorded %+v, want no record", exit)
	}
}

// TestExitMonitorRecordsExit covers the crash path: `runsc wait` returning
// means the container exited, and the first record wins on the workload's
// activation.
func TestExitMonitorRecordsExit(t *testing.T) {
	s := newHealthService()
	code := int32(7)
	rcmd := fakeRunsc{wait: func(ctx context.Context, container string) (*int32, error) {
		return &code, nil // the container exited immediately
	}}

	act := &activation{attribution: testActor}
	session := &workloadSession{rcmd: rcmd, containers: []string{"main"}}
	s.startExitMonitor(session, act)

	exit := waitForExitRecord(t, act)
	if exit.container != "main" {
		t.Errorf("recorded container = %q, want %q", exit.container, "main")
	}
	if exit.exitCode == nil || *exit.exitCode != code {
		t.Errorf("recorded exitCode = %v, want %d", exit.exitCode, code)
	}
}

// TestExitMonitorIgnoresExpectedExit pins the suppression contract: a
// container exit that follows stopExitMonitor (checkpoint, terminate,
// graceful shutdown) is not a crash.
func TestExitMonitorIgnoresExpectedExit(t *testing.T) {
	s := newHealthService()
	exited := make(chan struct{})
	rcmd := fakeRunsc{wait: func(ctx context.Context, container string) (*int32, error) {
		<-exited
		return nil, nil
	}}

	act := &activation{attribution: testActor}
	session := &workloadSession{rcmd: rcmd, containers: []string{"main"}}
	s.startExitMonitor(session, act)

	// The exits are marked expected before the container dies, which is
	// the order every control-plane teardown follows.
	session.stopExitMonitor()
	close(exited)

	assertNoExitRecord(t, act)
}

// TestExitMonitorConfirmsBeforeRecording covers the reaper false alarm: a
// failed `runsc wait` against a still-running container must not be recorded;
// once the state read reports the container gone, it must be.
func TestExitMonitorConfirmsBeforeRecording(t *testing.T) {
	for name, tc := range map[string]struct {
		status     string
		statusErr  error
		wantRecord bool
	}{
		"container still running": {status: "running", wantRecord: false},
		"container stopped":       {status: "stopped", wantRecord: true},
		// A gone container is runsc running and reporting "does not
		// exist" — an *exec.ExitError, unlike a fork failure.
		"container gone": {statusErr: fmt.Errorf("while running `runsc state`: %w", &exec.ExitError{}), wantRecord: true},
	} {
		t.Run(name, func(t *testing.T) {
			s := newHealthService()
			statusRead := make(chan struct{}, 1)
			rcmd := fakeRunsc{
				wait: func(ctx context.Context, container string) (*int32, error) {
					return nil, errors.New("waiting for process to exit: waitpid: no child processes")
				},
				state: func(ctx context.Context, container string) (string, error) {
					select {
					case statusRead <- struct{}{}:
					default:
					}
					return tc.status, tc.statusErr
				},
			}

			act := &activation{attribution: testActor}
			session := &workloadSession{rcmd: rcmd, containers: []string{"main"}}
			s.startExitMonitor(session, act)
			<-statusRead

			if tc.wantRecord {
				exit := waitForExitRecord(t, act)
				// The wait lost to the reaper, so the code is unknowable.
				if exit.exitCode != nil {
					t.Errorf("recorded exitCode = %d, want nil", *exit.exitCode)
				}
			} else {
				assertNoExitRecord(t, act)
			}
			session.stopExitMonitor()
		})
	}
}

// TestExitMonitorRetriesStateReadFailures pins the transient-failure buffer: a
// `runsc state` that fails and then reports the container running (a fork
// failure under load, not a dead container) must not be recorded as a crash.
func TestExitMonitorRetriesStateReadFailures(t *testing.T) {
	s := newHealthService()
	var calls atomic.Int32
	recovered := make(chan struct{}, 1)
	rcmd := fakeRunsc{
		wait: func(ctx context.Context, container string) (*int32, error) {
			return nil, errors.New("waiting for process to exit: waitpid: no child processes")
		},
		state: func(ctx context.Context, container string) (string, error) {
			if calls.Add(1) < maxStateReadAttempts {
				return "", errors.New("fork/exec runsc: resource temporarily unavailable")
			}
			select {
			case recovered <- struct{}{}:
			default:
			}
			return "running", nil
		},
	}

	act := &activation{attribution: testActor}
	session := &workloadSession{rcmd: rcmd, containers: []string{"main"}}
	s.startExitMonitor(session, act)
	<-recovered

	assertNoExitRecord(t, act)
	session.stopExitMonitor()
}

// TestExitMonitorStateFailuresMustBeConsecutive pins the reset: a state read
// that confirms the container running breaks the failure streak, so scattered
// `runsc state` errors over a long-lived workload never add up to a false
// crash. Only maxStateReadAttempts failures in a row may condemn it.
func TestExitMonitorStateFailuresMustBeConsecutive(t *testing.T) {
	s := newHealthService()
	var calls atomic.Int32
	pastNaiveLimit := make(chan struct{}, 1)
	rcmd := fakeRunsc{
		wait: func(ctx context.Context, container string) (*int32, error) {
			return nil, errors.New("waiting for process to exit: waitpid: no child processes")
		},
		state: func(ctx context.Context, container string) (string, error) {
			n := calls.Add(1)
			if n > maxStateReadAttempts {
				select {
				case pastNaiveLimit <- struct{}{}:
				default:
				}
			}
			// Every maxStateReadAttempts-th read confirms the container
			// running, so the streak never reaches the limit.
			if n%maxStateReadAttempts == 0 {
				return "running", nil
			}
			return "", fmt.Errorf("while running `runsc state`: %w", &exec.ExitError{})
		},
	}

	act := &activation{attribution: testActor}
	session := &workloadSession{rcmd: rcmd, containers: []string{"main"}}
	s.startExitMonitor(session, act)
	<-pastNaiveLimit

	assertNoExitRecord(t, act)
	session.stopExitMonitor()
}

// TestExitMonitorNeverPresumesGoneOnExecFailure pins the error-type split: a
// `runsc state` that never ran (fork failure under host load) says nothing
// about the container, and such failures correlate under sustained pressure —
// even maxStateReadAttempts in a row must not condemn the workload.
func TestExitMonitorNeverPresumesGoneOnExecFailure(t *testing.T) {
	s := newHealthService()
	var calls atomic.Int32
	pastOldLimit := make(chan struct{}, 1)
	rcmd := fakeRunsc{
		wait: func(ctx context.Context, container string) (*int32, error) {
			return nil, errors.New("waiting for process to exit: waitpid: no child processes")
		},
		state: func(ctx context.Context, container string) (string, error) {
			if calls.Add(1) > maxStateReadAttempts {
				select {
				case pastOldLimit <- struct{}{}:
				default:
				}
			}
			return "", fmt.Errorf("while running `runsc state`: %w", &exec.Error{Name: "runsc", Err: syscall.EAGAIN})
		},
	}

	act := &activation{attribution: testActor}
	session := &workloadSession{rcmd: rcmd, containers: []string{"main"}}
	s.startExitMonitor(session, act)
	<-pastOldLimit

	assertNoExitRecord(t, act)
	session.stopExitMonitor()
}

// TestExitMonitorScopesRecordToItsActivation pins the activation fence: a
// waiter left over from a previous activation records on that activation,
// never on the one that replaced it, so a late exit cannot condemn the new
// workload and GetWorkloadHealth keeps reporting it as EXECUTING.
func TestExitMonitorScopesRecordToItsActivation(t *testing.T) {
	s := newHealthService()
	exitNow := make(chan struct{})
	rcmd := fakeRunsc{wait: func(ctx context.Context, container string) (*int32, error) {
		<-exitNow
		return ptrInt32(1), nil
	}}

	prev := &activation{attribution: testActor}
	session := &workloadSession{rcmd: rcmd, containers: []string{"main"}}
	s.activeActor.Store(prev)
	s.startExitMonitor(session, prev)

	// A checkpoint plus a re-restore of the very same actor: same UID, fresh
	// activation. The previous waiter has not observed its cancellation yet.
	next := &activation{attribution: testActor}
	s.activeActor.Store(next)
	close(exitNow)

	waitForExitRecord(t, prev)
	if exit := next.exited.Load(); exit != nil {
		t.Errorf("next.exited = %+v, want nil: the stale waiter reached the new activation", exit)
	}
	got, err := s.GetWorkloadHealth(context.Background(), &ateompb.GetWorkloadHealthRequest{ActorUid: testActor.UID})
	if err != nil {
		t.Fatalf("GetWorkloadHealth() error = %v, want nil", err)
	}
	if got.GetHealth() != ateompb.WorkloadHealth_WORKLOAD_HEALTH_EXECUTING {
		t.Errorf("GetWorkloadHealth() = %v, want EXECUTING", got.GetHealth())
	}
	session.stopExitMonitor()
}

// TestGetWorkloadHealth covers the keyed contract's error codes and both
// health answers, plus the previous-activation fence.
func TestGetWorkloadHealth(t *testing.T) {
	for name, tc := range map[string]struct {
		exit     *workloadExit // stored on the active activation when non-nil
		actor    bool          // seeds active with a testActor activation
		actorUID string
		wantCode codes.Code // expected error code; OK means a healthy answer
		want     ateompb.WorkloadHealth
	}{
		"missing actor_uid": {
			actor:    true,
			actorUID: "",
			wantCode: codes.InvalidArgument,
		},
		// Not here at all. NOT_FOUND like GetWorkloadStats: what the caller
		// should do about it is re-resolve its mapping, not retry.
		"ateom is available": {
			actorUID: testActor.UID,
			wantCode: codes.NotFound,
		},
		// The worker was recycled between the caller's view of the world and
		// this call. Answering anyway could report another actor's exit as the
		// requested actor's crash.
		"actor_uid does not match the executing workload": {
			actor:    true,
			actorUID: "uid-b",
			wantCode: codes.NotFound,
		},
		"executing": {
			actor:    true,
			actorUID: testActor.UID,
			want:     ateompb.WorkloadHealth_WORKLOAD_HEALTH_EXECUTING,
		},
		"exited": {
			actor:    true,
			actorUID: testActor.UID,
			exit:     &workloadExit{container: "main", observedAt: time.Unix(0, 1700000000000000000), exitCode: ptrInt32(7)},
			want:     ateompb.WorkloadHealth_WORKLOAD_HEALTH_EXITED,
		},
		"exited with unknown code": {
			actor:    true,
			actorUID: testActor.UID,
			exit:     &workloadExit{container: "main", observedAt: time.Unix(0, 1700000000000000000)},
			want:     ateompb.WorkloadHealth_WORKLOAD_HEALTH_EXITED,
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newHealthService()
			if tc.actor {
				act := &activation{attribution: testActor}
				if tc.exit != nil {
					act.exited.Store(tc.exit)
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
			if got.GetAtespace() != testActor.Ref.Atespace || got.GetActorName() != testActor.Ref.Name || got.GetActorUid() != testActor.UID {
				t.Errorf("GetWorkloadHealth() attribution = (%q, %q, %q), want testActor's", got.GetAtespace(), got.GetActorName(), got.GetActorUid())
			}
			if tc.want == ateompb.WorkloadHealth_WORKLOAD_HEALTH_EXITED {
				if got.GetExitedContainer() != "main" || got.GetExitedAtUnixNano() != 1700000000000000000 {
					t.Errorf("GetWorkloadHealth() exit detail = (%q, %d), want (main, 1700000000000000000)", got.GetExitedContainer(), got.GetExitedAtUnixNano())
				}
				if tc.exit.exitCode != nil {
					if got.ExitCode == nil || got.GetExitCode() != *tc.exit.exitCode {
						t.Errorf("GetWorkloadHealth() exit_code = %v, want %d", got.ExitCode, *tc.exit.exitCode)
					}
				} else if got.ExitCode != nil {
					t.Errorf("GetWorkloadHealth() exit_code = %d, want unset", got.GetExitCode())
				}
			}
		})
	}
}

// TestFailedActivationDoesNotLinger pins the window between retaining the
// activation and registering the failure-cleanup defer: an egress validation
// error must drop the activation again, or GetWorkloadHealth would report a
// workload that never booted as EXECUTING until the next lifecycle RPC.
func TestFailedActivationDoesNotLinger(t *testing.T) {
	// An EgressGateway with no address fails validation in prepareActorEgress,
	// before the handler reaches netlink or runsc. Zero-value atunnel endpoints
	// make the preceding deactivation a no-op.
	gateway := &ateompb.EgressGateway{}
	for name, call := range map[string]func(s *AteomService) error{
		"RunWorkload": func(s *AteomService) error {
			_, err := s.RunWorkload(context.Background(), &ateompb.RunWorkloadRequest{
				Atespace:      testActor.Ref.Atespace,
				ActorName:     testActor.Ref.Name,
				ActorUid:      testActor.UID,
				EgressGateway: gateway,
			})
			return err
		},
		"RestoreWorkload": func(s *AteomService) error {
			_, err := s.RestoreWorkload(context.Background(), &ateompb.RestoreWorkloadRequest{
				Atespace:      testActor.Ref.Atespace,
				ActorName:     testActor.Ref.Name,
				ActorUid:      testActor.UID,
				EgressGateway: gateway,
			})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newHealthService()
			s.atunnelIngress = &atunnel.Server{}
			s.atunnelEgress = &atunnel.Egress{}

			if err := call(s); err == nil {
				t.Fatal("lifecycle RPC error = nil, want egress validation error")
			}
			if act := s.activeActor.Load(); act != nil {
				t.Errorf("activeActor = %+v after failed activation, want nil", act)
			}
			if _, err := s.GetWorkloadHealth(context.Background(), &ateompb.GetWorkloadHealthRequest{ActorUid: testActor.UID}); status.Code(err) != codes.NotFound {
				t.Errorf("GetWorkloadHealth() error = %v, want NotFound", err)
			}
		})
	}
}

// TestGetWorkloadHealthDoesNotTakeLock is the twin of
// TestGetWorkloadStatsDoesNotTakeLock: the health poll must not queue behind a
// lifecycle RPC, so a handler that reached for s.lock would deadlock here.
func TestGetWorkloadHealthDoesNotTakeLock(t *testing.T) {
	s := newHealthService()
	s.activeActor.Store(&activation{attribution: testActor})

	s.lock.Lock()
	defer s.lock.Unlock()

	if _, err := s.GetWorkloadHealth(context.Background(), &ateompb.GetWorkloadHealthRequest{ActorUid: testActor.UID}); err != nil {
		t.Errorf("GetWorkloadHealth() error = %v, want nil", err)
	}
}
