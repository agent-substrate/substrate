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
	"os/exec"
	"testing"
	"time"

	"github.com/google/uuid"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/child"
	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
)

type crashReport struct {
	actor        resources.ActorAttribution
	executionUID string
	message      string
}

type fakeCrashReporter struct {
	reports chan crashReport
	started chan struct{}
	release chan struct{}
}

func (f *fakeCrashReporter) ReportCrashed(ctx context.Context, actor resources.ActorAttribution, executionUID, message string) error {
	if f.started != nil {
		f.started <- struct{}{}
		select {
		case <-f.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.reports <- crashReport{actor: actor, executionUID: executionUID, message: message}
	return nil
}

// fakeVM is a running actor whose VMM exits at once and whose virtiofsd runs
// until the test ends.
func fakeVM(t *testing.T, executionUID string) *runningActor {
	t.Helper()
	chProc, err := child.Start(exec.Command("sh", "-c", "exit 3"))
	if err != nil {
		t.Fatal(err)
	}
	vfsdProc, err := child.Start(exec.Command("sleep", "30"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(vfsdProc.Stop)
	return &runningActor{executionUID: executionUID, chProc: chProc, vfsdProc: vfsdProc}
}

func TestWatchVMIgnoresExpectedExits(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*AteWorkerService, *runningActor)
	}{
		{"stopped by ateworker", func(_ *AteWorkerService, ra *runningActor) { ra.stopping.Store(true) }},
		{"worker shutting down", func(s *AteWorkerService, _ *runningActor) { s.shuttingDown.Store(true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reporter := &fakeCrashReporter{reports: make(chan crashReport, 1)}
			s := &AteWorkerService{locks: actorlock.New(), actors: map[string]*hostedActor{}, crashReporter: reporter}
			ra := fakeVM(t, "")
			tc.setup(s, ra)
			s.watchVM(resources.ActorAttribution{UID: "actor-a"}, nil, ra)
			select {
			case report := <-reporter.reports:
				t.Fatalf("reported %q for an expected exit", report.message)
			default:
			}
		})
	}
}

func TestWatchVMIgnoresAReplacedVM(t *testing.T) {
	reporter := &fakeCrashReporter{reports: make(chan crashReport, 1)}
	attribution := resources.ActorAttribution{UID: "actor-a"}
	// the actor is hosted with a different VM than the one that died
	s := &AteWorkerService{
		locks:         actorlock.New(),
		actors:        map[string]*hostedActor{"actor-a": {attribution: attribution, vm: &runningActor{}}},
		crashReporter: reporter,
	}
	done := make(chan struct{})
	go func() {
		s.watchVM(attribution, nil, fakeVM(t, ""))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("watchVM did not return")
	}
	select {
	case report := <-reporter.reports:
		t.Fatalf("reported %q for a VM the actor no longer runs", report.message)
	default:
	}
}

func TestWatchVMReportsItsExecutionAfterRestart(t *testing.T) {
	const firstUID = "754f3dc4-2996-41c4-8e5a-a1b79e6678d5"
	const nextUID = "a6ba5a75-ad36-48fc-ad2e-e845527a25b1"
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	reporter := &fakeCrashReporter{
		reports: make(chan crashReport, 2),
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	t.Cleanup(func() { close(reporter.release) })
	attribution := resources.ActorAttribution{Ref: testActor.Ref, UID: uuid.NewString()}
	s := &AteWorkerService{
		locks:         actorlock.New(),
		actors:        map[string]*hostedActor{},
		maxActors:     1,
		crashReporter: reporter,
		tunnel:        &atunnel.Tunnel{Ingress: &ingress.Server{}, Egress: &egress.Proxy{}},
	}
	start := func(executionUID string) (*runningActor, *ateworkerpb.ActorDirs) {
		t.Helper()
		if _, _, err := s.admitActor(attribution, false, 0); err != nil {
			t.Fatal(err)
		}
		ra := fakeVM(t, executionUID)
		s.setRunningVM(attribution.UID, ra)
		return ra, &ateworkerpb.ActorDirs{RootDir: t.TempDir(), OciBundleDir: t.TempDir()}
	}

	first, firstDirs := start(firstUID)
	go s.watchVM(attribution, firstDirs, first)
	select {
	case <-reporter.started:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the watcher to report")
	}
	second, secondDirs := start(nextUID)
	if first.executionUID != firstUID || second.executionUID != nextUID {
		t.Fatalf("execution UIDs = %q, %q, want %q, %q", first.executionUID, second.executionUID, firstUID, nextUID)
	}
	select {
	case reporter.release <- struct{}{}:
	case <-ctx.Done():
		t.Fatal("timed out releasing the crash report")
	}
	var oldReport crashReport
	select {
	case oldReport = <-reporter.reports:
	case <-ctx.Done():
		t.Fatal("timed out receiving the crash report")
	}
	if oldReport.actor != attribution || oldReport.executionUID != first.executionUID || oldReport.message != "cloud-hypervisor exited: exit status 3" {
		t.Errorf("old crash report = %+v, want execution %q", oldReport, first.executionUID)
	}
	if s.runningVM(attribution.UID) != second {
		t.Fatal("old watcher removed the current execution")
	}

	go s.watchVM(attribution, secondDirs, second)
	select {
	case <-reporter.started:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the watcher to report")
	}
	select {
	case reporter.release <- struct{}{}:
	case <-ctx.Done():
		t.Fatal("timed out releasing the crash report")
	}
	var currentReport crashReport
	select {
	case currentReport = <-reporter.reports:
	case <-ctx.Done():
		t.Fatal("timed out receiving the crash report")
	}
	if currentReport.actor != attribution || currentReport.executionUID != second.executionUID || currentReport.message != "cloud-hypervisor exited: exit status 3" {
		t.Errorf("current crash report = %+v, want execution %q", currentReport, second.executionUID)
	}
}
