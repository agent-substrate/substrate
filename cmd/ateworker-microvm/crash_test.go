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
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/child"
	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/workerstats"
)

type fakeCrashReporter struct {
	reports chan string
}

func (f *fakeCrashReporter) ReportCrashed(_ context.Context, _ resources.ActorAttribution, message string) error {
	f.reports <- message
	return nil
}

// fakeVM is a running actor whose VMM exits at once and whose virtiofsd runs
// until the test ends.
func fakeVM(t *testing.T) *runningActor {
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
	return &runningActor{chProc: chProc, vfsdProc: vfsdProc}
}

func TestWatchVMIgnoresTeardown(t *testing.T) {
	reporter := &fakeCrashReporter{reports: make(chan string, 1)}
	s := &AteWorkerService{locks: actorlock.New(), actors: map[string]*hostedActor{}, crashReporter: reporter}
	ra := fakeVM(t)
	ra.stopping.Store(true)
	s.watchVM(resources.ActorAttribution{UID: "actor-a"}, nil, ra)
	select {
	case msg := <-reporter.reports:
		t.Fatalf("reported %q for a VM being torn down", msg)
	default:
	}
}

// sleepingVM is a running actor whose VMM and virtiofsd stay up until stopped.
func sleepingVM(t *testing.T) *runningActor {
	t.Helper()
	chProc, err := child.Start(exec.Command("sleep", "30"))
	if err != nil {
		t.Fatal(err)
	}
	vfsdProc, err := child.Start(exec.Command("sleep", "30"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		chProc.Stop()
		vfsdProc.Stop()
	})
	return &runningActor{chProc: chProc, vfsdProc: vfsdProc}
}

// scriptedAgent is a guestClient whose WaitProcess blocks on a per-container
// channel. A nil channel with err set returns that error at once.
type scriptedAgent struct {
	exits map[string]<-chan struct{}
	codes map[string]int32
	err   error
}

func (a scriptedAgent) SignalProcess(context.Context, string, string, uint32) error { return nil }
func (a scriptedAgent) Close() error                                                { return nil }
func (a scriptedAgent) WaitProcess(ctx context.Context, id, _ string) (int32, error) {
	ch := a.exits[id]
	if ch == nil {
		if a.err != nil {
			return 0, a.err
		}
		return a.codes[id], nil
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-ch:
		return a.codes[id], a.err
	}
}

// waitAgent signals blocked when WaitProcess reaches blockID, then waits for
// that container's exit channel.
type waitAgent struct {
	exits   map[string]<-chan struct{}
	codes   map[string]int32
	blocked chan struct{}
	blockID string
}

func (a waitAgent) SignalProcess(context.Context, string, string, uint32) error { return nil }
func (a waitAgent) Close() error                                                { return nil }
func (a waitAgent) WaitProcess(ctx context.Context, id, _ string) (int32, error) {
	if id == a.blockID {
		select {
		case a.blocked <- struct{}{}:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-a.exits[id]:
		return a.codes[id], nil
	}
}

// mixedAgent fails one wait immediately and blocks the other until ctx ends.
type mixedAgent struct {
	returned chan struct{}
}

func (a *mixedAgent) SignalProcess(context.Context, string, string, uint32) error { return nil }
func (a *mixedAgent) Close() error                                                { return nil }
func (a *mixedAgent) WaitProcess(ctx context.Context, id, _ string) (int32, error) {
	if id == "hung" {
		<-ctx.Done()
		close(a.returned)
		return 0, ctx.Err()
	}
	return 0, errors.New("agent closed")
}

func crashFixture(t *testing.T, reporter *fakeCrashReporter) (*AteWorkerService, *ateworkerpb.ActorDirs) {
	t.Helper()
	return &AteWorkerService{
		locks:         actorlock.New(),
		actors:        map[string]*hostedActor{},
		crashReporter: reporter,
		tunnel:        &atunnel.Tunnel{Ingress: &ingress.Server{}, Egress: &egress.Proxy{}},
	}, &ateworkerpb.ActorDirs{RootDir: t.TempDir(), OciBundleDir: t.TempDir()}
}

func hostVM(s *AteWorkerService, ra *runningActor) resources.ActorAttribution {
	attr := resources.ActorAttribution{UID: "actor-a", Ref: resources.ActorRef{Atespace: "space", Name: "actor"}}
	s.actors[attr.UID] = &hostedActor{
		attribution: attr,
		vm:          ra,
		usage:       workerstats.NewActivation(time.Now(), false),
	}
	return attr
}

func awaitReport(t *testing.T, reports <-chan string) string {
	t.Helper()
	select {
	case msg := <-reports:
		return msg
	case <-time.After(10 * time.Second):
		t.Fatal("no crash report")
	}
	return ""
}

func TestWatchVMReportsContainerExit(t *testing.T) {
	reporter := &fakeCrashReporter{reports: make(chan string, 2)}
	s, dirs := crashFixture(t, reporter)
	ra := sleepingVM(t)
	exit := make(chan struct{})
	close(exit)
	ra.workloadIDs = []string{"app"}
	ra.guestAgent = scriptedAgent{exits: map[string]<-chan struct{}{"app": exit}, codes: map[string]int32{"app": 1}}
	attr := hostVM(s, ra)

	done := make(chan struct{})
	go func() {
		s.watchVM(attr, dirs, ra)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("watchVM did not return")
	}

	msg := awaitReport(t, reporter.reports)
	if msg != "container app exited: status 1" {
		t.Fatalf("report = %q", msg)
	}
	select {
	case extra := <-reporter.reports:
		t.Fatalf("second report %q", extra)
	default:
	}
	if s.lookupActor(attr.UID) != nil {
		t.Fatal("actor still hosted after its container exited")
	}
	select {
	case <-ra.chProc.Done():
	default:
		t.Fatal("VM still running after its container exited")
	}
}

func TestWatchVMWaitsForLastContainer(t *testing.T) {
	reporter := &fakeCrashReporter{reports: make(chan string, 1)}
	s, dirs := crashFixture(t, reporter)
	ra := sleepingVM(t)
	app := make(chan struct{})
	close(app)
	side := make(chan struct{})
	blocked := make(chan struct{})
	ra.workloadIDs = []string{"app", "side"}
	ra.guestAgent = waitAgent{
		exits:   map[string]<-chan struct{}{"app": app, "side": side},
		codes:   map[string]int32{"app": 1, "side": 0},
		blocked: blocked,
		blockID: "side",
	}
	attr := hostVM(s, ra)

	done := make(chan struct{})
	go func() {
		s.watchVM(attr, dirs, ra)
		close(done)
	}()
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("did not wait on the remaining container")
	}
	select {
	case msg := <-reporter.reports:
		t.Fatalf("reported %q before the last container exited", msg)
	default:
	}
	close(side)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("watchVM did not return")
	}
	msg := awaitReport(t, reporter.reports)
	if !strings.Contains(msg, "container app exited: status 1") || !strings.Contains(msg, "container side exited: status 0") {
		t.Fatalf("report = %q", msg)
	}
}

func TestWatchVMContainerExitDuringShutdown(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*AteWorkerService, *runningActor)
	}{
		{"shutting down", func(s *AteWorkerService, _ *runningActor) { s.shuttingDown.Store(true) }},
		{"stopping", func(_ *AteWorkerService, ra *runningActor) { ra.stopping.Store(true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reporter := &fakeCrashReporter{reports: make(chan string, 1)}
			s, dirs := crashFixture(t, reporter)
			ra := sleepingVM(t)
			exit := make(chan struct{})
			close(exit)
			ra.workloadIDs = []string{"app"}
			ra.guestAgent = scriptedAgent{exits: map[string]<-chan struct{}{"app": exit}, codes: map[string]int32{"app": 1}}
			tc.setup(s, ra)
			attr := hostVM(s, ra)

			done := make(chan struct{})
			go func() {
				s.watchVM(attr, dirs, ra)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("watchVM did not return")
			}

			select {
			case msg := <-reporter.reports:
				t.Fatalf("reported %q", msg)
			default:
			}
			select {
			case <-ra.chProc.Done():
				t.Fatal("tore the VM down")
			default:
			}
		})
	}
}

func TestWatchVMWaitErrorIsNotContainerExit(t *testing.T) {
	reporter := &fakeCrashReporter{reports: make(chan string, 2)}
	s, dirs := crashFixture(t, reporter)
	ra := fakeVM(t)
	returned := make(chan struct{})
	ra.workloadIDs = []string{"app", "hung"}
	ra.guestAgent = &mixedAgent{returned: returned}
	attr := hostVM(s, ra)

	done := make(chan struct{})
	go func() {
		s.watchVM(attr, dirs, ra)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("watchVM did not return")
	}

	msg := awaitReport(t, reporter.reports)
	if !strings.Contains(msg, "cloud-hypervisor exited") {
		t.Fatalf("report = %q, want the VM exit", msg)
	}
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("workload wait still blocked after the VM watcher returned")
	}
}

func TestWatchVMIgnoresAReplacedVM(t *testing.T) {
	reporter := &fakeCrashReporter{reports: make(chan string, 1)}
	attribution := resources.ActorAttribution{UID: "actor-a"}
	// the actor is hosted with a different VM than the one that died
	s := &AteWorkerService{
		locks:         actorlock.New(),
		actors:        map[string]*hostedActor{"actor-a": {attribution: attribution, vm: &runningActor{}}},
		crashReporter: reporter,
	}
	done := make(chan struct{})
	go func() {
		s.watchVM(attribution, nil, fakeVM(t))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("watchVM did not return")
	}
	select {
	case msg := <-reporter.reports:
		t.Fatalf("reported %q for a VM the actor no longer runs", msg)
	default:
	}
}
