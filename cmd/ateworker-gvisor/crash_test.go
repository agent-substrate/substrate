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
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/ocispec"
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

// exitingSandbox records a process that exits at once as the actor's sandbox.
func exitingSandbox(t *testing.T) *ateworkerpb.ActorDirs {
	t.Helper()
	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Wait() })
	actorDirs := &ateworkerpb.ActorDirs{RootDir: t.TempDir()}
	if err := os.MkdirAll(pidFileDir(actorDirs), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pidFilePath(actorDirs, ocispec.SandboxID), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
		t.Fatal(err)
	}
	return actorDirs
}

func TestWatchSandboxIgnoresExpectedExits(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*AteWorkerService, *hostedActor)
	}{
		{"stopped by ateworker", func(_ *AteWorkerService, h *hostedActor) { h.stopping.Store(true) }},
		{"worker shutting down", func(s *AteWorkerService, _ *hostedActor) { s.shuttingDown.Store(true) }},
		{"actor no longer hosted", func(s *AteWorkerService, _ *hostedActor) { delete(s.actors, "actor-a") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reporter := &fakeCrashReporter{reports: make(chan string, 1)}
			actorDirs := exitingSandbox(t)
			sandbox, err := openSandboxProcess(actorDirs)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(sandbox.Close)
			hosted := &hostedActor{attribution: resources.ActorAttribution{UID: "actor-a"}, sandbox: sandbox}
			s := &AteWorkerService{locks: actorlock.New(), actors: map[string]*hostedActor{"actor-a": hosted}, crashReporter: reporter}
			tc.setup(s, hosted)
			s.watchSandbox(context.Background(), hosted, "", nil, actorDirs, nil)
			select {
			case msg := <-reporter.reports:
				t.Fatalf("reported %q for an expected exit", msg)
			case <-time.After(500 * time.Millisecond):
			}
		})
	}
}

func TestReportObservedCrashIgnoresExpectedExits(t *testing.T) {
	attr := resources.ActorAttribution{UID: "actor-a"}
	for _, tc := range []struct {
		name  string
		setup func(*AteWorkerService, *hostedActor)
	}{
		{"stopping", func(_ *AteWorkerService, h *hostedActor) { h.stopping.Store(true) }},
		{"shutting down", func(s *AteWorkerService, _ *hostedActor) { s.shuttingDown.Store(true) }},
		{"replaced", func(_ *AteWorkerService, h *hostedActor) {
			ctx, cancel := context.WithCancel(context.Background())
			h.ctx, h.cancel = ctx, cancel
			cancel()
		}},
		{"no longer hosted", func(s *AteWorkerService, _ *hostedActor) { delete(s.actors, "actor-a") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reporter := &fakeCrashReporter{reports: make(chan string, 1)}
			hosted := &hostedActor{attribution: attr}
			s := &AteWorkerService{locks: actorlock.New(), actors: map[string]*hostedActor{"actor-a": hosted}, crashReporter: reporter}
			tc.setup(s, hosted)
			s.reportObservedCrash(hosted, "container app exited", true, 0, "", nil, nil)
			select {
			case msg := <-reporter.reports:
				t.Fatalf("reported %q for an expected exit", msg)
			default:
			}
		})
	}
}

type exitWait struct {
	exits   map[string]<-chan struct{}
	blocked chan struct{}
	block   string
	err     error
}

func (w exitWait) cmdWait(ctx context.Context, name string) error {
	if w.err != nil && w.exits[name] == nil {
		return w.err
	}
	if name == w.block {
		select {
		case w.blocked <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ch := w.exits[name]
	if ch == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

type blockWait struct {
	returned chan struct{}
}

func (b *blockWait) cmdWait(ctx context.Context, _ string) error {
	<-ctx.Done()
	close(b.returned)
	return ctx.Err()
}

func gvisorCrashService(t *testing.T, reporter *fakeCrashReporter) (*AteWorkerService, *ateworkerpb.ActorDirs) {
	t.Helper()
	flags, err := newFlagStore(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dirs := &ateworkerpb.ActorDirs{RootDir: t.TempDir(), OciBundleDir: t.TempDir()}
	s := &AteWorkerService{
		locks:         actorlock.New(),
		actors:        map[string]*hostedActor{},
		crashReporter: reporter,
		flags:         flags,
		tunnel:        &atunnel.Tunnel{Ingress: &ingress.Server{}, Egress: &egress.Proxy{}},
	}
	s.actors["actor-a"] = &hostedActor{
		attribution: resources.ActorAttribution{UID: "actor-a", Ref: resources.ActorRef{Atespace: "space", Name: "actor"}},
		ctx:         ctx,
		cancel:      cancel,
		usage:       workerstats.NewActivation(time.Now(), false),
	}
	return s, dirs
}

func TestWatchSandboxReportsContainerExit(t *testing.T) {
	reporter := &fakeCrashReporter{reports: make(chan string, 2)}
	s, dirs := gvisorCrashService(t, reporter)
	exit := make(chan struct{})
	close(exit)
	s.watchSandbox(context.Background(), s.actors["actor-a"], "", exitWait{exits: map[string]<-chan struct{}{"app": exit}}, dirs, []*ateworkerpb.Container{{Name: "app"}})

	select {
	case msg := <-reporter.reports:
		if msg != "container app exited" {
			t.Fatalf("report = %q", msg)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no crash report")
	}
	select {
	case extra := <-reporter.reports:
		t.Fatalf("second report %q", extra)
	default:
	}
	if s.lookupActor("actor-a") != nil {
		t.Fatal("actor still hosted after its container exited")
	}
}

func TestWatchSandboxWaitsForLastContainer(t *testing.T) {
	reporter := &fakeCrashReporter{reports: make(chan string, 1)}
	s, dirs := gvisorCrashService(t, reporter)
	app := make(chan struct{})
	close(app)
	side := make(chan struct{})
	blocked := make(chan struct{})
	s.watchSandbox(context.Background(), s.actors["actor-a"], "", exitWait{
		exits:   map[string]<-chan struct{}{"app": app, "side": side},
		blocked: blocked,
		block:   "side",
	}, dirs, []*ateworkerpb.Container{{Name: "app"}, {Name: "side"}})

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
	case msg := <-reporter.reports:
		if !strings.Contains(msg, "container app exited") || !strings.Contains(msg, "container side exited") {
			t.Fatalf("report = %q", msg)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no crash report")
	}
}

func TestWatchSandboxContainerWaitEndsWhenSandboxExits(t *testing.T) {
	reporter := &fakeCrashReporter{reports: make(chan string, 1)}
	s, dirs := gvisorCrashService(t, reporter)
	actorDirs := exitingSandbox(t)
	sandbox, err := openSandboxProcess(actorDirs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sandbox.Close)
	hosted := s.actors["actor-a"]
	hosted.sandbox = sandbox
	returned := make(chan struct{})
	s.watchSandbox(context.Background(), hosted, "", &blockWait{returned: returned}, dirs, []*ateworkerpb.Container{{Name: "app"}})

	select {
	case msg := <-reporter.reports:
		if msg != "gVisor sandbox exited" {
			t.Fatalf("report = %q", msg)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no crash report")
	}
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("container wait still blocked after the sandbox watcher returned")
	}
}

func TestWatchSandboxContainerWaitFailureIsNotAnExit(t *testing.T) {
	reporter := &fakeCrashReporter{reports: make(chan string, 1)}
	s, dirs := gvisorCrashService(t, reporter)
	actorDirs := exitingSandbox(t)
	sandbox, err := openSandboxProcess(actorDirs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sandbox.Close)
	hosted := s.actors["actor-a"]
	hosted.sandbox = sandbox
	s.watchSandbox(context.Background(), hosted, "", exitWait{err: errors.New("runsc wait exploded")}, dirs, []*ateworkerpb.Container{{Name: "app"}})

	select {
	case msg := <-reporter.reports:
		if strings.Contains(msg, "container") {
			t.Fatalf("report = %q, want the sandbox exit", msg)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no crash report")
	}
}
