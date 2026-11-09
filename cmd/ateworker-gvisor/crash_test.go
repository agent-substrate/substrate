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
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/ocispec"
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
			reporter := &fakeCrashReporter{reports: make(chan crashReport, 1)}
			actorDirs := exitingSandbox(t)
			sandbox, err := openSandboxProcess(actorDirs)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(sandbox.Close)
			hosted := &hostedActor{attribution: resources.ActorAttribution{UID: "actor-a"}, sandbox: sandbox}
			s := &AteWorkerService{locks: actorlock.New(), actors: map[string]*hostedActor{"actor-a": hosted}, crashReporter: reporter}
			tc.setup(s, hosted)
			s.watchSandbox(context.Background(), hosted, "", actorDirs, nil)
			select {
			case report := <-reporter.reports:
				t.Fatalf("reported %q for an expected exit", report.message)
			case <-time.After(500 * time.Millisecond):
			}
		})
	}
}

func TestWatchSandboxReportsItsExecutionAfterRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	reporter := &fakeCrashReporter{
		reports: make(chan crashReport, 2),
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	t.Cleanup(func() { close(reporter.release) })
	s := newStatsService(t, nil)
	s.crashReporter = reporter
	s.tunnel = &atunnel.Tunnel{Ingress: &ingress.Server{}, Egress: &egress.Proxy{}}
	s.flags = &flagStore{cfg: &flagConfig{}}
	runscPath, err := exec.LookPath("true")
	if err != nil {
		t.Fatal(err)
	}
	start := func() (*hostedActor, *ateworkerpb.ActorDirs) {
		t.Helper()
		hosted, _, err := s.admitActor(testActor, 0)
		if err != nil {
			t.Fatal(err)
		}
		actorDirs := exitingSandbox(t)
		actorDirs.OciBundleDir = t.TempDir()
		s.pinSandbox(t.Context(), hosted, actorDirs)
		return hosted, actorDirs
	}

	first, firstDirs := start()
	s.watchSandbox(t.Context(), first, runscPath, firstDirs, nil)
	select {
	case <-reporter.started:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the watcher to report")
	}
	second, secondDirs := start()
	if first.executionUID == "" || second.executionUID == "" || first.executionUID == second.executionUID {
		t.Fatalf("execution UIDs = %q, %q, want distinct nonempty values", first.executionUID, second.executionUID)
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
	if oldReport.actor != testActor || oldReport.executionUID != first.executionUID || oldReport.message != "gVisor sandbox exited" {
		t.Errorf("old crash report = %+v, want execution %q", oldReport, first.executionUID)
	}
	if s.lookupActor(testActor.UID) != second {
		t.Fatal("old watcher removed the current execution")
	}

	s.watchSandbox(t.Context(), second, runscPath, secondDirs, nil)
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
	if currentReport.actor != testActor || currentReport.executionUID != second.executionUID || currentReport.message != "gVisor sandbox exited" {
		t.Errorf("current crash report = %+v, want execution %q", currentReport, second.executionUID)
	}
}
