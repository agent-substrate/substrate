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
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
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
	if err := os.WriteFile(pidFilePath(actorDirs, ocispec.PauseContainer), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
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
			s.watchSandbox(context.Background(), hosted, "", actorDirs, nil)
			select {
			case msg := <-reporter.reports:
				t.Fatalf("reported %q for an expected exit", msg)
			case <-time.After(500 * time.Millisecond):
			}
		})
	}
}
