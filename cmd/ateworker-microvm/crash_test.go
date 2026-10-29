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
	"os/exec"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/child"
	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/resources"
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
