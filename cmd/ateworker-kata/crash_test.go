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
	"slices"
	"syscall"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/resources"
)

// hostWithVM hosts the actor with a VM on f, and returns what the crash watcher
// needs.
func hostWithVM(t *testing.T, s *AteWorkerService, f *fakeShim) (*hostedActor, *vm) {
	t.Helper()
	hosted, err := s.hostActor(t.Context(), testAttribution(testUID))
	if err != nil {
		t.Fatal(err)
	}
	v := testVM(f, "app")
	v.bundle = t.TempDir()
	s.setVM(testUID, v)
	return hosted, v
}

func expectReport(t *testing.T, r *fakeCrashReporter) resources.ActorAttribution {
	t.Helper()
	select {
	case a := <-r.reports:
		return a
	case <-time.After(10 * time.Second):
		t.Fatal("no crash report")
		return resources.ActorAttribution{}
	}
}

func TestWatchVMReportsACrash(t *testing.T) {
	s, tun := newTestService(t, 1)
	reporter := newFakeCrashReporter()
	s.crashReporter = reporter
	s.shimBinary = "/bin/true"
	f := newFakeShim(syscall.SIGKILL)
	hosted, v := hostWithVM(t, s, f)
	dirs := testActorDirs(t, testUID)

	done := s.watchVM(hosted, v, dirs)
	f.end()

	if got := expectReport(t, reporter); got.UID != testUID {
		t.Errorf("report for %q, want %q", got.UID, testUID)
	}
	<-done
	// The dead VM was cleaned up before the report.
	if s.lookupActor(testUID) != nil {
		t.Error("the crashed actor is still hosted")
	}
	if got := f.log(); !slices.Contains(got, "Shutdown "+testUID+" now=true") {
		t.Errorf("the dead VM was not shut down: %v", got)
	}
	if len(tun.sessions) != 1 {
		t.Errorf("sessions = %d, want 1", len(tun.sessions))
	}
}

func TestWatchVMIgnoresAnEndWeCaused(t *testing.T) {
	tests := []struct {
		name string
		// end makes the VM end the way a worker-caused end does.
		end func(s *AteWorkerService, h *hostedActor, f *fakeShim)
	}{
		{"actor terminated", func(_ *AteWorkerService, h *hostedActor, f *fakeShim) {
			h.stopping.Store(true)
			f.end()
		}},
		{"worker shutting down", func(s *AteWorkerService, _ *hostedActor, f *fakeShim) {
			s.shuttingDown.Store(true)
			f.end()
		}},
		{"actor replaced", func(s *AteWorkerService, h *hostedActor, f *fakeShim) {
			// A new activation takes the actor over while the old VM ends.
			h.cancel()
			f.end()
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestService(t, 1)
			reporter := newFakeCrashReporter()
			s.crashReporter = reporter
			f := newFakeShim()
			hosted, v := hostWithVM(t, s, f)
			done := s.watchVM(hosted, v, testActorDirs(t, testUID))
			tt.end(s, hosted, f)

			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("the watcher did not finish")
			}
			select {
			case <-reporter.reports:
				t.Error("a crash was reported")
			default:
			}
		})
	}
}
