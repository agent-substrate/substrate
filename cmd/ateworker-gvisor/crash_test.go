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
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/ateattr"
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
			s.watchSandbox(context.Background(), hosted, "", actorDirs, nil)
			select {
			case msg := <-reporter.reports:
				t.Fatalf("reported %q for an expected exit", msg)
			case <-time.After(500 * time.Millisecond):
			}
		})
	}
}

// crashFixture hosts testActor on a sweep-sampled cgroup fixture, then makes its
// sandbox die: the cgroup's CPU rises past the last sample and its memory drops
// to what a cgroup with no processes left holds.
func crashFixture(t *testing.T) (*AteWorkerService, *hostedActor, string) {
	t.Helper()
	s := newStatsService(t, healthyCgroup)
	h := hostWithEpoch(s, time.Now(), true)
	s.sweepUsage(context.Background())
	leaf := filepath.Join(s.cgroupRoot, ocispec.GVisorCgroupLeaf(testActor.UID, sandboxCgroupContainer))
	for name, content := range map[string]string{"cpu.stat": "usage_usec 2000000\n", "memory.current": "16384\n", "memory.stat": "", "cgroup.events": "populated 0\n"} {
		if err := os.WriteFile(filepath.Join(leaf, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return s, h, leaf
}

// TestSweepAfterDeathLeavesMemoryOut pins that a sweep reading the empty cgroup
// of a sandbox that died, before the crash path removes it, writes its CPU but
// no memory usage or working set, and stores no memory for the final record.
func TestSweepAfterDeathLeavesMemoryOut(t *testing.T) {
	s, h, _ := crashFixture(t)
	rec := withUsageRecorder(s)
	s.sweepUsage(context.Background())
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindPeriodic {
		t.Fatalf("records = %v, want one periodic", got)
	}
	if got := rec.Value(0, string(ateattr.StatsCPUTimeKey)); got != "2" {
		t.Errorf("periodic cpu.time = %q, want 2", got)
	}
	for _, key := range []attribute.Key{ateattr.StatsMemoryUsageKey, ateattr.StatsMemoryWorkingSetKey} {
		if got := rec.Value(0, string(key)); got != "" {
			t.Errorf("periodic %s = %q, want absent", key, got)
		}
	}
	if got := h.usage.Latest(); got.MemoryCurrentBytes != nil || got.MemoryWorkingSetBytes != nil {
		t.Errorf("stored memory = %d/%d, want both unset", got.GetMemoryCurrentBytes(), got.GetMemoryWorkingSetBytes())
	}
}

func TestReadDeadFinal(t *testing.T) {
	s, h, _ := crashFixture(t)
	s.readDeadFinal(context.Background(), h)
	got := h.usage.Latest()
	if got.GetCpuUsageUsec() != 2000000 {
		t.Errorf("CPU = %d, want the 2000000 read after the death", got.GetCpuUsageUsec())
	}
	if got.MemoryCurrentBytes != nil || got.MemoryWorkingSetBytes != nil {
		t.Errorf("memory = %d/%d, want both unset: not measured at the death", got.GetMemoryCurrentBytes(), got.GetMemoryWorkingSetBytes())
	}
}

// TestWatchSandboxCrashWritesFinal drives the watcher through a sandbox that
// dies on its own: the actor is torn down after its cgroup is read, its final
// record carries the CPU and memory peak at death but no memory usage or
// working set, and the crash is reported.
func TestWatchSandboxCrashWritesFinal(t *testing.T) {
	s, h, leaf := crashFixture(t)
	rec := withUsageRecorder(s)
	reporter := &fakeCrashReporter{reports: make(chan string, 1)}
	s.crashReporter = reporter
	actorDirs := exitingSandbox(t)
	sandbox, err := openSandboxProcess(actorDirs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sandbox.Close)
	h.sandbox = sandbox
	s.terminateDead = func(context.Context, resources.ActorRef, string, string, *ateworkerpb.ActorDirs, []*ateworkerpb.Container) error {
		setHostedActor(s, nil)
		return os.RemoveAll(leaf) // as runsc delete does
	}

	s.watchSandbox(context.Background(), h, "", actorDirs, nil)
	select {
	case msg := <-reporter.reports:
		if msg != "gVisor sandbox exited" {
			t.Errorf("reported %q", msg)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("crash not reported")
	}
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
		t.Fatalf("records = %v, want one final", got)
	}
	if got := rec.Value(0, string(ateattr.StatsCPUTimeKey)); got != "2" {
		t.Errorf("final cpu.time = %q, want 2 (seconds) read after the death", got)
	}
	for _, key := range []attribute.Key{ateattr.StatsMemoryUsageKey, ateattr.StatsMemoryWorkingSetKey} {
		if got := rec.Value(0, string(key)); got != "" {
			t.Errorf("final %s = %q, want absent", key, got)
		}
	}
	if got := rec.Value(0, string(ateattr.StatsMemoryPeakKey)); got == "" {
		t.Error("final memory.peak absent, want the cgroup's high-water mark")
	}
}

// TestEndActivationLiveFinalKeepsMemory pins that a final record read while the
// sandbox runs, as Terminate's is, keeps every measurement.
func TestEndActivationLiveFinalKeepsMemory(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), true)
	if err := s.endActivation(context.Background(), h, s.readFinal, func() error {
		setHostedActor(s, nil)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
		t.Fatalf("records = %v, want one final", got)
	}
	for _, key := range []attribute.Key{ateattr.StatsCPUTimeKey, ateattr.StatsMemoryUsageKey, ateattr.StatsMemoryWorkingSetKey, ateattr.StatsMemoryPeakKey} {
		if rec.Value(0, string(key)) == "" {
			t.Errorf("final %s absent, want it measured", key)
		}
	}
}
