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
	s := newStatsService(t, nil)
	s.crashReporter = reporter
	s.terminateDead = func(ctx context.Context, _ resources.ActorRef, actorUID, _ string, _ *ateworkerpb.ActorDirs, _ []*ateworkerpb.Container, _ bool) error {
		return s.unhostActor(ctx, actorUID)
	}
	start := func(executionUID string) (*hostedActor, *ateworkerpb.ActorDirs) {
		t.Helper()
		hosted, _, err := s.admitActor(testActor, executionUID, 0)
		if err != nil {
			t.Fatal(err)
		}
		actorDirs := exitingSandbox(t)
		s.pinSandbox(t.Context(), hosted, actorDirs)
		return hosted, actorDirs
	}

	first, firstDirs := start(firstUID)
	s.watchSandbox(t.Context(), first, "", firstDirs, nil)
	select {
	case <-reporter.started:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the old execution's report")
	}
	second, secondDirs := start(nextUID)
	select {
	case reporter.release <- struct{}{}:
	case <-ctx.Done():
		t.Fatal("timed out releasing the old execution's report")
	}
	select {
	case report := <-reporter.reports:
		if report.actor != testActor || report.executionUID != firstUID || report.message != "gVisor sandbox exited" {
			t.Errorf("old crash report = %+v, want execution %q", report, firstUID)
		}
	case <-ctx.Done():
		t.Fatal("timed out receiving the old execution's report")
	}
	if s.lookupActor(testActor.UID) != second {
		t.Fatal("old watcher removed the current execution")
	}

	s.watchSandbox(t.Context(), second, "", secondDirs, nil)
	select {
	case <-reporter.started:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the current execution's report")
	}
	select {
	case reporter.release <- struct{}{}:
	case <-ctx.Done():
		t.Fatal("timed out releasing the current execution's report")
	}
	select {
	case report := <-reporter.reports:
		if report.actor != testActor || report.executionUID != nextUID || report.message != "gVisor sandbox exited" {
			t.Errorf("current crash report = %+v, want execution %q", report, nextUID)
		}
	case <-ctx.Done():
		t.Fatal("timed out receiving the current execution's report")
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
	if got := h.usage.Latest(); !got.GetMemoryUnmeasured() || got.GetMemoryCurrentBytes() != 0 || got.GetMemoryWorkingSetBytes() != 0 {
		t.Errorf("stored memory = %d/%d unmeasured=%v, want zero and unmeasured", got.GetMemoryCurrentBytes(), got.GetMemoryWorkingSetBytes(), got.GetMemoryUnmeasured())
	}
}

func TestReadDeadFinal(t *testing.T) {
	s, h, _ := crashFixture(t)
	s.readDeadFinal(context.Background(), h)
	got := h.usage.Latest()
	if got.GetCpuUsageUsec() != 2000000 {
		t.Errorf("CPU = %d, want the 2000000 read after the death", got.GetCpuUsageUsec())
	}
	if !got.GetMemoryUnmeasured() || got.GetMemoryCurrentBytes() != 0 || got.GetMemoryWorkingSetBytes() != 0 {
		t.Errorf("memory = %d/%d unmeasured=%v, want zero and unmeasured: not measured at the death", got.GetMemoryCurrentBytes(), got.GetMemoryWorkingSetBytes(), got.GetMemoryUnmeasured())
	}
}

// TestWatchSandboxCrashWritesFinal drives the watcher through a sandbox that
// dies on its own: the actor is torn down after its cgroup is read, its final
// record carries the CPU and memory peak at death but no memory usage or
// working set, and the crash is reported.
func TestWatchSandboxCrashWritesFinal(t *testing.T) {
	s, h, leaf := crashFixture(t)
	rec := withUsageRecorder(s)
	reporter := &fakeCrashReporter{reports: make(chan crashReport, 1)}
	s.crashReporter = reporter
	actorDirs := exitingSandbox(t)
	sandbox, err := openSandboxProcess(actorDirs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sandbox.Close)
	h.sandbox = sandbox
	s.terminateDead = func(context.Context, resources.ActorRef, string, string, *ateworkerpb.ActorDirs, []*ateworkerpb.Container, bool) error {
		setHostedActor(s, nil)
		return os.RemoveAll(leaf) // as runsc delete does
	}

	s.watchSandbox(context.Background(), h, "", actorDirs, nil)
	select {
	case report := <-reporter.reports:
		if report.message != "gVisor sandbox exited" || report.executionUID != h.executionUID {
			t.Errorf("crash report = %+v, want execution %q", report, h.executionUID)
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
