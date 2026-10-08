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

package ateomstats

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/agent-substrate/substrate/internal/actorevent"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
)

var testPool = Pool{Namespace: "team-a", Name: "default"}

func measuredSample() *ateworkerpb.WorkloadStatsSample {
	return &ateworkerpb.WorkloadStatsSample{
		Atespace: "ns", ActorName: "a", ActorUid: "uid-1", ActorTemplateAtespace: "ns", ActorTemplateName: "t",
		SandboxClass:          ateworkerpb.SandboxClass_SANDBOX_CLASS_GVISOR,
		Source:                ateworkerpb.StatsSource_STATS_SOURCE_CGROUP,
		MemoryCurrentBytes:    40 << 20,
		MemoryPeakBytes:       48 << 20,
		MemoryWorkingSetBytes: 32 << 20,
		CpuUsageUsec:          1_500_000,
		ObservedAtUnixNano:    1_700_000_000_000_000_000,
		EpochUnixNano:         1_699_999_990_000_000_000,
	}
}

func attrMap(attrs []slog.Attr) map[string]slog.Value {
	m := make(map[string]slog.Value, len(attrs))
	for _, a := range attrs {
		m[a.Key] = a.Value
	}
	return m
}

// checkShape holds the attributes to the event's registered shape: every
// required key, nothing outside the required and conditional sets.
func checkShape(t *testing.T, got map[string]slog.Value) {
	t.Helper()
	ev := actorevent.UsageSampled
	for _, k := range ev.Keys {
		if _, ok := got[k]; !ok {
			t.Errorf("required key %q is missing", k)
		}
	}
	for k := range got {
		if !slices.Contains(ev.Keys, k) && !slices.Contains(ev.Conditional, k) {
			t.Errorf("key %q is not declared on %s", k, ev.Name)
		}
	}
}

func TestUsageAttrsMeasured(t *testing.T) {
	t.Parallel()
	got := attrMap(UsageAttrs(testPool, ateattr.StatsKindPeriodic, measuredSample()))
	checkShape(t, got)
	for _, k := range actorevent.UsageSampled.Conditional {
		if _, ok := got[k]; !ok {
			t.Errorf("measured record is missing %q", k)
		}
	}
	if v := got[string(ateattr.StatsCPUTimeKey)]; v.Kind() != slog.KindFloat64 || v.Float64() != 1.5 {
		t.Errorf("cpu.time = %v, want 1.5 seconds as a float", v)
	}
	for k, want := range map[string]string{
		string(ateattr.WorkerPoolNamespaceKey): "team-a",
		string(ateattr.WorkerPoolNameKey):      "default",
		string(ateattr.SandboxClassKey):        "gvisor",
		string(ateattr.StatsSourceKey):         ateattr.StatsSourceCgroup,
		string(ateattr.StatsKindKey):           ateattr.StatsKindPeriodic,
		string(ateattr.ActorUIDKey):            "uid-1",
	} {
		if got[k].String() != want {
			t.Errorf("%s = %q, want %q", k, got[k].String(), want)
		}
	}
	if got := got[string(ateattr.ActorEpochKey)].Int64(); got != 1_699_999_990_000_000_000 {
		t.Errorf("epoch = %d", got)
	}
}

func TestUsageAttrsPendingHasNoMeasurements(t *testing.T) {
	t.Parallel()
	s := measuredSample()
	s.Source = ateworkerpb.StatsSource_STATS_SOURCE_UNSPECIFIED
	got := attrMap(UsageAttrs(testPool, ateattr.StatsKindPeriodic, s))
	checkShape(t, got)
	for _, k := range actorevent.UsageSampled.Conditional {
		if _, ok := got[k]; ok {
			t.Errorf("pending record carries %q", k)
		}
	}
}

func TestUsageAttrsOmitsUnreportedPeak(t *testing.T) {
	t.Parallel()
	s := measuredSample()
	s.MemoryPeakBytes = 0
	got := attrMap(UsageAttrs(testPool, ateattr.StatsKindPeriodic, s))
	if _, ok := got[string(ateattr.StatsMemoryPeakKey)]; ok {
		t.Error("record carries a peak the source did not report")
	}
	if _, ok := got[string(ateattr.StatsMemoryUsageKey)]; !ok {
		t.Error("record lost memory.usage along with the peak")
	}
}

// recorder is a stdout handler that keeps what it is given.
type recorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (r *recorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *recorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec)
	return nil
}
func (r *recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *recorder) WithGroup(string) slog.Handler      { return r }
func (r *recorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.records)
}

func newTestEmitter(flush func(context.Context) error) (*UsageEmitter, *recorder) {
	rec := &recorder{}
	e := NewUsageEmitter(nil, rec, testPool)
	e.flush = flush
	return e, rec
}

func TestEmitDatesRecordAtReadTime(t *testing.T) {
	t.Parallel()
	e, rec := newTestEmitter(nil)
	s := measuredSample()
	e.Emit(context.Background(), ateattr.StatsKindInitial, s)
	if rec.len() != 1 {
		t.Fatalf("wrote %d records, want 1", rec.len())
	}
	if got, want := rec.records[0].Time, time.Unix(0, s.GetObservedAtUnixNano()); !got.Equal(want) {
		t.Errorf("record time = %v, want the read time %v", got, want)
	}
}

func TestNilEmitterWritesNothing(t *testing.T) {
	t.Parallel()
	var e *UsageEmitter
	e.Emit(context.Background(), ateattr.StatsKindInitial, measuredSample())
	e.EmitFinal(context.Background(), measuredSample())
}

func TestEmitFinalFlushes(t *testing.T) {
	t.Parallel()
	deadlines := make(chan time.Time, 1)
	e, rec := newTestEmitter(func(ctx context.Context) error {
		d, _ := ctx.Deadline()
		deadlines <- d
		return nil
	})
	start := time.Now()
	e.EmitFinal(context.Background(), measuredSample())
	if rec.len() != 1 {
		t.Fatalf("wrote %d records, want 1", rec.len())
	}
	if kind := attrMap(recordAttrs(rec.records[0]))[string(ateattr.StatsKindKey)].String(); kind != ateattr.StatsKindFinal {
		t.Errorf("kind = %q, want final", kind)
	}
	select {
	case d := <-deadlines:
		if d.IsZero() || d.Sub(start) > finalFlushTimeout+time.Second {
			t.Errorf("flush deadline = %v, want about %v after the record", d, finalFlushTimeout)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("final record was not flushed")
	}
}

func recordAttrs(r slog.Record) []slog.Attr {
	var attrs []slog.Attr
	r.Attrs(func(a slog.Attr) bool { attrs = append(attrs, a); return true })
	return attrs
}

// TestStartSamplerSweepsOnSchedule runs six intervals and stops: six sweeps,
// and none after the stop returns.
func TestStartSamplerSweepsOnSchedule(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const interval = time.Minute
		sweeps := 0
		stop := StartSampler(context.Background(), interval, func(context.Context) { sweeps++ })
		time.Sleep(6*interval + time.Second)
		stop()
		time.Sleep(2 * interval)
		if sweeps != 6 {
			t.Errorf("swept %d times, want 6", sweeps)
		}
	})
}

// TestStartSamplerSurvivesAPanic pins that a sweep that panics skips its tick
// and the sampler keeps going.
func TestStartSamplerSurvivesAPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const interval = time.Minute
		sweeps := 0
		stop := StartSampler(context.Background(), interval, func(context.Context) {
			sweeps++
			if sweeps == 1 {
				panic("first sweep fails")
			}
		})
		time.Sleep(3*interval + time.Second)
		stop()
		if sweeps != 3 {
			t.Errorf("swept %d times, want 3: the panic must not stop the sampler", sweeps)
		}
	})
}

func TestLabels(t *testing.T) {
	t.Parallel()
	for c, want := range map[ateworkerpb.SandboxClass]string{
		ateworkerpb.SandboxClass_SANDBOX_CLASS_GVISOR:      "gvisor",
		ateworkerpb.SandboxClass_SANDBOX_CLASS_MICROVM:     "microvm",
		ateworkerpb.SandboxClass_SANDBOX_CLASS_UNSPECIFIED: ateattr.SandboxClassUnknown,
	} {
		if got := SandboxClassLabel(c); got != want {
			t.Errorf("SandboxClassLabel(%v) = %q, want %q", c, got, want)
		}
	}
	for src, want := range map[ateworkerpb.StatsSource]string{
		ateworkerpb.StatsSource_STATS_SOURCE_CGROUP:      ateattr.StatsSourceCgroup,
		ateworkerpb.StatsSource_STATS_SOURCE_GUEST_AGENT: ateattr.StatsSourceGuestAgent,
		ateworkerpb.StatsSource_STATS_SOURCE_UNSPECIFIED: ateattr.StatsSourceUnspecified,
	} {
		if got := StatsSourceLabel(src); got != want {
			t.Errorf("StatsSourceLabel(%v) = %q, want %q", src, got, want)
		}
	}
}
