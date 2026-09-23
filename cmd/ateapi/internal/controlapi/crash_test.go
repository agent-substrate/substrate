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

package controlapi

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/actorevent"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// seedActor stores a running actor with all worker-binding fields populated, so
// tests can assert they are cleared when the actor crashes.
func seedActor(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef) {
	t.Helper()

	storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: actorRef.Name, Atespace: actorRef.Atespace},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: "uid"},
				WorkerNamespace: "ns",
				WorkerPool:      "pool",
				WorkerPod:       "pod",
				WorkerPodUid:    "uid",
				WorkerPodIp:     "1.2.3.4",
			},
			InProgressSnapshotUri: "gs://bucket/atespaces/as/actors/uid/snapshots/reserved-snapshot",
		},
	})
}

// seedWorker registers the worker referenced by seedActor's binding fields,
// assigned to the given actor (unassigned if assigned is the zero ActorRef).
func seedWorker(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef) {
	t.Helper()
	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "uid"},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod",
		WorkerPodUid:    "uid",
		Status:          &ateapipb.WorkerStatus{},
	}
	if _, err := st.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("seed worker: %v", err)
	}
	if actorRef == (resources.ActorRef{}) {
		return
	}
	assignment := &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorUid: "synthetic-" + actorRef.Name,
	}
	if actor, err := st.GetActor(ctx, actorRef); err == nil {
		assignment = &ateapipb.ActorAssignment{
			Actor:    &ateapipb.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()},
			ActorUid: actor.GetMetadata().GetUid(),
		}
	}
	seedAssignment(t, st, "uid", assignment)
}

// seedUnboundActor stores a running actor whose worker-binding fields were
// already cleared, e.g. by a prior release.
func seedUnboundActor(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef) {
	t.Helper()
	storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: actorRef.Name, Atespace: actorRef.Atespace},
		Status: &ateapipb.ActorStatus{
			State:                 ateapipb.ActorState_ACTOR_STATE_RUNNING,
			InProgressSnapshotUri: "gs://bucket/atespaces/as/actors/uid/snapshots/reserved-snapshot",
		},
	})
}

// assertCrashed reloads the actor and verifies it is CRASHED with its worker
// binding cleared.
func assertCrashed(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef) {
	t.Helper()
	got, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor(%v) = %v, want nil", actorRef, err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("status = %v, want %v", got.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_CRASHED)
	}
	// Keep the snapshot uri for debugging.
	if got.GetStatus().GetInProgressSnapshotUri() == "" {
		t.Error(`InProgressSnapshotUri = "", want preserved`)
	}
	if got.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("WorkerAssignment = %v, want cleared", got.GetStatus().GetWorkerAssignment())
	}
}

func TestCrashActor(t *testing.T) {
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

	tests := []struct {
		name string
		seed bool
		// setup runs after the actor is seeded, e.g. to register a worker.
		setup func(t *testing.T, ctx context.Context, st store.Interface)
		// check inspects the returned error; nil-safe.
		check func(t *testing.T, ctx context.Context, st store.Interface, err error)
	}{
		{
			name: "crashes running actor with no registered worker",
			seed: true,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err != nil {
					t.Fatalf("crashActor() = %v, want nil", err)
				}
				assertCrashed(t, ctx, st, actorRef)
			},
		},
		{
			name: "releases worker assigned to crashed actor",
			seed: true,
			setup: func(t *testing.T, ctx context.Context, st store.Interface) {
				seedWorker(t, ctx, st, actorRef)
			},
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err != nil {
					t.Fatalf("crashActor() = %v, want nil", err)
				}
				assertCrashed(t, ctx, st, actorRef)
				if got := firstAssignment(t, st, "uid"); got != nil {
					t.Errorf("worker assignment = %v, want none", got)
				}
			},
		},
		{
			name: "keeps worker assigned to another actor",
			seed: true,
			setup: func(t *testing.T, ctx context.Context, st store.Interface) {
				seedWorker(t, ctx, st, resources.ActorRef{Atespace: actorRef.Atespace, Name: "actor-2"})
			},
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err != nil {
					t.Fatalf("crashActor() = %v, want nil", err)
				}
				assertCrashed(t, ctx, st, actorRef)
				assigned := firstAssignment(t, st, "uid")
				if assigned == nil {
					t.Fatal("worker assignment = nil, want the other actor's, untouched")
				}
				if got := assigned.GetActor().GetName(); got != "actor-2" {
					t.Errorf("worker assigned actor name = %q, want %q", got, "actor-2")
				}
				if got := assigned.GetActorUid(); got != "synthetic-actor-2" {
					t.Errorf("worker assigned actor uid = %q, want %q", got, "synthetic-actor-2")
				}
			},
		},
		{
			name: "keeps worker assigned to previous incarnation of same actor",
			seed: true,
			setup: func(t *testing.T, ctx context.Context, st store.Interface) {
				// Create a worker assigned to the same actorRef, but with a stale UID
				worker := &ateapipb.Worker{
					Metadata:        &ateapipb.ResourceMetadata{Name: "uid"},
					WorkerNamespace: "ns",
					WorkerPool:      "pool",
					WorkerPod:       "pod",
					WorkerPodUid:    "uid",
					Status:          &ateapipb.WorkerStatus{},
				}
				if _, err := st.CreateWorker(ctx, worker); err != nil {
					t.Fatalf("CreateWorker: %v", err)
				}
				seedAssignment(t, st, "uid", &ateapipb.ActorAssignment{
					Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
					ActorUid: "stale-incarnation-uid",
				})
			},
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err != nil {
					t.Fatalf("crashActor() = %v, want nil", err)
				}
				assertCrashed(t, ctx, st, actorRef)
				assigned := firstAssignment(t, st, "uid")
				if assigned == nil {
					t.Fatal("worker assignment = nil, want the stale incarnation's, untouched")
				}
				if got := assigned.GetActor().GetName(); got != actorRef.Name {
					t.Errorf("worker assigned actor name = %q, want %q", got, actorRef.Name)
				}
				if got := assigned.GetActorUid(); got != "stale-incarnation-uid" {
					t.Errorf("worker assigned actor uid = %q, want %q", got, "stale-incarnation-uid")
				}
			},
		},
		{
			name: "skips release for actor with no worker binding",
			seed: false,
			setup: func(t *testing.T, ctx context.Context, st store.Interface) {
				seedUnboundActor(t, ctx, st, actorRef)
				seedWorker(t, ctx, st, actorRef)
			},
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err != nil {
					t.Fatalf("crashActor() = %v, want nil", err)
				}
				assertCrashed(t, ctx, st, actorRef)
				// Without a binding the worker cannot be looked up, so its
				// assignment must be left untouched even though it names
				// the crashed actor.
				if firstAssignment(t, st, "uid") == nil {
					t.Error("worker assignment = nil, want untouched")
				}
			},
		},
		{
			name: "actor not found",
			seed: false,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err == nil {
					t.Fatal("crashActor() = nil, want error")
				}
				if !errors.Is(err, store.ErrNotFound) {
					t.Errorf("crashActor() error = %v, want errors.Is(store.ErrNotFound)", err)
				}
				if !strings.Contains(err.Error(), "while loading actor to crash") {
					t.Errorf("crashActor() error = %q, want it to contain %q", err, "while loading actor to crash")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()

			if tt.seed {
				seedActor(t, ctx, st, actorRef)
			}
			if tt.setup != nil {
				tt.setup(t, ctx, st)
			}

			err := crashActor(ctx, st, actorRef, ateattr.OperationUnknown)

			tt.check(t, ctx, st, err)
		})
	}
}

func TestCrashActor_Metrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter("test")
	if err := RegisterActorCrashes(meter); err != nil {
		t.Fatalf("RegisterActorCrashes: %v", err)
	}

	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	actorRef := resources.ActorRef{Atespace: "demo-ns", Name: "counter-actor"}
	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "pod-uid-1"},
		WorkerNamespace: "demo-ns",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
		WorkerPodUid:    "pod-uid-1",
		SandboxClass:    "gvisor",
		Status:          &ateapipb.WorkerStatus{},
	}
	if _, err := st.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "demo-ns",
			Name:     "counter-actor",
			Uid:      "actor-uid-1",
		},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "demo-ns", Name: "counter-template"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: "pod-uid-1"},
				WorkerNamespace: "demo-ns",
				WorkerPool:      "pool-1",
				WorkerPod:       "pod-1",
				WorkerPodUid:    "pod-uid-1",
			},
		},
	}
	storetest.MustCreateActor(t, ctx, st, actor)

	if err := crashActor(ctx, st, actorRef, ateattr.OperationResume); err != nil {
		t.Fatalf("crashActor: %v", err)
	}

	assertCrashMetricDatapoint(t, reader, ateattr.OperationResume, "demo-ns", "counter-template", "pool-1", "gvisor", 1)
}

func assertCrashMetricDatapoint(t *testing.T, reader *sdkmetric.ManualReader, wantOpName, wantTmplNS, wantTmplName, wantWorkerPool, wantSandboxClass string, wantValue int64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "ate.actor.crashes" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				op, _ := dp.Attributes.Value(ateattr.ActorOperationNameKey)
				tNS, _ := dp.Attributes.Value(ateattr.TemplateAtespaceKey)
				tName, _ := dp.Attributes.Value(ateattr.TemplateNameKey)
				wp, _ := dp.Attributes.Value(ateattr.WorkerPoolNameKey)
				sc, _ := dp.Attributes.Value(ateattr.SandboxClassKey)

				if op.AsString() == wantOpName &&
					tNS.AsString() == wantTmplNS &&
					tName.AsString() == wantTmplName &&
					wp.AsString() == wantWorkerPool &&
					sc.AsString() == wantSandboxClass {
					if dp.Value != wantValue {
						t.Errorf("metric value = %d, want %d", dp.Value, wantValue)
					}
					return
				}
			}
		}
	}
	t.Errorf("did not find ate.actor.crashes metric with attrs: opName=%q, tmplNS=%q, tmplName=%q, workerPool=%q, sandboxClass=%q",
		wantOpName, wantTmplNS, wantTmplName, wantWorkerPool, wantSandboxClass)
}

// assertNoCrashMetricDatapoint fails if anything counted a crash. It is the
// assertion for a path that transitions an actor without that being a fresh
// crash — an actor already counted as crashed, or one that never crashed at all.
func assertNoCrashMetricDatapoint(t *testing.T, reader *sdkmetric.ManualReader) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "ate.actor.crashes" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				t.Errorf("counted %d crash(es) with attrs %v, want none", dp.Value, dp.Attributes)
			}
		}
	}
}

// failingReleaseStore wraps a store and fails every release, simulating a
// transient state-store error while releasing a worker.
type failingReleaseStore struct {
	store.Interface
	err error
}

func (f failingReleaseStore) ReleaseActorFromWorker(context.Context, string, string) (*ateapipb.Worker, error) {
	return nil, f.err
}

// A transient failure releasing the worker must not move the actor to the
// CRASHED state: doing so would strand the still-assigned worker with
// no actor left to drive a retry, permanently consuming the worker slot.
// crashActor must return the error with the actor and worker left intact so the
// caller retries and the worker is reclaimed.
func TestCrashActorReleaseFailureLeavesWorkerReclaimable(t *testing.T) {
	ctx := context.Background()
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	seedActor(t, ctx, st, actorRef)
	seedWorker(t, ctx, st, actorRef)

	releaseErr := errors.New("state store unavailable")
	err := crashActor(ctx, failingReleaseStore{Interface: st, err: releaseErr}, actorRef, ateattr.OperationUnknown)

	if err == nil {
		t.Fatal("crashActor() = nil, want error")
	}
	if !errors.Is(err, releaseErr) {
		t.Errorf("crashActor() error = %v, want it to wrap %v", err, releaseErr)
	}

	// The actor must stay RUNNING with its worker assignment intact, so a retry
	// can re-release the worker.
	got, gerr := st.GetActor(ctx, actorRef)
	if gerr != nil {
		t.Fatalf("GetActor() = %v, want nil", gerr)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("status = %v, want %v (actor must not be crashed when the release fails)", got.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_RUNNING)
	}
	if got.GetStatus().GetWorkerAssignment() == nil {
		t.Error("WorkerAssignment cleared, want preserved so the release can be retried")
	}

	// The worker must still be assigned to the actor (the failed release did not
	// persist): it is not leaked, and a retry will reclaim it.
	if firstAssignment(t, st, "uid") == nil {
		t.Error("worker assignment = nil, want still assigned (release failed, must remain retriable)")
	}
}

// crashRecords captures the "Actor crashed" records a crash emits, so a test can
// assert the identity that ate.actor.crashes is barred from carrying. crashActor
// logs through the slog default, so this swaps it and the caller cannot be parallel.
func crashRecords(t *testing.T) *[]stdoutRecord {
	t.Helper()
	return logRecords(t, actorevent.Crashed.Body)
}

// stdoutRecord is one captured record from the stdout copy. It keeps the level
// and the time, not just the attributes, so a test can hold those against the
// OTLP copy. The message is whatever logRecords filtered on.
type stdoutRecord struct {
	level slog.Level
	time  time.Time
	attrs map[string]string
}

// logRecords captures every record with the given message.
func logRecords(t *testing.T, msg string) *[]stdoutRecord {
	t.Helper()

	var records []stdoutRecord
	prev := slog.Default()
	slog.SetDefault(slog.New(slogHandlerFunc(func(r slog.Record) {
		if r.Message != msg {
			return
		}
		rec := stdoutRecord{level: r.Level, time: r.Time, attrs: map[string]string{}}
		r.Attrs(func(a slog.Attr) bool {
			rec.attrs[a.Key] = a.Value.String()
			return true
		})
		records = append(records, rec)
	})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &records
}

// otlpEvent is one captured record from the OTLP copy of a log record.
type otlpEvent struct {
	name      string
	body      string
	severity  otellog.Severity
	timestamp time.Time
	attrs     map[string]string
}

var (
	otlpSinkOnce sync.Once
	otlpSinkMu   sync.Mutex
	otlpSink     []otlpEvent
)

type otlpSinkExporter struct{}

func (otlpSinkExporter) Export(_ context.Context, records []sdklog.Record) error {
	otlpSinkMu.Lock()
	defer otlpSinkMu.Unlock()
	for _, r := range records {
		e := otlpEvent{
			name:      r.EventName(),
			body:      r.Body().String(),
			severity:  r.Severity(),
			timestamp: r.Timestamp(),
			attrs:     map[string]string{},
		}
		r.WalkAttributes(func(kv otellog.KeyValue) bool {
			e.attrs[kv.Key] = kv.Value.String()
			return true
		})
		otlpSink = append(otlpSink, e)
	}
	return nil
}

func (otlpSinkExporter) Shutdown(context.Context) error   { return nil }
func (otlpSinkExporter) ForceFlush(context.Context) error { return nil }

// otlpEvents captures the events emitted while a test runs, so a test can assert
// the OTLP copy beside the stdout one. The global logger provider only ever
// delegates once, so one provider serves the whole binary and each call clears
// the sink. Like logRecords, this makes the caller non-parallel.
func otlpEvents(t *testing.T) func() []otlpEvent {
	t.Helper()

	otlpSinkOnce.Do(func() {
		global.SetLoggerProvider(sdklog.NewLoggerProvider(
			sdklog.WithProcessor(sdklog.NewSimpleProcessor(otlpSinkExporter{}))))
	})

	clear := func() {
		otlpSinkMu.Lock()
		defer otlpSinkMu.Unlock()
		otlpSink = nil
	}
	clear()
	t.Cleanup(clear)

	return func() []otlpEvent {
		otlpSinkMu.Lock()
		defer otlpSinkMu.Unlock()
		return slices.Clone(otlpSink)
	}
}

type slogHandlerFunc func(slog.Record)

func (f slogHandlerFunc) Enabled(context.Context, slog.Level) bool { return true }
func (f slogHandlerFunc) Handle(_ context.Context, r slog.Record) error {
	f(r)
	return nil
}
func (f slogHandlerFunc) WithAttrs([]slog.Attr) slog.Handler { return f }
func (f slogHandlerFunc) WithGroup(string) slog.Handler      { return f }

// assertCopiesAgree checks the fields that no longer live at a call site. Both
// copies take their severity and body from ev, so neither can hold its own. A
// stdout body that drifted fails earlier, when logRecords matches nothing.
func assertCopiesAgree(t *testing.T, stdout stdoutRecord, otlp otlpEvent, ev actorevent.Event) {
	t.Helper()

	if stdout.level != ev.Level() {
		t.Errorf("stdout level = %v, want %v", stdout.level, ev.Level())
	}
	if otlp.severity != ev.Severity {
		t.Errorf("OTLP severity = %v, want %v", otlp.severity, ev.Severity)
	}
	if otlp.body != ev.Body {
		t.Errorf("OTLP body = %q, want %q", otlp.body, ev.Body)
	}
	// One time.Now() serves both, so a consumer can join them on it.
	if !stdout.time.Equal(otlp.timestamp) {
		t.Errorf("timestamps differ: stdout %v, OTLP %v", stdout.time, otlp.timestamp)
	}
}

// The crash record is the only signal carrying actor identity, so it must fire
// exactly when the counter does. A crash counted but not logged is unattributable;
// one logged but not counted double-counts on a retry.
func TestCrashActor_RecordAndCounterAgree(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	if err := RegisterActorCrashes(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")); err != nil {
		t.Fatalf("RegisterActorCrashes: %v", err)
	}
	records := crashRecords(t)
	events := otlpEvents(t)

	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	actorRef := resources.ActorRef{Atespace: "demo-ns", Name: "counter-actor"}
	storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "demo-ns", Name: "counter-template"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	if err := crashActor(ctx, st, actorRef, ateattr.OperationResume); err != nil {
		t.Fatalf("crashActor: %v", err)
	}
	if len(*records) != 1 {
		t.Fatalf("got %d crash records, want 1", len(*records))
	}

	got := (*records)[0].attrs
	stored, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	want := map[string]string{
		string(ateattr.AtespaceKey):           actorRef.Atespace,
		string(ateattr.ActorNameKey):          actorRef.Name,
		string(ateattr.ActorUIDKey):           stored.GetMetadata().GetUid(),
		string(ateattr.TemplateAtespaceKey):   "demo-ns",
		string(ateattr.TemplateNameKey):       "counter-template",
		string(ateattr.ActorOperationNameKey): ateattr.OperationResume,
		string(ateattr.ActorStateKey):         ateattr.ActorStateCrashed,
	}
	if !maps.Equal(got, want) {
		t.Errorf("crash record = %v, want %v", got, want)
	}
	if got[string(ateattr.ActorUIDKey)] == "" {
		t.Error("crash record carries no ate.actor.uid; it cannot survive a name reuse")
	}

	// The OTLP copy is the same record under an event name. One call writes both,
	// so anything either copy holds alone is a bug in actorevent.Log.
	gotEvents := events()
	if len(gotEvents) != 1 {
		t.Fatalf("got %d crash events, want 1: %v", len(gotEvents), gotEvents)
	}
	if gotEvents[0].name != actorevent.Crashed.Name {
		t.Errorf("event name = %q, want %q", gotEvents[0].name, actorevent.Crashed.Name)
	}
	if !maps.Equal(gotEvents[0].attrs, got) {
		t.Errorf("crash event attributes = %v, want the stdout record's %v", gotEvents[0].attrs, got)
	}
	assertCopiesAgree(t, (*records)[0], gotEvents[0], actorevent.Crashed)

	// Re-crashing an already-crashed actor must move neither signal.
	if err := crashActor(ctx, st, actorRef, ateattr.OperationResume); err != nil {
		t.Fatalf("second crashActor: %v", err)
	}
	if len(*records) != 1 {
		t.Errorf("got %d crash records after re-crashing, want 1", len(*records))
	}
	if gotEvents := events(); len(gotEvents) != 1 {
		t.Errorf("got %d crash events after re-crashing, want 1", len(gotEvents))
	}
}
