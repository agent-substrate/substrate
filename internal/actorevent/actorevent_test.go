// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package actorevent

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
)

const (
	testAtespace     = "ate-demo-counter"
	testActorName    = "counter-1"
	testActorUID     = "8f2a1c4e6b0d47f1"
	testTemplateName = "counter"
)

func testAttribution() resources.ActorAttribution {
	return resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: testAtespace, Name: testActorName},
		UID:              testActorUID,
		TemplateAtespace: testAtespace,
		TemplateName:     testTemplateName,
	}
}

// stateChangedAttrs mirrors what controlapi.logActorState builds.
func stateChangedAttrs(state string) []slog.Attr {
	return append(ateattr.ActorLogAttrs(testAttribution()),
		slog.String(string(ateattr.ActorOperationNameKey), ateattr.OperationResume),
		slog.String(string(ateattr.ActorStateKey), state))
}

// crashedAttrs mirrors what controlapi.logActorCrashed builds.
func crashedAttrs() []slog.Attr {
	return append(ateattr.ActorLogAttrs(testAttribution()),
		slog.String(string(ateattr.ActorOperationNameKey), ateattr.OperationResume),
		slog.String(string(ateattr.ActorStateKey), ateattr.ActorStateCrashed))
}

func recordAttrs(rec log.Record) map[string]string {
	got := make(map[string]string, rec.AttributesLen())
	rec.WalkAttributes(func(kv log.KeyValue) bool {
		got[kv.Key] = kv.Value.String()
		return true
	})
	return got
}

func TestBuildRecord(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		event    Event
		attrs    []slog.Attr
		wantName string
		wantBody string
		wantSev  log.Severity
		wantVals map[string]string
	}{
		{
			name:     "state changed",
			event:    StateChanged,
			attrs:    stateChangedAttrs(ateattr.ActorStateRunning),
			wantName: "ate.actor.state_changed",
			wantBody: "Actor state changed",
			wantSev:  log.SeverityInfo,
			wantVals: map[string]string{
				string(ateattr.ActorStateKey):         ateattr.ActorStateRunning,
				string(ateattr.ActorOperationNameKey): ateattr.OperationResume,
				string(ateattr.ActorUIDKey):           testActorUID,
			},
		},
		{
			name:     "deleted is a state, not a name of its own",
			event:    StateChanged,
			attrs:    stateChangedAttrs(ateattr.ActorStateDeleted),
			wantName: "ate.actor.state_changed",
			wantBody: "Actor state changed",
			wantSev:  log.SeverityInfo,
			wantVals: map[string]string{
				string(ateattr.ActorStateKey): ateattr.ActorStateDeleted,
			},
		},
		{
			name:     "crashed",
			event:    Crashed,
			attrs:    crashedAttrs(),
			wantName: "ate.actor.crashed",
			wantBody: "Actor crashed",
			wantSev:  log.SeverityError,
			wantVals: map[string]string{
				string(ateattr.ActorStateKey): ateattr.ActorStateCrashed,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := BuildRecord(tt.event, now, tt.attrs)

			if got := rec.EventName(); got != tt.wantName {
				t.Errorf("EventName() = %q, want %q", got, tt.wantName)
			}
			if got := rec.Body().String(); got != tt.wantBody {
				t.Errorf("Body() = %q, want %q", got, tt.wantBody)
			}
			if got := rec.Severity(); got != tt.wantSev {
				t.Errorf("Severity() = %v, want %v", got, tt.wantSev)
			}
			if got := rec.Timestamp(); !got.Equal(now) {
				t.Errorf("Timestamp() = %v, want %v", got, now)
			}
			// SeverityText is the stdout format's concern, not the event's.
			if got := rec.SeverityText(); got != "" {
				t.Errorf("SeverityText() = %q, want empty", got)
			}

			got := recordAttrs(rec)
			for key, want := range tt.wantVals {
				if got[key] != want {
					t.Errorf("attribute %q = %q, want %q", key, got[key], want)
				}
			}

			// The event name promises a fixed shape, so the emitted set and the
			// declared set must match both ways.
			for _, key := range tt.event.Keys {
				if _, ok := got[key]; !ok {
					t.Errorf("declared key %q is missing from the record", key)
				}
			}
			for key := range got {
				if !slices.Contains(tt.event.Keys, key) {
					t.Errorf("record carries %q, which %s does not declare", key, tt.event.Name)
				}
			}
		})
	}
}

func TestEventLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sev  log.Severity
		want slog.Level
	}{
		{"unset falls to the quietest level", log.SeverityUndefined, slog.LevelDebug},
		{"trace", log.SeverityTrace, slog.LevelDebug},
		{"debug", log.SeverityDebug, slog.LevelDebug},
		{"info", log.SeverityInfo, slog.LevelInfo},
		{"warn", log.SeverityWarn, slog.LevelWarn},
		{"error", log.SeverityError, slog.LevelError},
		{"fatal is as high as slog goes", log.SeverityFatal, slog.LevelError},
		{"a sub-level keeps its range", log.SeverityInfo3, slog.LevelInfo},
		{"the state_changed event", StateChanged.Severity, slog.LevelInfo},
		{"the crashed event", Crashed.Severity, slog.LevelError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := (Event{Severity: tt.sev}).Level(); got != tt.want {
				t.Errorf("Level() for severity %v = %v, want %v", tt.sev, got, tt.want)
			}
		})
	}
}

// memExporter collects records in memory. Small enough to keep here rather than
// vendoring the SDK's test package.
type memExporter struct {
	records []sdklog.Record
}

func (e *memExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.records = append(e.records, records...)
	return nil
}
func (e *memExporter) Shutdown(context.Context) error   { return nil }
func (e *memExporter) ForceFlush(context.Context) error { return nil }

// captureHandler keeps the stdout copy so a test can hold it beside the OTLP one.
type captureHandler struct {
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

// TestLogWritesBothCopies is the invariant this package exists for: one call,
// two copies, and no field either copy can hold alone.
//
// It swaps the slog default, so it cannot be parallel. Go finishes every
// non-parallel test before it resumes the parallel ones, so it does not race
// the rest of this file.
func TestLogWritesBothCopies(t *testing.T) {
	tests := []struct {
		name  string
		event Event
		attrs []slog.Attr
	}{
		{"state changed", StateChanged, stateChangedAttrs(ateattr.ActorStateRunning)},
		{"crashed", Crashed, crashedAttrs()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdout := &captureHandler{}
			prev := slog.Default()
			slog.SetDefault(slog.New(stdout))
			t.Cleanup(func() { slog.SetDefault(prev) })

			exp := &memExporter{}
			lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
			t.Cleanup(func() { _ = lp.Shutdown(context.Background()) })

			NewEmitter(lp).Log(context.Background(), tt.event, tt.attrs)

			if len(stdout.records) != 1 {
				t.Fatalf("wrote %d stdout records, want 1", len(stdout.records))
			}
			if len(exp.records) != 1 {
				t.Fatalf("exported %d OTLP records, want 1", len(exp.records))
			}
			stdoutRec, otlpRec := stdout.records[0], exp.records[0]

			if stdoutRec.Message != tt.event.Body {
				t.Errorf("stdout message = %q, want %q", stdoutRec.Message, tt.event.Body)
			}
			if got := otlpRec.Body().String(); got != tt.event.Body {
				t.Errorf("OTLP body = %q, want %q", got, tt.event.Body)
			}
			if stdoutRec.Level != tt.event.Level() {
				t.Errorf("stdout level = %v, want %v", stdoutRec.Level, tt.event.Level())
			}
			if got := otlpRec.Severity(); got != tt.event.Severity {
				t.Errorf("OTLP severity = %v, want %v", got, tt.event.Severity)
			}
			// One time.Now() serves both, so a consumer can join them on it.
			if !stdoutRec.Time.Equal(otlpRec.Timestamp()) {
				t.Errorf("timestamps differ: stdout %v, OTLP %v", stdoutRec.Time, otlpRec.Timestamp())
			}

			stdoutAttrs := map[string]string{}
			stdoutRec.Attrs(func(a slog.Attr) bool {
				stdoutAttrs[a.Key] = a.Value.String()
				return true
			})
			otlpAttrs := map[string]string{}
			otlpRec.WalkAttributes(func(kv log.KeyValue) bool {
				otlpAttrs[kv.Key] = kv.Value.String()
				return true
			})
			if !maps.Equal(stdoutAttrs, otlpAttrs) {
				t.Errorf("attributes differ: stdout %v, OTLP %v", stdoutAttrs, otlpAttrs)
			}
		})
	}
}

func TestEmitCarriesTraceContext(t *testing.T) {
	t.Parallel()

	exp := &memExporter{}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	t.Cleanup(func() { _ = lp.Shutdown(context.Background()) })

	// A local tracer provider, never the global, so this stays parallel-safe.
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(context.Background(), "test")
	defer span.End()

	NewEmitter(lp).emit(ctx, StateChanged, time.Now(), stateChangedAttrs(ateattr.ActorStateRunning))

	if len(exp.records) != 1 {
		t.Fatalf("exported %d records, want 1", len(exp.records))
	}
	rec := exp.records[0]

	sc := span.SpanContext()
	if got := rec.TraceID(); got != sc.TraceID() {
		t.Errorf("TraceID() = %v, want %v", got, sc.TraceID())
	}
	if got := rec.SpanID(); got != sc.SpanID() {
		t.Errorf("SpanID() = %v, want %v", got, sc.SpanID())
	}

	// Trace context belongs on the record's own fields. The stdout copy carries
	// it as attributes; the OTLP copy must not, or it is there twice.
	rec.WalkAttributes(func(kv log.KeyValue) bool {
		switch kv.Key {
		case ateattr.LogTraceIDField, ateattr.LogSpanIDField, ateattr.LogTraceFlagsField:
			t.Errorf("record carries trace context as the attribute %q", kv.Key)
		}
		return true
	})

	if got := rec.ObservedTimestamp(); got.IsZero() {
		t.Error("ObservedTimestamp() is zero, want the SDK to have set it")
	}
}

func TestEmitIsANoOpWithoutAProvider(t *testing.T) {
	t.Parallel()

	// The package default resolves the global provider, which no test installs.
	// This asserts it does not panic rather than that it drops the record.
	defaultEmitter().emit(context.Background(), StateChanged, time.Now(), stateChangedAttrs(ateattr.ActorStateRunning))
}

func TestBuildRecordKeepsValueKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		attr slog.Attr
		want log.Value
	}{
		{"string", slog.String("k", "v"), log.StringValue("v")},
		{"int", slog.Int64("k", 7), log.Int64Value(7)},
		{"uint", slog.Uint64("k", 7), log.Int64Value(7)},
		{"float", slog.Float64("k", 1.5), log.Float64Value(1.5)},
		{"bool", slog.Bool("k", true), log.BoolValue(true)},
		{"duration is nanoseconds, as in the stdout copy", slog.Duration("k", 1500*time.Millisecond), log.Int64Value(1_500_000_000)},
		{"anything else falls back to its string form", slog.Any("k", struct{}{}), log.StringValue("{}")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := BuildRecord(StateChanged, time.Now(), []slog.Attr{tt.attr})
			var got log.Value
			rec.WalkAttributes(func(kv log.KeyValue) bool {
				got = kv.Value
				return false
			})
			if got.Kind() != tt.want.Kind() {
				t.Fatalf("kind = %v, want %v", got.Kind(), tt.want.Kind())
			}
			if got.String() != tt.want.String() {
				t.Errorf("value = %v, want %v", got, tt.want)
			}
		})
	}
}
