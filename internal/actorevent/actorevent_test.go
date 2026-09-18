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

package actorevent_test

import (
	"context"
	"log/slog"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/agent-substrate/substrate/internal/actorevent"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
)

const (
	testAtespace     = "ate-demo-counter"
	testActorName    = "counter-1"
	testActorUID     = "8f2a1c4e6b0d47f1"
	testTemplateName = "counter"
	testReason       = "ACTOR_EXITED"
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
	attrs := append(ateattr.ActorLogAttrs(testAttribution()),
		slog.String(string(ateattr.ActorOperationNameKey), ateattr.OperationResume),
		slog.String(string(ateattr.ActorStateKey), ateattr.ActorStateCrashed))
	return append(attrs, ateattr.FailureLogAttrs(testReason)...)
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
		event    actorevent.Event
		attrs    []slog.Attr
		wantName string
		wantBody string
		wantSev  log.Severity
		wantVals map[string]string
	}{
		{
			name:     "state changed",
			event:    actorevent.StateChanged,
			attrs:    stateChangedAttrs(ateattr.ActorStateRunning),
			wantName: "ate.actor.state_changed",
			wantBody: actorevent.StateChangedBody,
			wantSev:  log.SeverityInfo,
			wantVals: map[string]string{
				string(ateattr.ActorStateKey):         ateattr.ActorStateRunning,
				string(ateattr.ActorOperationNameKey): ateattr.OperationResume,
				string(ateattr.ActorUIDKey):           testActorUID,
			},
		},
		{
			name:     "deleted is a state, not a name of its own",
			event:    actorevent.StateChanged,
			attrs:    stateChangedAttrs(ateattr.ActorStateDeleted),
			wantName: "ate.actor.state_changed",
			wantBody: actorevent.StateChangedBody,
			wantSev:  log.SeverityInfo,
			wantVals: map[string]string{
				string(ateattr.ActorStateKey): ateattr.ActorStateDeleted,
			},
		},
		{
			name:     "crashed",
			event:    actorevent.Crashed,
			attrs:    crashedAttrs(),
			wantName: "ate.actor.crashed",
			wantBody: actorevent.CrashedBody,
			wantSev:  log.SeverityError,
			wantVals: map[string]string{
				string(ateattr.ActorStateKey):    ateattr.ActorStateCrashed,
				string(ateattr.FailureReasonKey): testReason,
				string(ateattr.FailureDomainKey): ateattr.FailureDomain(testReason),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := actorevent.BuildRecord(tt.event, now, tt.attrs)

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

	actorevent.NewEmitter(lp).Emit(ctx, actorevent.StateChanged, stateChangedAttrs(ateattr.ActorStateRunning))

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
	actorevent.Emit(context.Background(), actorevent.StateChanged, stateChangedAttrs(ateattr.ActorStateRunning))
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

			rec := actorevent.BuildRecord(actorevent.StateChanged, time.Now(), []slog.Attr{tt.attr})
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
