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

// Package actorevent emits the actor lifecycle events. Log writes both copies of
// a record, the stdout one and the OTLP one, from a single call, so nothing
// about a record is kept in step by hand. Ordinary component logs stay on
// stdout.
//
// This is not an slog bridge. A bridge would put every component record on the
// wire, cannot set EventName, and would loop, because serverboot routes OTel SDK
// errors through slog.
//
// serverboot.InitLogging pairs this with a batching processor. These records sit
// on the actor resume path, so exporting inside Log would put a blocking gRPC
// call there and make a slow collector look like control-plane latency.
package actorevent

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

// ScopeName is how a consumer selects this stream.
const ScopeName = "github.com/agent-substrate/substrate/internal/actorevent"

// Event is one name in the closed vocabulary. Name is the LogRecord's own event
// name field, not an attribute. Body and Severity live here rather than at a
// call site, so the two copies of a record cannot differ.
//
// Keys is the attribute set the name promises. An event name means a fixed
// shape, so the tests hold the two in step and a caller cannot widen the record.
type Event struct {
	Name     string
	Body     string
	Severity log.Severity
	Keys     []string
}

// Level is the stdout level for this event. slog has four levels to OTel's
// twenty-four, so a sub-level collapses onto the range it sits in.
func (ev Event) Level() slog.Level {
	switch {
	case ev.Severity >= log.SeverityError:
		return slog.LevelError
	case ev.Severity >= log.SeverityWarn:
		return slog.LevelWarn
	case ev.Severity >= log.SeverityInfo:
		return slog.LevelInfo
	default:
		return slog.LevelDebug
	}
}

// identityKeys is what ateattr.ActorLogAttrs writes, in its order.
var identityKeys = []string{
	string(ateattr.AtespaceKey),
	string(ateattr.ActorNameKey),
	string(ateattr.ActorUIDKey),
	string(ateattr.TemplateAtespaceKey),
	string(ateattr.TemplateNameKey),
}

// Two names, because a crash has a different shape and severity. Only two,
// because ate.actor.state already says which transition happened.
var (
	StateChanged = Event{
		Name:     "ate.actor.state_changed",
		Body:     "Actor state changed",
		Severity: log.SeverityInfo,
		Keys: append(append([]string{}, identityKeys...),
			string(ateattr.ActorOperationNameKey),
			string(ateattr.ActorStateKey)),
	}

	Crashed = Event{
		Name:     "ate.actor.crashed",
		Body:     "Actor crashed",
		Severity: log.SeverityError,
		Keys: append(append([]string{}, identityKeys...),
			string(ateattr.ActorOperationNameKey),
			string(ateattr.ActorStateKey)),
	}
)

// events is the whole vocabulary, which the registry test walks. An event left
// out of it is never checked against docs/metrics/registry/events.yaml.
var events = []Event{StateChanged, Crashed}

// BuildRecord turns the stdout record into its OTLP form. Attributes carry
// everything machine-readable, so the body stays the display string.
//
// It sets no trace context: the SDK lifts that from ctx onto the record's own
// TraceId/SpanId fields.
func BuildRecord(ev Event, t time.Time, attrs []slog.Attr) log.Record {
	var rec log.Record
	rec.SetEventName(ev.Name)
	rec.SetTimestamp(t)
	rec.SetSeverity(ev.Severity)
	rec.SetBody(log.StringValue(ev.Body))

	kvs := make([]log.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		kvs = append(kvs, log.KeyValue{Key: a.Key, Value: logValue(a.Value)})
	}
	rec.AddAttributes(kvs...)
	return rec
}

// logValue keeps the kind slog's JSON handler writes, so the two copies match.
func logValue(v slog.Value) log.Value {
	switch v.Kind() {
	case slog.KindString:
		return log.StringValue(v.String())
	case slog.KindInt64:
		return log.Int64Value(v.Int64())
	case slog.KindUint64:
		return log.Int64Value(int64(v.Uint64()))
	case slog.KindFloat64:
		return log.Float64Value(v.Float64())
	case slog.KindBool:
		return log.BoolValue(v.Bool())
	case slog.KindDuration:
		// nanoseconds, not "1.5s"
		return log.Int64Value(int64(v.Duration()))
	default:
		return log.StringValue(v.String())
	}
}

// Emitter writes the OTLP copy through one log.Logger. Tests construct one
// directly, so emit needs no global provider and can run in parallel.
type Emitter struct {
	logger log.Logger
}

func NewEmitter(lp log.LoggerProvider) *Emitter {
	return &Emitter{logger: lp.Logger(ScopeName)}
}

// Log writes both copies of ev from one call, off one time.Now(), so a consumer
// can join them on an exact timestamp. That is why the stdout record is built
// here rather than through slog.LogAttrs, which would take its own reading.
//
// --log-level=warn silences the stdout copy of an info event while the OTLP copy
// still ships.
func (e *Emitter) Log(ctx context.Context, ev Event, attrs []slog.Attr) {
	now := time.Now()

	level := ev.Level()
	if l := slog.Default(); l.Enabled(ctx, level) {
		rec := slog.NewRecord(now, level, ev.Body, 0)
		rec.AddAttrs(attrs...)
		_ = l.Handler().Handle(ctx, rec)
	}

	e.emit(ctx, ev, now, attrs)
}

// emit writes the OTLP copy. It is a no-op, and cheap, until InitLogging
// installs a provider.
func (e *Emitter) emit(ctx context.Context, ev Event, t time.Time, attrs []slog.Attr) {
	params := log.EnabledParameters{Severity: ev.Severity, EventName: ev.Name}
	if !e.logger.Enabled(ctx, params) {
		return
	}
	e.logger.Emit(ctx, BuildRecord(ev, t, attrs))
}

// The global provider delegates, so a Logger taken before InitLogging still
// reaches the one it installs.
var defaultEmitter = sync.OnceValue(func() *Emitter {
	return NewEmitter(global.GetLoggerProvider())
})

// Log records ev through the process-wide provider.
func Log(ctx context.Context, ev Event, attrs []slog.Attr) {
	defaultEmitter().Log(ctx, ev, attrs)
}
