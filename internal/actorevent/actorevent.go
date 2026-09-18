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

// Package actorevent emits the actor lifecycle events over OTLP. Events go over
// OTLP; ordinary component logs stay on stdout. The caller passes the same
// []slog.Attr to both copies, so the two cannot drift.
//
// This is not an slog bridge. A bridge would put every component record on the
// wire, cannot set EventName, and would loop, because serverboot routes OTel SDK
// errors through slog.
//
// serverboot.InitLogging pairs this with a batching processor. These records sit
// on the actor resume path, so exporting inside Emit would put a blocking gRPC
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

// Bodies match the stdout record's message, so both read alike.
const (
	StateChangedBody = "Actor state changed"
	CrashedBody      = "Actor crashed"
)

// Event is one name in the closed vocabulary. Name is the LogRecord's own event
// name field, not an attribute.
//
// Keys is the attribute set the name promises. An event name means a fixed
// shape, so the tests hold the two in step and a caller cannot widen the record.
type Event struct {
	Name     string
	Body     string
	Severity log.Severity
	Keys     []string
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
		Body:     StateChangedBody,
		Severity: log.SeverityInfo,
		Keys: append(append([]string{}, identityKeys...),
			string(ateattr.ActorOperationNameKey),
			string(ateattr.ActorStateKey)),
	}

	Crashed = Event{
		Name:     "ate.actor.crashed",
		Body:     CrashedBody,
		Severity: log.SeverityError,
		Keys: append(append([]string{}, identityKeys...),
			string(ateattr.ActorOperationNameKey),
			string(ateattr.ActorStateKey),
			string(ateattr.FailureReasonKey),
			string(ateattr.FailureDomainKey)),
	}
)

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

// Emitter writes events through one log.Logger. Tests construct one directly, so
// they need no global provider and can run in parallel.
type Emitter struct {
	logger log.Logger
}

func NewEmitter(lp log.LoggerProvider) *Emitter {
	return &Emitter{logger: lp.Logger(ScopeName)}
}

// Emit records ev. attrs is the same slice the caller gave its stdout record.
// It is a no-op, and cheap, until InitLogging installs a provider.
func (e *Emitter) Emit(ctx context.Context, ev Event, attrs []slog.Attr) {
	params := log.EnabledParameters{Severity: ev.Severity, EventName: ev.Name}
	if !e.logger.Enabled(ctx, params) {
		return
	}
	e.logger.Emit(ctx, BuildRecord(ev, time.Now(), attrs))
}

// The global provider delegates, so a Logger taken before InitLogging still
// reaches the one it installs.
var defaultEmitter = sync.OnceValue(func() *Emitter {
	return NewEmitter(global.GetLoggerProvider())
})

// Emit records ev through the process-wide provider.
func Emit(ctx context.Context, ev Event, attrs []slog.Attr) {
	defaultEmitter().Emit(ctx, ev, attrs)
}
