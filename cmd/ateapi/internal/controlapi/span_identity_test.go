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
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// installSpanRecorder swaps in a recording global TracerProvider. Span-producing
// code fetches its tracer from the global provider at call time, so this must not
// run in parallel; the prior provider is restored on cleanup. Shared by the
// per-method span tests (create/delete/resume_actor_test.go).
func installSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return sr
}

// rootSpanAttrs runs fn under a fresh recording root span and returns that span's
// attributes, so a test can observe what the code under test stamps on the span
// carried in ctx.
func rootSpanAttrs(t *testing.T, sr *tracetest.SpanRecorder, fn func(ctx context.Context)) map[attribute.Key]attribute.Value {
	t.Helper()
	ctx, root := otel.Tracer("test").Start(context.Background(), "root")
	fn(ctx)
	root.End()
	for _, s := range sr.Ended() {
		if s.Name() == "root" {
			m := make(map[attribute.Key]attribute.Value, len(s.Attributes()))
			for _, kv := range s.Attributes() {
				m[kv.Key] = kv.Value
			}
			return m
		}
	}
	t.Fatal("root span not recorded")
	return nil
}

func assertSpanStr(t *testing.T, attrs map[attribute.Key]attribute.Value, key attribute.Key, want string) {
	t.Helper()
	v, ok := attrs[key]
	if !ok {
		t.Errorf("missing %s", key)
		return
	}
	if v.AsString() != want {
		t.Errorf("%s = %q, want %q", key, v.AsString(), want)
	}
}

func TestSetSpanActorIdentity(t *testing.T) {
	sr := installSpanRecorder(t)
	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "a1", Version: 3},
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
	}

	attrs := rootSpanAttrs(t, sr, func(ctx context.Context) {
		setSpanActorIdentity(ctx, actor)
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, "team-a")
	assertSpanStr(t, attrs, ateattr.ActorIDKey, "a1")
	assertSpanStr(t, attrs, ateattr.ActorTemplateNameKey, "tmpl1")
	assertSpanStr(t, attrs, ateattr.ActorTemplateNamespaceKey, "ns1")
	if v, ok := attrs[ateattr.ActorVersionKey]; !ok || v.Type() != attribute.INT64 || v.AsInt64() != 3 {
		t.Errorf("%s = %v, want int64 3", ateattr.ActorVersionKey, v.Emit())
	}
}

func TestSetSpanActorRefIdentity(t *testing.T) {
	sr := installSpanRecorder(t)

	attrs := rootSpanAttrs(t, sr, func(ctx context.Context) {
		setSpanActorRefIdentity(ctx, "team-a", "a1")
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, "team-a")
	assertSpanStr(t, attrs, ateattr.ActorIDKey, "a1")
	// The ref-only stamp must not invent template/version (not known pre-resolve).
	for _, k := range []attribute.Key{ateattr.ActorTemplateNameKey, ateattr.ActorTemplateNamespaceKey, ateattr.ActorVersionKey} {
		if _, ok := attrs[k]; ok {
			t.Errorf("unexpected %s on ref-only stamp", k)
		}
	}
}

// A context with no recording span must be a safe no-op, so call sites need no guard.
func TestSetSpanActorIdentity_NoRecordingSpanIsNoop(t *testing.T) {
	setSpanActorIdentity(context.Background(), &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: "a1"}})
	setSpanActorRefIdentity(context.Background(), "team-a", "a1")
}
