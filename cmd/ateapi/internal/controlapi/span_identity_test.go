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

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// recordRootSpanAttrs runs fn under a fresh recording root span from a local
// TracerProvider and returns that span's attributes, so a test can observe what
// the code under test stamps on the span carried in ctx. It never swaps the
// global provider (the code under test reads its span via trace.SpanFromContext,
// not the global provider), so span tests stay parallel-safe. Shared by the
// per-method span tests (create/delete/resume/pause_actor_test.go).
func recordRootSpanAttrs(t *testing.T, fn func(ctx context.Context)) map[attribute.Key]attribute.Value {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	ctx, root := tp.Tracer("test").Start(context.Background(), "root")
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

func TestSetSpanActorAttributes(t *testing.T) {
	t.Parallel()
	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "a1", Uid: "uid-a1", Version: 3},
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
	}

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		setSpanActorAttributes(ctx, actor)
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, "team-a")
	assertSpanStr(t, attrs, ateattr.ActorNameKey, "a1")
	assertSpanStr(t, attrs, ateattr.ActorUIDKey, "uid-a1")
	assertSpanStr(t, attrs, ateattr.TemplateNameKey, "tmpl1")
	assertSpanStr(t, attrs, ateattr.TemplateNamespaceKey, "ns1")
	if v, ok := attrs[ateattr.ActorVersionKey]; !ok || v.Type() != attribute.INT64 || v.AsInt64() != 3 {
		t.Errorf("%s = %v, want int64 3", ateattr.ActorVersionKey, v.Emit())
	}
}

func TestSetSpanActorRefAttributes(t *testing.T) {
	t.Parallel()

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		setSpanActorRefAttributes(ctx, "team-a", "a1")
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, "team-a")
	assertSpanStr(t, attrs, ateattr.ActorNameKey, "a1")
	// The ref-only stamp must not invent uid/template/version (not known pre-resolve).
	for _, k := range []attribute.Key{ateattr.ActorUIDKey, ateattr.TemplateNameKey, ateattr.TemplateNamespaceKey, ateattr.ActorVersionKey} {
		if _, ok := attrs[k]; ok {
			t.Errorf("unexpected %s on ref-only stamp", k)
		}
	}
}

// A context with no recording span must be a safe no-op, so call sites need no guard.
func TestSetSpanActorAttributes_NoRecordingSpanIsNoop(t *testing.T) {
	t.Parallel()
	setSpanActorAttributes(context.Background(), &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: "a1"}})
	setSpanActorRefAttributes(context.Background(), "team-a", "a1")
}
