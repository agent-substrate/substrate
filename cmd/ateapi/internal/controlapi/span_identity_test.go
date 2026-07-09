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

// The functional harness registers no otelgrpc StatsHandler, so there is no
// server span on the client path. These tests instead call the Service methods
// in-process under a self-provided recording root span, which stands in for the
// span the otelgrpc handler injects in production and exercises the same
// trace.SpanFromContext(ctx).SetAttributes path.
func installSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return sr
}

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

func TestCreateActor_StampsFullSpanIdentity(t *testing.T) {
	ns := namespaceForTest("ns-span-create")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	sr := installSpanRecorder(t)
	attrs := rootSpanAttrs(t, sr, func(ctx context.Context) {
		if _, err := tc.service.CreateActor(ctx, &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
				ActorTemplateNamespace: ns,
				ActorTemplateName:      "tmpl1",
			},
		}); err != nil {
			t.Fatalf("CreateActor: %v", err)
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorIDKey, "id1")
	assertSpanStr(t, attrs, ateattr.ActorTemplateNameKey, "tmpl1")
	assertSpanStr(t, attrs, ateattr.ActorTemplateNamespaceKey, ns)
	if v, ok := attrs[ateattr.ActorVersionKey]; !ok || v.Type() != attribute.INT64 || v.AsInt64() != 1 {
		t.Errorf("%s = %v, want int64 1", ateattr.ActorVersionKey, v.Emit())
	}
}

func TestDeleteActor_StampsRefSpanIdentity(t *testing.T) {
	ns := namespaceForTest("ns-span-delete")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)
	if _, err := tc.service.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		},
	}); err != nil {
		t.Fatalf("seed CreateActor: %v", err)
	}

	sr := installSpanRecorder(t)
	attrs := rootSpanAttrs(t, sr, func(ctx context.Context) {
		if _, err := tc.service.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
		}); err != nil {
			t.Fatalf("DeleteActor: %v", err)
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorIDKey, "id1")
}

// The early ref stamp must land on the span even when the operation fails, so a
// failed resume is still attributable to who/where.
func TestResumeActor_ErrorStillStampsRefSpanIdentity(t *testing.T) {
	ns := namespaceForTest("ns-span-resume-err")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	sr := installSpanRecorder(t)
	attrs := rootSpanAttrs(t, sr, func(ctx context.Context) {
		if _, err := tc.service.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "missing"},
		}); err == nil {
			t.Fatal("expected error resuming missing actor")
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorIDKey, "missing")
}
