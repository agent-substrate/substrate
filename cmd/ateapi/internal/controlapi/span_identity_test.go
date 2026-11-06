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

package controlapi

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/resources"
)

// recordRootSpanAttrs runs fn under a fresh recording root span from a local
// TracerProvider and returns that span's attributes, so a test can observe what
// the code under test stamps on the span carried in ctx. It never swaps the
// global provider (the code under test reads its span via trace.SpanFromContext,
// not the global provider), so span tests stay parallel-safe.
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
	actor := &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: "team-a", Name: "a1", Uid: "uid-a1", Version: 3},
		ActorTemplate: &ateapi.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
	}

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		setSpanActorAttributes(ctx, actor)
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, "team-a")
	assertSpanStr(t, attrs, ateattr.ActorNameKey, "a1")
	assertSpanStr(t, attrs, ateattr.ActorUIDKey, "uid-a1")
	assertSpanStr(t, attrs, ateattr.TemplateNameKey, "tmpl1")
	assertSpanStr(t, attrs, ateattr.TemplateAtespaceKey, "ns1")
	if v, ok := attrs[ateattr.ActorVersionKey]; !ok || v.Type() != attribute.INT64 || v.AsInt64() != 3 {
		t.Errorf("%s = %v, want int64 3", ateattr.ActorVersionKey, v.String())
	}
}

func TestSetSpanActorRefAttributes(t *testing.T) {
	t.Parallel()

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		setSpanActorRefAttributes(ctx, resources.ActorRef{Atespace: "team-a", Name: "a1"})
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, "team-a")
	assertSpanStr(t, attrs, ateattr.ActorNameKey, "a1")
	// The ref-only stamp must not invent uid/template/version (not known pre-resolve).
	for _, k := range []attribute.Key{ateattr.ActorUIDKey, ateattr.TemplateNameKey, ateattr.TemplateAtespaceKey, ateattr.ActorVersionKey} {
		if _, ok := attrs[k]; ok {
			t.Errorf("unexpected %s on ref-only stamp", k)
		}
	}
}

// A context with no recording span must be a safe no-op, so call sites need no guard.
func TestSetSpanActorAttributes_NoRecordingSpanIsNoop(t *testing.T) {
	t.Parallel()
	setSpanActorAttributes(context.Background(), &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Name: "a1"}})
	setSpanActorRefAttributes(context.Background(), resources.ActorRef{Atespace: "team-a", Name: "a1"})
}

// TestActorRPCs_StampSpanIdentity checks what each actor RPC stamps on its span:
// the actor's full identity once it holds the stored actor, and only the ref
// when it has not read one.
func TestActorRPCs_StampSpanIdentity(t *testing.T) {
	ref := &ateapi.ObjectRef{Atespace: "team-a", Name: "actor-1"}
	newService := func(t *testing.T) *RPCService {
		t.Helper()
		persistence := newTestPersistence(t)
		return &RPCService{impl: newServiceImpl(persistence, nil), actorWorkflow: newTestActorWorkflow(t, persistence, "ns", "tmpl1")}
	}
	createRequest := &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: ref.GetAtespace(), Name: ref.GetName()},
		ActorTemplate: &ateapi.ObjectRef{Atespace: "ns", Name: "tmpl1"},
	}}
	assertFullIdentity := func(t *testing.T, attrs map[attribute.Key]attribute.Value, actor *ateapi.Actor) {
		t.Helper()
		assertSpanStr(t, attrs, ateattr.AtespaceKey, ref.GetAtespace())
		assertSpanStr(t, attrs, ateattr.ActorNameKey, ref.GetName())
		assertSpanStr(t, attrs, ateattr.ActorUIDKey, actor.GetMetadata().GetUid())
		assertSpanStr(t, attrs, ateattr.TemplateNameKey, "tmpl1")
		assertSpanStr(t, attrs, ateattr.TemplateAtespaceKey, "ns")
		if v, ok := attrs[ateattr.ActorVersionKey]; !ok || v.AsInt64() != actor.GetMetadata().GetVersion() {
			t.Errorf("%s = %v, want %d", ateattr.ActorVersionKey, v.String(), actor.GetMetadata().GetVersion())
		}
	}
	assertRefOnly := func(t *testing.T, attrs map[attribute.Key]attribute.Value) {
		t.Helper()
		assertSpanStr(t, attrs, ateattr.AtespaceKey, ref.GetAtespace())
		assertSpanStr(t, attrs, ateattr.ActorNameKey, ref.GetName())
		for _, k := range []attribute.Key{ateattr.ActorUIDKey, ateattr.TemplateNameKey, ateattr.TemplateAtespaceKey, ateattr.ActorVersionKey} {
			if _, ok := attrs[k]; ok {
				t.Errorf("unexpected %s on a ref-only span", k)
			}
		}
	}

	t.Run("CreateActor stamps the full identity", func(t *testing.T) {
		svc := newService(t)
		var created *ateapi.Actor
		attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
			var err error
			if created, err = svc.CreateActor(ctx, createRequest); err != nil {
				t.Fatalf("CreateActor: %v", err)
			}
		})
		assertFullIdentity(t, attrs, created)
	})

	t.Run("UpdateActor stamps the full identity", func(t *testing.T) {
		svc := newService(t)
		toUpdate, err := svc.CreateActor(context.Background(), createRequest)
		if err != nil {
			t.Fatalf("CreateActor: %v", err)
		}
		toUpdate.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"tier": "1"}}
		var updated *ateapi.Actor
		attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
			var err error
			if updated, err = svc.UpdateActor(ctx, &ateapi.UpdateActorRequest{Actor: toUpdate}); err != nil {
				t.Fatalf("UpdateActor: %v", err)
			}
		})
		assertFullIdentity(t, attrs, updated)
	})

	t.Run("UpdateActor of a missing actor stamps the ref only", func(t *testing.T) {
		svc := newService(t)
		attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
			_, err := svc.UpdateActor(ctx, &ateapi.UpdateActorRequest{Actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{
				Atespace: ref.GetAtespace(),
				Name:     ref.GetName(),
				Uid:      "9a2b1c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d",
				Version:  1,
			}}})
			if got := apierror.Code(err); got != codes.NotFound {
				t.Fatalf("UpdateActor = %v, want NotFound", err)
			}
		})
		assertRefOnly(t, attrs)
	})

	t.Run("DeleteActor stamps the ref", func(t *testing.T) {
		svc := newService(t)
		if _, err := svc.CreateActor(context.Background(), createRequest); err != nil {
			t.Fatalf("CreateActor: %v", err)
		}
		attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
			if _, err := svc.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
				t.Fatalf("DeleteActor: %v", err)
			}
		})
		assertSpanStr(t, attrs, ateattr.AtespaceKey, ref.GetAtespace())
		assertSpanStr(t, attrs, ateattr.ActorNameKey, ref.GetName())
	})

	t.Run("ResumeActor of a missing actor stamps the ref only", func(t *testing.T) {
		svc := newService(t)
		attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
			if _, err := svc.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref}); err == nil {
				t.Fatal("ResumeActor of a missing actor succeeded")
			}
		})
		assertRefOnly(t, attrs)
	})

	t.Run("SuspendActor of a missing actor stamps the ref only", func(t *testing.T) {
		svc := newService(t)
		attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
			_, err := svc.SuspendActor(ctx, &ateapi.SuspendActorRequest{
				Actor:   ref,
				Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
			})
			if got := apierror.Code(err); got != codes.NotFound {
				t.Fatalf("SuspendActor = %v, want NotFound", err)
			}
		})
		assertRefOnly(t, attrs)
	})
}
