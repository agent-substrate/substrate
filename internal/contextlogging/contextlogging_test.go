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

package contextlogging

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/protoredact"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	testTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	testSpanID  = "00f067aa0ba902b7"
)

func spanContext(t *testing.T, flags trace.TraceFlags) trace.SpanContext {
	t.Helper()
	traceID, err := trace.TraceIDFromHex(testTraceID)
	if err != nil {
		t.Fatalf("TraceIDFromHex(%q): %v", testTraceID, err)
	}
	spanID, err := trace.SpanIDFromHex(testSpanID)
	if err != nil {
		t.Fatalf("SpanIDFromHex(%q): %v", testSpanID, err)
	}
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: flags})
}

func TestHandleTraceCorrelation(t *testing.T) {
	tests := []struct {
		name           string
		ctx            func(t *testing.T) context.Context
		wantTraceID    string
		wantSpanID     string
		wantTraceFlags string
	}{
		{
			name: "no span in context",
			ctx:  func(*testing.T) context.Context { return context.Background() },
		},
		{
			name: "invalid span context contributes nothing",
			ctx: func(*testing.T) context.Context {
				return trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{}))
			},
		},
		{
			name: "sampled span",
			ctx: func(t *testing.T) context.Context {
				return trace.ContextWithSpanContext(context.Background(), spanContext(t, trace.FlagsSampled))
			},
			wantTraceID:    testTraceID,
			wantSpanID:     testSpanID,
			wantTraceFlags: "01",
		},
		{
			// An unsampled record still says which request it belongs to; the flags
			// are what tells a reader why the trace is not in the backend.
			name: "unsampled span still correlates",
			ctx: func(t *testing.T) context.Context {
				return trace.ContextWithSpanContext(context.Background(), spanContext(t, 0))
			},
			wantTraceID:    testTraceID,
			wantSpanID:     testSpanID,
			wantTraceFlags: "00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			logger := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil)))
			logger.InfoContext(tt.ctx(t), "something happened")

			var rec map[string]any
			if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
				t.Fatalf("failed to parse log record %q: %v", buf.String(), err)
			}

			for field, want := range map[string]string{
				ateattr.LogTraceIDField:    tt.wantTraceID,
				ateattr.LogSpanIDField:     tt.wantSpanID,
				ateattr.LogTraceFlagsField: tt.wantTraceFlags,
			} {
				got, present := rec[field]
				if want == "" {
					if present {
						t.Errorf("%s = %v, want absent", field, got)
					}
					continue
				}
				if got != want {
					t.Errorf("%s = %v, want %q", field, got, want)
				}
			}
		})
	}
}

// TestHandleUngroupedKeepsTraceFieldsTopLevel pins the placement the spec
// requires: a collector only lifts these onto the log record from the top level.
func TestHandleUngroupedKeepsTraceFieldsTopLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil)))
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext(t, trace.FlagsSampled))
	logger.With(slog.String("component", "atelet")).InfoContext(ctx, "something happened", slog.String("id", "abc"))

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("failed to parse log record %q: %v", buf.String(), err)
	}
	if rec[ateattr.LogTraceIDField] != testTraceID {
		t.Errorf("%s = %v, want %q at the top level, got record %v", ateattr.LogTraceIDField, rec[ateattr.LogTraceIDField], testTraceID, rec)
	}
}

// tokenValuer stands in for a type that implements slog.LogValuer and
// resolves to a proto; the handler must resolve it before deciding.
type tokenValuer struct {
	m *ateapipb.MintActorJWTResponse
}

func (v tokenValuer) LogValue() slog.Value { return slog.AnyValue(v.m) }

func TestHandleRedactsProtoAttrsAnywhereInTheRecord(t *testing.T) {
	const token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhY3RvciJ9.c2lnbmF0dXJl"
	resp := &ateapipb.MintActorJWTResponse{ActorJwt: token}
	ref := &ateapipb.ObjectRef{Atespace: "ate-demo-sandbox", Name: "agent"}
	tpl := &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{{
		Name: "c", Env: []*ateapipb.EnvVar{{Name: "API_KEY", Value: "sk-secret"}},
	}}}

	var buf bytes.Buffer
	logger := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil)))

	// Direct attr, attr inside a group, LogValuer resolving to a proto, and a
	// pre-bound attr via With (WithAttrs), plus clean values that must survive.
	logger.With(slog.Any("bound", tpl)).InfoContext(context.Background(), "test",
		slog.Any("resp", resp),
		slog.Group("nested", slog.String("plain", "keep"), slog.Any("inner", resp)),
		slog.Any("valuer", tokenValuer{resp}),
		slog.Any("actor", ref),
		slog.String("host", "example.com"),
		slog.Int("n", 3),
	)
	got := buf.String()

	for _, leak := range []string{token, "sk-secret"} {
		if strings.Contains(got, leak) {
			t.Fatalf("log contains %q: %s", leak, got)
		}
	}
	for _, want := range []string{
		`"resp":{"actor_jwt":"[REDACTED]"}`,
		`"nested":{"plain":"keep","inner":{"actor_jwt":"[REDACTED]"}}`,
		`"valuer":{"actor_jwt":"[REDACTED]"}`,
		`"bound":{"containers":[{"name":"c","env":[{"name":"API_KEY","value":"[REDACTED]"}]}]}`,
		`"actor":{"atespace":"ate-demo-sandbox","name":"agent"}`,
		`"host":"example.com"`,
		`"n":3`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("log missing %s\n got: %s", want, got)
		}
	}
	// Originals are untouched: the caller still holds the real values.
	if resp.GetActorJwt() != token || tpl.GetContainers()[0].GetEnv()[0].GetValue() != "sk-secret" {
		t.Fatal("handler mutated the logged message")
	}
}

type stringValuer struct{}

func (stringValuer) LogValue() slog.Value { return slog.StringValue("resolved") }

// groupValuer resolves to a group that holds a proto, the shape a typed
// reference with a nested message would take.
type groupValuer struct {
	m *ateapipb.MintActorJWTResponse
}

func (g groupValuer) LogValue() slog.Value {
	return slog.GroupValue(slog.String("kind", "jwt"), slog.Any("resp", g.m))
}

// countingValuer counts how many times slog resolves it.
type countingValuer struct{ n *int }

func (c countingValuer) LogValue() slog.Value { *c.n++; return slog.StringValue("once") }

func TestRedactValueReportsWhatItChanges(t *testing.T) {
	sensitive := &ateapipb.MintActorJWTResponse{ActorJwt: "token"}
	envless := &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{{Name: "c"}}}
	for _, tc := range []struct {
		name    string
		in      slog.Value
		changed bool
	}{
		{"string", slog.StringValue("s"), false},
		{"int", slog.IntValue(1), false},
		{"non-proto any", slog.AnyValue(struct{ X int }{1}), false},
		{"nil any", slog.AnyValue(nil), false},
		{"clean type", slog.AnyValue(&ateapipb.ObjectRef{Name: "n"}), false},
		{"labeled type, nothing set", slog.AnyValue(envless), false},
		{"typed nil labeled type", slog.AnyValue((*ateapipb.MintActorJWTResponse)(nil)), false},
		{"group of plain values", slog.GroupValue(slog.String("a", "b"), slog.Any("ref", &ateapipb.ObjectRef{})), false},
		{"empty group", slog.GroupValue(), false},
		{"sensitive", slog.AnyValue(sensitive), true},
		{"group holding sensitive", slog.GroupValue(slog.String("a", "b"), slog.Group("in", slog.Any("r", sensitive))), true},
		{"valuer", slog.AnyValue(stringValuer{}), true},
		{"valuer to group holding sensitive", slog.AnyValue(groupValuer{sensitive}), true},
	} {
		got, changed := redactValue(tc.in)
		if changed != tc.changed {
			t.Errorf("%s: changed = %v, want %v", tc.name, changed, tc.changed)
		}
		if !changed && !got.Equal(tc.in) {
			t.Errorf("%s: value altered although reported unchanged", tc.name)
		}
		if changed && got.Kind() == slog.KindLogValuer {
			t.Errorf("%s: LogValuer returned unresolved", tc.name)
		}
		if strings.Contains(got.String(), "token") {
			t.Errorf("%s: secret survived: %s", tc.name, got)
		}
	}
	if sensitive.GetActorJwt() != "token" {
		t.Fatal("redactValue mutated the caller's message")
	}
}

func TestRedactAttrsReturnsTheInputWhenNothingChanges(t *testing.T) {
	in := []slog.Attr{slog.String("a", "b"), slog.Any("ref", &ateapipb.ObjectRef{Name: "n"}), slog.Group("g", slog.Int("n", 1))}
	out := redactAttrs(in)
	if len(out) != len(in) || &out[0] != &in[0] {
		t.Fatal("redactAttrs copied a slice with nothing to change")
	}
	sensitive := &ateapipb.MintActorJWTResponse{ActorJwt: "token"}
	in = append(in, slog.Group("g2", slog.Any("r", sensitive)))
	out = redactAttrs(in)
	if &out[0] == &in[0] {
		t.Fatal("redactAttrs returned the input although an attribute changed")
	}
	if in[3].Value.Group()[0].Value.Any().(*ateapipb.MintActorJWTResponse).GetActorJwt() != "token" {
		t.Fatal("redactAttrs modified the input slice")
	}
	if out[3].Value.Group()[0].Value.Any().(*ateapipb.MintActorJWTResponse).GetActorJwt() != protoredact.Placeholder {
		t.Fatal("redactAttrs did not mask the group member")
	}
}

func TestHandleCoversGroupsBoundAttrsAndNilsTogether(t *testing.T) {
	const token = "eyJhbGciOiJSUzI1NiJ9.token"
	sensitive := &ateapipb.MintActorJWTResponse{ActorJwt: token}
	var buf bytes.Buffer
	base := slog.New(NewHandler(slog.NewJSONHandler(&buf, &slog.HandlerOptions{ReplaceAttr: dropTime})))

	// A sensitive proto inside a group bound with With, under WithGroup on
	// both sides, next to a typed nil of the same type and a valuer that
	// resolves to a group holding the secret.
	base.WithGroup("outer").
		With(slog.Group("bound", slog.Any("resp", sensitive))).
		WithGroup("inner").
		Info("x",
			slog.Any("nilresp", (*ateapipb.MintActorJWTResponse)(nil)),
			slog.Any("valuer", groupValuer{sensitive}),
			slog.String("plain", "keep"),
		)
	got := buf.String()
	if strings.Contains(got, token) {
		t.Fatalf("log contains the token: %s", got)
	}
	want := `{"level":"INFO","msg":"x","outer":{"bound":{"resp":{"actor_jwt":"[REDACTED]"}},"inner":{"nilresp":null,"valuer":{"kind":"jwt","resp":{"actor_jwt":"[REDACTED]"}},"plain":"keep"}}}` + "\n"
	if got != want {
		t.Errorf("log line\n got: %s\nwant: %s", got, want)
	}
	if sensitive.GetActorJwt() != token {
		t.Fatal("handler mutated the logged message")
	}
}

func TestHandleResolvesLogValuersOnce(t *testing.T) {
	// Before the record reaches the terminal handler a LogValuer is
	// resolved here and replaced by its value, so LogValue runs once per
	// line whether or not the record carries a proto.
	for _, withProto := range []bool{false, true} {
		calls := 0
		attrs := []slog.Attr{slog.Any("v", countingValuer{&calls}), slog.String("a", "b")}
		if withProto {
			attrs = append(attrs, slog.Any("resp", &ateapipb.MintActorJWTResponse{ActorJwt: "t"}))
		}
		slog.New(NewHandler(slog.NewJSONHandler(io.Discard, nil))).LogAttrs(context.Background(), slog.LevelInfo, "x", attrs...)
		if calls != 1 {
			t.Errorf("withProto=%v: LogValue called %d times, want 1", withProto, calls)
		}
	}
}

func TestHandleRebuildsOnlyWhenAnAttributeChanges(t *testing.T) {
	// Same record through the bare JSON handler and through ours must match
	// byte for byte when it carries only values redactValue leaves alone,
	// including a proto of a labeled type with nothing set.
	envless := &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{{Name: "c", Image: "img"}}}
	var plainBuf, oursBuf bytes.Buffer
	plain := slog.New(slog.NewJSONHandler(&plainBuf, &slog.HandlerOptions{ReplaceAttr: dropTime}))
	ours := slog.New(NewHandler(slog.NewJSONHandler(&oursBuf, &slog.HandlerOptions{ReplaceAttr: dropTime})))
	for _, l := range []*slog.Logger{plain, ours} {
		l.Info("x", slog.Any("tpl", envless), slog.Any("ref", &ateapipb.ObjectRef{Name: "n"}), slog.Int("n", 1), slog.String("a", "b"), slog.Bool("c", true), slog.Float64("f", 1.5), slog.Duration("d", time.Second))
	}
	if plainBuf.String() != oursBuf.String() {
		t.Fatalf("records differ:\n plain: %s\n ours:  %s", plainBuf.String(), oursBuf.String())
	}
	// A record of plain values and clean types is scanned without
	// allocating, whatever its size.
	for _, attrs := range [][]slog.Attr{
		{slog.Int("n", 1), slog.String("a", "b"), slog.Bool("c", true)},
		{slog.Any("ref", &ateapipb.ObjectRef{Name: "n"}), slog.Any("resp", &ateapipb.ListActorsResponse{}), slog.Int("n", 1)},
		{slog.Int("n", 1), slog.String("a", "b"), slog.Bool("c", true), slog.Float64("f", 1.5), slog.Duration("d", time.Second), slog.Any("ref", &ateapipb.ObjectRef{Name: "n"}), slog.Group("g", slog.String("k", "v"))},
	} {
		rec := slog.NewRecord(time.Now(), slog.LevelInfo, "x", 0)
		rec.AddAttrs(attrs...)
		if allocs := testing.AllocsPerRun(100, func() { redactRecord(rec) }); allocs != 0 {
			t.Errorf("redactRecord allocated %v times on %d attrs with nothing to change", allocs, len(attrs))
		}
	}
}

func TestHandleLeavesRecordsWithoutProtosAlone(t *testing.T) {
	// Same record through the bare JSON handler and through ours must match
	// byte for byte (no span in ctx, so no trace fields are added).
	var plainBuf, oursBuf bytes.Buffer
	plain := slog.New(slog.NewJSONHandler(&plainBuf, &slog.HandlerOptions{ReplaceAttr: dropTime}))
	ours := slog.New(NewHandler(slog.NewJSONHandler(&oursBuf, &slog.HandlerOptions{ReplaceAttr: dropTime})))
	for _, l := range []*slog.Logger{plain, ours} {
		l.Info("x", slog.String("a", "b"), slog.Int("n", 1), slog.Any("nilproto", (*ateapipb.ObjectRef)(nil)), slog.Any("m", map[string]int{"k": 1}))
	}
	if plainBuf.String() != oursBuf.String() {
		t.Fatalf("records differ:\n plain: %s\n ours:  %s", plainBuf.String(), oursBuf.String())
	}
}

func dropTime(_ []string, a slog.Attr) slog.Attr {
	if a.Key == slog.TimeKey {
		return slog.Attr{}
	}
	return a
}

// BenchmarkHandle measures the cost the shared handler adds per log line for
// the three shapes that matter: no proto at all, a proto whose type has no
// debug_redact field (returned without a copy), and one that has.
func BenchmarkHandle(b *testing.B) {
	listResp := &ateapipb.ListActorsResponse{}
	for i := 0; i < 50; i++ {
		listResp.Actors = append(listResp.Actors, &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: "s", Name: "agent", Uid: "86fae7b8-c9b4-479c-b7c8-6ef3ede0ab0b", Version: 1},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: "s", Name: "tpl"},
		})
	}
	tpl := &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{{Name: "c"}}}
	for i := 0; i < 32; i++ {
		tpl.Containers[0].Env = append(tpl.Containers[0].Env, &ateapipb.EnvVar{Name: "VAR", Value: "some-fairly-long-value-1234567890"})
	}
	cases := []struct {
		name  string
		attrs []slog.Attr
	}{
		{"no_proto", []slog.Attr{slog.String("host", "example.com"), slog.String("leg", "mitm"), slog.Int("n", 3)}},
		{"clean_objectref", []slog.Attr{slog.Any("actor", &ateapipb.ObjectRef{Atespace: "s", Name: "agent"})}},
		{"clean_listactors_50", []slog.Attr{slog.Any("resp", listResp)}},
		{"sensitive_template_32env", []slog.Attr{slog.Any("resp", tpl)}},
	}
	for _, tc := range cases {
		for _, h := range []struct {
			name  string
			build func() slog.Handler
		}{
			{"plain_json", func() slog.Handler { return slog.NewJSONHandler(io.Discard, nil) }},
			{"contextlogging", func() slog.Handler { return NewHandler(slog.NewJSONHandler(io.Discard, nil)) }},
		} {
			b.Run(tc.name+"/"+h.name, func(b *testing.B) {
				l := slog.New(h.build())
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					l.LogAttrs(context.Background(), slog.LevelInfo, "x", tc.attrs...)
				}
			})
		}
	}
}
