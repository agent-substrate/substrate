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

package actorlog

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
)

const (
	testAtespace     = "default"
	testActorName    = "act-1"
	testActorUID     = "uid-1"
	testTemplateNS   = "tmpl-ns"
	testTemplateName = "tmpl-1"
	testContainer    = "ctr-1"
)

var testAttribution = resources.ActorAttribution{
	Ref:              resources.ActorRef{Atespace: testAtespace, Name: testActorName},
	UID:              testActorUID,
	TemplateAtespace: testTemplateNS,
	TemplateName:     testTemplateName,
}

// The registry owns the spellings (pinned by ateattr.TestKeySpellings), so the
// tests read them from there rather than restating them and drifting.
var (
	atespaceLabel      = string(ateattr.AtespaceKey)
	actorNameLabel     = string(ateattr.ActorNameKey)
	actorUIDLabel      = string(ateattr.ActorUIDKey)
	containerNameLabel = string(ateattr.ActorContainerNameKey)
	templateNSLabel    = string(ateattr.TemplateNamespaceKey)
	templateNameLabel  = string(ateattr.TemplateNameKey)
)

// decodeLine parses the single record written to buf, preserving number literals
// so precision checks are meaningful.
func decodeLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}
	return m
}

func labelGroup(t *testing.T, al *ActorLogger, m map[string]any) map[string]any {
	t.Helper()
	labelsAny, ok := m[al.labelsKey]
	if !ok {
		t.Fatal("missing labels group")
	}
	labels, ok := labelsAny.(map[string]any)
	if !ok {
		t.Fatalf("labels group is not a map: %T", labelsAny)
	}
	return labels
}

func assertLabels(t *testing.T, labels map[string]any, want map[string]string) {
	t.Helper()
	for k, wv := range want {
		if got := labels[k]; got != wv {
			t.Errorf("label %s = %v, want %q", k, got, wv)
		}
	}
}

// identityLabels is the full set every container record carries.
func identityLabels() map[string]string {
	return map[string]string{
		atespaceLabel:      testAtespace,
		actorNameLabel:     testActorName,
		actorUIDLabel:      testActorUID,
		containerNameLabel: testContainer,
		templateNSLabel:    testTemplateNS,
		templateNameLabel:  testTemplateName,
	}
}

func TestWrapContainerLogs(t *testing.T) {
	var buf bytes.Buffer
	al := NewActorLogger(&buf, false)
	al.WrapContainerLogs(strings.NewReader("Test application log output\n"), testAttribution, testContainer)

	m := decodeLine(t, &buf)

	if m["message"] != "Test application log output" {
		t.Errorf("got message = %v, want 'Test application log output'", m["message"])
	}
	if _, ok := m["level"]; ok {
		t.Errorf("level should be absent for plain text logs (no guessing)")
	}

	assertLabels(t, labelGroup(t, al, m), identityLabels())
}

func TestWrapContainerLogs_JSONInput(t *testing.T) {
	// A large 64-bit integer and a pre-existing time field: neither may be
	// rewritten on the way through.
	input := `{"level":"info","msg":"Started container","custom_attr":"value","count":1234567890123456789,"time":"2026-05-16T01:03:37Z"}` + "\n"

	var buf bytes.Buffer
	al := NewActorLogger(&buf, false)
	al.WrapContainerLogs(strings.NewReader(input), testAttribution, testContainer)

	m := decodeLine(t, &buf)

	if m["msg"] != "Started container" {
		t.Errorf("got msg = %v, want 'Started container'", m["msg"])
	}
	if m["level"] != "info" {
		t.Errorf("got level = %v, want 'info'", m["level"])
	}
	if m["custom_attr"] != "value" {
		t.Errorf("got custom_attr = %v, want 'value'", m["custom_attr"])
	}
	if m["time"] != "2026-05-16T01:03:37Z" {
		t.Errorf("got time = %v, want '2026-05-16T01:03:37Z' (pre-existing time should be preserved)", m["time"])
	}
	if m["count"] != json.Number("1234567890123456789") {
		t.Errorf("got count = %v, want json.Number('1234567890123456789') (large integer should be preserved exactly)", m["count"])
	}

	assertLabels(t, labelGroup(t, al, m), identityLabels())
}

// TestWrapContainerLogs_ActorTraceContextPassthrough covers the only way an actor
// log line can join a trace: the actor emitting the spec fields itself. ateom
// cannot supply them, because one forwarder covers a whole stream.
func TestWrapContainerLogs_ActorTraceContextPassthrough(t *testing.T) {
	const (
		traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
		spanID  = "00f067aa0ba902b7"
	)
	input := `{"msg":"handled","` + ateattr.LogTraceIDField + `":"` + traceID + `","` + ateattr.LogSpanIDField + `":"` + spanID + `"}` + "\n"

	var buf bytes.Buffer
	al := NewActorLogger(&buf, false)
	al.WrapContainerLogs(strings.NewReader(input), testAttribution, testContainer)

	m := decodeLine(t, &buf)
	if m[ateattr.LogTraceIDField] != traceID {
		t.Errorf("got %s = %v, want %q", ateattr.LogTraceIDField, m[ateattr.LogTraceIDField], traceID)
	}
	if m[ateattr.LogSpanIDField] != spanID {
		t.Errorf("got %s = %v, want %q", ateattr.LogSpanIDField, m[ateattr.LogSpanIDField], spanID)
	}
}

func TestSyncedWriter_Concurrency(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSyncedWriter(&buf)

	const numWorkers = 10
	const writesPerWorker = 100
	const lineLen = 10
	var wg sync.WaitGroup

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < writesPerWorker; j++ {
				if _, err := sw.Write([]byte(strings.Repeat("a", lineLen) + "\n")); err != nil {
					t.Errorf("write failed: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != numWorkers*writesPerWorker {
		t.Errorf("got %d lines, want %d", len(lines), numWorkers*writesPerWorker)
	}
	for i, line := range lines {
		if len(line) != lineLen {
			t.Errorf("line %d has length %d, want %d (interleaved write detected?): %q", i, len(line), lineLen, line)
		}
	}
}

func TestWrapContainerLogs_MergeLabels(t *testing.T) {
	input := `{"level":"info","msg":"App log","labels":{"app":"my-app","version":"v1"}}` + "\n"

	var buf bytes.Buffer
	al := NewActorLogger(&buf, false)
	al.WrapContainerLogs(strings.NewReader(input), testAttribution, testContainer)

	labels := labelGroup(t, al, decodeLine(t, &buf))

	want := identityLabels()
	want["app"] = "my-app"
	want["version"] = "v1"
	assertLabels(t, labels, want)
}

// TestWrapContainerLogs_ReservedNamespace pins that an actor cannot write into
// substrate's namespace: not the identity labels it would be forging, and not any
// other ate.* key that would read as platform-issued attribution downstream.
func TestWrapContainerLogs_ReservedNamespace(t *testing.T) {
	input := `{"level":"info","msg":"App log","` + actorNameLabel + `":"top-level-forgery","ate.tenant":"forged",` +
		`"labels":{"` + actorNameLabel + `":"malicious-name","` + actorUIDLabel + `":"malicious-uid","ate.tenant":"forged","app":"my-app"}}` + "\n"

	var buf bytes.Buffer
	al := NewActorLogger(&buf, false)
	al.WrapContainerLogs(strings.NewReader(input), testAttribution, testContainer)

	m := decodeLine(t, &buf)
	for _, k := range []string{actorNameLabel, "ate.tenant"} {
		if v, ok := m[k]; ok {
			t.Errorf("reserved top-level key %s survived with value %v", k, v)
		}
	}

	labels := labelGroup(t, al, m)
	if _, ok := labels["ate.tenant"]; ok {
		t.Error("unrecognized reserved label ate.tenant survived; it reads as platform-issued")
	}
	want := identityLabels()
	want["app"] = "my-app"
	assertLabels(t, labels, want)
}

// TestWrapContainerLogs_ForeignLabelGroup covers the label group this logger does
// not write. An actor can set either spelling, and the unwritten one is not inert:
// off GCE a forged logging.googleapis.com/labels is the very key Cloud Logging
// promotes into LogEntry.labels, so leaving it alone would let the actor outrank
// substrate's own attribution, and let it match another actor's kubectl ate logs
// stream. Both spellings fold into one sanitized group.
func TestWrapContainerLogs_ForeignLabelGroup(t *testing.T) {
	tests := []struct {
		name    string
		onGCE   bool
		foreign string
	}{
		{name: "on GCE the plain group is the foreign one", onGCE: true, foreign: labelsKeyPlain},
		{name: "off GCE the Cloud Logging group is the foreign one", onGCE: false, foreign: labelsKeyGCE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := `{"msg":"App log","` + tt.foreign + `":{"` + actorNameLabel + `":"forged-name",` +
				`"ate.tenant":"forged","app":"my-app"}}` + "\n"

			var buf bytes.Buffer
			al := NewActorLogger(&buf, tt.onGCE)
			al.WrapContainerLogs(strings.NewReader(input), testAttribution, testContainer)

			m := decodeLine(t, &buf)
			if _, ok := m[tt.foreign]; ok {
				t.Errorf("foreign label group %q survived: %v", tt.foreign, m[tt.foreign])
			}

			labels := labelGroup(t, al, m)
			if _, ok := labels["ate.tenant"]; ok {
				t.Error("reserved label ate.tenant survived the fold")
			}
			want := identityLabels()
			want["app"] = "my-app"
			assertLabels(t, labels, want)
			if len(labels) != len(want) {
				t.Errorf("got %d labels, want %d: %v", len(labels), len(want), labels)
			}
		})
	}
}

// TestWrapContainerLogs_NonStringLabelValue guards the GKE label contract: one
// non-string value under logging.googleapis.com/labels costs the whole record its
// labels, so actor-supplied values are stringified rather than forwarded as-is.
func TestWrapContainerLogs_NonStringLabelValue(t *testing.T) {
	input := `{"msg":"App log","labels":{"replicas":3,"enabled":true,"nested":{"a":1},"empty":null}}` + "\n"

	var buf bytes.Buffer
	al := NewActorLogger(&buf, false)
	al.WrapContainerLogs(strings.NewReader(input), testAttribution, testContainer)

	labels := labelGroup(t, al, decodeLine(t, &buf))
	for k, v := range labels {
		if _, ok := v.(string); !ok {
			t.Errorf("label %s = %v (%T), want a string", k, v, v)
		}
	}
	assertLabels(t, labels, map[string]string{
		"replicas": "3",
		"enabled":  "true",
		"nested":   `{"a":1}`,
		"empty":    "",
	})
}

func TestWrapContainerLogs_JSONNull(t *testing.T) {
	var buf bytes.Buffer
	al := NewActorLogger(&buf, false)
	al.WrapContainerLogs(strings.NewReader("null\n"), testAttribution, testContainer)

	m := decodeLine(t, &buf)
	if m["message"] != "null" {
		t.Errorf("got message = %v, want 'null'", m["message"])
	}
	assertLabels(t, labelGroup(t, al, m), identityLabels())
}

func TestWrapContainerLogs_TrailingGarbage(t *testing.T) {
	const line = `{"count": 1} garbage`

	var buf bytes.Buffer
	al := NewActorLogger(&buf, false)
	al.WrapContainerLogs(strings.NewReader(line+"\n"), testAttribution, testContainer)

	m := decodeLine(t, &buf)
	if m["message"] != line {
		t.Errorf("got message = %v, want %q", m["message"], line)
	}
	assertLabels(t, labelGroup(t, al, m), identityLabels())
}

func TestEmitLifecycleLog(t *testing.T) {
	const (
		traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
		spanID  = "00f067aa0ba902b7"
		msg     = "Actor restored"
	)

	sampled := trace.SpanContextConfig{
		TraceID:    mustTraceID(t, traceID),
		SpanID:     mustSpanID(t, spanID),
		TraceFlags: trace.FlagsSampled,
	}
	unsampled := sampled
	unsampled.TraceFlags = 0

	tests := []struct {
		name           string
		ctx            context.Context
		wantTraceID    string
		wantSpanID     string
		wantTraceFlags string
	}{
		{
			name: "no span in context",
			ctx:  context.Background(),
		},
		{
			name: "invalid span context contributes nothing",
			ctx:  trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{})),
		},
		{
			name:           "sampled span",
			ctx:            trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(sampled)),
			wantTraceID:    traceID,
			wantSpanID:     spanID,
			wantTraceFlags: "01",
		},
		{
			name:           "unsampled span still correlates",
			ctx:            trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(unsampled)),
			wantTraceID:    traceID,
			wantSpanID:     spanID,
			wantTraceFlags: "00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			al := NewActorLogger(&buf, false)
			al.EmitLifecycleLog(tt.ctx, msg, testAttribution)

			m := decodeLine(t, &buf)
			if m["message"] != msg {
				t.Errorf("got message = %v, want %q", m["message"], msg)
			}
			if m["time"] == nil {
				t.Error("lifecycle records must carry a timestamp")
			}

			for field, want := range map[string]string{
				ateattr.LogTraceIDField:    tt.wantTraceID,
				ateattr.LogSpanIDField:     tt.wantSpanID,
				ateattr.LogTraceFlagsField: tt.wantTraceFlags,
			} {
				got, present := m[field]
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

			labels := labelGroup(t, al, m)
			// A lifecycle event is actor-scoped: no container produced it.
			if v, ok := labels[containerNameLabel]; ok {
				t.Errorf("%s = %v, want absent on a lifecycle record", containerNameLabel, v)
			}
			want := identityLabels()
			delete(want, containerNameLabel)
			assertLabels(t, labels, want)
			if len(labels) != len(want) {
				t.Errorf("got %d labels, want %d: %v", len(labels), len(want), labels)
			}
		})
	}
}

func mustTraceID(t *testing.T, s string) trace.TraceID {
	t.Helper()
	id, err := trace.TraceIDFromHex(s)
	if err != nil {
		t.Fatalf("TraceIDFromHex(%q): %v", s, err)
	}
	return id
}

func mustSpanID(t *testing.T, s string) trace.SpanID {
	t.Helper()
	id, err := trace.SpanIDFromHex(s)
	if err != nil {
		t.Fatalf("SpanIDFromHex(%q): %v", s, err)
	}
	return id
}
