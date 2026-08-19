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

// Package actorlog provides structured JSON logging for actor sandboxes shared
// by the gVisor and micro-VM ateom runtimes. It forwards an actor container's
// stdout/stderr to the worker pod's stdout, annotated with the ate.* identity
// labels from internal/ateattr, and emits synthetic actor lifecycle events.
//
// Only the lifecycle events carry trace context: one forwarder goroutine covers a
// whole container stream, so it cannot tell which request produced a given line.
// An actor emitting its own trace_id/span_id keeps them, verbatim like the rest of
// its fields.
package actorlog

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
)

// SyncedWriter wraps an io.Writer and synchronizes writes across goroutines.
type SyncedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

// Write writes the byte slice to the underlying writer, synchronized by a mutex.
func (sw *SyncedWriter) Write(p []byte) (n int, err error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.w.Write(p)
}

// NewSyncedWriter returns a new SyncedWriter wrapping the given io.Writer.
func NewSyncedWriter(w io.Writer) *SyncedWriter {
	return &SyncedWriter{w: w}
}

// ActorLogger handles structured logging for actor sandboxes and lifecycle events.
type ActorLogger struct {
	writer    io.Writer
	labelsKey string
}

// The two spellings of the label group. Cloud Logging promotes the second into
// LogEntry.labels, so that is the one to use on GCE.
const (
	labelsKeyPlain = "labels"
	labelsKeyGCE   = "logging.googleapis.com/labels"
)

// NewActorLogger creates a new ActorLogger wrapping the provided destination writer.
func NewActorLogger(w io.Writer, isOnGCE bool) *ActorLogger {
	labelsKey := labelsKeyPlain
	if isOnGCE {
		labelsKey = labelsKeyGCE
	}
	return &ActorLogger{
		writer:    w,
		labelsKey: labelsKey,
	}
}

// EmitLifecycleLog logs a synthetic actor lifecycle event. The record joins the
// trace of the RPC that drove the transition whenever ctx carries one.
func (al *ActorLogger) EmitLifecycleLog(ctx context.Context, msg string, a resources.ActorAttribution) {
	envelope := map[string]any{
		"time":       time.Now().Format(time.RFC3339Nano),
		"message":    msg,
		al.labelsKey: ateattr.ActorLogLabels(a, ""),
	}
	addTraceContext(ctx, envelope)
	al.write(envelope)
}

// StartJSONLogPipe intercepts container raw stdout/stderr streams and pipes them
// through the logger. containerName tags every line with the originating container;
// callers that multiplex multiple containers should give each its own pipe so the
// tag is meaningful.
func (al *ActorLogger) StartJSONLogPipe(a resources.ActorAttribution, containerName string) (io.WriteCloser, error) {
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	go func() {
		al.WrapContainerLogs(pr, a, containerName)
		pr.Close()
	}()
	return pw, nil
}

// WrapContainerLogs reads log lines from r, parses them, and logs them in a unified
// structured format. containerName is added as the ate.actor.container.name label
// so multi-container actors can be demultiplexed.
func (al *ActorLogger) WrapContainerLogs(r io.Reader, a resources.ActorAttribution, containerName string) {
	rdr := bufio.NewReader(r)
	for {
		lineBytes, err := rdr.ReadBytes('\n')

		// Strip trailing newline from ReadBytes if present
		if len(lineBytes) > 0 && lineBytes[len(lineBytes)-1] == '\n' {
			lineBytes = lineBytes[:len(lineBytes)-1]
		}

		if len(lineBytes) > 0 {
			var m map[string]any
			var envelope map[string]any

			dec := json.NewDecoder(bytes.NewReader(lineBytes))
			dec.UseNumber()

			unmarshalErr := dec.Decode(&m)
			if unmarshalErr == nil && m == nil {
				unmarshalErr = errors.New("JSON value is not an object")
			}
			if unmarshalErr == nil {
				var trailing any
				if err := dec.Decode(&trailing); err != io.EOF {
					unmarshalErr = errors.New("trailing garbage detected after JSON object")
				}
			}

			if unmarshalErr != nil {
				envelope = map[string]any{
					"time":       time.Now().Format(time.RFC3339Nano),
					"message":    string(lineBytes),
					al.labelsKey: ateattr.ActorLogLabels(a, containerName),
				}
			} else {
				if _, ok := m["time"]; !ok {
					m["time"] = time.Now().Format(time.RFC3339Nano)
				}
				for k := range m {
					if strings.HasPrefix(k, ateattr.ReservedNamespace) {
						delete(m, k)
					}
				}
				labels := al.foldLabelGroups(m)
				for k, v := range ateattr.ActorLogLabels(a, containerName) {
					labels[k] = v
				}
				m[al.labelsKey] = labels
				envelope = m
			}

			al.write(envelope)
		}

		if err != nil {
			break
		}
	}
}

func (al *ActorLogger) write(envelope map[string]any) {
	if envBytes, err := json.Marshal(envelope); err == nil {
		envBytes = append(envBytes, '\n')
		_, _ = al.writer.Write(envBytes)
	}
}

func addTraceContext(ctx context.Context, envelope map[string]any) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return
	}
	envelope[ateattr.LogTraceIDField] = sc.TraceID().String()
	envelope[ateattr.LogSpanIDField] = sc.SpanID().String()
	envelope[ateattr.LogTraceFlagsField] = fmt.Sprintf("%02x", byte(sc.TraceFlags()))
}

// foldLabelGroups reduces the record to a single sanitized label group, under the
// key this logger writes.
//
// Both spellings have to be handled whichever one is ours, because nothing stops
// an actor setting either, and the one we do not write is not inert: off GCE, a
// forged logging.googleapis.com/labels is precisely the key Cloud Logging promotes
// into LogEntry.labels, so it would outrank the group we wrote. Keys the active
// group already holds win, so the fold cannot change what this logger's own group
// says.
func (al *ActorLogger) foldLabelGroups(m map[string]any) map[string]any {
	labels := sanitizeLabels(m[al.labelsKey])
	for _, key := range []string{labelsKeyPlain, labelsKeyGCE} {
		if key == al.labelsKey {
			continue
		}
		for k, v := range sanitizeLabels(m[key]) {
			if _, taken := labels[k]; !taken {
				labels[k] = v
			}
		}
		delete(m, key)
	}
	return labels
}

// sanitizeLabels drops reserved keys from the actor's label group and stringifies
// the rest: GKE promotes the group into LogEntry.labels, where one non-string value
// costs the whole record its labels.
func sanitizeLabels(v any) map[string]any {
	labels, ok := v.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	out := make(map[string]any, len(labels))
	for k, val := range labels {
		if strings.HasPrefix(k, ateattr.ReservedNamespace) {
			continue
		}
		out[k] = labelValueString(val)
	}
	return out
}

func labelValueString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case nil:
		return ""
	default:
		if b, err := json.Marshal(val); err == nil {
			return string(b)
		}
		return fmt.Sprint(val)
	}
}
