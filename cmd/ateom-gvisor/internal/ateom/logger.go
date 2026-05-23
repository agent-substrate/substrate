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

package ateom

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"time"
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

// NewActorLogger creates a new ActorLogger wrapping the provided destination writer.
func NewActorLogger(w io.Writer, isOnGCE bool) *ActorLogger {
	labelsKey := "labels"
	if isOnGCE {
		labelsKey = "logging.googleapis.com/labels"
	}
	return &ActorLogger{
		writer:    w,
		labelsKey: labelsKey,
	}
}

// EmitLifecycleLog logs a synthetic actor lifecycle event.
func (al *ActorLogger) EmitLifecycleLog(msg, actorID, actorTemplate, actorNamespace string) {
	envelope := map[string]any{
		"time":    time.Now().Format(time.RFC3339Nano),
		"message": msg,
		al.labelsKey: map[string]string{
			"ate.dev/actor_id":        actorID,
			"ate.dev/actor_template":  actorTemplate,
			"ate.dev/actor_namespace": actorNamespace,
		},
	}
	if envBytes, err := json.Marshal(envelope); err == nil {
		envBytes = append(envBytes, '\n')
		_, _ = al.writer.Write(envBytes)
	}
}

// StartJSONLogPipe intercepts container raw stdout/stderr streams and pipes them through the logger.
func (al *ActorLogger) StartJSONLogPipe(actorID, actorTemplate, actorNamespace string) (io.WriteCloser, error) {
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	go func() {
		al.WrapContainerLogs(pr, actorID, actorTemplate, actorNamespace)
		pr.Close()
	}()
	return pw, nil
}

// WrapContainerLogs reads log lines from r, parses them, and logs them in a unified structured format.
func (al *ActorLogger) WrapContainerLogs(r io.Reader, actorID, actorTemplate, actorNamespace string) {
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
			if unmarshalErr == nil {
				var trailing any
				if err := dec.Decode(&trailing); err != io.EOF {
					unmarshalErr = errors.New("trailing garbage detected after JSON object")
				}
			}

			if unmarshalErr != nil {
				envelope = map[string]any{
					"time":    time.Now().Format(time.RFC3339Nano),
					"message": string(lineBytes),
					al.labelsKey: map[string]string{
						"ate.dev/actor_id":        actorID,
						"ate.dev/actor_template":  actorTemplate,
						"ate.dev/actor_namespace": actorNamespace,
					},
				}
			} else {
				if _, ok := m["time"]; !ok {
					m["time"] = time.Now().Format(time.RFC3339Nano)
				}
				labels, ok := m[al.labelsKey].(map[string]any)
				if !ok {
					labels = make(map[string]any)
					m[al.labelsKey] = labels
				}
				labels["ate.dev/actor_id"] = actorID
				labels["ate.dev/actor_template"] = actorTemplate
				labels["ate.dev/actor_namespace"] = actorNamespace
				envelope = m
			}

			if envBytes, err := json.Marshal(envelope); err == nil {
				envBytes = append(envBytes, '\n')
				_, _ = al.writer.Write(envBytes)
			}
		}

		if err != nil {
			break
		}
	}
}
