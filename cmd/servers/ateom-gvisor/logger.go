//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strings"
)

// ActorLogger handles structured logging for actor sandboxes and lifecycle events.
type ActorLogger struct {
	labelsKey string
	logger    *slog.Logger
}

// NewActorLogger creates a new ActorLogger wrapping the provided destination writer.
func NewActorLogger(logger *slog.Logger, isOnGCE bool) *ActorLogger {
	labelsKey := "labels"
	if isOnGCE {
		labelsKey = "logging.googleapis.com/labels"
	}
	return &ActorLogger{
		labelsKey: labelsKey,
		logger:    logger,
	}
}

// EmitLifecycleLog logs a synthetic actor lifecycle event.
func (al *ActorLogger) EmitLifecycleLog(msg, actorID, actorTemplate, actorNamespace string) {
	slog.LogAttrs(context.Background(), slog.LevelInfo, msg,
		slog.Group(al.labelsKey,
			slog.String("ate.dev/actor_id", actorID),
			slog.String("ate.dev/actor_template", actorTemplate),
			slog.String("ate.dev/actor_namespace", actorNamespace),
		),
	)
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
	ctx := context.Background()
	rdr := bufio.NewReader(r)
	for {
		lineBytes, err := rdr.ReadBytes('\n')

		// Strip trailing newline from ReadBytes if present
		if len(lineBytes) > 0 && lineBytes[len(lineBytes)-1] == '\n' {
			lineBytes = lineBytes[:len(lineBytes)-1]
		}

		if len(lineBytes) > 0 {
			var m map[string]any
			if unmarshalErr := json.Unmarshal(lineBytes, &m); unmarshalErr != nil {
				al.logger.LogAttrs(ctx, slog.LevelInfo, string(lineBytes),
					slog.Group(al.labelsKey,
						slog.String("ate.dev/actor_id", actorID),
						slog.String("ate.dev/actor_template", actorTemplate),
						slog.String("ate.dev/actor_namespace", actorNamespace),
					),
				)
			} else {
				al.parseAndLogContainerJSONLine(ctx, m, actorID, actorTemplate, actorNamespace)
			}
		}

		if err != nil {
			break
		}
	}
}

func (al *ActorLogger) parseAndLogContainerJSONLine(ctx context.Context, m map[string]any, actorID, actorTemplate, actorNamespace string) {
	msg := ""
	if mMsg, ok := m["msg"].(string); ok {
		msg = mMsg
	} else if mMessage, ok := m["message"].(string); ok {
		msg = mMessage
	}

	level := slog.LevelInfo
	if mLevel, ok := m["level"].(string); ok {
		switch strings.ToLower(mLevel) {
		case "debug":
			level = slog.LevelDebug
		case "info":
			level = slog.LevelInfo
		case "warn", "warning":
			level = slog.LevelWarn
		case "error":
			level = slog.LevelError
		}
	}

	var attrs []slog.Attr
	for k, v := range m {
		if k == "msg" || k == "message" || k == "level" || k == "time" {
			continue
		}
		attrs = append(attrs, slog.Any(k, v))
	}

	attrs = append(attrs, slog.Group(al.labelsKey,
		slog.String("ate.dev/actor_id", actorID),
		slog.String("ate.dev/actor_template", actorTemplate),
		slog.String("ate.dev/actor_namespace", actorNamespace),
	))

	al.logger.LogAttrs(ctx, level, msg, attrs...)
}
