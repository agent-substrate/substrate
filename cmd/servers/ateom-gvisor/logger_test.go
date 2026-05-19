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
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/contextlogging"
)

func TestWrapContainerLogs(t *testing.T) {
	input := "Test application log output\n"
	rdr := strings.NewReader(input)

	var buf bytes.Buffer
	logger := slog.New(contextlogging.NewHandler(slog.NewJSONHandler(&buf, nil)))

	al := NewActorLogger(logger, false)
	al.WrapContainerLogs(rdr, "act-1", "tmpl-1", "default")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

	if m["msg"] != "Test application log output" {
		t.Errorf("got msg = %v, want 'Test application log output'", m["msg"])
	}
	if m["level"] != "INFO" {
		t.Errorf("got level = %v, want 'INFO'", m["level"])
	}

	labelsAny, ok := m[al.labelsKey]
	if !ok {
		t.Fatal("missing labels group")
	}
	labels, ok := labelsAny.(map[string]any)
	if !ok {
		t.Fatal("labels group is not a map")
	}

	if labels["ate.dev/actor_id"] != "act-1" {
		t.Errorf("got actor_id = %v, want 'act-1'", labels["ate.dev/actor_id"])
	}
	if labels["ate.dev/actor_template"] != "tmpl-1" {
		t.Errorf("got actor_template = %v, want 'tmpl-1'", labels["ate.dev/actor_template"])
	}
	if labels["ate.dev/actor_namespace"] != "default" {
		t.Errorf("got actor_namespace = %v, want 'default'", labels["ate.dev/actor_namespace"])
	}
}

func TestWrapContainerLogs_JSONInput(t *testing.T) {
	input := `{"level":"info","msg":"Started container","custom_attr":"value"}` + "\n"
	rdr := strings.NewReader(input)

	var buf bytes.Buffer
	logger := slog.New(contextlogging.NewHandler(slog.NewJSONHandler(&buf, nil)))

	al := NewActorLogger(logger, false)
	al.WrapContainerLogs(rdr, "act-1", "tmpl-1", "default")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

	if m["msg"] != "Started container" {
		t.Errorf("got msg = %v, want 'Started container'", m["msg"])
	}
	if m["level"] != "INFO" {
		t.Errorf("got level = %v, want 'INFO'", m["level"])
	}
	if m["custom_attr"] != "value" {
		t.Errorf("got custom_attr = %v, want 'value'", m["custom_attr"])
	}

	labelsAny, ok := m[al.labelsKey]
	if !ok {
		t.Fatal("missing labels group")
	}
	labels, ok := labelsAny.(map[string]any)
	if !ok {
		t.Fatal("labels group is not a map")
	}

	if labels["ate.dev/actor_id"] != "act-1" {
		t.Errorf("got actor_id = %v, want 'act-1'", labels["ate.dev/actor_id"])
	}
}
