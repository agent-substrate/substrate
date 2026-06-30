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
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

func TestWrapContainerLogs(t *testing.T) {
	input := "Test application log output\n"
	rdr := strings.NewReader(input)

	var buf bytes.Buffer
	al := NewActorLogger(&buf, false)
	al.WrapContainerLogs(rdr, "act-1", "tmpl-1", "default", "ctr-1")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

	if m["message"] != "Test application log output" {
		t.Errorf("got message = %v, want 'Test application log output'", m["message"])
	}
	if _, ok := m["level"]; ok {
		t.Errorf("level should be absent for plain text logs (no guessing)")
	}
	if _, ok := m["actor_log"]; ok {
		t.Errorf("actor_log should be absent for text logs")
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
	if labels["ate.dev/actor_template_name"] != "tmpl-1" {
		t.Errorf("got actor_template_name = %v, want 'tmpl-1'", labels["ate.dev/actor_template_name"])
	}
	if labels["ate.dev/actor_template_namespace"] != "default" {
		t.Errorf("got actor_template_namespace = %v, want 'default'", labels["ate.dev/actor_template_namespace"])
	}
	if labels["ate.dev/container_name"] != "ctr-1" {
		t.Errorf("got container_name = %v, want 'ctr-1'", labels["ate.dev/container_name"])
	}
}

func TestWrapContainerLogs_JSONInput(t *testing.T) {
	// Include large 64-bit integer and pre-existing time field
	input := `{"level":"info","msg":"Started container","custom_attr":"value","trace_id":1234567890123456789,"time":"2026-05-16T01:03:37Z"}` + "\n"
	rdr := strings.NewReader(input)

	var buf bytes.Buffer
	al := NewActorLogger(&buf, false)
	al.WrapContainerLogs(rdr, "act-1", "tmpl-1", "default", "ctr-1")

	dec := json.NewDecoder(&buf)
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

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
	if m["trace_id"] != json.Number("1234567890123456789") {
		t.Errorf("got trace_id = %v, want json.Number('1234567890123456789') (large integer should be preserved exactly)", m["trace_id"])
	}
	if _, ok := m["actor_log"]; ok {
		t.Errorf("actor_log should be absent for flat JSON logs")
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

func TestSyncedWriter_Concurrency(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSyncedWriter(&buf)

	const numWorkers = 10
	const writesPerWorker = 100
	var wg sync.WaitGroup

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < writesPerWorker; j++ {
				line := []byte(strings.Repeat("a", 10) + "\n")
				_, err := sw.Write(line)
				if err != nil {
					t.Errorf("write failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	lines := strings.Split(buf.String(), "\n")
	if len(lines) != numWorkers*writesPerWorker+1 {
		t.Errorf("got %d lines, want %d", len(lines)-1, numWorkers*writesPerWorker)
	}
	for i, line := range lines {
		if i == len(lines)-1 {
			if line != "" {
				t.Errorf("last line should be empty")
			}
			continue
		}
		if len(line) != 10 {
			t.Errorf("line %d has length %d, want 10 (interleaved write detected?): %q", i, len(line), line)
		}
	}
}

func TestWrapContainerLogs_MergeLabels(t *testing.T) {
	input := `{"level":"info","msg":"App log","labels":{"app":"my-app","version":"v1"}}` + "\n"
	rdr := strings.NewReader(input)

	var buf bytes.Buffer
	al := NewActorLogger(&buf, false) // labelsKey will be "labels"
	al.WrapContainerLogs(rdr, "act-1", "tmpl-1", "default", "ctr-1")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

	labelsAny, ok := m[al.labelsKey]
	if !ok {
		t.Fatal("missing labels group")
	}
	labels, ok := labelsAny.(map[string]any)
	if !ok {
		t.Fatal("labels group is not a map")
	}

	if labels["app"] != "my-app" {
		t.Errorf("got app = %v, want 'my-app'", labels["app"])
	}
	if labels["version"] != "v1" {
		t.Errorf("got version = %v, want 'v1'", labels["version"])
	}
	if labels["ate.dev/actor_id"] != "act-1" {
		t.Errorf("got actor_id = %v, want 'act-1'", labels["ate.dev/actor_id"])
	}
	if labels["ate.dev/container_name"] != "ctr-1" {
		t.Errorf("got container_name = %v, want 'ctr-1'", labels["ate.dev/container_name"])
	}
}

func TestWrapContainerLogs_LabelCollision(t *testing.T) {
	input := `{"level":"info","msg":"App log","labels":{"ate.dev/actor_id":"malicious-id","app":"my-app"}}` + "\n"
	rdr := strings.NewReader(input)

	var buf bytes.Buffer
	al := NewActorLogger(&buf, false)
	al.WrapContainerLogs(rdr, "act-1", "tmpl-1", "default", "ctr-1")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

	labelsAny, ok := m[al.labelsKey]
	if !ok {
		t.Fatal("missing labels group")
	}
	labels, ok := labelsAny.(map[string]any)
	if !ok {
		t.Fatal("labels group is not a map")
	}

	if labels["app"] != "my-app" {
		t.Errorf("got app = %v, want 'my-app'", labels["app"])
	}
	if labels["ate.dev/actor_id"] != "act-1" {
		t.Errorf("got actor_id = %v, want 'act-1' (Substrate metadata should take precedence)", labels["ate.dev/actor_id"])
	}
}

func TestWrapContainerLogs_CrossKeyLabelSpoofing(t *testing.T) {
	// Non-GCE logger writes under "labels"; an app must not be able to spoof
	// metadata under the other recognized key and have a consumer trust it.
	input := `{"level":"info","msg":"App log","logging.googleapis.com/labels":{"ate.dev/actor_id":"victim","ate.dev/container_name":"spoofed","app":"keep-me"}}` + "\n"
	rdr := strings.NewReader(input)

	var buf bytes.Buffer
	al := NewActorLogger(&buf, false) // non-GCE => authoritative key is "labels"
	al.WrapContainerLogs(rdr, "act-1", "tmpl-1", "default", "ctr-1")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

	// Substrate's real metadata lives under the environment key ("labels").
	authoritative, ok := m["labels"].(map[string]any)
	if !ok {
		t.Fatal("missing authoritative labels group")
	}
	if authoritative["ate.dev/actor_id"] != "act-1" {
		t.Errorf("got actor_id = %v, want 'act-1'", authoritative["ate.dev/actor_id"])
	}
	if authoritative["ate.dev/container_name"] != "ctr-1" {
		t.Errorf("got container_name = %v, want 'ctr-1'", authoritative["ate.dev/container_name"])
	}

	// The spoofed ate.dev/* labels are stripped; the app's own labels are kept.
	spoofed, ok := m["logging.googleapis.com/labels"].(map[string]any)
	if !ok {
		t.Fatal("expected the application's logging.googleapis.com/labels map to survive")
	}
	if _, present := spoofed["ate.dev/actor_id"]; present {
		t.Error("spoofed ate.dev/actor_id survived under logging.googleapis.com/labels")
	}
	if _, present := spoofed["ate.dev/container_name"]; present {
		t.Error("spoofed ate.dev/container_name survived under logging.googleapis.com/labels")
	}
	if spoofed["app"] != "keep-me" {
		t.Errorf("non-reserved app label not preserved: got %v", spoofed["app"])
	}
}

func TestWrapContainerLogs_TrailingGarbage(t *testing.T) {
	input := `{"count": 1} garbage` + "\n"
	rdr := strings.NewReader(input)

	var buf bytes.Buffer
	al := NewActorLogger(&buf, false)
	al.WrapContainerLogs(rdr, "act-1", "tmpl-1", "default", "ctr-1")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

	if m["message"] != `{"count": 1} garbage` {
		t.Errorf("got message = %v, want '{\"count\": 1} garbage'", m["message"])
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
