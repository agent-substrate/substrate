//go:build linux

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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
)

func phaseLogAttribution() resources.ActorAttribution {
	return resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: "team-a", Name: "support-agent-42"},
		UID:              "uid-abc",
		TemplateAtespace: "templates",
		TemplateName:     "support-agent",
	}
}

// renderPhaseRecord logs attrs through a local JSON handler, so the test sees
// the record a collector would parse rather than the slice that built it.
func renderPhaseRecord(t *testing.T, attrs []slog.Attr) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).LogAttrs(context.Background(), slog.LevelInfo, "Checkpoint timing breakdown", attrs...)
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal record %q: %v", buf.String(), err)
	}
	return rec
}

func TestSnapshotPhaseAttrs(t *testing.T) {
	t.Parallel()

	attrs := snapshotPhaseAttrs(phaseLogAttribution(), ateworkerpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		checkpointDurationKey, nil, []phase{
			{phasePrep, 40 * time.Millisecond},
			{phasePause, 3 * time.Millisecond},
			{phaseSnapshot, 850 * time.Millisecond},
			// A capture that did not run stays off the record.
			{phaseDurableDir, 0},
			{phaseTeardown, 230 * time.Millisecond},
			{phaseTotal, 1250 * time.Millisecond},
		})

	// json.Unmarshal keeps the last of a repeated key, so a duplicate is
	// invisible in the rendered record and has to be caught on the slice.
	seen := make(map[string]bool, len(attrs))
	for _, attr := range attrs {
		if seen[attr.Key] {
			t.Errorf("key %s emitted twice", attr.Key)
		}
		seen[attr.Key] = true
	}

	rec := renderPhaseRecord(t, attrs)
	for k, want := range map[string]string{
		"ate.atespace":          "team-a",
		"ate.actor.name":        "support-agent-42",
		"ate.actor.uid":         "uid-abc",
		"ate.template.atespace": "templates",
		"ate.template.name":     "support-agent",
		"ate.snapshot.scope":    ateattr.SnapshotScopeFull,
	} {
		if got, ok := rec[k]; !ok {
			t.Errorf("missing %s", k)
		} else if got != want {
			t.Errorf("%s = %v, want %q", k, got, want)
		}
	}
	// Seconds, not slog.Duration's nanoseconds: the keys extend the atelet
	// histograms' names, and those declare unit s.
	for k, want := range map[string]float64{
		"ateworker.actor.checkpoint.duration.prep":     0.04,
		"ateworker.actor.checkpoint.duration.pause":    0.003,
		"ateworker.actor.checkpoint.duration.snapshot": 0.85,
		"ateworker.actor.checkpoint.duration.teardown": 0.23,
		"ateworker.actor.checkpoint.duration.total":    1.25,
	} {
		got, ok := rec[k].(float64)
		if !ok {
			t.Errorf("%s = %v (%T), want a number", k, rec[k], rec[k])
		} else if got != want {
			t.Errorf("%s = %v, want %v", k, got, want)
		}
	}
	for _, k := range []string{
		"ateworker.actor.checkpoint.duration.durable_dir",
		// The phase key names the one step a datapoint timed; this record
		// carries them all.
		"ate.snapshot.phase",
		// Absent error.type is success, as on the instruments.
		"error.type",
	} {
		if v, ok := rec[k]; ok {
			t.Errorf("%s is present with %v, want absent", k, v)
		}
	}
}

// TestSnapshotPhaseAttrsFailure: a checkpoint that died keeps the phases it
// completed and is marked, so a reader can exclude it from percentiles.
func TestSnapshotPhaseAttrsFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{"a wrapped context deadline reports DeadlineExceeded",
			fmt.Errorf("while snapshotting guest: %w", context.DeadlineExceeded), "DeadlineExceeded"},
		{"a wrapped context cancellation reports Canceled",
			fmt.Errorf("while pausing guest: %w", context.Canceled), "Canceled"},
		{"a plain error is a bounded Unknown, not its message",
			fmt.Errorf("while clearing checkpoint dir: disk on fire"), "Unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := renderPhaseRecord(t, snapshotPhaseAttrs(phaseLogAttribution(),
				ateworkerpb.SnapshotScope_SNAPSHOT_SCOPE_FULL, checkpointDurationKey, tt.err,
				[]phase{{phasePrep, 40 * time.Millisecond}, {phasePause, 3 * time.Millisecond}, {phaseTotal, 30 * time.Second}}))
			if got := rec["error.type"]; got != tt.want {
				t.Errorf("error.type = %v, want %q", got, tt.want)
			}
			// The phases that ran before the failure are still on the record;
			// the ones that never started are not.
			if _, ok := rec["ateworker.actor.checkpoint.duration.pause"]; !ok {
				t.Error("missing ateworker.actor.checkpoint.duration.pause")
			}
			if v, ok := rec["ateworker.actor.checkpoint.duration.snapshot"]; ok {
				t.Errorf("snapshot phase present with %v, want absent", v)
			}
		})
	}
}

// TestScopeLogValue pins the mapping onto the shared scope values, so the
// ateworker and atelet records of one operation agree on the scope they carry.
func TestScopeLogValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scope ateworkerpb.SnapshotScope
		want  string
	}{
		{ateworkerpb.SnapshotScope_SNAPSHOT_SCOPE_FULL, ateattr.SnapshotScopeFull},
		{ateworkerpb.SnapshotScope_SNAPSHOT_SCOPE_DATA, ateattr.SnapshotScopeData},
		{ateworkerpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED, ateattr.SnapshotScopeUnknown},
		{ateworkerpb.SnapshotScope(99), ateattr.SnapshotScopeUnknown},
	}
	for _, tt := range tests {
		if got := scopeLogValue(tt.scope); got != tt.want {
			t.Errorf("scopeLogValue(%v) = %q, want %q", tt.scope, got, tt.want)
		}
	}
}
