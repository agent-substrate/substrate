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

package ateomphaselog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
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

	attrs := SnapshotPhaseAttrs(phaseLogAttribution(), ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		CheckpointDurationKey, nil, []Phase{
			{Name: "prep", D: 40 * time.Millisecond},
			{Name: "pause", D: 3 * time.Millisecond},
			{Name: "snapshot", D: 850 * time.Millisecond},
			// A capture that did not run stays off the record.
			{Name: "durable_dir", D: 0},
			{Name: "teardown", D: 230 * time.Millisecond},
			{Name: ateattr.SnapshotPhaseTotal, D: 1250 * time.Millisecond},
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
		"ate.snapshot.fidelity": ateattr.SnapshotFidelityMemory,
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
		"ateom.actor.checkpoint.duration.prep":     0.04,
		"ateom.actor.checkpoint.duration.pause":    0.003,
		"ateom.actor.checkpoint.duration.snapshot": 0.85,
		"ateom.actor.checkpoint.duration.teardown": 0.23,
		"ateom.actor.checkpoint.duration.total":    1.25,
	} {
		got, ok := rec[k].(float64)
		if !ok {
			t.Errorf("%s = %v (%T), want a number", k, rec[k], rec[k])
		} else if got != want {
			t.Errorf("%s = %v, want %v", k, got, want)
		}
	}
	for _, k := range []string{
		"ateom.actor.checkpoint.duration.durable_dir",
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
			rec := renderPhaseRecord(t, SnapshotPhaseAttrs(phaseLogAttribution(),
				ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, CheckpointDurationKey, tt.err,
				[]Phase{
					{Name: "prep", D: 40 * time.Millisecond},
					{Name: "pause", D: 3 * time.Millisecond},
					{Name: ateattr.SnapshotPhaseTotal, D: 30 * time.Second},
				}))
			if got := rec["error.type"]; got != tt.want {
				t.Errorf("error.type = %v, want %q", got, tt.want)
			}
			// The phases that ran before the failure are still on the record;
			// the ones that never started are not.
			if _, ok := rec["ateom.actor.checkpoint.duration.pause"]; !ok {
				t.Error("missing ateom.actor.checkpoint.duration.pause")
			}
			if v, ok := rec["ateom.actor.checkpoint.duration.snapshot"]; ok {
				t.Errorf("snapshot phase present with %v, want absent", v)
			}
		})
	}
}

// TestFidelityLogValue pins the mapping onto the shared fidelity values, so the
// ateom and atelet records of one operation agree on the fidelity they carry.
func TestFidelityLogValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scope ateompb.SnapshotFidelity
		want  string
	}{
		{ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, ateattr.SnapshotFidelityMemory},
		{ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, ateattr.SnapshotFidelityVolumes},
		{ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, ateattr.SnapshotFidelityRootfs},
		{ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED, ateattr.SnapshotFidelityUnknown},
		{ateompb.SnapshotFidelity(99), ateattr.SnapshotFidelityUnknown},
	}
	for _, tt := range tests {
		if got := FidelityLogValue(tt.scope); got != tt.want {
			t.Errorf("FidelityLogValue(%v) = %q, want %q", tt.scope, got, tt.want)
		}
	}
}

// TestLogSnapshotPhases: the record goes out under the caller's message and
// at Info, through the default logger the ateoms write to.
func TestLogSnapshotPhases(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	LogSnapshotPhases(context.Background(), "Restore timing breakdown", phaseLogAttribution(),
		ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, RestoreDurationKey, nil,
		[]Phase{{Name: ateattr.SnapshotPhaseTotal, D: 2 * time.Second}})

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal record %q: %v", buf.String(), err)
	}
	if rec["msg"] != "Restore timing breakdown" {
		t.Errorf("msg = %v, want %q", rec["msg"], "Restore timing breakdown")
	}
	if rec["level"] != "INFO" {
		t.Errorf("level = %v, want INFO", rec["level"])
	}
	if rec["ate.snapshot.fidelity"] != ateattr.SnapshotFidelityVolumes {
		t.Errorf("ate.snapshot.fidelity = %v, want %q", rec["ate.snapshot.fidelity"], ateattr.SnapshotFidelityVolumes)
	}
	if got := rec["ateom.actor.restore.duration.total"]; got != 2.0 {
		t.Errorf("ateom.actor.restore.duration.total = %v, want 2", got)
	}
}
