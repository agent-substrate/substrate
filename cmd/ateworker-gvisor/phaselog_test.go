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
	"log/slog"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateworker/phaselog"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// renderPhaseRecord logs attrs through a JSON handler, so the test checks the
// record a collector would parse rather than the slice that built it.
func renderPhaseRecord(t *testing.T, attrs []slog.Attr) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).LogAttrs(context.Background(), slog.LevelInfo, "Restore timing breakdown", attrs...)
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal record %q: %v", buf.String(), err)
	}
	return rec
}

// TestRestoreTimingPhases pins the keys a Full restore record carries, in
// the layout benchmarking tooling joins with atelet's record on.
func TestRestoreTimingPhases(t *testing.T) {
	t.Parallel()

	timing := restoreTiming{
		prep:          10 * time.Millisecond,
		egressPrepare: 120 * time.Millisecond,
		netSetup:      30 * time.Millisecond,
		pauseRootfs:   5 * time.Millisecond,
		pauseCreate:   200 * time.Millisecond,
		pauseRestore:  900 * time.Millisecond,
		appRootfs:     15 * time.Millisecond,
		appCreate:     50 * time.Millisecond,
		appRestore:    400 * time.Millisecond,
		wakeupProbe:   250 * time.Millisecond,
		activate:      2 * time.Millisecond,
		total:         2 * time.Second,
	}
	attrs := phaselog.SnapshotPhaseAttrs(resources.ActorAttribution{UID: "uid-abc"},
		ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, phaselog.RestoreDurationKey, nil, timing.phases())
	attrs = append(attrs, slog.Int(containerCountKey, 2))

	seen := make(map[string]bool, len(attrs))
	for _, attr := range attrs {
		if seen[attr.Key] {
			t.Errorf("key %s emitted twice", attr.Key)
		}
		seen[attr.Key] = true
	}

	rec := renderPhaseRecord(t, attrs)
	for k, want := range map[string]float64{
		"ateworker.actor.restore.duration.prep":           0.01,
		"ateworker.actor.restore.duration.egress_prepare": 0.12,
		"ateworker.actor.restore.duration.net_setup":      0.03,
		"ateworker.actor.restore.duration.pause_rootfs":   0.005,
		"ateworker.actor.restore.duration.pause_create":   0.2,
		"ateworker.actor.restore.duration.pause_restore":  0.9,
		"ateworker.actor.restore.duration.app_rootfs":     0.015,
		"ateworker.actor.restore.duration.app_create":     0.05,
		"ateworker.actor.restore.duration.app_restore":    0.4,
		"ateworker.actor.restore.duration.wakeup_probe":   0.25,
		"ateworker.actor.restore.duration.activate":       0.002,
		"ateworker.actor.restore.duration.total":          2,
		"ate.actor.container.count":                       2,
	} {
		got, ok := rec[k].(float64)
		if !ok {
			t.Errorf("%s = %v (%T), want a number", k, rec[k], rec[k])
		} else if got != want {
			t.Errorf("%s = %v, want %v", k, got, want)
		}
	}
	if rec["ate.actor.uid"] != "uid-abc" {
		t.Errorf("ate.actor.uid = %v, want uid-abc", rec["ate.actor.uid"])
	}
	if rec["ate.snapshot.fidelity"] != "memory" {
		t.Errorf("ate.snapshot.fidelity = %v, want memory", rec["ate.snapshot.fidelity"])
	}
}

// TestCheckpointTimingPhases: a Data checkpoint runs the durable capture and
// never runs the full runsc checkpoint, and the record says so.
func TestCheckpointTimingPhases(t *testing.T) {
	t.Parallel()

	timing := checkpointTiming{
		prep:       20 * time.Millisecond,
		durableDir: 300 * time.Millisecond,
		teardown:   150 * time.Millisecond,
		total:      500 * time.Millisecond,
	}
	rec := renderPhaseRecord(t, phaselog.SnapshotPhaseAttrs(resources.ActorAttribution{UID: "uid-abc"},
		ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, phaselog.CheckpointDurationKey, nil, timing.phases()))

	for k, want := range map[string]float64{
		"ateworker.actor.checkpoint.duration.prep":        0.02,
		"ateworker.actor.checkpoint.duration.durable_dir": 0.3,
		"ateworker.actor.checkpoint.duration.teardown":    0.15,
		"ateworker.actor.checkpoint.duration.total":       0.5,
	} {
		got, ok := rec[k].(float64)
		if !ok {
			t.Errorf("%s = %v (%T), want a number", k, rec[k], rec[k])
		} else if got != want {
			t.Errorf("%s = %v, want %v", k, got, want)
		}
	}
	if v, ok := rec["ateworker.actor.checkpoint.duration.checkpoint"]; ok {
		t.Errorf("checkpoint present with %v, want absent", v)
	}
	if rec["ate.snapshot.fidelity"] != "volumes" {
		t.Errorf("ate.snapshot.fidelity = %v, want volumes", rec["ate.snapshot.fidelity"])
	}
}

// TestLap: consecutive laps measure consecutive intervals from one cursor.
func TestLap(t *testing.T) {
	t.Parallel()

	last := time.Now().Add(-time.Second)
	before := last
	d := lap(&last)
	if d < time.Second {
		t.Errorf("lap = %v, want >= 1s", d)
	}
	if !last.After(before) {
		t.Error("lap did not advance the cursor")
	}
	if d2 := lap(&last); d2 >= time.Second {
		t.Errorf("second lap = %v, want far below 1s", d2)
	}
}
