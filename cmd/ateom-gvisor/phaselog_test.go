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

	"github.com/agent-substrate/substrate/internal/ateomphaselog"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
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
// the layout benchmarking tooling joins with atelet's record on, and that a
// phase which never ran stays off it.
func TestRestoreTimingPhases(t *testing.T) {
	t.Parallel()

	timing := restoreTiming{
		prep:          10 * time.Millisecond,
		egressPrepare: 120 * time.Millisecond,
		netSetup:      30 * time.Millisecond,
		// No durable volumes: the phase never ran.
		durableDir:   0,
		pauseRootfs:  5 * time.Millisecond,
		pauseCreate:  200 * time.Millisecond,
		pauseRestore: 900 * time.Millisecond,
		appRootfs:    15 * time.Millisecond,
		appCreate:    50 * time.Millisecond,
		appRestore:   400 * time.Millisecond,
		wakeupProbe:  250 * time.Millisecond,
		activate:     2 * time.Millisecond,
		total:        2 * time.Second,
	}
	attrs := ateomphaselog.SnapshotPhaseAttrs(resources.ActorAttribution{UID: "uid-abc"},
		ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL, ateomphaselog.RestoreDurationKey, nil, timing.phases())
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
		"ateom.actor.restore.duration.prep":           0.01,
		"ateom.actor.restore.duration.egress_prepare": 0.12,
		"ateom.actor.restore.duration.net_setup":      0.03,
		"ateom.actor.restore.duration.pause_rootfs":   0.005,
		"ateom.actor.restore.duration.pause_create":   0.2,
		"ateom.actor.restore.duration.pause_restore":  0.9,
		"ateom.actor.restore.duration.app_rootfs":     0.015,
		"ateom.actor.restore.duration.app_create":     0.05,
		"ateom.actor.restore.duration.app_restore":    0.4,
		"ateom.actor.restore.duration.wakeup_probe":   0.25,
		"ateom.actor.restore.duration.activate":       0.002,
		"ateom.actor.restore.duration.total":          2,
		"ate.actor.container.count":                   2,
	} {
		got, ok := rec[k].(float64)
		if !ok {
			t.Errorf("%s = %v (%T), want a number", k, rec[k], rec[k])
		} else if got != want {
			t.Errorf("%s = %v, want %v", k, got, want)
		}
	}
	if v, ok := rec["ateom.actor.restore.duration.durable_dir"]; ok {
		t.Errorf("durable_dir present with %v, want absent", v)
	}
	if rec["ate.actor.uid"] != "uid-abc" {
		t.Errorf("ate.actor.uid = %v, want uid-abc", rec["ate.actor.uid"])
	}
	if rec["ate.snapshot.scope"] != "full" {
		t.Errorf("ate.snapshot.scope = %v, want full", rec["ate.snapshot.scope"])
	}
}

// TestCheckpointTimingPhases: a Data checkpoint pauses and resumes around the
// durable capture and never runs the runsc checkpoint, and the record says so.
func TestCheckpointTimingPhases(t *testing.T) {
	t.Parallel()

	timing := checkpointTiming{
		prep:       20 * time.Millisecond,
		pause:      3 * time.Millisecond,
		durableDir: 300 * time.Millisecond,
		resume:     4 * time.Millisecond,
		teardown:   150 * time.Millisecond,
		total:      500 * time.Millisecond,
	}
	rec := renderPhaseRecord(t, ateomphaselog.SnapshotPhaseAttrs(resources.ActorAttribution{UID: "uid-abc"},
		ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA, ateomphaselog.CheckpointDurationKey, nil, timing.phases()))

	for k, want := range map[string]float64{
		"ateom.actor.checkpoint.duration.prep":        0.02,
		"ateom.actor.checkpoint.duration.pause":       0.003,
		"ateom.actor.checkpoint.duration.durable_dir": 0.3,
		"ateom.actor.checkpoint.duration.resume":      0.004,
		"ateom.actor.checkpoint.duration.teardown":    0.15,
		"ateom.actor.checkpoint.duration.total":       0.5,
	} {
		got, ok := rec[k].(float64)
		if !ok {
			t.Errorf("%s = %v (%T), want a number", k, rec[k], rec[k])
		} else if got != want {
			t.Errorf("%s = %v, want %v", k, got, want)
		}
	}
	if v, ok := rec["ateom.actor.checkpoint.duration.checkpoint"]; ok {
		t.Errorf("checkpoint present with %v, want absent", v)
	}
	if rec["ate.snapshot.scope"] != "data" {
		t.Errorf("ate.snapshot.scope = %v, want data", rec["ate.snapshot.scope"])
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
