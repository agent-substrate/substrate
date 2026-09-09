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
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/resources"
)

const (
	testActorAtespace = "team-a"
	testActorName     = "support-agent-42"
	testActorUID      = "uid-abc"
)

func testAttribution() resources.ActorAttribution {
	return resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: testActorAtespace, Name: testActorName},
		UID:              testActorUID,
		TemplateAtespace: testTemplateNamespace,
		TemplateName:     testTemplateName,
	}
}

// renderRecord logs attrs through a local JSON handler, so the test sees the
// record a collector would parse rather than the slice that built it. A local
// logger keeps this parallel-safe: nothing touches slog.Default.
func renderRecord(t *testing.T, attrs []slog.Attr) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).LogAttrs(context.Background(), slog.LevelInfo, "Restore timing breakdown", attrs...)

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal record %q: %v", buf.String(), err)
	}
	return rec
}

func wantNumber(t *testing.T, rec map[string]any, key string, want float64) {
	t.Helper()
	v, ok := rec[key]
	if !ok {
		t.Errorf("missing %s", key)
		return
	}
	got, ok := v.(float64)
	if !ok {
		t.Errorf("%s is %T (%v), want a number", key, v, v)
		return
	}
	if got != want {
		t.Errorf("%s = %v, want %v", key, got, want)
	}
}

func wantString(t *testing.T, rec map[string]any, key, want string) {
	t.Helper()
	if v, ok := rec[key]; !ok {
		t.Errorf("missing %s", key)
	} else if v != want {
		t.Errorf("%s = %v, want %q", key, v, want)
	}
}

func wantAbsent(t *testing.T, rec map[string]any, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if v, ok := rec[k]; ok {
			t.Errorf("%s is present with %v, want absent", k, v)
		}
	}
}

// TestSnapshotLogAttrsSeconds is the point of the record: the key is named after
// an instrument that declares unit s, so the value has to be seconds. slog.Duration
// would have written 2500000000 here, and a dashboard reading it as seconds would
// be off by nine orders of magnitude.
func TestSnapshotLogAttrsSeconds(t *testing.T) {
	t.Parallel()

	op := snapshotOp{
		templateNamespace: testTemplateNamespace,
		templateName:      testTemplateName,
		kind:              ateattr.SnapshotKindLatest,
		scope:             ateattr.SnapshotScopeFull,
		sandboxClass:      "gvisor",
	}
	attrs := snapshotLogAttrs(testAttribution(), op, restoreDurationMetric, nil,
		[]phase{
			{ateattr.SnapshotPhaseDownload, 2500 * time.Millisecond},
			{ateattr.SnapshotPhaseTotal, 3 * time.Second},
		})

	rec := renderRecord(t, attrs)
	wantNumber(t, rec, "ate.actor.restore.duration.download", 2.5)
	wantNumber(t, rec, "ate.actor.restore.duration.total", 3)
}

func TestSnapshotLogAttrs(t *testing.T) {
	fullOp := snapshotOp{
		templateNamespace: testTemplateNamespace,
		templateName:      testTemplateName,
		kind:              ateattr.SnapshotKindLatest,
		scope:             ateattr.SnapshotScopeFull,
		sandboxClass:      "gvisor",
	}

	tests := []struct {
		name        string
		op          snapshotOp
		err         error
		phases      []phase
		wantStrings map[string]string
		wantNumbers map[string]float64
		wantAbsent  []string
	}{
		{
			name: "a successful restore carries identity, the metric dimensions and every phase that ran",
			op:   fullOp,
			phases: []phase{
				{ateattr.SnapshotPhaseVolumeMount, 4 * time.Millisecond},
				{ateattr.SnapshotPhaseManifestFetch, 21 * time.Millisecond},
				{ateattr.SnapshotPhaseSandboxAssets, 10 * time.Millisecond},
				{ateattr.SnapshotPhaseDownload, 310 * time.Millisecond},
				{ateattr.SnapshotPhaseOCIUnpack, 50 * time.Millisecond},
				{ateattr.SnapshotPhaseAteomRestore, 60 * time.Millisecond},
				{ateattr.SnapshotPhaseTotal, 420 * time.Millisecond},
			},
			wantStrings: map[string]string{
				"ate.atespace":          testActorAtespace,
				"ate.actor.name":        testActorName,
				"ate.actor.uid":         testActorUID,
				"ate.template.atespace": testTemplateNamespace,
				"ate.template.name":     testTemplateName,
				"ate.snapshot.kind":     ateattr.SnapshotKindLatest,
				"ate.snapshot.scope":    ateattr.SnapshotScopeFull,
				"ate.sandbox.class":     "gvisor",
			},
			wantNumbers: map[string]float64{
				"ate.actor.restore.duration.volume_mount":   0.004,
				"ate.actor.restore.duration.manifest_fetch": 0.021,
				"ate.actor.restore.duration.download":       0.310,
				"ate.actor.restore.duration.total":          0.420,
			},
			// The phase key names the one step a datapoint timed; this record has
			// them all, so borrowing it here would give one key two meanings.
			wantAbsent: []string{"ate.failure.reason", "ate.snapshot.phase", "actor", "total", "download"},
		},
		{
			name:   "a phase that never ran is absent, not zero",
			op:     fullOp,
			phases: []phase{{ateattr.SnapshotPhaseDownload, 0}, {ateattr.SnapshotPhaseTotal, time.Second}},
			wantNumbers: map[string]float64{
				"ate.actor.restore.duration.total": 1,
			},
			wantAbsent: []string{"ate.actor.restore.duration.download"},
		},
		{
			name: "a restore that died before the manifest omits the dimensions it never learned",
			op: snapshotOp{
				templateNamespace: testTemplateNamespace,
				templateName:      testTemplateName,
				scope:             ateattr.SnapshotScopeFull,
			},
			err:    fmt.Errorf("%w: while fetching snapshot manifest", ateerrors.ReasonFailedGetExternalObject),
			phases: []phase{{ateattr.SnapshotPhaseTotal, 700 * time.Millisecond}},
			wantStrings: map[string]string{
				"ate.actor.uid":      testActorUID,
				"ate.failure.reason": string(ateerrors.ReasonFailedGetExternalObject),
			},
			wantNumbers: map[string]float64{
				"ate.actor.restore.duration.total": 0.7,
			},
			wantAbsent: []string{"ate.snapshot.kind", "ate.sandbox.class"},
		},
		{
			name:   "an unclassified failure collapses onto UNKNOWN rather than leaking the message",
			op:     fullOp,
			err:    fmt.Errorf("dial tcp 10.96.192.187:9000: connect: connection refused"),
			phases: []phase{{ateattr.SnapshotPhaseTotal, time.Second}},
			wantStrings: map[string]string{
				"ate.failure.reason": ateattr.ReasonUnknown,
			},
		},
		{
			// A restore that panicked leaves the named err nil, so the defer
			// substitutes this. Without a reason on the record it would read as a
			// fast success right before atelet dies.
			name:   "a restore that unwound reports as a failure, not a fast success",
			op:     fullOp,
			err:    errRestoreUnwound,
			phases: []phase{{ateattr.SnapshotPhaseVolumeMount, 4 * time.Millisecond}, {ateattr.SnapshotPhaseTotal, 9 * time.Millisecond}},
			wantStrings: map[string]string{
				"ate.failure.reason": ateattr.ReasonUnknown,
			},
			wantNumbers: map[string]float64{
				"ate.actor.restore.duration.total": 0.009,
			},
		},
		{
			name: "a sandbox class the manifest invented is bounded, not passed through",
			op: snapshotOp{
				templateNamespace: testTemplateNamespace,
				templateName:      testTemplateName,
				scope:             ateattr.SnapshotScopeFull,
				sandboxClass:      "../../etc/passwd",
			},
			phases: []phase{{ateattr.SnapshotPhaseTotal, time.Second}},
			wantStrings: map[string]string{
				"ate.sandbox.class": ateattr.SandboxClassUnknown,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			attrs := snapshotLogAttrs(testAttribution(), tt.op, restoreDurationMetric, tt.err, tt.phases)

			// json.Unmarshal keeps the last of a repeated key, so a duplicate is
			// invisible in the rendered record and has to be caught on the slice.
			seen := make(map[string]bool, len(attrs))
			for _, attr := range attrs {
				if seen[attr.Key] {
					t.Errorf("key %s emitted twice", attr.Key)
				}
				seen[attr.Key] = true
			}

			rec := renderRecord(t, attrs)
			for k, want := range tt.wantStrings {
				wantString(t, rec, k, want)
			}
			for k, want := range tt.wantNumbers {
				wantNumber(t, rec, k, want)
			}
			wantAbsent(t, rec, tt.wantAbsent...)
		})
	}
}
