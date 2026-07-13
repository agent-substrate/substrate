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
	"context"
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const (
	restoreDurationMetric    = "ate.actor.restore.duration"
	checkpointDurationMetric = "ate.actor.checkpoint.duration"
)

// coldStartBuckets (seconds) spans the cold-start range shared by both the
// restore and checkpoint phase histograms.
var coldStartBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60}

var (
	restoreDuration    metric.Float64Histogram
	checkpointDuration metric.Float64Histogram
)

func buildDurationHistogram(meter metric.Meter, name, description string) (metric.Float64Histogram, error) {
	h, err := meter.Float64Histogram(
		name,
		metric.WithUnit("s"),
		metric.WithDescription(description),
		metric.WithExplicitBucketBoundaries(coldStartBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", name, err)
	}
	return h, nil
}

func initRestoreDurationMetric() error {
	var err error
	restoreDuration, err = buildDurationHistogram(otel.Meter("atelet"), restoreDurationMetric,
		"Per-phase duration of restoring an actor from a snapshot on atelet.")
	return err
}

func initCheckpointDurationMetric() error {
	var err error
	checkpointDuration, err = buildDurationHistogram(otel.Meter("atelet"), checkpointDurationMetric,
		"Per-phase duration of checkpointing an actor to a snapshot on atelet.")
	return err
}

// recordRestorePhase records one restore phase. snapshotKind (golden|latest)
// comes from ateapi via RestoreRequest and is always stamped so the metric shape
// stays fixed; phases are recorded independently, not as a partition of a total
// (download and oci_unpack overlap in a concurrent errgroup).
func recordRestorePhase(ctx context.Context, phase string, d time.Duration, tmplNamespace, tmplName, sandboxClass, snapshotKind string) {
	if restoreDuration == nil {
		return
	}
	restoreDuration.Record(ctx, d.Seconds(), metric.WithAttributes(
		ateattr.SnapshotPhaseKey.String(phase),
		ateattr.ActorTemplateNamespaceKey.String(tmplNamespace),
		ateattr.ActorTemplateNameKey.String(tmplName),
		ateattr.SandboxClassKey.String(sandboxClass),
		ateattr.SnapshotKindKey.String(snapshotKind),
	))
}

// recordCheckpointPhase records one checkpoint phase. There is no ate.snapshot.kind
// here: a checkpoint always writes the latest snapshot, and the pause-vs-suspend
// distinction already lives on the ateapi lifecycle metric's operation attribute.
func recordCheckpointPhase(ctx context.Context, phase string, d time.Duration, tmplNamespace, tmplName, sandboxClass string) {
	if checkpointDuration == nil {
		return
	}
	checkpointDuration.Record(ctx, d.Seconds(), metric.WithAttributes(
		ateattr.SnapshotPhaseKey.String(phase),
		ateattr.ActorTemplateNamespaceKey.String(tmplNamespace),
		ateattr.ActorTemplateNameKey.String(tmplName),
		ateattr.SandboxClassKey.String(sandboxClass),
	))
}
