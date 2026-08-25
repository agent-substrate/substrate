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

package scheduling

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/metric"
)

// Metric identifier for tracking eligible worker counts.
const eligibleWorkersMetric = "ate.scheduler.eligible_workers"

// newEligibleWorkers creates the ate.scheduler.eligible_workers histogram instrument against meter.
// Returns (nil, nil) if meter is nil. If registration fails, an error is returned.
func newEligibleWorkers(meter metric.Meter) (metric.Int64Histogram, error) {
	if meter == nil {
		return nil, nil
	}
	hist, err := meter.Int64Histogram(
		eligibleWorkersMetric,
		metric.WithUnit("{worker}"),
		metric.WithDescription("Number of eligible workers available during scheduling given the constraint filters."),
		metric.WithExplicitBucketBoundaries(0, 1, 2, 3, 5, 10, 20, 50, 100, 250),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", eligibleWorkersMetric, err)
	}
	return hist, nil
}

// WithMeter configures the meter used to create telemetry instruments for the scheduler.
// If meter is nil, the option is a no-op. If instrument creation fails, an error is explicitly logged.
func WithMeter(meter metric.Meter) Option {
	return func(s *scheduler) {
		hist, err := newEligibleWorkers(meter)
		if err != nil {
			slog.Error("Failed to register ate.scheduler.eligible_workers histogram", "metric", eligibleWorkersMetric, "error", err)
			return
		}
		s.eligibleWorkers = hist
	}
}

// Records candidate worker counts grouped by WorkerPool namespace and WorkerPool name,
// stamping sandbox class and scheduling constraint attributes on all histogram datapoints.
func (s *scheduler) recordEligibleWorkers(ctx context.Context, matching []*ateapipb.Worker, constraints Constraints) {
	if s.eligibleWorkers == nil {
		return
	}

	// Sandbox class and constraint are constant across every key: Applies requires
	// an exact class match, and the classification is per call. They belong on the
	// Record call, not in the key.
	type key struct{ namespace, pool string }
	eligibleByPool := make(map[key]int64)
	for _, w := range matching {
		k := key{namespace: w.GetWorkerNamespace(), pool: w.GetWorkerPool()}
		if _, ok := eligibleByPool[k]; !ok {
			eligibleByPool[k] = 0
		}
		if w.GetStatus().GetAssignment() == nil {
			eligibleByPool[k]++
		}
	}

	// No pool matched the constraints at all. Emit a single zero-valued series so
	// "nothing is schedulable" stays visible; empty namespace/pool marks it. The
	// label set matches the per-pool series, so dashboards need no special case.
	if len(eligibleByPool) == 0 {
		eligibleByPool[key{}] = 0
	}

	constraintStr := classifyConstraint(constraints)
	for k, count := range eligibleByPool {
		s.eligibleWorkers.Record(ctx, count, metric.WithAttributes(
			ateattr.WorkerPoolNamespaceKey.String(k.namespace),
			ateattr.WorkerPoolNameKey.String(k.pool),
			ateattr.SandboxClassKey.String(constraints.SandboxClass),
			ateattr.SchedulingConstraintKey.String(constraintStr),
		))
	}
}

func classifyConstraint(c Constraints) string {
	if len(c.RequiredNodes) > 0 {
		return ateattr.ConstraintRequiredNodes
	}
	if (c.TemplateSelector != nil && !c.TemplateSelector.Empty()) || (c.ActorSelector != nil && !c.ActorSelector.Empty()) {
		return ateattr.ConstraintSelector
	}
	return ateattr.ConstraintNone
}
