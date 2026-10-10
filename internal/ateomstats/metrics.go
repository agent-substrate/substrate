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

package ateomstats

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// CPUTimeMetric is the cumulative actor CPU time exported by each ateom.
const CPUTimeMetric = "ate.actor.stats.cpu.time"

// CPUCounter adds accepted activation CPU increases to the ateom's counter.
// A nil receiver records nothing.
type CPUCounter struct {
	counter metric.Float64Counter
	pool    Pool
}

// NewCPUCounter creates the actor CPU counter on an ateom's meter provider.
func NewCPUCounter(meter metric.Meter, pool Pool) (*CPUCounter, error) {
	counter, err := meter.Float64Counter(CPUTimeMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Cumulative CPU time consumed by running actors, in seconds."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s counter: %w", CPUTimeMetric, err)
	}
	return &CPUCounter{counter: counter, pool: pool}, nil
}

// Add records a CPU increase in microseconds. A pending sample has no CPU
// measurement. Actor identity stays off the metric label set.
func (c *CPUCounter) Add(ctx context.Context, s *ateompb.WorkloadStatsSample, deltaUsec uint64) {
	if c == nil || deltaUsec == 0 || s.GetSource() == ateompb.StatsSource_STATS_SOURCE_UNSPECIFIED {
		return
	}
	attrs := []attribute.KeyValue{
		ateattr.TemplateAtespaceKey.String(s.GetActorTemplateAtespace()),
		ateattr.TemplateNameKey.String(s.GetActorTemplateName()),
		ateattr.SandboxClassKey.String(SandboxClassLabel(s.GetSandboxClass())),
		ateattr.StatsSourceKey.String(StatsSourceLabel(s.GetSource())),
	}
	attrs = append(attrs, ateattr.WorkerPoolAttributes(c.pool.Namespace, c.pool.Name)...)
	// A final reading must still count if its lifecycle RPC context was canceled.
	c.counter.Add(context.WithoutCancel(ctx), float64(deltaUsec)/1e6, metric.WithAttributes(attrs...))
}
