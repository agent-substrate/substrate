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
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

func TestCPUCounterAggregatesActorsWithoutIdentityLabels(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())
	counter, err := NewCPUCounter(mp.Meter("ateom"), testPool)
	if err != nil {
		t.Fatal(err)
	}
	first := measuredSample()
	first.CpuUsageUsec = 200_000
	second := measuredSample()
	second.ActorUid = "uid-2"
	second.CpuUsageUsec = 300_000
	counter.Add(context.Background(), first, first.CpuUsageUsec)
	counter.Add(context.Background(), second, second.CpuUsageUsec)
	counter.Add(context.Background(), second, 0)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if len(rm.ScopeMetrics) != 1 || len(rm.ScopeMetrics[0].Metrics) != 1 {
		t.Fatalf("metrics = %v, want one counter", rm.ScopeMetrics)
	}
	m := rm.ScopeMetrics[0].Metrics[0]
	if m.Name != CPUTimeMetric || m.Unit != "s" {
		t.Errorf("metric = %q unit %q, want %q in seconds", m.Name, m.Unit, CPUTimeMetric)
	}
	sum, ok := m.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("data = %T, want Sum[float64]", m.Data)
	}
	if !sum.IsMonotonic || len(sum.DataPoints) != 1 {
		t.Fatalf("sum = %+v, want one monotonic series", sum)
	}
	dp := sum.DataPoints[0]
	if dp.Value != 0.5 {
		t.Errorf("CPU = %g seconds, want 0.5", dp.Value)
	}
	for _, key := range []struct {
		key  string
		want string
	}{
		{string(ateattr.TemplateAtespaceKey), "ns"},
		{string(ateattr.TemplateNameKey), "t"},
		{string(ateattr.SandboxClassKey), "gvisor"},
		{string(ateattr.StatsSourceKey), ateattr.StatsSourceCgroup},
		{string(ateattr.WorkerPoolNamespaceKey), testPool.Namespace},
		{string(ateattr.WorkerPoolNameKey), testPool.Name},
	} {
		v, ok := dp.Attributes.Value(attribute.Key(key.key))
		if !ok || v.AsString() != key.want {
			t.Errorf("attribute %q = %v (present %v), want %q", key.key, v, ok, key.want)
		}
	}
	if dp.Attributes.Len() != 6 {
		t.Errorf("attributes = %v, want six bounded labels and no actor identity", dp.Attributes)
	}
}
