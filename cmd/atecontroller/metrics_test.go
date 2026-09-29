// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"slices"
	"testing"

	prombridge "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"k8s.io/client-go/util/workqueue"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

type stubProducer struct {
	scopeMetrics []metricdata.ScopeMetrics
}

func (s stubProducer) Produce(context.Context) ([]metricdata.ScopeMetrics, error) {
	return s.scopeMetrics, nil
}

func TestPadEmptyExponentialHistograms(t *testing.T) {
	t.Parallel()

	stub := stubProducer{scopeMetrics: []metricdata.ScopeMetrics{{
		Metrics: []metricdata.Metrics{
			{
				Name: "workqueue_work_duration_seconds",
				Data: metricdata.ExponentialHistogram[float64]{
					DataPoints: []metricdata.ExponentialHistogramDataPoint[float64]{
						{}, // idle queue: no positive buckets.
						{PositiveBucket: metricdata.ExponentialBucket{Offset: 2, Counts: []uint64{3, 1}}},
					},
				},
			},
			{
				Name: "go_goroutines",
				Data: metricdata.Gauge[float64]{DataPoints: []metricdata.DataPoint[float64]{{Value: 5}}},
			},
		},
	}}}

	sm, err := padEmptyExponentialHistograms(stub).Produce(context.Background())
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if len(sm) != 1 || len(sm[0].Metrics) != 2 {
		t.Fatalf("got %#v", sm)
	}

	hist, ok := sm[0].Metrics[0].Data.(metricdata.ExponentialHistogram[float64])
	if !ok {
		t.Fatalf("metric 0: want ExponentialHistogram, got %T", sm[0].Metrics[0].Data)
	}
	if len(hist.DataPoints) != 2 {
		t.Fatalf("want both data points kept, got %d", len(hist.DataPoints))
	}
	if got := hist.DataPoints[0].PositiveBucket.Counts; !slices.Equal(got, []uint64{0}) {
		t.Errorf("empty point: want one zero-count bucket, got %v", got)
	}
	if got := hist.DataPoints[1].PositiveBucket; got.Offset != 2 || !slices.Equal(got.Counts, []uint64{3, 1}) {
		t.Errorf("non-empty point: want it unchanged, got %+v", got)
	}

	if _, ok := sm[0].Metrics[1].Data.(metricdata.Gauge[float64]); !ok {
		t.Errorf("metric 1: a gauge should pass through unchanged, got %T", sm[0].Metrics[1].Data)
	}
}

// TestPadEmptyExponentialHistogramsIdleWorkqueue reproduces the reported bug
// end to end: a workqueue that has never processed an item bridges as an
// exponential histogram with no positive buckets, the shape the Telemetry API
// rejects.
func TestPadEmptyExponentialHistogramsIdleWorkqueue(t *testing.T) {
	t.Parallel()

	q := workqueue.NewNamed("atecontroller-metrics-pad-probe")
	defer q.ShutDown()

	base := prombridge.NewMetricProducer(prombridge.WithGatherer(ctrlmetrics.Registry))
	produced, err := padEmptyExponentialHistograms(base).Produce(context.Background())
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	seen := 0
	for _, sm := range produced {
		for _, m := range sm.Metrics {
			hist, ok := m.Data.(metricdata.ExponentialHistogram[float64])
			if !ok {
				continue
			}
			for _, dp := range hist.DataPoints {
				seen++
				if len(dp.PositiveBucket.Counts) == 0 {
					t.Errorf("%s: still has an empty exponential histogram data point after padding", m.Name)
				}
			}
		}
	}
	if seen == 0 {
		t.Error("no exponential histogram data points produced, so the test checked nothing")
	}
}
