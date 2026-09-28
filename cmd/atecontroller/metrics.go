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

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// dropEmptyExponentialHistograms wraps a Producer and removes exponential
// histogram data points with no positive buckets.
//
// controller-runtime's workqueue metrics use native (exponential) Prometheus
// histograms, and the bridge produces one the moment a queue is created, before
// any item is ever processed. Google Cloud Monitoring rejects a data point in
// that state with "num_finite_buckets" less than 1 and drops it, spamming the
// collector log every push tick. The observation carries no information (an
// idle queue), so dropping it here is no loss.
func dropEmptyExponentialHistograms(inner sdkmetric.Producer) sdkmetric.Producer {
	return &filteringProducer{inner: inner}
}

type filteringProducer struct {
	inner sdkmetric.Producer
}

func (p *filteringProducer) Produce(ctx context.Context) ([]metricdata.ScopeMetrics, error) {
	sm, err := p.inner.Produce(ctx)
	for i := range sm {
		sm[i].Metrics = filterEmptyExponentialHistograms(sm[i].Metrics)
	}
	return sm, err
}

func filterEmptyExponentialHistograms(metrics []metricdata.Metrics) []metricdata.Metrics {
	out := metrics[:0]
	for _, m := range metrics {
		hist, ok := m.Data.(metricdata.ExponentialHistogram[float64])
		if !ok {
			out = append(out, m)
			continue
		}
		hist.DataPoints = filterEmptyDataPoints(hist.DataPoints)
		if len(hist.DataPoints) == 0 {
			continue
		}
		m.Data = hist
		out = append(out, m)
	}
	return out
}

func filterEmptyDataPoints(points []metricdata.ExponentialHistogramDataPoint[float64]) []metricdata.ExponentialHistogramDataPoint[float64] {
	out := points[:0]
	for _, dp := range points {
		// No positive bucket means no finite bucket once this reaches Cloud
		// Monitoring, whatever the zero count or negative buckets hold.
		if len(dp.PositiveBucket.Counts) == 0 {
			continue
		}
		out = append(out, dp)
	}
	return out
}
