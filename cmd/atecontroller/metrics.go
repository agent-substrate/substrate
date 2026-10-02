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

// padEmptyExponentialHistograms wraps a Producer and gives each exponential
// histogram data point with no positive buckets one positive bucket with a
// count of 0.
//
// controller-runtime's workqueue metrics use native (exponential) Prometheus
// histograms, and the bridge produces one the moment a queue is created, before
// any item is ever processed. The Telemetry API (the Cloud Monitoring OTLP
// endpoint) rejects a data point in that state with "num_finite_buckets" less
// than 1 and drops it, spamming the collector log every push tick. The padding
// adds no observation, so it does not change the data for other backends.
func padEmptyExponentialHistograms(inner sdkmetric.Producer) sdkmetric.Producer {
	return paddingProducer{inner: inner}
}

type paddingProducer struct {
	inner sdkmetric.Producer
}

func (p paddingProducer) Produce(ctx context.Context) ([]metricdata.ScopeMetrics, error) {
	sm, err := p.inner.Produce(ctx)
	for _, s := range sm {
		for _, m := range s.Metrics {
			hist, ok := m.Data.(metricdata.ExponentialHistogram[float64])
			if !ok {
				continue
			}
			for i := range hist.DataPoints {
				if len(hist.DataPoints[i].PositiveBucket.Counts) == 0 {
					hist.DataPoints[i].PositiveBucket = metricdata.ExponentialBucket{Counts: []uint64{0}}
				}
			}
		}
	}
	return sm, err
}
