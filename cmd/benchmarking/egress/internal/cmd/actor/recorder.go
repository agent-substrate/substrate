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

// Package actor is the workload of the egress scale test: the subcommand
// that holds one tunnel per endpoint open through the egress gateway and
// counts what happened.
package actor

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"

	"go.ate.dev/substrate/cmd/benchmarking/egress/internal/contract"
)

// Recorder holds one loop's four distributions as OTel histograms on a
// manual reader in delta temporality: Collect returns what was recorded since
// the previous Collect, so each call is one window. Nothing exports; the
// points travel inside the loop's Stats.
type Recorder struct {
	reader   *sdkmetric.ManualReader
	provider *sdkmetric.MeterProvider
	hists    map[contract.Distribution]metric.Float64Histogram
}

// NewRecorder creates the four instruments on a private MeterProvider.
func NewRecorder() (*Recorder, error) {
	reader := sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(
		func(sdkmetric.InstrumentKind) metricdata.Temporality { return metricdata.DeltaTemporality }))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithResource(resource.Empty()))
	meter := provider.Meter("benchmark-egress")
	r := &Recorder{reader: reader, provider: provider, hists: make(map[contract.Distribution]metric.Float64Histogram, len(contract.DistributionNames))}
	for _, name := range contract.DistributionNames {
		h, err := meter.Float64Histogram(string(name), metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(contract.Bounds...))
		if err != nil {
			_ = provider.Shutdown(context.Background())
			return nil, fmt.Errorf("creating the %s histogram: %w", name, err)
		}
		r.hists[name] = h
	}
	return r, nil
}

// Record adds one sample, in seconds, to the named distribution.
func (r *Recorder) Record(ctx context.Context, name contract.Distribution, seconds float64) {
	r.hists[name].Record(ctx, seconds)
}

// Collect returns each distribution's samples since the previous Collect,
// keyed by name. A distribution with no new sample is absent.
func (r *Recorder) Collect(ctx context.Context) (map[contract.Distribution]contract.Point, error) {
	var rm metricdata.ResourceMetrics
	if err := r.reader.Collect(ctx, &rm); err != nil {
		return nil, err
	}
	out := make(map[contract.Distribution]contract.Point, len(contract.DistributionNames))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || len(h.DataPoints) == 0 {
				continue
			}
			out[contract.Distribution(m.Name)] = pointOf(h.DataPoints[0])
		}
	}
	return out, nil
}

// Shutdown releases the provider. Record is a no-op afterwards.
func (r *Recorder) Shutdown(ctx context.Context) error { return r.provider.Shutdown(ctx) }

// pointOf copies the merge fields out of an SDK data point.
func pointOf(dp metricdata.HistogramDataPoint[float64]) contract.Point {
	p := contract.Point{Bounds: dp.Bounds, BucketCounts: dp.BucketCounts, Count: dp.Count, Sum: dp.Sum}
	p.Min, _ = dp.Min.Value()
	p.Max, _ = dp.Max.Value()
	return p
}
