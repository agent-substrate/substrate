//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package ingress

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
)

// Request-parking instruments. parking.active is the live count of parked
// requests; parking.wait.duration is how long each request stayed parked
// (labeled by outcome); parking.rejected counts requests shed because the
// parking lot was full.
const (
	parkingActiveMetricName   = "atenet.router.parking.active"
	parkingWaitMetricName     = "atenet.router.parking.wait.duration"
	parkingRejectedMetricName = "atenet.router.parking.rejected"
)

// ParkingMetrics bundles the OpenTelemetry instruments used by the parking lot.
// A nil *ParkingMetrics is safe to use: every method becomes a no-op, which
// keeps tests and metric-free deployments simple.
type ParkingMetrics struct {
	active   metric.Int64UpDownCounter
	wait     metric.Float64Histogram
	rejected metric.Int64Counter
}

// NewParkingMetrics creates the request-parking instruments from the global
// MeterProvider.
func NewParkingMetrics() (*ParkingMetrics, error) {
	meter := otel.Meter(extproc.ServiceName)

	active, err := meter.Int64UpDownCounter(
		parkingActiveMetricName,
		metric.WithUnit("{request}"),
		metric.WithDescription("number of requests currently parked in the router awaiting actor resume"),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s counter: %w", parkingActiveMetricName, err)
	}

	wait, err := meter.Float64Histogram(
		parkingWaitMetricName,
		metric.WithUnit("s"),
		metric.WithDescription("time a request spent parked in the router before being served, timing out, or failing"),
		metric.WithExplicitBucketBoundaries(
			0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 30, 45, 60,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", parkingWaitMetricName, err)
	}

	rejected, err := meter.Int64Counter(
		parkingRejectedMetricName,
		metric.WithUnit("{request}"),
		metric.WithDescription("number of requests shed because the router parking lot was full"),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s counter: %w", parkingRejectedMetricName, err)
	}

	return &ParkingMetrics{active: active, wait: wait, rejected: rejected}, nil
}

func (m *ParkingMetrics) addActive(ctx context.Context, delta int64) {
	if m == nil || m.active == nil {
		return
	}
	m.active.Add(ctx, delta)
}

func (m *ParkingMetrics) recordWait(ctx context.Context, d time.Duration, outcome parkOutcome) {
	if m == nil || m.wait == nil {
		return
	}
	m.wait.Record(ctx, d.Seconds(), metric.WithAttributes(attribute.String("outcome", string(outcome))))
}

func (m *ParkingMetrics) recordRejected(ctx context.Context) {
	if m == nil || m.rejected == nil {
		return
	}
	m.rejected.Add(ctx, 1)
}
