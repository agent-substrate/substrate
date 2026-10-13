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

package ingress

import (
	"context"
	"maps"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

// newTestParkingMetrics returns ParkingMetrics whose rejected counter reports
// to the returned reader.
func newTestParkingMetrics(t *testing.T) (*ParkingMetrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	rejected, err := mp.Meter("atenet-router").Int64Counter(parkingRejectedMetricName)
	if err != nil {
		t.Fatalf("create %s counter: %v", parkingRejectedMetricName, err)
	}
	return &ParkingMetrics{rejected: rejected}, reader
}

// rejectedByOutcome returns the parking.rejected count for each value of
// ate.router.outcome.
func rejectedByOutcome(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != parkingRejectedMetricName {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				outcome, ok := dp.Attributes.Value(ateattr.RouterOutcomeKey)
				if !ok {
					t.Errorf("%s data point has no %s attribute", parkingRejectedMetricName, ateattr.RouterOutcomeKey)
				}
				got[outcome.AsString()] += dp.Value
			}
		}
	}
	return got
}

func TestParkingLot_RejectedLabeledByShedOutcome(t *testing.T) {
	m, reader := newTestParkingMetrics(t)
	lot := newParkingLot(ParkedRequestConfig{Budget: time.Second, Max: 1}, m)
	ctx := context.Background()

	release, ok := lot.enter(ctx, ateattr.RouterOutcomeNoCapacity)
	if !ok {
		t.Fatal("first enter should be admitted")
	}
	defer release(parkOutcomeServed)

	for _, outcome := range []string{ateattr.RouterOutcomeNoCapacity, ateattr.RouterOutcomeNoCapacity, ateattr.RouterOutcomeUnavailable} {
		if _, ok := lot.enter(ctx, outcome); ok {
			t.Fatalf("enter(%q) admitted, want rejected when the lot is full", outcome)
		}
	}

	got := rejectedByOutcome(t, reader)
	want := map[string]int64{ateattr.RouterOutcomeNoCapacity: 2, ateattr.RouterOutcomeUnavailable: 1}
	if !maps.Equal(got, want) {
		t.Errorf("parking.rejected by outcome = %v, want %v", got, want)
	}
}

func TestParkingLot_DisabledRecordsNoRejection(t *testing.T) {
	m, reader := newTestParkingMetrics(t)
	lot := newParkingLot(ParkedRequestConfig{Budget: time.Second, Max: 0}, m)

	if _, ok := lot.enter(context.Background(), ateattr.RouterOutcomeNoCapacity); !ok {
		t.Fatal("enter should always be admitted when parking is disabled")
	}
	if got := rejectedByOutcome(t, reader); len(got) != 0 {
		t.Errorf("parking.rejected by outcome = %v, want none", got)
	}
}
