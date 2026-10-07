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

package actorjwt

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

func newMeteredMinter(t *testing.T, ctl *fakeControl) (*Minter, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	inst, err := NewInstruments(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("atenet-router"))
	if err != nil {
		t.Fatalf("NewInstruments: %v", err)
	}
	return New(newClient(t, ctl), inst), reader
}

// series is one datapoint's labels. An empty field is an absent label.
type series struct {
	outcome   string
	errorType string
}

// collect returns the lookup count and the mint count of each series.
func collect(t *testing.T, reader *sdkmetric.ManualReader) (lookups map[series]int64, mints map[series]uint64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	lookups, mints = map[series]int64{}, map[series]uint64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case lookupsMetric:
				if m.Unit != "{lookup}" {
					t.Errorf("%s unit = %q, want {lookup}", m.Name, m.Unit)
				}
				sum, ok := m.Data.(metricdata.Sum[int64])
				if !ok || !sum.IsMonotonic {
					t.Fatalf("%s data = %T (monotonic %v), want a monotonic Sum[int64]", m.Name, m.Data, sum.IsMonotonic)
				}
				for _, dp := range sum.DataPoints {
					lookups[seriesOf(t, dp.Attributes)] += dp.Value
				}
			case mintDurationMetric:
				if m.Unit != "s" {
					t.Errorf("%s unit = %q, want s", m.Name, m.Unit)
				}
				hist, ok := m.Data.(metricdata.Histogram[float64])
				if !ok {
					t.Fatalf("%s data = %T, want Histogram[float64]", m.Name, m.Data)
				}
				for _, dp := range hist.DataPoints {
					mints[seriesOf(t, dp.Attributes)] += dp.Count
				}
			default:
				t.Errorf("unexpected instrument %s", m.Name)
			}
		}
	}
	return lookups, mints
}

func seriesOf(t *testing.T, set attribute.Set) series {
	t.Helper()
	var s series
	n := 0
	if v, ok := set.Value(ateattr.EgressActorJWTOutcomeKey); ok {
		s.outcome = v.AsString()
		n++
	}
	if v, ok := set.Value(ateattr.ErrorTypeKey); ok {
		s.errorType = v.AsString()
		n++
	}
	if set.Len() != n {
		t.Errorf("series has labels %v, want only %s and %s", set.ToSlice(), ateattr.EgressActorJWTOutcomeKey, ateattr.ErrorTypeKey)
	}
	return s
}

func TestMetricsCountMissThenHit(t *testing.T) {
	m, reader := newMeteredMinter(t, &fakeControl{})
	src := jwtSource(900, "a")
	wantToken(t, m, testActor, src, "jwt-1")
	wantToken(t, m, testActor, src, "jwt-1")

	lookups, mints := collect(t, reader)
	wantLookups := map[series]int64{
		{outcome: ateattr.EgressActorJWTOutcomeMiss}: 1,
		{outcome: ateattr.EgressActorJWTOutcomeHit}:  1,
	}
	if diff := cmp.Diff(wantLookups, lookups, cmp.AllowUnexported(series{})); diff != "" {
		t.Errorf("lookups (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(map[series]uint64{{}: 1}, mints, cmp.AllowUnexported(series{})); diff != "" {
		t.Errorf("mints (-want +got):\n%s", diff)
	}
}

func TestMetricsRecordAFailedMint(t *testing.T) {
	m, reader := newMeteredMinter(t, &fakeControl{err: status.Error(codes.Unavailable, "ateapi is down")})
	if _, err := m.Token(context.Background(), testActor, jwtSource(900, "a")); err == nil {
		t.Fatal("Token succeeded, want an error")
	}

	lookups, mints := collect(t, reader)
	wantLookups := map[series]int64{{outcome: ateattr.EgressActorJWTOutcomeError, errorType: "Unavailable"}: 1}
	if diff := cmp.Diff(wantLookups, lookups, cmp.AllowUnexported(series{})); diff != "" {
		t.Errorf("lookups (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(map[series]uint64{{errorType: "Unavailable"}: 1}, mints, cmp.AllowUnexported(series{})); diff != "" {
		t.Errorf("mints (-want +got):\n%s", diff)
	}
}

func TestMetricsSeparateCallersThatGaveUp(t *testing.T) {
	ctl := &fakeControl{gate: make(chan struct{})}
	m, reader := newMeteredMinter(t, ctl)
	t.Cleanup(func() { close(ctl.gate) })
	src := jwtSource(900, "a")

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Token(canceled, testActor, src); err == nil {
		t.Fatal("Token with a canceled context succeeded, want an error")
	}
	expired, cancel := context.WithDeadline(context.Background(), time.Now())
	defer cancel()
	if _, err := m.Token(expired, testActor, src); err == nil {
		t.Fatal("Token with an expired context succeeded, want an error")
	}

	lookups, _ := collect(t, reader)
	wantLookups := map[series]int64{
		{outcome: ateattr.EgressActorJWTOutcomeCancelled}: 1,
		{outcome: ateattr.EgressActorJWTOutcomeTimeout}:   1,
	}
	if diff := cmp.Diff(wantLookups, lookups, cmp.AllowUnexported(series{})); diff != "" {
		t.Errorf("lookups (-want +got):\n%s", diff)
	}
}
