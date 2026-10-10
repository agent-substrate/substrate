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

// Package ateomstatstest records the usage telemetry an ateom writes, for tests.
package ateomstatstest

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateomstats"
)

// Recorder is a stdout handler that keeps the kind and source of each record.
type Recorder struct {
	mu    sync.Mutex
	kinds []string
	srcs  []string
}

// NewEmitter returns an emitter that writes every record to a new Recorder.
func NewEmitter() (*ateomstats.UsageEmitter, *Recorder) {
	rec := &Recorder{}
	return ateomstats.NewUsageEmitter(nil, rec, ateomstats.Pool{}), rec
}

// NewCPUCounter returns a counter backed by a manual reader under scope.
func NewCPUCounter(t *testing.T, scope string) (*ateomstats.CPUCounter, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	counter, err := ateomstats.NewCPUCounter(mp.Meter(scope), ateomstats.Pool{Namespace: "ns", Name: "pool"})
	if err != nil {
		t.Fatal(err)
	}
	return counter, reader
}

// CPUSeconds collects the actor CPU counter, returning zero before any Add.
func CPUSeconds(t *testing.T, reader *sdkmetric.ManualReader) float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != ateomstats.CPUTimeMetric {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[float64])
			if !ok {
				t.Fatalf("CPU metric has data %T, want Sum[float64]", m.Data)
			}
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
		}
	}
	return total
}

// Kinds is the ate.stats.kind of each record, in order.
func (r *Recorder) Kinds() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.kinds...)
}

// Sources is the ate.stats.source of each record, in order.
func (r *Recorder) Sources() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.srcs...)
}

func (r *Recorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *Recorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case string(ateattr.StatsKindKey):
			r.kinds = append(r.kinds, a.Value.String())
		case string(ateattr.StatsSourceKey):
			r.srcs = append(r.srcs, a.Value.String())
		}
		return true
	})
	return nil
}

func (r *Recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *Recorder) WithGroup(string) slog.Handler      { return r }
