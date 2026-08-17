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

package imagecache

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

// newMeteredStore opens a store against a local ManualReader-backed provider,
// so the tests never touch the global meter provider.
func newMeteredStore(t *testing.T, opts ...Option) (*Store, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return newTestStore(t, append(opts, WithMeter(mp.Meter("atelet")))...), reader
}

// requestSeries is one ate.imagecache.requests datapoint, flattened to its
// label values.
type requestSeries struct {
	outcome   string
	errorType string
}

func collectRequests(t *testing.T, reader *sdkmetric.ManualReader) map[requestSeries]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	got := make(map[requestSeries]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != requestsMetric {
				continue
			}
			if m.Unit != "{request}" {
				t.Errorf("unit = %q, want {request}", m.Unit)
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("data type = %T, want Sum[int64]", m.Data)
			}
			if !sum.IsMonotonic {
				t.Error("IsMonotonic = false, want true (a counter, not an updowncounter)")
			}
			for _, dp := range sum.DataPoints {
				got[seriesOf(t, dp.Attributes)] += dp.Value
			}
		}
	}
	return got
}

func seriesOf(t *testing.T, set attribute.Set) requestSeries {
	t.Helper()
	s := requestSeries{}
	if v, ok := set.Value(ateattr.ImageCacheOutcomeKey); ok {
		s.outcome = v.AsString()
	}
	// An absent error.type means success; the zero value stands for it.
	if v, ok := set.Value(ateattr.ErrorTypeKey); ok {
		s.errorType = v.AsString()
	}
	return s
}

func pushOneLayerImage(t *testing.T, ref string) {
	t.Helper()
	layer := layerFromEntries(t, []tarEntry{
		{name: "app/", typeflag: tar.TypeDir},
		{name: "app/main", typeflag: tar.TypeReg, mode: 0o755, body: "main"},
	})
	pushImage(t, ref, v1.Config{}, layer)
}

// TestRequestsCountsMissThenHit: the first lookup pays for the pull, every
// later one is free.
func TestRequestsCountsMissThenHit(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/metrics:latest"
	pushOneLayerImage(t, ref)

	store, reader := newMeteredStore(t)
	ctx := context.Background()

	for range 3 {
		if _, err := store.EnsureImage(ctx, ref); err != nil {
			t.Fatalf("EnsureImage: %v", err)
		}
	}

	got := collectRequests(t, reader)
	want := map[requestSeries]int64{
		{outcome: ateattr.ImageCacheOutcomeMiss}: 1,
		{outcome: ateattr.ImageCacheOutcomeHit}:  2,
	}
	for series, count := range want {
		if got[series] != count {
			t.Errorf("series %+v = %d, want %d (all series: %+v)", series, got[series], count, got)
		}
	}
	if len(got) != len(want) {
		t.Errorf("collected %d series, want %d: %+v", len(got), len(want), got)
	}
}

// TestRequestsRecordsFailures pins the outcome of each failure kind, and that
// only the error outcome carries an error.type.
func TestRequestsRecordsFailures(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/fails:latest"
	pushOneLayerImage(t, ref)

	tests := []struct {
		name string
		ref  string
		ctx  func() context.Context
		want requestSeries
	}{
		{
			// The ref never reached a registry, so there is no status.
			name: "unparseable ref",
			ref:  "NOT A REFERENCE",
			want: requestSeries{outcome: ateattr.ImageCacheOutcomeError, errorType: errTypeOther},
		},
		{
			// The registry's own identifier, reported verbatim.
			name: "no such tag",
			ref:  host + "/test/fails:absent",
			want: requestSeries{outcome: ateattr.ImageCacheOutcomeError, errorType: "404"},
		},
		{
			// The cache is healthy; the caller went away. No error.type.
			name: "caller gave up",
			ref:  ref,
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			want: requestSeries{outcome: ateattr.ImageCacheOutcomeCancelled},
		},
		{
			name: "caller ran out of time",
			ref:  ref,
			ctx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 0)
				t.Cleanup(cancel)
				return ctx
			},
			want: requestSeries{outcome: ateattr.ImageCacheOutcomeTimeout},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, reader := newMeteredStore(t)
			ctx := context.Background()
			if tt.ctx != nil {
				ctx = tt.ctx()
			}

			if _, err := store.EnsureImage(ctx, tt.ref); err == nil {
				t.Fatal("EnsureImage succeeded, want a failure")
			}

			got := collectRequests(t, reader)
			if got[tt.want] != 1 {
				t.Errorf("series %+v = %d, want 1 (all series: %+v)", tt.want, got[tt.want], got)
			}
		})
	}
}

// TestClassifyFailure pins the bounded label set. Each value comes from a
// sentinel or a typed error, never from a message.
func TestClassifyFailure(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantOutcome   string
		wantErrorType string
	}{
		{"caller cancelled", fmt.Errorf("while resolving tag: %w", context.Canceled), ateattr.ImageCacheOutcomeCancelled, ""},
		{"caller timed out", fmt.Errorf("while resolving tag: %w", context.DeadlineExceeded), ateattr.ImageCacheOutcomeTimeout, ""},
		{"registry rejection", &transport.Error{StatusCode: 404}, ateattr.ImageCacheOutcomeError, "404"},
		{"credentials expired", fmt.Errorf("in remote.Image: %w", &transport.Error{StatusCode: 401}), ateattr.ImageCacheOutcomeError, "401"},
		{"throttled", &transport.Error{StatusCode: 429}, ateattr.ImageCacheOutcomeError, "429"},
		// A remote can return any status; only the reported set is a label.
		{"unlisted status", &transport.Error{StatusCode: 418}, ateattr.ImageCacheOutcomeError, errTypeOther},
		{"disk full", &os.PathError{Op: "write", Path: "/var/lib/x", Err: syscall.ENOSPC}, ateattr.ImageCacheOutcomeError, errTypeOther},
		{"unclassified", errors.New("layer dir vanished during pull"), ateattr.ImageCacheOutcomeError, errTypeOther},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := failureOutcome(tt.err); got != tt.wantOutcome {
				t.Errorf("failureOutcome(%v) = %q, want %q", tt.err, got, tt.wantOutcome)
			}
			if tt.wantErrorType == "" {
				return
			}
			if got := errorType(tt.err); got != tt.wantErrorType {
				t.Errorf("errorType(%v) = %q, want %q", tt.err, got, tt.wantErrorType)
			}
		})
	}
}

// TestRequestsWithoutMeterIsNoOp: the validation tool and most tests open the
// cache with no metrics pipeline.
func TestRequestsWithoutMeterIsNoOp(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/nometer:latest"
	pushOneLayerImage(t, ref)

	store := newTestStore(t)
	if _, err := store.EnsureImage(context.Background(), ref); err != nil {
		t.Fatalf("EnsureImage: %v", err)
	}
}
