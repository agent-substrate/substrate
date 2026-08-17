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

package extproc

import (
	"context"
	"errors"
	"testing"
	"time"

	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

func TestClassifyOutcome(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error maps to ok",
			err:      nil,
			expected: "ok",
		},
		{
			name:     "context Canceled maps to cancelled",
			err:      context.Canceled,
			expected: "cancelled",
		},
		{
			name:     "context DeadlineExceeded maps to timeout",
			err:      context.DeadlineExceeded,
			expected: "timeout",
		},
		{
			name:     "FailedPrecondition gRPC code maps to no_capacity",
			err:      status.Error(codes.FailedPrecondition, "capacity full"),
			expected: "no_capacity",
		},
		{
			name:     "Aborted gRPC code maps to lock_conflict",
			err:      status.Error(codes.Aborted, "lock conflict"),
			expected: "lock_conflict",
		},
		{
			name:     "NotFound gRPC code maps to not_found",
			err:      status.Error(codes.NotFound, "missing"),
			expected: "not_found",
		},
		{
			name:     "Unavailable gRPC code maps to unavailable",
			err:      status.Error(codes.Unavailable, "control-plane down"),
			expected: "unavailable",
		},
		{
			name:     "ResourceExhausted gRPC code maps to rate_limited",
			err:      status.Error(codes.ResourceExhausted, "rate limit exceeded"),
			expected: "rate_limited",
		},
		{
			name:     "StatusCode_NotFound ReqError maps to not_found",
			err:      NewReqError(envoy_type.StatusCode_NotFound, "missing"),
			expected: "not_found",
		},
		{
			name:     "StatusCode_ServiceUnavailable ReqError maps to no_capacity",
			err:      NewReqError(envoy_type.StatusCode_ServiceUnavailable, "no free workers"),
			expected: "no_capacity",
		},
		{
			name:     "StatusCode_TooManyRequests ReqError maps to rate_limited",
			err:      NewReqError(envoy_type.StatusCode_TooManyRequests, "rate limited"),
			expected: "rate_limited",
		},
		{
			name:     "Unknown error maps to resume_error",
			err:      errors.New("internal storage glitch"),
			expected: "resume_error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyOutcome(tc.err); got != tc.expected {
				t.Errorf("classifyOutcome(%v) = %q, want %q", tc.err, got, tc.expected)
			}
		})
	}
}

func TestRecordRouteDuration_Attributes(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	h, err := mp.Meter("atenet-router").Float64Histogram(routeDurationMetricName)
	if err != nil {
		t.Fatalf("failed to create histogram: %v", err)
	}

	s := NewServer(50051, h, nil)
	s.recordRouteDuration(context.Background(), 10*time.Millisecond, "team-a-ns", "tmpl-a", classifyOutcome(nil), ateattr.RouterResumeTriggered)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	dp := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Histogram[float64]).DataPoints[0]
	wantAttrs := map[string]string{
		"ate.template.namespace": "team-a-ns",
		"ate.template.name":      "tmpl-a",
		"ate.router.outcome":     "ok",
		"ate.router.resume":      "triggered",
	}

	for k, want := range wantAttrs {
		val, exists := dp.Attributes.Value(attribute.Key(k))
		if !exists {
			t.Errorf("missing metric attribute %q", k)
		} else if val.AsString() != want {
			t.Errorf("attribute %q = %q, want %q", k, val.AsString(), want)
		}
	}
}
