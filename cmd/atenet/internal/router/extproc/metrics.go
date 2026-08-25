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

package extproc

import (
	"context"
	"errors"
	"fmt"
	"time"

	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

// ServiceName is the OpenTelemetry service name and instrumentation scope
// shared by every part of the atenet router process — the ext_proc mux here,
// the direction handlers, and the router's own servers. It lives in this
// package because it is the one package all of them already depend on.
const ServiceName = "atenet-router"

// atenet.router.route.duration measures the latency from when the ext_proc handler receives a request
// (dataplane -> EPP) until the target worker endpoint is resolved
const routeDurationMetricName = "atenet.router.route.duration"

// NewRouteDurationHistogram creates the atenet.router.route.duration histogram from
// the global MeterProvider.
func NewRouteDurationHistogram() (metric.Float64Histogram, error) {
	h, err := otel.Meter(ServiceName).Float64Histogram(
		routeDurationMetricName,
		metric.WithUnit("s"),
		metric.WithDescription(
			"latency between Substrate router receiving a request and resolving "+
				"the target worker endpoint, excluding actor execution and response",
		),
		metric.WithExplicitBucketBoundaries(
			0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
			0.075, 0.1, 0.15, 0.2, 0.25, 0.5, 1, 2.5, 5, 10, 15, 30,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", routeDurationMetricName, err)
	}
	return h, nil
}

func (s *Server) recordRouteDuration(ctx context.Context, d time.Duration, tmplNs, tmplName, outcome, resume string) {
	if s.routeDuration == nil {
		return
	}
	s.routeDuration.Record(ctx, d.Seconds(), metric.WithAttributes(
		ateattr.TemplateNamespaceKey.String(tmplNs),
		ateattr.TemplateNameKey.String(tmplName),
		ateattr.RouterOutcomeKey.String(outcome),
		ateattr.RouterResumeKey.String(resume),
	))
}

func classifyOutcome(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		return "cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		return "timeout"
	}
	switch status.Code(err) {
	case codes.ResourceExhausted:
		return "no_capacity"
	case codes.FailedPrecondition:
		return "failed_precondition"
	case codes.Aborted:
		return "lock_conflict"
	case codes.NotFound:
		return "not_found"
	case codes.Unavailable:
		return "unavailable"
	}
	var re *ReqError
	if errors.As(err, &re) {
		switch envoy_type.StatusCode(re.StatusCode) {
		case envoy_type.StatusCode_NotFound:
			return "not_found"
		case envoy_type.StatusCode_ServiceUnavailable:
			return "no_capacity"
		case envoy_type.StatusCode_GatewayTimeout:
			return "timeout"
		case envoy_type.StatusCode_TooManyRequests:
			return "rate_limited"
		}
	}
	return "resume_error"
}
