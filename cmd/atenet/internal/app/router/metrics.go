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

package router

import (
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const (
	routerServiceName = "atenet-router"

	// atenet.router.route.duration measures the latency from when the ext_proc handler receives a request
	// (Envoy -> EPP) until the target worker endpoint is resolved
	routeDurationMetricName = "atenet.router.route.duration"
)

// newRouteDurationHistogram creates the atenet.router.route.duration histogram from
// the global MeterProvider.
func newRouteDurationHistogram() (metric.Float64Histogram, error) {
	h, err := otel.Meter(routerServiceName).Float64Histogram(
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
