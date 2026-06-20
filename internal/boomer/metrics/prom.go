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

// Package metrics mirrors the Prometheus surface emitted by
// benchmarking/locust/common/metrics.py — same metric names and labels so
// dashboards built against the Python locust workers keep working when the
// load source is a boomer-Go worker.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/myzhan/boomer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "locust_requests_total",
			Help: "Total number of requests, by method/name/status/user_class.",
		},
		[]string{"method", "name", "status", "user_class"},
	)

	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "locust_request_duration_milliseconds",
			Help:    "Request latency in milliseconds, by method/name/status/user_class.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 16),
		},
		[]string{"method", "name", "status", "user_class"},
	)

	activeUsers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "locust_users",
			Help: "Active locust users, by user_class.",
		},
		[]string{"user_class"},
	)
)

func init() {
	prometheus.MustRegister(requestsTotal, requestDuration, activeUsers)
}

// Serve starts a /metrics HTTP server on addr. Returns when the server stops
// (or when ctx is cancelled, after which the server is gracefully shut down).
func Serve(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// RecordSuccess reports a successful request to both boomer (→ locust master
// via ZMQ) and the local Prometheus surface.
func RecordSuccess(method, name, userClass string, latency time.Duration, responseBytes int64) {
	ms := latency.Milliseconds()
	boomer.RecordSuccess(method, name, ms, responseBytes)
	requestsTotal.WithLabelValues(method, name, "success", userClass).Inc()
	requestDuration.WithLabelValues(method, name, "success", userClass).Observe(float64(ms))
}

// RecordFailure reports a failed request to both boomer and Prometheus.
func RecordFailure(method, name, userClass string, latency time.Duration, errMsg string) {
	ms := latency.Milliseconds()
	boomer.RecordFailure(method, name, ms, errMsg)
	requestsTotal.WithLabelValues(method, name, "failure", userClass).Inc()
	requestDuration.WithLabelValues(method, name, "failure", userClass).Observe(float64(ms))
}

// UpdateUsers shifts the active-users gauge for a class by delta (positive on
// user start, negative on stop).
func UpdateUsers(userClass string, delta float64) {
	activeUsers.WithLabelValues(userClass).Add(delta)
}
