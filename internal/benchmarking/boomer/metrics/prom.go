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

	serverDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "locust_server_duration_milliseconds",
			Help:    "Server-side latency in milliseconds, read from the response trailer or header, by method/name/user_class.",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 20),
		},
		[]string{"method", "name", "user_class"},
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
	prometheus.MustRegister(requestsTotal, requestDuration, serverDuration, activeUsers)
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
	boomer.RecordSuccess(method, name, latency.Milliseconds(), responseBytes)
	RecordPromSuccess(method, name, userClass, latency)
}

// RecordFailure reports a failed request to both boomer and Prometheus.
func RecordFailure(method, name, userClass string, latency time.Duration, errMsg string) {
	boomer.RecordFailure(method, name, latency.Milliseconds(), errMsg)
	RecordPromFailure(method, name, userClass, latency)
}

// RecordPromSuccess reports a successful request to Prometheus only. Use it
// once boomer.Run has returned: boomer may have stopped draining its stats
// channel by then, and a send to it blocks once the channel's buffer fills.
func RecordPromSuccess(method, name, userClass string, latency time.Duration) {
	ms := float64(latency.Milliseconds())
	requestsTotal.WithLabelValues(method, name, "success", userClass).Inc()
	requestDuration.WithLabelValues(method, name, "success", userClass).Observe(ms)
}

// RecordPromFailure reports a failed request to Prometheus only, as
// RecordPromSuccess does a successful one.
func RecordPromFailure(method, name, userClass string, latency time.Duration) {
	ms := float64(latency.Milliseconds())
	requestsTotal.WithLabelValues(method, name, "failure", userClass).Inc()
	requestDuration.WithLabelValues(method, name, "failure", userClass).Observe(ms)
}

// RecordServerLatency reports the server's own elapsed time for a request,
// to Prometheus only: locust has one latency per request, the client's.
func RecordServerLatency(method, name, userClass string, latency time.Duration) {
	serverDuration.WithLabelValues(method, name, userClass).Observe(float64(latency) / float64(time.Millisecond))
}

// UpdateUsers shifts the active-users gauge for a class by delta (positive on
// user start, negative on stop).
func UpdateUsers(userClass string, delta float64) {
	activeUsers.WithLabelValues(userClass).Add(delta)
}
