// Copyright 2026 The Agent Substrate Authors
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

package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const maxDebugActorUIDs = 16

var (
	guestMetricLabels     = []string{"actor_uid", "actor_name", "actor_atespace"}
	guestCPUCores         = prometheus.NewDesc("ate_debug_actor_guest_cpu_usage_cores", "Mean guest CPU cores used between two samples in the same activation.", guestMetricLabels, nil)
	guestMemoryUsage      = prometheus.NewDesc("ate_debug_actor_guest_memory_usage_bytes", "Guest memory usage including reclaimable page cache.", guestMetricLabels, nil)
	guestMemoryWorkingSet = prometheus.NewDesc("ate_debug_actor_guest_memory_working_set_bytes", "Guest memory working set.", guestMetricLabels, nil)
)

type actorGuestReading struct {
	sample *ateworkerpb.WorkloadStatsSample
	cores  float64
	hasCPU bool
}

// actorGuestMetrics serves only the operator's allowlist on a separate registry.
// The stats poller publishes a complete snapshot after each sweep.
type actorGuestMetrics struct {
	allowed   map[string]struct{}
	interval  time.Duration
	tokenHash [sha256.Size]byte
	latest    atomic.Pointer[map[string]actorGuestReading]
}

func newActorGuestMetrics(uids []string, interval time.Duration, tokenFile string) (*actorGuestMetrics, error) {
	if len(uids) == 0 {
		return nil, nil
	}
	if interval <= 0 {
		return nil, errors.New("actor stats polling must be enabled")
	}
	if len(uids) > maxDebugActorUIDs {
		return nil, fmt.Errorf("at most %d actor UIDs are allowed", maxDebugActorUIDs)
	}
	m := &actorGuestMetrics{allowed: make(map[string]struct{}, len(uids)), interval: interval}
	for _, uid := range uids {
		if errs := resources.ValidateUUID(uid, field.NewPath("uid")); len(errs) > 0 {
			return nil, errs.ToAggregate()
		}
		m.allowed[uid] = struct{}{}
	}
	if tokenFile == "" {
		return nil, errors.New("--debug-actor-guest-metrics-token-file is required")
	}
	token, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read actor guest metrics bearer token: %w", err)
	}
	value := strings.TrimSpace(string(token))
	if len(value) < 32 || strings.ContainsAny(value, " \t\r\n") {
		return nil, errors.New("actor guest metrics bearer token must contain at least 32 bytes and no whitespace")
	}
	m.tokenHash = sha256.Sum256([]byte(value))
	return m, nil
}

func (m *actorGuestMetrics) selectSample(samples map[string]*ateworkerpb.WorkloadStatsSample, sample *ateworkerpb.WorkloadStatsSample) {
	uid := sample.GetActorUid()
	if _, ok := m.allowed[uid]; !ok || sample.GetEpochUnixNano() <= 0 {
		return
	}
	switch sample.GetSource() {
	case ateworkerpb.StatsSource_STATS_SOURCE_UNSPECIFIED, ateworkerpb.StatsSource_STATS_SOURCE_CGROUP:
		// A restore can supersede the guest reading before the new activation
		// is measured, or move the actor to gVisor.
	case ateworkerpb.StatsSource_STATS_SOURCE_GUEST_AGENT:
		if sample.GetObservedAtUnixNano() <= 0 {
			return
		}
	default:
		return
	}
	old := samples[uid]
	if sample.GetEpochUnixNano() > old.GetEpochUnixNano() ||
		(sample.GetEpochUnixNano() == old.GetEpochUnixNano() && sample.GetObservedAtUnixNano() > old.GetObservedAtUnixNano()) {
		samples[uid] = sample
	}
}

func (m *actorGuestMetrics) publish(samples map[string]*ateworkerpb.WorkloadStatsSample) {
	previous := m.latest.Load()
	next := make(map[string]actorGuestReading, len(samples))
	for uid, sample := range samples {
		if sample.GetSource() != ateworkerpb.StatsSource_STATS_SOURCE_GUEST_AGENT {
			continue
		}
		reading := actorGuestReading{sample: sample}
		if previous != nil {
			last := (*previous)[uid]
			if last.sample != nil && last.sample.GetEpochUnixNano() == sample.GetEpochUnixNano() {
				elapsed := sample.GetObservedAtUnixNano() - last.sample.GetObservedAtUnixNano()
				if elapsed == 0 {
					reading = last
				} else if elapsed > 0 && sample.GetCpuUsageUsec() >= last.sample.GetCpuUsageUsec() {
					reading.cores = 1000 * float64(sample.GetCpuUsageUsec()-last.sample.GetCpuUsageUsec()) / float64(elapsed)
					reading.hasCPU = true
				}
			}
		}
		next[uid] = reading
	}
	m.latest.Store(&next)
}

func (m *actorGuestMetrics) Describe(ch chan<- *prometheus.Desc) {
	ch <- guestCPUCores
	ch <- guestMemoryUsage
	ch <- guestMemoryWorkingSet
}

func (m *actorGuestMetrics) Collect(ch chan<- prometheus.Metric) {
	snapshot := m.latest.Load()
	if snapshot == nil {
		return
	}
	cutoff := time.Now().Add(-m.interval).Add(-m.interval).UnixNano()
	for uid, reading := range *snapshot {
		sample := reading.sample
		if sample.GetObservedAtUnixNano() < cutoff {
			continue
		}
		labels := []string{uid, sample.GetActorName(), sample.GetAtespace()}
		ch <- prometheus.MustNewConstMetric(guestMemoryUsage, prometheus.GaugeValue, float64(sample.GetMemoryCurrentBytes()), labels...)
		ch <- prometheus.MustNewConstMetric(guestMemoryWorkingSet, prometheus.GaugeValue, float64(sample.GetMemoryWorkingSetBytes()), labels...)
		if reading.hasCPU {
			ch <- prometheus.MustNewConstMetric(guestCPUCores, prometheus.GaugeValue, reading.cores, labels...)
		}
	}
}

func (m *actorGuestMetrics) handler() http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(m)
	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		scheme, token, _ := strings.Cut(r.Header.Get("Authorization"), " ")
		hash := sha256.Sum256([]byte(token))
		if !strings.EqualFold(scheme, "Bearer") || subtle.ConstantTimeCompare(hash[:], m.tokenHash[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r)
	})
	return mux
}

func (m *actorGuestMetrics) serve(ctx context.Context) {
	server := &http.Server{
		Addr:              "127.0.0.1:9092",
		Handler:           m.handler(),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
	stop := context.AfterFunc(ctx, func() { _ = server.Close() })
	defer stop()
	slog.InfoContext(ctx, "Starting actor guest debug metrics", slog.String("addr", server.Addr))
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.ErrorContext(ctx, "Actor guest debug metrics server failed", slog.Any("err", err))
	}
}
