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

// Package statusz serves ateapi's replica-local operator status page.
package statusz

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	maxWorkerGroups         = 100
	diagnosticCacheInterval = 5 * time.Second

	poolCoverage = "Connection occupancy as of the diagnostic sample; replica-local and not a connectivity health check."
	rpcCoverage  = "Completed non-OK authenticated unary Control RPCs retained on this replica as of the diagnostic sample."
	rpcRetention = "Newest 100 matching completions; resets on process restart; not an error rate."
)

// Build identifies the running binary.
type Build struct {
	Version  string `json:"version"`
	Revision string `json:"revision"`
}

// Listeners records the configured process listeners.
type Listeners struct {
	GRPC    string `json:"grpc"`
	Metrics string `json:"metrics"`
	Status  string `json:"status"`
}

// Drain records the configured shutdown timing.
type Drain struct {
	Delay   string `json:"delay"`
	Timeout string `json:"timeout"`
}

// Flag is a display-safe projection of one resolved process flag.
type Flag struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Source string `json:"source"`
}

// Config is immutable process configuration captured at startup.
type Config struct {
	Build     Build
	StartedAt time.Time
	Listeners Listeners
	Drain     Drain
	Flags     []Flag
}

// WorkerList reads the existing worker-cache snapshot.
type WorkerList func() ([]*ateapipb.Worker, error)

// PoolCounts is the sampled occupancy of one connection pool.
type PoolCounts struct {
	AcquiredConns int32
	IdleConns     int32
	MaxConns      int32
}

// PoolReader reads the operational and watch pools, in that order.
type PoolReader func() (PoolCounts, PoolCounts)

// RPCFailureList reads a detached newest-first failure snapshot.
type RPCFailureList func() []RPCFailure

// RPCFailure is one completed authenticated unary Control call retained by
// this process. Its fields are bounded and omit request, response, and error
// details.
type RPCFailure struct {
	CompletedAt   string `json:"completed_at"`
	Method        string `json:"method"`
	PrincipalKind string `json:"principal_kind"`
	PrincipalID   string `json:"principal_id"`
	Code          string `json:"code"`
	Elapsed       string `json:"elapsed"`
}

// Snapshot is the common response model rendered as JSON or HTML.
type Snapshot struct {
	Build                 Build                         `json:"build"`
	Process               ProcessSnapshot               `json:"process"`
	Configuration         ConfigurationSnapshot         `json:"configuration"`
	DiagnosticSample      DiagnosticSampleSnapshot      `json:"diagnostic_sample"`
	Workers               WorkersSnapshot               `json:"workers"`
	Readiness             ReadinessSnapshot             `json:"readiness"`
	PostgreSQLPools       PostgreSQLPoolsSnapshot       `json:"postgresql_pools"`
	RecentControlFailures RecentControlFailuresSnapshot `json:"recent_control_failures"`
}

// DiagnosticSampleSnapshot describes the shared cached sample used by the
// worker, pool, and recent-failure sections.
type DiagnosticSampleSnapshot struct {
	SampledAt         string `json:"sampled_at"`
	Age               string `json:"age"`
	RefreshInProgress bool   `json:"refresh_in_progress"`
}

// ProcessSnapshot describes the lifetime of the current process.
type ProcessSnapshot struct {
	StartedAt string `json:"started_at"`
	Uptime    string `json:"uptime"`
}

// ConfigurationSnapshot contains display-safe startup configuration.
type ConfigurationSnapshot struct {
	Listeners Listeners `json:"listeners"`
	Drain     Drain     `json:"drain"`
	Flags     []Flag    `json:"flags"`
}

// WorkerGroup is the number of cached workers in one namespace and pool.
type WorkerGroup struct {
	Namespace string `json:"namespace"`
	Pool      string `json:"pool"`
	Count     int    `json:"count"`
}

// WorkersSnapshot describes the current worker-cache view. Counts are absent
// when the cache is unavailable, distinguishing that state from a ready empty
// cache with explicit zero counts.
type WorkersSnapshot struct {
	Available    bool          `json:"available"`
	Empty        bool          `json:"empty"`
	TotalWorkers *int          `json:"total_workers,omitempty"`
	TotalGroups  *int          `json:"total_groups,omitempty"`
	Truncated    bool          `json:"truncated"`
	Groups       []WorkerGroup `json:"groups"`
}

// ReadinessSnapshot mirrors the existing process readiness predicate.
type ReadinessSnapshot struct {
	Ready bool   `json:"ready"`
	State string `json:"state"`
}

// PoolSnapshot is one role's sampled connection occupancy.
type PoolSnapshot struct {
	Role          string `json:"role"`
	AcquiredConns int32  `json:"acquired_conns"`
	IdleConns     int32  `json:"idle_conns"`
	MaxConns      int32  `json:"max_conns"`
}

// PostgreSQLPoolsSnapshot distinguishes an unavailable reader from available
// pools whose occupancy values happen to be zero.
type PostgreSQLPoolsSnapshot struct {
	Available bool           `json:"available"`
	Coverage  string         `json:"coverage"`
	Pools     []PoolSnapshot `json:"pools"`
}

// RecentControlFailuresSnapshot describes the recorder's exact local coverage
// and retention alongside its newest-first events.
type RecentControlFailuresSnapshot struct {
	Coverage  string       `json:"coverage"`
	Retention string       `json:"retention"`
	Empty     bool         `json:"empty"`
	Failures  []RPCFailure `json:"failures"`
}

type handler struct {
	config   Config
	workers  WorkerList
	ready    func() bool
	pools    PoolReader
	failures RPCFailureList
	now      func() time.Time

	cacheMu    sync.Mutex
	cached     *cachedDiagnostics
	refreshing bool
}

type cachedDiagnostics struct {
	sampledAt time.Time
	workers   WorkersSnapshot
	pools     PostgreSQLPoolsSnapshot
	failures  RecentControlFailuresSnapshot
}

// NewHandler constructs the status handler from immutable startup config and
// the existing thread-safe runtime readers.
func NewHandler(config Config, workers WorkerList, ready func() bool, pools PoolReader, failures RPCFailureList, now func() time.Time) http.Handler {
	config.Flags = append([]Flag(nil), config.Flags...)
	return &handler{config: config, workers: workers, ready: ready, pools: pools, failures: failures, now: now}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	snapshot, ok := h.snapshot()
	if !ok {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "diagnostic sample is being collected", http.StatusServiceUnavailable)
		return
	}
	if req.URL.Query().Get("format") == "json" || strings.Contains(req.Header.Get("Accept"), "application/json") {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snapshot)
		return
	}

	var rendered bytes.Buffer
	if err := dashboardTemplate.Execute(&rendered, snapshot); err != nil {
		http.Error(w, "status page rendering failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = rendered.WriteTo(w)
}

func (h *handler) snapshot() (Snapshot, bool) {
	requestedAt := h.now()
	diagnostics, sample, ok := h.diagnostics(requestedAt)
	if !ok {
		return Snapshot{}, false
	}
	liveAt := h.now()
	uptime := liveAt.Sub(h.config.StartedAt)
	if uptime < 0 {
		uptime = 0
	}
	ready := h.ready()
	state := "Not ready"
	if ready {
		state = "Ready"
	}
	return Snapshot{
		Build: h.config.Build,
		Process: ProcessSnapshot{
			StartedAt: h.config.StartedAt.UTC().Format(time.RFC3339),
			Uptime:    uptime.Round(time.Second).String(),
		},
		Configuration: ConfigurationSnapshot{
			Listeners: h.config.Listeners,
			Drain:     h.config.Drain,
			Flags:     append([]Flag(nil), h.config.Flags...),
		},
		DiagnosticSample:      sample,
		Workers:               diagnostics.workers,
		Readiness:             ReadinessSnapshot{Ready: ready, State: state},
		PostgreSQLPools:       diagnostics.pools,
		RecentControlFailures: diagnostics.failures,
	}, true
}

func (h *handler) diagnostics(now time.Time) (cachedDiagnostics, DiagnosticSampleSnapshot, bool) {
	h.cacheMu.Lock()
	cached := h.cached
	refreshing := h.refreshing
	if cached != nil {
		age := diagnosticSampleAge(now, cached.sampledAt)
		if age < diagnosticCacheInterval || refreshing {
			h.cacheMu.Unlock()
			return *cached, diagnosticSample(cached.sampledAt, age, refreshing), true
		}
	}
	if refreshing {
		h.cacheMu.Unlock()
		return cachedDiagnostics{}, DiagnosticSampleSnapshot{}, false
	}
	h.refreshing = true
	h.cacheMu.Unlock()

	collected := cachedDiagnostics{
		workers:  collectWorkers(h.workers),
		pools:    collectPools(h.pools),
		failures: collectRPCFailures(h.failures),
	}
	collected.sampledAt = h.now()

	h.cacheMu.Lock()
	h.cached = &collected
	h.refreshing = false
	h.cacheMu.Unlock()
	return collected, diagnosticSample(collected.sampledAt, 0, false), true
}

func diagnosticSampleAge(now, sampledAt time.Time) time.Duration {
	age := now.Sub(sampledAt)
	if age < 0 {
		return 0
	}
	return age
}

func diagnosticSample(sampledAt time.Time, age time.Duration, refreshing bool) DiagnosticSampleSnapshot {
	return DiagnosticSampleSnapshot{
		SampledAt:         sampledAt.UTC().Format(time.RFC3339Nano),
		Age:               age.Truncate(time.Millisecond).String(),
		RefreshInProgress: refreshing,
	}
}

func collectPools(read PoolReader) PostgreSQLPoolsSnapshot {
	snapshot := PostgreSQLPoolsSnapshot{
		Coverage: poolCoverage,
		Pools:    []PoolSnapshot{},
	}
	if read == nil {
		return snapshot
	}
	operational, watch := read()
	snapshot.Available = true
	snapshot.Pools = []PoolSnapshot{
		poolSnapshot("Operational", operational),
		poolSnapshot("Watch", watch),
	}
	return snapshot
}

func poolSnapshot(role string, counts PoolCounts) PoolSnapshot {
	return PoolSnapshot{
		Role:          role,
		AcquiredConns: counts.AcquiredConns,
		IdleConns:     counts.IdleConns,
		MaxConns:      counts.MaxConns,
	}
}

func collectRPCFailures(list RPCFailureList) RecentControlFailuresSnapshot {
	failures := []RPCFailure{}
	if list != nil {
		failures = append(failures, list()...)
	}
	return RecentControlFailuresSnapshot{
		Coverage:  rpcCoverage,
		Retention: rpcRetention,
		Empty:     len(failures) == 0,
		Failures:  failures,
	}
}

func collectWorkers(list WorkerList) WorkersSnapshot {
	workers, err := list()
	if err != nil {
		return WorkersSnapshot{Groups: []WorkerGroup{}}
	}

	counts := make(map[string]int)
	groupsByKey := make(map[string]WorkerGroup)
	for _, worker := range workers {
		if worker == nil {
			continue
		}
		key := worker.GetWorkerNamespace() + "\x00" + worker.GetWorkerPool()
		counts[key]++
		groupsByKey[key] = WorkerGroup{Namespace: worker.GetWorkerNamespace(), Pool: worker.GetWorkerPool()}
	}
	groups := make([]WorkerGroup, 0, len(counts))
	for key, count := range counts {
		group := groupsByKey[key]
		group.Count = count
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].Namespace != groups[j].Namespace {
			return groups[i].Namespace < groups[j].Namespace
		}
		return groups[i].Pool < groups[j].Pool
	})

	totalWorkers := len(workers)
	totalGroups := len(groups)
	truncated := totalGroups > maxWorkerGroups
	if truncated {
		boundedGroups := make([]WorkerGroup, maxWorkerGroups)
		copy(boundedGroups, groups)
		groups = boundedGroups
	}
	return WorkersSnapshot{
		Available:    true,
		Empty:        totalWorkers == 0,
		TotalWorkers: &totalWorkers,
		TotalGroups:  &totalGroups,
		Truncated:    truncated,
		Groups:       groups,
	}
}

//go:embed dashboard.html
var dashboardHTML string

var dashboardTemplate = template.Must(template.New("ateapi-status").Parse(dashboardHTML))
