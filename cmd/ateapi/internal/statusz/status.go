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
	"strings"
	"time"
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

// Snapshot is the common response model rendered as JSON or HTML.
type Snapshot struct {
	Build         Build                 `json:"build"`
	Process       ProcessSnapshot       `json:"process"`
	Configuration ConfigurationSnapshot `json:"configuration"`
	Readiness     ReadinessSnapshot     `json:"readiness"`
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

// ReadinessSnapshot mirrors the existing process readiness predicate.
type ReadinessSnapshot struct {
	Ready bool   `json:"ready"`
	State string `json:"state"`
}

type handler struct {
	config Config
	ready  func() bool
	now    func() time.Time
}

// NewHandler constructs the status handler from immutable startup config and
// the existing thread-safe readiness reader.
func NewHandler(config Config, ready func() bool, now func() time.Time) http.Handler {
	config.Flags = append([]Flag(nil), config.Flags...)
	return &handler{config: config, ready: ready, now: now}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	snapshot := h.snapshot()
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

func (h *handler) snapshot() Snapshot {
	now := h.now()
	uptime := now.Sub(h.config.StartedAt)
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
		Readiness: ReadinessSnapshot{Ready: ready, State: state},
	}
}

//go:embed dashboard.html
var dashboardHTML string

var dashboardTemplate = template.Must(template.New("ateapi-status").Parse(dashboardHTML))
