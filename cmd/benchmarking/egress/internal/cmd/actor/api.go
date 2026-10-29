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

// Package actor is the workload of the egress scale test: the subcommand
// that holds one tunnel per endpoint open through the egress gateway and
// counts what happened. Its contract, what the programs of the test agree
// on, is in api.go, endpoints.go, stats.go and hist.go: the endpoint names
// the loop calls, the control routes and the /start body a driver sends it,
// and the Stats it answers with, down to the bounds of its distributions.
package actor

import "fmt"

// Control routes the actor serves on port 80, the only inbound port of a
// sandbox.
const (
	ReadyzRoute = "/readyz"
	StartRoute  = "/start"
	// StatsRoute, GET, serves the running loop's Stats without its
	// distributions: the counters, cheap enough to poll, and nothing is
	// consumed by a read. ?endpoints=1 adds the per-endpoint counts.
	StatsRoute = "/stats"
	// CollectRoute, POST, cuts a window: it serves the Stats with the
	// distributions recorded since the previous cut, or since /start for the
	// first. Each sample is served once, so the route has one consumer: the
	// driver, which cuts the run into windows with it. The driver numbers the
	// cuts with WindowParam so a cut it did not hear can be asked for again.
	// ?endpoints=1 as on /stats.
	CollectRoute = "/collect"
	// StopRoute, POST, ends the loop and serves its final Stats: the counters
	// and the distributions since the previous cut. The actor keeps that
	// final Stats once the loop is gone, so a repeated /stop answers it again
	// with 200 until the next /start, while /stats and /collect answer 404.
	// ?endpoints=1 as on /stats.
	StopRoute = "/stop"
)

// WindowParam is the query parameter of POST /collect: the id of the window
// the driver asks for, a positive integer. The first cut is 1 and each cut
// is the previous id plus one. The actor keeps the last cut's Stats, so the
// same id asked again is answered from it, byte for byte, and consumes
// nothing. A missing or malformed value is 400; an id that is neither the
// last cut nor the next is 409; either body names the next id.
const WindowParam = "window"

// NoLoopMessage is the body of the 404 that /stats, /collect and /stop answer
// while no loop runs.
const NoLoopMessage = "no loop is running"

// ProtocolHeader is on every answer the actor gives, ProtocolVersion its
// value, so a driver can tell what it is talking to before it starts
// anything: an actor whose image speaks another contract is refused rather
// than misread. The images that pushed their measurements over OTLP sent 1
// and served no /collect; a driver must require 2.
const (
	ProtocolHeader  = "Benchmark-Egress-Protocol"
	ProtocolVersion = "2"
)

// DefaultRequestTimeoutMs applies when StartRequest.RequestTimeoutMs is zero.
const DefaultRequestTimeoutMs = 5000

// The schemes the loop can request endpoints over.
const (
	SchemeHTTP  = "http"
	SchemeHTTPS = "https"
)

// StartRequest is the JSON body of POST /start.
type StartRequest struct {
	// Endpoints is C: the loop requests EndpointURL(0) through
	// EndpointURL(Endpoints-1), each on its own goroutine, until it is stopped.
	Endpoints int `json:"endpoints"`
	// RequestTimeoutMs bounds one request; zero means DefaultRequestTimeoutMs.
	RequestTimeoutMs int64 `json:"requestTimeoutMs"`
	// IntervalMs paces the actor: endpoint i's first request starts OffsetMs
	// plus i intervals after the loop, and each endpoint is then requested once
	// per period of C intervals, so the actor sends one request per interval
	// throughout. Zero sends every endpoint's requests back to back.
	IntervalMs int64 `json:"intervalMs"`
	// OffsetMs delays the whole schedule: endpoint i's first request starts
	// OffsetMs plus i intervals after the loop. A driver gives each actor its
	// own value so a fleet started at once does not send in lockstep.
	OffsetMs int64 `json:"offsetMs,omitempty"`
	// Scheme is SchemeHTTP or SchemeHTTPS; empty means SchemeHTTP.
	Scheme string `json:"scheme,omitempty"`
}

// URLScheme is the scheme the loop requests endpoints over.
func (r *StartRequest) URLScheme() string {
	if r.Scheme == "" {
		return SchemeHTTP
	}
	return r.Scheme
}

// Validate reports whether r can start a loop.
func (r *StartRequest) Validate() error {
	if r.Endpoints < 1 || r.Endpoints > MaxEndpoints {
		return fmt.Errorf("endpoints must be between 1 and %d, got %d", MaxEndpoints, r.Endpoints)
	}
	if r.RequestTimeoutMs < 0 {
		return fmt.Errorf("requestTimeoutMs cannot be negative: %d", r.RequestTimeoutMs)
	}
	if r.IntervalMs < 0 {
		return fmt.Errorf("intervalMs cannot be negative: %d", r.IntervalMs)
	}
	if r.OffsetMs < 0 {
		return fmt.Errorf("offsetMs cannot be negative: %d", r.OffsetMs)
	}
	if period := int64(r.Endpoints) * r.IntervalMs; r.OffsetMs > period {
		return fmt.Errorf("offsetMs cannot exceed one period, endpoints x intervalMs = %d: %d", period, r.OffsetMs)
	}
	if r.Scheme != "" && r.Scheme != SchemeHTTP && r.Scheme != SchemeHTTPS {
		return fmt.Errorf("scheme must be %s or %s, got %q", SchemeHTTP, SchemeHTTPS, r.Scheme)
	}
	return nil
}
