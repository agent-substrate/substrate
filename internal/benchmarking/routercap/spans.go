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

// Splitting the average request into hops: how much of it is the generator
// getting to Envoy, Envoy's own handling, the sidecar callout, and the worker.

package routercap

// LatencySpans divides the average request across the path
//
//	client -> Envoy -> sidecar (resume the actor) -> worker -> back
//
// into four spans that do not overlap and sum to the whole:
//
//	BeforeEnvoyMs    client mean latency  -  time inside Envoy       (residual)
//	EnvoyInternalMs  time inside Envoy  -  worker hop  -  sidecar hop (residual)
//	SidecarMs        the sidecar's route-duration histogram          (measured)
//	WorkerMs         Envoy's upstream_rq_time on the actor cluster   (measured)
//
// Everything here is a mean, never a percentile: percentiles do not decompose
// across hops, so TotalClientMs is not the p50 quoted elsewhere.
//
// BeforeEnvoyMs is the rig's contribution (generator queueing, dial, handshake),
// timed from scheduled send. EnvoyInternalMs is Envoy's own work plus the
// microsecond-scale loopback wire to the sidecar, which cannot be separated.
//
// Envoy stores request times as whole milliseconds, so both Envoy-derived
// quantities are biased low, which biases the two residuals up;
// ResolutionMsShare bounds that as a fraction of the total. Envoy records no
// rq_time for the ext_proc cluster (every stream ends in a reset), so the
// sidecar's own histogram is the only instrument for that hop. See
// benchmarking/routercap/RESULTS.md.
type LatencySpans struct {
	// Measured is false when Envoy reported no request-time samples for the
	// window, which makes every field below meaningless rather than zero.
	Measured bool `json:"measured"`
	// SidecarMeasured is false when the sidecar exposed no route-duration
	// series. The other three spans still hold; SidecarMs is then zero and its
	// time sits inside EnvoyInternalMs.
	SidecarMeasured bool `json:"sidecar_measured"`

	BeforeEnvoyMs   float64 `json:"before_envoy_ms"`
	EnvoyInternalMs float64 `json:"envoy_internal_ms"`
	SidecarMs       float64 `json:"sidecar_ms"`
	WorkerMs        float64 `json:"worker_ms"`

	// ResumeMs is the control-plane round trip the sidecar makes to wake the
	// actor; it is not part of the stack and must never be added to the four
	// spans. The resume nests inside the route per request, not in the means —
	// the two histograms have different denominators, so ResumeMs can
	// legitimately exceed SidecarMs; do not "correct" it or assert the
	// inequality.
	ResumeMs float64 `json:"resume_ms"`

	// TotalClientMs and InEnvoyMs are the two totals the residuals are taken
	// from, carried so a reader can redo the subtraction.
	TotalClientMs float64 `json:"total_client_ms"`
	InEnvoyMs     float64 `json:"in_envoy_ms"`

	// ResolutionMsShare is Envoy's whole-millisecond floor as a fraction of the
	// mean request: 0.01 means the split can be read as it stands, 0.4 means it
	// is mostly an artifact of the instrument.
	ResolutionMsShare float64 `json:"resolution_ms_share"`

	// The four spans come from four instruments in three processes, each with
	// its own request-count denominator. CountSpread is the largest of the four
	// counts divided by the smallest, minus one: 0.01 means the arithmetic is
	// sound, 0.5 means the spans describe substantially different populations.
	ClientRequests  float64 `json:"client_requests"`
	InEnvoyRequests float64 `json:"in_envoy_requests"`
	SidecarRequests float64 `json:"sidecar_requests"`
	WorkerRequests  float64 `json:"worker_requests"`
	CountSpread     float64 `json:"count_spread"`
}

// latencySpans derives the breakdown for one window, returning nil when Envoy
// gave no request-time samples to divide. Residuals are not clamped: a negative
// residual is a fact about instrument disagreement and belongs in the record.
func latencySpans(load GenStats, e *EnvoyDelta, r *RouterDelta) *LatencySpans {
	if e == nil || e.InEnvoySamples <= 0 {
		return nil
	}
	s := LatencySpans{
		Measured:        true,
		InEnvoyMs:       e.MeanInEnvoyMs,
		InEnvoyRequests: e.InEnvoySamples,
		TotalClientMs:   load.Latency.MeanMs,
		ClientRequests:  float64(load.Latency.Count),
	}
	if actor, ok := e.Clusters[ActorClusterName]; ok {
		s.WorkerMs, s.WorkerRequests = actor.MeanRqTimeMs, actor.RqTimeSamples
	}
	if r != nil && r.RouteMeasured {
		s.SidecarMeasured = true
		s.SidecarMs, s.SidecarRequests = r.MeanRouteMs, r.RouteCalls
		s.ResumeMs = r.MeanResumeMs
	}
	s.EnvoyInternalMs = s.InEnvoyMs - s.WorkerMs - s.SidecarMs
	s.BeforeEnvoyMs = s.TotalClientMs - s.InEnvoyMs
	s.CountSpread = countSpread(s.ClientRequests, s.InEnvoyRequests, s.SidecarRequests, s.WorkerRequests)
	if s.TotalClientMs > 0 {
		s.ResolutionMsShare = envoyResolutionMs / s.TotalClientMs
	}
	return &s
}

// envoyResolutionMs is the granularity Envoy stores a request time at. Not a
// tunable: it is the unit of the counter.
const envoyResolutionMs = 1.0

// countSpread reports how far apart the non-zero denominators are, as a
// fraction of the smallest. Zeros are skipped: an unmeasured span is already
// flagged by its own Measured field.
func countSpread(counts ...float64) float64 {
	lo, hi := 0.0, 0.0
	for _, c := range counts {
		if c <= 0 {
			continue
		}
		if lo == 0 || c < lo {
			lo = c
		}
		if c > hi {
			hi = c
		}
	}
	if lo == 0 {
		return 0
	}
	return hi/lo - 1
}
