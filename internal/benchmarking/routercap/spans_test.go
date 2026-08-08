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

// Tests for the per-hop breakdown of the average request.

package routercap

import (
	"math"
	"testing"
)

// spanInputs builds the three sources of one window's breakdown: a client mean
// of 100ms, 80ms of it inside Envoy, a 16ms worker hop and a 1.25ms sidecar hop
// of which 1.2ms is the resume. That leaves 20ms before Envoy and 62.75ms
// inside it — roughly the shape the 8-core arm actually produced.
func spanInputs() (GenStats, *EnvoyDelta, *RouterDelta) {
	load := GenStats{Latency: LatencyStats{Count: 10000, MeanMs: 100}}
	envoy := &EnvoyDelta{
		MeanInEnvoyMs:  80,
		InEnvoySamples: 10000,
		Clusters: map[string]ClusterDelta{
			ActorClusterName: {MeanRqTimeMs: 16, RqTimeSamples: 10000},
			// The ext_proc cluster reports no rq_time, as on the live proxy.
			ExtProcClusterName: {Requests: 10000},
		},
	}
	router := &RouterDelta{
		Measured: true, RouteMeasured: true,
		MeanRouteMs: 1.25, RouteCalls: 10000, MeanResumeMs: 1.2,
	}
	return load, envoy, router
}

func TestLatencySpansSumToTheWhole(t *testing.T) {
	s := latencySpans(spanInputs())
	if s == nil {
		t.Fatal("latencySpans returned nil on a fully measured window")
	}
	// The defining property: four non-overlapping spans covering the whole
	// request. If this drifts, the stacked chart is drawing a fiction.
	sum := s.BeforeEnvoyMs + s.EnvoyInternalMs + s.SidecarMs + s.WorkerMs
	if math.Abs(sum-s.TotalClientMs) > 1e-9 {
		t.Errorf("spans sum to %v ms, want the client mean %v ms", sum, s.TotalClientMs)
	}
	if s.BeforeEnvoyMs != 20 {
		t.Errorf("BeforeEnvoyMs = %v, want 20", s.BeforeEnvoyMs)
	}
	if s.WorkerMs != 16 || s.SidecarMs != 1.25 {
		t.Errorf("worker/sidecar = %v/%v, want 16/1.25", s.WorkerMs, s.SidecarMs)
	}
	if want := 62.75; math.Abs(s.EnvoyInternalMs-want) > 1e-9 {
		t.Errorf("EnvoyInternalMs = %v, want %v", s.EnvoyInternalMs, want)
	}
}

// TestLatencySpansKeepsResumeOutOfTheStack pins the nesting. The resume happens
// inside the sidecar's handler, so counting it as a fifth span would attribute
// its time twice and inflate the total past what any client saw.
func TestLatencySpansKeepsResumeOutOfTheStack(t *testing.T) {
	s := latencySpans(spanInputs())
	if s.ResumeMs != 1.2 {
		t.Errorf("ResumeMs = %v, want 1.2", s.ResumeMs)
	}
	sum := s.BeforeEnvoyMs + s.EnvoyInternalMs + s.SidecarMs + s.WorkerMs + s.ResumeMs
	if math.Abs(sum-s.TotalClientMs) < 1e-9 {
		t.Error("the four spans plus the resume still sum to the total, so the resume is being double-counted")
	}
}

// TestLatencySpansCarriesAResumeLargerThanTheHandler pins a shape that looks
// broken and is not: the route and parking histograms cover different
// populations, so under shedding ResumeMs can exceed SidecarMs. The record
// must carry it as measured.
func TestLatencySpansCarriesAResumeLargerThanTheHandler(t *testing.T) {
	load, envoy, router := spanInputs()
	// The shape of one observed window: 27397 routes against 16326 resumes,
	// mean route 521ms, mean resume 874ms.
	router.RouteCalls, router.MeanRouteMs = 27397, 520.67
	router.MeanResumeMs = 873.69

	s := latencySpans(load, envoy, router)
	if s.ResumeMs <= s.SidecarMs {
		t.Fatalf("ResumeMs %v, SidecarMs %v: the fixture no longer covers the case", s.ResumeMs, s.SidecarMs)
	}
	if s.SidecarMs != 520.67 {
		t.Errorf("SidecarMs = %v, want the route mean 520.67 unaltered", s.SidecarMs)
	}
	// The four spans still partition the request. The resume is outside them, so
	// its size relative to the sidecar cannot break the sum.
	sum := s.BeforeEnvoyMs + s.EnvoyInternalMs + s.SidecarMs + s.WorkerMs
	if math.Abs(sum-s.TotalClientMs) > 1e-9 {
		t.Errorf("spans sum to %v ms, want the client mean %v ms", sum, s.TotalClientMs)
	}
}

// TestLatencySpansWithoutTheSidecarInstrument covers an older router image
// with no route-duration series. The sidecar's time folds into
// EnvoyInternalMs rather than being silently deducted as zero.
func TestLatencySpansWithoutTheSidecarInstrument(t *testing.T) {
	load, envoy, _ := spanInputs()
	s := latencySpans(load, envoy, &RouterDelta{Measured: true})
	if s.SidecarMeasured {
		t.Error("SidecarMeasured = true with no route-duration series")
	}
	if s.SidecarMs != 0 {
		t.Errorf("SidecarMs = %v, want 0 when unmeasured", s.SidecarMs)
	}
	if want := 64.0; s.EnvoyInternalMs != want {
		t.Errorf("EnvoyInternalMs = %v, want %v (the sidecar's 1.25ms stays fused in)", s.EnvoyInternalMs, want)
	}
	sum := s.BeforeEnvoyMs + s.EnvoyInternalMs + s.SidecarMs + s.WorkerMs
	if math.Abs(sum-s.TotalClientMs) > 1e-9 {
		t.Errorf("spans sum to %v ms, want %v", sum, s.TotalClientMs)
	}
}

func TestLatencySpansIsNilWithoutEnvoyRequestTimes(t *testing.T) {
	load, _, router := spanInputs()
	if s := latencySpans(load, nil, router); s != nil {
		t.Error("latencySpans returned a breakdown with no Envoy delta")
	}
	// An idle window: Envoy answered nothing, so there is no mean to divide.
	// Nil, not a record full of zeros that would plot as a real observation.
	if s := latencySpans(load, &EnvoyDelta{}, router); s != nil {
		t.Error("latencySpans returned a breakdown from zero request-time samples")
	}
}

// TestLatencySpansReportsDisagreeingDenominators covers the case that makes the
// breakdown untrustworthy: the four instruments describing different sets of
// requests. It is reported, not corrected — the arithmetic is still the best
// available, and the reader needs to know how far to trust it.
func TestLatencySpansReportsDisagreeingDenominators(t *testing.T) {
	load, envoy, router := spanInputs()
	router.RouteCalls = 5000 // the sidecar saw half as many requests

	s := latencySpans(load, envoy, router)
	if want := 1.0; math.Abs(s.CountSpread-want) > 1e-9 {
		t.Errorf("CountSpread = %v, want %v (10000 against 5000)", s.CountSpread, want)
	}

	// Agreement reads as near zero, so a threshold can be applied to either.
	router.RouteCalls = 9900
	if s := latencySpans(load, envoy, router); s.CountSpread > 0.02 {
		t.Errorf("CountSpread = %v on denominators agreeing to 1%%", s.CountSpread)
	}
}

// TestLatencySpansCarriesTheResolutionFloor pins the readability number:
// Envoy stores request times as whole milliseconds. At a 2.5ms mean the
// truncation is worth 40% of the request; at 100ms, 1%.
func TestLatencySpansCarriesTheResolutionFloor(t *testing.T) {
	load, envoy, router := spanInputs()
	if s := latencySpans(load, envoy, router); math.Abs(s.ResolutionMsShare-0.01) > 1e-9 {
		t.Errorf("ResolutionMsShare = %v at a 100ms mean, want 0.01", s.ResolutionMsShare)
	}

	load.Latency.MeanMs = 2.5
	if s := latencySpans(load, envoy, router); math.Abs(s.ResolutionMsShare-0.4) > 1e-9 {
		t.Errorf("ResolutionMsShare = %v at a 2.5ms mean, want 0.4", s.ResolutionMsShare)
	}

	// An idle window divides by nothing rather than reporting an infinite floor.
	load.Latency.MeanMs = 0
	if s := latencySpans(load, envoy, router); s.ResolutionMsShare != 0 {
		t.Errorf("ResolutionMsShare = %v with no client mean, want 0", s.ResolutionMsShare)
	}
}

// TestLatencySpansDoesNotClampANegativeResidual keeps a broken window visibly
// broken. If the instruments disagree enough to push a residual below zero,
// silently flooring it at zero would turn a detectable fault into a plausible
// looking chart.
func TestLatencySpansDoesNotClampANegativeResidual(t *testing.T) {
	load, envoy, router := spanInputs()
	envoy.Clusters[ActorClusterName] = ClusterDelta{MeanRqTimeMs: 200, RqTimeSamples: 10000}

	s := latencySpans(load, envoy, router)
	if s.EnvoyInternalMs >= 0 {
		t.Errorf("EnvoyInternalMs = %v, want negative: the worker hop alone exceeded the in-Envoy total",
			s.EnvoyInternalMs)
	}
}
