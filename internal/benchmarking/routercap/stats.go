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

// Package routercap measures how much offered load one atenet-router pod
// absorbs, as a time series rather than a single number. The four headline
// series — offered load, client latency, router CPU, router memory — share
// the same wall-clock window; see window.go for how that is arranged.
package routercap

import (
	"math"
	"sort"
	"time"
)

// LatencyStats is an exact summary of a set of latencies — exact because the
// tail must distinguish 200ms from 2s, which a histogram that wide cannot.
// p95 is the tail metric; over a few-thousand-sample window p99 and max are
// mostly sampling noise.
type LatencyStats struct {
	Count  int     `json:"count"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	MeanMs float64 `json:"mean_ms"`
}

// summarize computes exact percentiles over ds. It sorts in place, so callers
// that still need the original order must pass a copy.
func summarize(ds []time.Duration) LatencyStats {
	s := LatencyStats{Count: len(ds)}
	if len(ds) == 0 {
		return s
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	var total float64
	for _, d := range ds {
		total += msOf(d)
	}
	s.MeanMs = total / float64(len(ds))
	s.P50Ms = quantileSorted(ds, 0.50)
	s.P95Ms = quantileSorted(ds, 0.95)
	return s
}

// quantileSorted returns the q-quantile of an already-sorted slice using the
// nearest-rank method: the smallest value at or above the q fraction. Every
// reported value is therefore a latency some request actually experienced.
func quantileSorted(sorted []time.Duration, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(q * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return msOf(sorted[rank-1])
}

func msOf(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
