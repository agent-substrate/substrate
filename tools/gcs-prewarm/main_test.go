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

package main

import (
	"math"
	"testing"
	"time"
)

func TestRateAt(t *testing.T) {
	const doubleEvery = 5 * time.Minute
	for _, tc := range []struct {
		name    string
		elapsed time.Duration
		start   float64
		target  float64
		want    float64
	}{
		{name: "starts at start rate", elapsed: 0, start: 50, target: 800, want: 50},
		{name: "doubles after one period", elapsed: doubleEvery, start: 50, target: 800, want: 100},
		{name: "quadruples after two periods", elapsed: 2 * doubleEvery, start: 50, target: 800, want: 200},
		{name: "caps at target", elapsed: 10 * doubleEvery, start: 50, target: 800, want: 800},
		{name: "start above target holds target", elapsed: 0, start: 900, target: 800, want: 800},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := rateAt(tc.elapsed, tc.start, tc.target, doubleEvery)
			if math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("rateAt(%s, %.0f, %.0f) = %f, want %f", tc.elapsed, tc.start, tc.target, got, tc.want)
			}
		})
	}
}

func TestRampDuration(t *testing.T) {
	const doubleEvery = 5 * time.Minute
	// 50 -> 800 is four doublings.
	if got, want := rampDuration(50, 800, doubleEvery), 4*doubleEvery; got != want {
		t.Fatalf("rampDuration(50, 800) = %s, want %s", got, want)
	}
	if got := rampDuration(800, 50, doubleEvery); got != 0 {
		t.Fatalf("rampDuration(800, 50) = %s, want 0", got)
	}
	// The rate must actually be at target when the ramp ends.
	d := rampDuration(50, 800, doubleEvery)
	if got := rateAt(d, 50, 800, doubleEvery); math.Abs(got-800) > 1e-6 {
		t.Fatalf("rateAt(rampDuration) = %f, want 800", got)
	}
}
