// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package autoscaler

import (
	"testing"
	"time"
)

func p(v int32) *int32 { return &v }

var base = time.Unix(1_700_000_000, 0)

func TestBoundsEnabled(t *testing.T) {
	for _, tc := range []struct {
		name string
		b    Bounds
		want bool
	}{
		{"none", Bounds{}, false},
		{"maxOnly", Bounds{MaxReplicas: p(5)}, false},
		{"minReady", Bounds{MinReady: p(1)}, true},
		{"targetBuffer", Bounds{TargetBuffer: p(2)}, true},
	} {
		if got := tc.b.Enabled(); got != tc.want {
			t.Errorf("%s: Enabled()=%v want %v", tc.name, got, tc.want)
		}
	}
}

func TestStep(t *testing.T) {
	const stab = 30 * time.Second
	for _, tc := range []struct {
		name        string
		b           Bounds
		o           Observation
		c           Config
		now         time.Time
		downSince   time.Time
		wantTarget  int32
		wantChanged bool
		wantDown    time.Time // zero unless a timer should be carried
	}{
		{
			name:       "buffer deficit scales up immediately",
			b:          Bounds{TargetBuffer: p(3)},
			o:          Observation{Current: 5, Free: 0},
			now:        base,
			wantTarget: 8, wantChanged: true,
		},
		{
			name:       "inflight counts toward buffer (anti-windup)",
			b:          Bounds{TargetBuffer: p(3)},
			o:          Observation{Current: 5, Free: 0, InFlight: 3},
			now:        base,
			wantTarget: 5, wantChanged: false,
		},
		{
			name:       "partial deficit",
			b:          Bounds{TargetBuffer: p(3)},
			o:          Observation{Current: 5, Free: 1},
			now:        base,
			wantTarget: 7, wantChanged: true,
		},
		{
			name:       "scale up capped by MaxScaleUpStep",
			b:          Bounds{TargetBuffer: p(10)},
			o:          Observation{Current: 5},
			c:          Config{MaxScaleUpStep: 2},
			now:        base,
			wantTarget: 7, wantChanged: true,
		},
		{
			name:       "reservation floor reached in one step despite small cap",
			b:          Bounds{MinReady: p(10)},
			o:          Observation{Current: 0},
			c:          Config{MaxScaleUpStep: 2},
			now:        base,
			wantTarget: 10, wantChanged: true,
		},
		{
			name:       "maxReplicas caps scale up",
			b:          Bounds{TargetBuffer: p(100), MaxReplicas: p(8)},
			o:          Observation{Current: 5},
			now:        base,
			wantTarget: 8, wantChanged: true,
		},
		{
			name:       "surplus buffer: shrink held, timer starts",
			b:          Bounds{TargetBuffer: p(2)},
			o:          Observation{Current: 10, Free: 8},
			c:          Config{ScaleDownStabilization: stab},
			now:        base,
			wantTarget: 10, wantChanged: false, wantDown: base,
		},
		{
			name:       "shrink applied once stabilization elapsed",
			b:          Bounds{TargetBuffer: p(2)},
			o:          Observation{Current: 10, Free: 8},
			c:          Config{ScaleDownStabilization: stab},
			now:        base.Add(stab),
			downSince:  base,
			wantTarget: 4, wantChanged: true,
		},
		{
			name:       "shrink still pending within window",
			b:          Bounds{TargetBuffer: p(2)},
			o:          Observation{Current: 10, Free: 8},
			c:          Config{ScaleDownStabilization: stab},
			now:        base.Add(10 * time.Second),
			downSince:  base,
			wantTarget: 10, wantChanged: false, wantDown: base,
		},
		{
			name:       "no targetBuffer: minReady raises under-floored pool",
			b:          Bounds{MinReady: p(3)},
			o:          Observation{Current: 1},
			now:        base,
			wantTarget: 3, wantChanged: true,
		},
		{
			name:       "no targetBuffer: maxReplicas trims oversized pool after window",
			b:          Bounds{MaxReplicas: p(5)},
			o:          Observation{Current: 8},
			c:          Config{ScaleDownStabilization: stab},
			now:        base.Add(stab),
			downSince:  base,
			wantTarget: 5, wantChanged: true,
		},
		{
			name:       "no autoscaling fields: steady no-op",
			b:          Bounds{},
			o:          Observation{Current: 4},
			now:        base,
			wantTarget: 4, wantChanged: false,
		},
		{
			name:       "reaching steady clears a pending shrink timer",
			b:          Bounds{TargetBuffer: p(2)},
			o:          Observation{Current: 4, Free: 2},
			c:          Config{ScaleDownStabilization: stab},
			now:        base.Add(5 * time.Second),
			downSince:  base,                  // a shrink was pending...
			wantTarget: 4, wantChanged: false, // ...but ideal now equals current, so timer resets
		},
		{
			name:       "minReady=0 allows shrink to zero after window",
			b:          Bounds{MinReady: p(0), TargetBuffer: p(0)},
			o:          Observation{Current: 3, Free: 3},
			c:          Config{ScaleDownStabilization: stab},
			now:        base.Add(stab),
			downSince:  base,
			wantTarget: 0, wantChanged: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, gotDown := Step(tc.b, tc.o, tc.c, tc.now, tc.downSince)
			if got.Target != tc.wantTarget || got.Changed != tc.wantChanged {
				t.Errorf("Step => {Target:%d Changed:%v Reason:%q}, want {Target:%d Changed:%v}",
					got.Target, got.Changed, got.Reason, tc.wantTarget, tc.wantChanged)
			}
			if !gotDown.Equal(tc.wantDown) {
				t.Errorf("downWantedSince => %v, want %v", gotDown, tc.wantDown)
			}
		})
	}
}

// TestStepScaleDownLifecycle walks the hysteresis state machine across ticks.
func TestStepScaleDownLifecycle(t *testing.T) {
	b := Bounds{TargetBuffer: p(2)}
	c := Config{ScaleDownStabilization: 30 * time.Second}
	surplus := Observation{Current: 10, Free: 8} // ideal 4, wants shrink

	// Tick 1: shrink wanted, timer starts, held.
	d, down := Step(b, surplus, c, base, time.Time{})
	if d.Changed || !down.Equal(base) {
		t.Fatalf("tick1: got changed=%v down=%v, want held with timer=%v", d.Changed, down, base)
	}
	// Tick 2: still within window, still held, timer carried.
	d, down = Step(b, surplus, c, base.Add(10*time.Second), down)
	if d.Changed || !down.Equal(base) {
		t.Fatalf("tick2: got changed=%v down=%v, want held", d.Changed, down)
	}
	// Tick 3: window elapsed, shrink applied, timer cleared.
	d, down = Step(b, surplus, c, base.Add(31*time.Second), down)
	if !d.Changed || d.Target != 4 || !down.IsZero() {
		t.Fatalf("tick3: got {changed:%v target:%d} down=%v, want shrink to 4 + cleared timer", d.Changed, d.Target, down)
	}
}

// TestStepScaleDownTimerResetsOnDemand verifies a returning burst cancels a
// pending shrink so a brief lull never throws away warm capacity.
func TestStepScaleDownTimerResetsOnDemand(t *testing.T) {
	b := Bounds{TargetBuffer: p(2)}
	c := Config{ScaleDownStabilization: 30 * time.Second}

	// Shrink wanted: timer starts.
	_, down := Step(b, Observation{Current: 10, Free: 8}, c, base, time.Time{})
	if !down.Equal(base) {
		t.Fatalf("expected timer to start at %v, got %v", base, down)
	}
	// Demand returns (free drops below buffer): scale up now, timer cleared.
	d, down := Step(b, Observation{Current: 10, Free: 0}, c, base.Add(5*time.Second), down)
	if !d.Changed || d.Target != 12 || !down.IsZero() {
		t.Fatalf("burst: got {changed:%v target:%d} down=%v, want scale up to 12 + cleared timer", d.Changed, d.Target, down)
	}
}
