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

package userclass

import (
	"math"
	"strings"
	"testing"
)

func TestParsePools(t *testing.T) {
	for _, tc := range []struct {
		name    string
		spec    string
		want    []Pool
		wantErr string
	}{
		{name: "empty", spec: ""},
		{name: "blanks only", spec: "   "},
		{
			name: "single",
			spec: "n4:528",
			want: []Pool{{Name: "n4", Weight: 528}},
		},
		{
			name: "several with spaces",
			spec: " n4:528 , n4d:1056 ",
			want: []Pool{{Name: "n4", Weight: 528}, {Name: "n4d", Weight: 1056}},
		},
		{
			name: "trailing comma is ignored",
			spec: "n4:528,",
			want: []Pool{{Name: "n4", Weight: 528}},
		},
		{
			name:    "missing weight",
			spec:    "n4",
			wantErr: "want name:weight",
		},
		{
			name:    "weight is not a number",
			spec:    "n4:many",
			wantErr: "not an integer",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParsePools(tc.spec)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ParsePools(%q) error = %v, want one containing %q", tc.spec, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParsePools(%q) = %v", tc.spec, err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("ParsePools(%q) = %+v, want %+v", tc.spec, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("ParsePools(%q)[%d] = %+v, want %+v", tc.spec, i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestNewPoolPickerRejects(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pools   []Pool
		wantErr string
	}{
		{
			name:    "empty name",
			pools:   []Pool{{Name: "", Weight: 1}},
			wantErr: "must not be empty",
		},
		{
			name:    "name is not a label value",
			pools:   []Pool{{Name: "not a label", Weight: 1}},
			wantErr: "not a valid Kubernetes label value",
		},
		{
			name:    "name too long",
			pools:   []Pool{{Name: strings.Repeat("a", maxLabelValueLen+1), Weight: 1}},
			wantErr: "label value limit",
		},
		{
			name:    "duplicate name",
			pools:   []Pool{{Name: "n4", Weight: 1}, {Name: "n4", Weight: 2}},
			wantErr: "duplicate name",
		},
		{
			name:    "zero weight",
			pools:   []Pool{{Name: "n4", Weight: 0}},
			wantErr: "weight must be positive",
		},
		{
			name:    "negative weight",
			pools:   []Pool{{Name: "n4", Weight: -1}},
			wantErr: "weight must be positive",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewPoolPicker(tc.pools); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("NewPoolPicker(%+v) error = %v, want one containing %q", tc.pools, err, tc.wantErr)
			}
		})
	}
}

// A nil picker is the single-pool default and every method must tolerate it:
// the user classes call through unconditionally rather than branching.
func TestNilPickerPicksNothing(t *testing.T) {
	var p *PoolPicker
	if got := p.Pick(); got != "" {
		t.Errorf("nil.Pick() = %q, want empty", got)
	}
	if got := p.SelectorFor("n4"); got != nil {
		t.Errorf("nil.SelectorFor() = %v, want nil", got)
	}
	if got := p.Names(); got != nil {
		t.Errorf("nil.Names() = %v, want nil", got)
	}
}

func TestNewPoolPickerNoPoolsIsNil(t *testing.T) {
	p, err := NewPoolPicker(nil)
	if err != nil {
		t.Fatalf("NewPoolPicker(nil) = %v", err)
	}
	if p != nil {
		t.Fatalf("NewPoolPicker(nil) = %+v, want nil picker", p)
	}
}

func TestSelectorFor(t *testing.T) {
	p, err := NewPoolPicker([]Pool{{Name: "n4", Weight: 1}})
	if err != nil {
		t.Fatalf("NewPoolPicker() = %v", err)
	}
	sel := p.SelectorFor("n4")
	if sel == nil {
		t.Fatal("SelectorFor(\"n4\") = nil, want a selector")
	}
	if got := sel.GetMatchLabels()[PoolLabelKey]; got != "n4" {
		t.Errorf("selector[%q] = %q, want %q", PoolLabelKey, got, "n4")
	}
	// An actor that drew no pool must not be constrained, even when the
	// worker is running with pools configured.
	if got := p.SelectorFor(""); got != nil {
		t.Errorf("SelectorFor(\"\") = %v, want nil", got)
	}
}

// Actors must land in proportion to the weights, which is what makes the
// weights usable as vCPU counts: a pool with twice the capacity has to take
// twice the actors, or the smaller pool saturates first and caps the run.
func TestPickIsWeighted(t *testing.T) {
	p, err := NewPoolPicker([]Pool{
		{Name: "small", Weight: 1},
		{Name: "medium", Weight: 3},
		{Name: "large", Weight: 6},
	})
	if err != nil {
		t.Fatalf("NewPoolPicker() = %v", err)
	}

	const draws = 200_000
	counts := map[string]int{}
	for range draws {
		counts[p.Pick()]++
	}

	// 3 percentage points is far outside the sampling noise of 200k draws
	// (sigma is under 0.12pp for every share here) and far inside any real
	// weighting bug, which would be off by tens of points.
	const tolerance = 0.03
	for name, wantShare := range map[string]float64{"small": 0.1, "medium": 0.3, "large": 0.6} {
		gotShare := float64(counts[name]) / draws
		if math.Abs(gotShare-wantShare) > tolerance {
			t.Errorf("pool %q got %.4f of %d draws, want %.2f (+/- %.2f)", name, gotShare, draws, wantShare, tolerance)
		}
	}
	if len(counts) != 3 {
		t.Errorf("drew %d distinct pools, want 3: %v", len(counts), counts)
	}
}

// Every configured pool has to be reachable. A cumulative-weight off-by-one
// would strand the first or last pool while the shares still look plausible.
func TestPickReachesEveryPool(t *testing.T) {
	p, err := NewPoolPicker([]Pool{
		{Name: "first", Weight: 1},
		{Name: "middle", Weight: 1},
		{Name: "last", Weight: 1},
	})
	if err != nil {
		t.Fatalf("NewPoolPicker() = %v", err)
	}
	seen := map[string]bool{}
	for range 1000 {
		seen[p.Pick()] = true
	}
	for _, want := range []string{"first", "middle", "last"} {
		if !seen[want] {
			t.Errorf("pool %q never drawn in 1000 picks", want)
		}
	}
	if seen[""] {
		t.Error("Pick() returned the empty pool name")
	}
}

func TestNamesIsACopy(t *testing.T) {
	p, err := NewPoolPicker([]Pool{{Name: "n4", Weight: 1}})
	if err != nil {
		t.Fatalf("NewPoolPicker() = %v", err)
	}
	names := p.Names()
	names[0] = "mutated"
	if got := p.Names()[0]; got != "n4" {
		t.Errorf("Names() exposed internal state: got %q after caller mutation, want %q", got, "n4")
	}
}
