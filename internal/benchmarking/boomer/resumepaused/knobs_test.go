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

package resumepaused

import (
	"testing"

	"go.ate.dev/substrate/internal/benchmarking/boomer/dynconfig"
)

// The codec takes target_rps and burst_size, reads absent ones as unpaced and
// smooth, and refuses negative values or a positive burst_size without a
// positive target_rps in favor of the config before it.
func TestKnobsCodec(t *testing.T) {
	h := dynconfig.NewHolder(knobsCodec)
	if got := dynconfig.Get[knobs](h); got != (knobs{}) {
		t.Fatalf("fresh holder = %+v, want unpaced", got)
	}
	if _, _, err := h.Apply([]byte(`{"target_rps": 2500.5, "burst_size": 50, "someone_elses_key": 1}`)); err != nil {
		t.Fatalf("Apply(valid): %v", err)
	}
	want := knobs{TargetRPS: 2500.5, BurstSize: 50}
	if got := dynconfig.Get[knobs](h); got != want {
		t.Errorf("decoded = %+v, want %+v", got, want)
	}
	for name, bad := range map[string]string{
		"negative target_rps":      `{"target_rps": -1}`,
		"negative burst_size":      `{"target_rps": 10, "burst_size": -1}`,
		"burst_size without rate":  `{"burst_size": 10}`,
		"burst_size with zero rps": `{"target_rps": 0, "burst_size": 10}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := h.Apply([]byte(bad)); err == nil {
				t.Errorf("Apply(%s) took the payload, want a refusal", bad)
			}
			if got := dynconfig.Get[knobs](h); got != want {
				t.Errorf("a refused payload changed the config to %+v", got)
			}
		})
	}
	if _, _, err := h.Apply([]byte(`{"target_rps": null, "burst_size": null}`)); err != nil {
		t.Fatalf("Apply(null): %v", err)
	}
	if got := dynconfig.Get[knobs](h); got != (knobs{}) {
		t.Errorf("nulls left %+v, want unpaced", got)
	}
}
