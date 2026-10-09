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
package sweperf

import (
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
)

// The codec takes a payload naming every key the class reads and refuses
// the values it cannot run on.
func TestKnobsCodec(t *testing.T) {
	h := dynconfig.NewHolder(knobsCodec)
	if got := dynconfig.Get[knobs](h); got != (knobs{}) {
		t.Fatalf("fresh holder = %+v, want the zero defaults", got)
	}
	if _, _, err := h.Apply([]byte(`{
		"min_wait_time": 0.1, "max_wait_time": 0.5,
		"sweperf_template": "swebench-astropy-7336",
		"sweperf_total_steps": 21,
		"sweperf_num_cycles": 4,
		"sweperf_poll_interval_ms": 100
	}`)); err != nil {
		t.Fatalf("Apply(valid): %v", err)
	}
	want := knobs{
		WaitTime:       dynconfig.WaitTime{MinWait: dynconfig.Seconds(100 * time.Millisecond), MaxWait: dynconfig.Seconds(500 * time.Millisecond)},
		Template:       "swebench-astropy-7336",
		TotalSteps:     21,
		NumCycles:      4,
		PollIntervalMs: 100,
	}
	if got := dynconfig.Get[knobs](h); got != want {
		t.Errorf("decoded =\n %+v, want\n %+v", got, want)
	}
	for name, payload := range map[string]string{
		"negative total steps":    `{"sweperf_total_steps": -1}`,
		"negative num cycles":     `{"sweperf_num_cycles": -1}`,
		"negative poll interval":  `{"sweperf_poll_interval_ms": -1}`,
		"max wait below min wait": `{"min_wait_time": 2, "max_wait_time": 1}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := h.Apply([]byte(payload)); err == nil {
				t.Errorf("Apply(%s) took the payload, want a refusal", payload)
			}
			if got := dynconfig.Get[knobs](h); got != want {
				t.Errorf("a refused payload changed the config to %+v", got)
			}
		})
	}
}
