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

package resumepaused

import (
	"testing"

	"go.ate.dev/substrate/internal/benchmarking/boomer/dynconfig"
)

// The codec takes a target rate, reads an absent one as unpaced, and
// refuses a negative one in favor of the config before it.
func TestKnobsCodec(t *testing.T) {
	h := dynconfig.NewHolder(knobsCodec)
	if got := dynconfig.Get[knobs](h); got != (knobs{}) {
		t.Fatalf("fresh holder = %+v, want unpaced", got)
	}
	if _, _, err := h.Apply([]byte(`{"target_rps": 2500.5, "someone_elses_key": 1}`)); err != nil {
		t.Fatalf("Apply(valid): %v", err)
	}
	want := knobs{TargetRPS: 2500.5}
	if got := dynconfig.Get[knobs](h); got != want {
		t.Errorf("decoded = %+v, want %+v", got, want)
	}
	if _, _, err := h.Apply([]byte(`{"target_rps": -1}`)); err == nil {
		t.Error("Apply(negative target_rps) took the payload, want a refusal")
	}
	if got := dynconfig.Get[knobs](h); got != want {
		t.Errorf("a refused payload changed the config to %+v", got)
	}
	if _, _, err := h.Apply([]byte(`{"target_rps": null}`)); err != nil {
		t.Fatalf("Apply(null): %v", err)
	}
	if got := dynconfig.Get[knobs](h); got != (knobs{}) {
		t.Errorf("a null target_rps left %+v, want unpaced", got)
	}
}
