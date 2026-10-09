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
package agentsession

import (
	"testing"

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
		"resume_mode": "explicit", "lifecycle_mode": "pause",
		"agentsession_script": "coding-session",
		"agentsession_script_file": "/etc/agentsession/script.yaml",
		"agentsession_think_scale": 0.5
	}`)); err != nil {
		t.Fatalf("Apply(valid): %v", err)
	}
	want := knobs{
		Lifecycle:  dynconfig.Lifecycle{ResumeMode: dynconfig.ResumeModeExplicit, LifecycleMode: dynconfig.LifecycleModePause},
		Script:     "coding-session",
		ScriptFile: "/etc/agentsession/script.yaml",
		ThinkScale: 0.5,
	}
	if got := dynconfig.Get[knobs](h); got != want {
		t.Errorf("decoded =\n %+v, want\n %+v", got, want)
	}
	for name, payload := range map[string]string{
		"negative think scale": `{"agentsession_think_scale": -1}`,
		"invalid resume mode":  `{"resume_mode": "invalid_mode"}`,
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
