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
	"time"

	"go.ate.dev/substrate/internal/benchmarking/boomer/dynconfig"
)

// The codec takes a payload naming every key the class reads and refuses
// the values it cannot run on.
func TestKnobsCodec(t *testing.T) {
	h := dynconfig.NewHolder(knobsCodec)
	if got := dynconfig.Get[knobs](h); got != knobsCodec.Defaults || got.ResumeRetryBudget.Duration() != defaultResumeRetryBudget {
		t.Fatalf("fresh holder = %+v, want the defaults with a %v retry budget", got, defaultResumeRetryBudget)
	}
	if _, _, err := h.Apply([]byte(`{
		"resume_mode": "explicit", "lifecycle_mode": "async",
		"agentsession_script": "coding-session",
		"agentsession_script_file": "/etc/agentsession/script.yaml",
		"agentsession_think_scale": 0.5,
		"agentsession_snapshot_steps": "3,10,20,30,40,50,61",
		"agentsession_phase_in": 300,
		"agentsession_resume_retry_budget": 45.5
	}`)); err != nil {
		t.Fatalf("Apply(valid): %v", err)
	}
	want := knobs{
		Lifecycle:         dynconfig.Lifecycle{ResumeMode: dynconfig.ResumeModeExplicit, LifecycleMode: dynconfig.LifecycleModeAsync},
		Script:            "coding-session",
		ScriptFile:        "/etc/agentsession/script.yaml",
		ThinkScale:        0.5,
		SnapshotSteps:     "3,10,20,30,40,50,61",
		PhaseIn:           dynconfig.Seconds(5 * time.Minute),
		ResumeRetryBudget: dynconfig.Seconds(45500 * time.Millisecond),
	}
	if got := dynconfig.Get[knobs](h); got != want {
		t.Errorf("decoded =\n %+v, want\n %+v", got, want)
	}
	for name, payload := range map[string]string{
		"negative think scale":                    `{"agentsession_think_scale": -1}`,
		"negative phase-in":                       `{"agentsession_phase_in": -1}`,
		"negative resume retry budget":            `{"agentsession_resume_retry_budget": -1}`,
		"invalid resume mode":                     `{"resume_mode": "invalid_mode"}`,
		"invalid snapshot steps entry":            `{"agentsession_snapshot_steps": "3,zero,10"}`,
		"non-positive snapshot steps entry":       `{"agentsession_snapshot_steps": "0,10"}`,
		"snapshot steps with lifecycle mode none": `{"lifecycle_mode": "none", "agentsession_snapshot_steps": "3,10"}`,
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
