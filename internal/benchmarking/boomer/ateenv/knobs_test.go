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

package ateenv

import (
	"reflect"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/benchmarking/boomer/dynconfig"
)

func TestAteEnvCodec(t *testing.T) {
	h := dynconfig.NewHolder(ateEnvCodec)
	initial := ateEnvKnobs{
		WaitTime:        dynconfig.WaitTime{MaxWait: dynconfig.Seconds(500 * time.Millisecond)},
		MaxPingsPerWake: 1,
	}
	if got := h.Load(); !reflect.DeepEqual(got, initial) {
		t.Fatalf("fresh holder = %+v, want %+v", got, initial)
	}

	valid := `{
		"min_wait_time": 0.2, "max_wait_time": 1.0,
		"min_live_time": 9, "max_live_time": 14,
		"resume_mode": "explicit", "lifecycle_mode": "async",
		"max_pings_per_wake": 2
	}`
	if _, _, err := h.Apply([]byte(valid)); err != nil {
		t.Fatalf("Apply(valid): %v", err)
	}
	want := ateEnvKnobs{
		WaitTime:        dynconfig.WaitTime{MinWait: dynconfig.Seconds(200 * time.Millisecond), MaxWait: dynconfig.Seconds(time.Second)},
		Lifecycle:       dynconfig.Lifecycle{ResumeMode: dynconfig.ResumeModeExplicit, LifecycleMode: dynconfig.LifecycleModeAsync},
		MinLive:         dynconfig.Seconds(9 * time.Second),
		MaxLive:         dynconfig.Seconds(14 * time.Second),
		MaxPingsPerWake: 2,
	}
	if got := h.Load(); !reflect.DeepEqual(got, want) {
		t.Errorf("decoded =\n %+v, want\n %+v", got, want)
	}

	for name, payload := range map[string]string{
		"negative min wait":       `{"min_wait_time": -1}`,
		"negative max wait":       `{"max_wait_time": -1}`,
		"max wait below min wait": `{"min_wait_time": 2, "max_wait_time": 1}`,
		"negative min live":       `{"min_live_time": -1}`,
		"negative max live":       `{"max_live_time": -1}`,
		"max live below min live": `{"min_live_time": 14, "max_live_time": 9}`,
		"invalid resume mode":     `{"resume_mode": "invalid_mode"}`,
		"invalid lifecycle mode":  `{"lifecycle_mode": "invalid_lifecycle"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := h.Apply([]byte(payload)); err == nil {
				t.Errorf("Apply(%s) took the payload, want a refusal", payload)
			}
			if got := h.Load(); !reflect.DeepEqual(got, want) {
				t.Errorf("a refused payload changed the config to %+v", got)
			}
		})
	}
}
