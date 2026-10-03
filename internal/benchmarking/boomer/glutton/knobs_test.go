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
package glutton

import (
	"reflect"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
)

// Each class codec starts a holder at its defaults, takes a payload naming
// every key the class reads, and refuses the values the class cannot run
// on.
func TestClassCodecs(t *testing.T) {
	window := dynconfig.WaitTime{MinWait: dynconfig.Seconds(100 * time.Millisecond), MaxWait: dynconfig.Seconds(500 * time.Millisecond)}
	for _, tc := range []struct {
		name    string
		codec   dynconfig.Codec
		initial any
		valid   string
		want    any
		invalid map[string]string
	}{
		{
			name:    "glutton",
			codec:   gluttonCodec,
			initial: gluttonKnobs{WaitTime: dynconfig.WaitTime{MaxWait: dynconfig.Seconds(500 * time.Millisecond)}, MaxPingsPerWake: 1},
			valid: `{
				"min_wait_time": 0.1, "max_wait_time": 0.5,
				"min_live_time": 9, "max_live_time": 14,
				"resume_mode": "explicit", "lifecycle_mode": "pause",
				"mem_target": "2Gi", "mem_churn": "512Mi", "mem_read": "all",
				"cpu_cores": 2, "cpu_duty_cycle": 0.1,
				"max_pings_per_wake": 3
			}`,
			want: gluttonKnobs{
				WaitTime:        window,
				Lifecycle:       dynconfig.Lifecycle{ResumeMode: dynconfig.ResumeModeExplicit, LifecycleMode: dynconfig.LifecycleModePause},
				MinLive:         dynconfig.Seconds(9 * time.Second),
				MaxLive:         dynconfig.Seconds(14 * time.Second),
				MemTarget:       "2Gi",
				MemChurn:        "512Mi",
				MemRead:         "all",
				CPUCores:        2,
				CPUDutyCycle:    0.1,
				MaxPingsPerWake: 3,
			},
			invalid: map[string]string{
				"negative min wait":       `{"min_wait_time": -1}`,
				"negative max wait":       `{"max_wait_time": -1}`,
				"max wait below min wait": `{"min_wait_time": 2, "max_wait_time": 1}`,
				"negative min live":       `{"min_live_time": -1}`,
				"negative max live":       `{"max_live_time": -1}`,
				"max live below min live": `{"min_live_time": 14, "max_live_time": 9}`,
				"invalid resume mode":     `{"resume_mode": "invalid_mode"}`,
				"invalid lifecycle mode":  `{"lifecycle_mode": "invalid_lifecycle"}`,
				"negative cpu cores":      `{"cpu_cores": -1}`,
				"negative cpu duty cycle": `{"cpu_duty_cycle": -0.1}`,
				"cpu duty cycle above 1":  `{"cpu_duty_cycle": 1.5}`,
				"negative trace (common)": `{"trace_probability": -0.1}`,
				"trace above 1 (common)":  `{"trace_probability": 1.5}`,
				"wrong type for a count":  `{"cpu_cores": "two"}`,
			},
		},
		{
			name:    "durdir",
			codec:   durDirCodec,
			initial: durDirKnobs{},
			valid: `{
				"min_wait_time": 0.1, "max_wait_time": 0.5,
				"resume_mode": "implicit", "lifecycle_mode": "suspend",
				"durdir_file_size_bytes": 1048576,
				"durdir_read_mode": "digest",
				"durdir_template": "glutton-durdir-data"
			}`,
			want: durDirKnobs{
				WaitTime:  window,
				Lifecycle: dynconfig.Lifecycle{ResumeMode: dynconfig.ResumeModeImplicit, LifecycleMode: dynconfig.LifecycleModeSuspend},
				FileSize:  1048576,
				ReadMode:  readModeDigest,
				Template:  "glutton-durdir-data",
			},
			invalid: map[string]string{
				"negative file size":      `{"durdir_file_size_bytes": -100}`,
				"file size over 2 GiB":    `{"durdir_file_size_bytes": 2147483648}`,
				"invalid read mode":       `{"durdir_read_mode": "invalid_read"}`,
				"invalid lifecycle mode":  `{"lifecycle_mode": "invalid_lifecycle"}`,
				"max wait below min wait": `{"min_wait_time": 2, "max_wait_time": 1}`,
			},
		},
		{
			name:    "spawn",
			codec:   spawnCodec,
			initial: spawnKnobs{},
			valid:   `{"total_actors": 50, "spawn_concurrency": 5, "actor_deadline": 60}`,
			want:    spawnKnobs{TotalActors: 50, SpawnConcurrency: 5, ActorDeadline: dynconfig.Seconds(60 * time.Second)},
			invalid: map[string]string{
				"negative total actors":      `{"total_actors": -1}`,
				"negative spawn concurrency": `{"spawn_concurrency": -1}`,
				"negative actor deadline":    `{"actor_deadline": -1}`,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := dynconfig.NewHolder(tc.codec)
			if got := h.Load(); !reflect.DeepEqual(got, tc.initial) {
				t.Fatalf("fresh holder = %+v, want %+v", got, tc.initial)
			}
			if _, _, err := h.Apply([]byte(tc.valid)); err != nil {
				t.Fatalf("Apply(valid): %v", err)
			}
			if got := h.Load(); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("decoded =\n %+v, want\n %+v", got, tc.want)
			}
			for name, payload := range tc.invalid {
				t.Run(name, func(t *testing.T) {
					if _, _, err := h.Apply([]byte(payload)); err == nil {
						t.Errorf("Apply(%s) took the payload, want a refusal", payload)
					}
					if got := h.Load(); !reflect.DeepEqual(got, tc.want) {
						t.Errorf("a refused payload changed the config to %+v", got)
					}
				})
			}
		})
	}
}
