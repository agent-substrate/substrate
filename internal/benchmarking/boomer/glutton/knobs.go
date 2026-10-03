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

package glutton

import (
	"fmt"
	"math"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
)

// The three user classes in this package each read their own slice of the
// runtime config. The structs below name the keys; the sections carry the
// defaults a missing key falls back to and the rules a fetched value must
// pass before the worker takes it on.

// gluttonKnobs is the GluttonUser slice: the wait and live windows, the
// lifecycle modes, the working-set sizes, the CPU load, and the ping cap.
type gluttonKnobs struct {
	dynconfig.WaitTime
	dynconfig.Lifecycle
	// MinLive and MaxLive bound how long an actor stays resumed between its
	// first ping and its suspend; a zero window suspends right after the ping.
	MinLive dynconfig.Seconds `json:"min_live_time"`
	MaxLive dynconfig.Seconds `json:"max_live_time"`
	// MemTarget is the resident RAM filled via WriteRAM, suffixed ("2Gi");
	// MemChurn the RAM re-randomized in place each cycle; MemRead the RAM
	// walked after each resume, or "all". Empty disables each. Glutton
	// parses the sizes, so a bad one fails loudly as a GluttonFillRAM,
	// GluttonChurnRAM, or GluttonReadRAM error rather than here.
	MemTarget string `json:"mem_target"`
	MemChurn  string `json:"mem_churn"`
	MemRead   string `json:"mem_read"`
	// CPUCores goroutines each burn CPUDutyCycle of one core via UseCPU; 0
	// disables.
	CPUCores     int     `json:"cpu_cores"`
	CPUDutyCycle float64 `json:"cpu_duty_cycle"`
	// MaxPingsPerWake caps the pings sent during one resume/suspend cycle;
	// values below 1 read as 1.
	MaxPingsPerWake int `json:"max_pings_per_wake"`
}

var gluttonCodec = dynconfig.Typed[gluttonKnobs]{
	Defaults: gluttonKnobs{
		WaitTime:        dynconfig.WaitTime{MaxWait: dynconfig.Seconds(500 * time.Millisecond)},
		MaxPingsPerWake: 1,
	},
	Validate: func(k gluttonKnobs) error {
		if err := k.WaitTime.Validate(); err != nil {
			return err
		}
		if err := k.Lifecycle.Validate(); err != nil {
			return err
		}
		if k.MinLive < 0 {
			return fmt.Errorf("min_live_time cannot be negative: %v", k.MinLive.Duration())
		}
		if k.MaxLive < 0 {
			return fmt.Errorf("max_live_time cannot be negative: %v", k.MaxLive.Duration())
		}
		if k.MaxLive < k.MinLive {
			return fmt.Errorf("max_live_time (%v) cannot be less than min_live_time (%v)", k.MaxLive.Duration(), k.MinLive.Duration())
		}
		if k.CPUCores < 0 {
			return fmt.Errorf("cpu_cores cannot be negative: %d", k.CPUCores)
		}
		if k.CPUDutyCycle < 0 || k.CPUDutyCycle > 1 {
			return fmt.Errorf("cpu_duty_cycle must be between 0.0 and 1.0, got: %f", k.CPUDutyCycle)
		}
		return nil
	},
}

// DurDir read modes: whether the actor ships the file's bytes back or only
// its digest.
const (
	readModeData   = "data"
	readModeDigest = "digest"
)

// durDirKnobs is the DurdirUser slice.
type durDirKnobs struct {
	dynconfig.WaitTime
	dynconfig.Lifecycle
	// FileSize is the file written and re-read each cycle, in bytes; 0
	// falls back to defaultFileSize.
	FileSize int64 `json:"durdir_file_size_bytes"`
	// ReadMode is readModeData or readModeDigest; empty reads as data.
	ReadMode string `json:"durdir_read_mode"`
	// Template is the ActorTemplate name; empty falls back to
	// defaultDurTemplate.
	Template string `json:"durdir_template"`
}

var durDirCodec = dynconfig.Typed[durDirKnobs]{
	Validate: func(k durDirKnobs) error {
		if err := k.WaitTime.Validate(); err != nil {
			return err
		}
		if err := k.Lifecycle.Validate(); err != nil {
			return err
		}
		if k.FileSize < 0 {
			return fmt.Errorf("durdir_file_size_bytes cannot be negative: %d", k.FileSize)
		}
		if k.FileSize > math.MaxInt32 {
			return fmt.Errorf("durdir_file_size_bytes cannot exceed %d (2 GiB), got: %d", math.MaxInt32, k.FileSize)
		}
		if k.ReadMode != "" && k.ReadMode != readModeData && k.ReadMode != readModeDigest {
			return fmt.Errorf("invalid durdir_read_mode %q: must be %q or %q", k.ReadMode, readModeData, readModeDigest)
		}
		return nil
	},
}

// spawnKnobs is the SpawnUser slice. Each zero keeps the matching
// boomer-worker flag.
type spawnKnobs struct {
	TotalActors      int               `json:"total_actors"`
	SpawnConcurrency int               `json:"spawn_concurrency"`
	ActorDeadline    dynconfig.Seconds `json:"actor_deadline"`
}

var spawnCodec = dynconfig.Typed[spawnKnobs]{
	Validate: func(k spawnKnobs) error {
		if k.TotalActors < 0 {
			return fmt.Errorf("total_actors cannot be negative: %d", k.TotalActors)
		}
		if k.SpawnConcurrency < 0 {
			return fmt.Errorf("spawn_concurrency cannot be negative: %d", k.SpawnConcurrency)
		}
		if k.ActorDeadline < 0 {
			return fmt.Errorf("actor_deadline cannot be negative: %v", k.ActorDeadline.Duration())
		}
		return nil
	},
}
