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
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
)

func TestUniformWaitStaysInRange(t *testing.T) {
	lo, hi := 200*time.Millisecond, time.Second
	for i := 0; i < 1000; i++ {
		got := dynconfig.Uniform(lo, hi)
		if got < lo || got > hi {
			t.Fatalf("dynconfig.Uniform(%v, %v) = %v, outside range", lo, hi, got)
		}
	}
}

func TestUniformWaitDegenerateRanges(t *testing.T) {
	if got := dynconfig.Uniform(0, 0); got != 0 {
		t.Errorf("dynconfig.Uniform(0, 0) = %v, want 0", got)
	}
	if got := dynconfig.Uniform(3*time.Second, 3*time.Second); got != 3*time.Second {
		t.Errorf("equal bounds: got %v, want 3s", got)
	}
	// An inverted range yields the lower bound rather than a negative wait.
	if got := dynconfig.Uniform(5*time.Second, time.Second); got != 5*time.Second {
		t.Errorf("inverted bounds: got %v, want 5s", got)
	}
}

// The wait window and the live window read different config fields, so a
// run that sets only one of them must not leak into the other.
func TestWaitAndLiveWindowsAreIndependent(t *testing.T) {
	rt := &taskRuntime{cfg: &userclass.Config{Dyn: dynconfig.Static(gluttonKnobs{
		WaitTime: dynconfig.WaitTime{MinWait: dynconfig.Seconds(200 * time.Millisecond), MaxWait: dynconfig.Seconds(time.Second)},
	})}}
	for i := 0; i < 100; i++ {
		if got := rt.liveWait(); got != 0 {
			t.Fatalf("liveWait with zero live window = %v, want 0", got)
		}
		if got := rt.dynamicWait(); got < 200*time.Millisecond || got > time.Second {
			t.Fatalf("dynamicWait = %v, want within [200ms, 1s]", got)
		}
	}

	rt.cfg.Dyn = dynconfig.Static(gluttonKnobs{MinLive: dynconfig.Seconds(9 * time.Second), MaxLive: dynconfig.Seconds(14 * time.Second)})
	for i := 0; i < 100; i++ {
		if got := rt.dynamicWait(); got != 0 {
			t.Fatalf("dynamicWait with zero wait window = %v, want 0", got)
		}
		if got := rt.liveWait(); got < 9*time.Second || got > 14*time.Second {
			t.Fatalf("liveWait = %v, want within [9s, 14s]", got)
		}
	}
}
