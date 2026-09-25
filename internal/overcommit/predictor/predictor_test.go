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

package predictor

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/resources"
)

func TestWindowedMaxPredictorName(t *testing.T) {
	if got, want := NewWindowedMaxPredictor(time.Minute, time.Hour).Name(), "windowed-max"; got != want {
		t.Errorf("Name() = %q, want %q", got, want)
	}
}

// quantities is never nil, even when both are zero: actor reads a nil usage
// as having no CPU rate.
func quantities(t *testing.T, cpuMilli, memoryBytes int64) *ateapi.Resources {
	t.Helper()
	return &ateapi.Resources{CpuMillicores: cpuMilli, MemoryBytes: memoryBytes}
}

// actor is a sample for an actor started at startedAt with the given limits.
// A nil usage means it has no CPU rate yet.
func actor(t *testing.T, uid string, startedAt time.Time, limits *ateapi.Resources, usage *ateapi.Resources) ActorUsageSample {
	t.Helper()
	return ActorUsageSample{
		ActorUID:   uid,
		StartedAt:  startedAt,
		Limits:     limits,
		Usage:      usage,
		HasCPURate: usage != nil,
	}
}

func predict(t *testing.T, p *WindowedMaxPredictor, now time.Time, actors ...ActorUsageSample) *ateapi.Resources {
	t.Helper()
	snapshot := GroupUsageSnapshot{Timestamp: now, Actors: make(map[string]ActorUsageSample, len(actors))}
	for _, a := range actors {
		snapshot.Actors[a.ActorUID] = a
	}
	got, err := p.Predict(now, snapshot)
	if err != nil {
		t.Fatalf("Predict() failed: %v", err)
	}
	return got
}

func wantPrediction(t *testing.T, what string, got *ateapi.Resources, cpuMilli, memoryBytes int64) {
	t.Helper()
	if diff := cmp.Diff(resources.CPUMemory(cpuMilli, memoryBytes), got, protocmp.Transform()); diff != "" {
		t.Errorf("%s: prediction mismatch (-want +got):\n%s", what, diff)
	}
}

// An actor is predicted at its limits until it matures, then at the peaks of
// its CPU and memory within the history window, taken separately. An immature
// actor adds its full limits on top, and peaks are forgotten once they fall
// out of the window.
func TestWindowedMaxPredictorMaturationAndSeparateMax(t *testing.T) {
	p := NewWindowedMaxPredictor(30*time.Second, time.Minute)
	t0 := time.Now()
	limitsA := quantities(t, 1000, 1000)

	wantPrediction(t, "no CPU rate yet", predict(t, p, t0, actor(t, "a", t0, limitsA, nil)), 1000, 1000)
	wantPrediction(t, "immature at 10s", predict(t, p, t0.Add(10*time.Second), actor(t, "a", t0, limitsA, quantities(t, 200, 300))), 1000, 1000)
	wantPrediction(t, "immature at 20s", predict(t, p, t0.Add(20*time.Second), actor(t, "a", t0, limitsA, quantities(t, 450, 400))), 1000, 1000)
	wantPrediction(t, "mature at 35s", predict(t, p, t0.Add(35*time.Second), actor(t, "a", t0, limitsA, quantities(t, 300, 600))), 450, 600)

	b := actor(t, "b", t0.Add(36*time.Second), quantities(t, 500, 500), nil)
	wantPrediction(t, "mature plus immature", predict(t, p, t0.Add(40*time.Second), actor(t, "a", t0, limitsA, quantities(t, 100, 200)), b), 950, 1100)

	// The 20s peak falls out of the window at 80s.
	wantPrediction(t, "20s peak expired", predict(t, p, t0.Add(85*time.Second), actor(t, "a", t0, limitsA, quantities(t, 50, 50))), 300, 600)
	// The 35s peak falls out of the window at 95s.
	wantPrediction(t, "35s peak expired", predict(t, p, t0.Add(100*time.Second), actor(t, "a", t0, limitsA, quantities(t, 50, 50))), 100, 200)
}

// The prediction for mature actors never exceeds their limits, even when they
// were measured above them.
func TestWindowedMaxPredictorCapsAtLimits(t *testing.T) {
	p := NewWindowedMaxPredictor(0, time.Minute)
	t0 := time.Now()
	limits := quantities(t, 500, 1000)
	predict(t, p, t0, actor(t, "a", t0, limits, quantities(t, 800, 200)))
	got := predict(t, p, t0.Add(time.Second), actor(t, "a", t0, limits, quantities(t, 100, 1500)))
	wantPrediction(t, "capped", got, 500, 1000)
}

// A departed actor's history no longer counts, and the peak of the remaining
// actors is taken over their sum at each poll, not the sum of their peaks.
func TestWindowedMaxPredictorSumsPerPollAndForgetsDepartedActors(t *testing.T) {
	p := NewWindowedMaxPredictor(0, time.Minute)
	t0 := time.Now()
	limits := quantities(t, 1000, 1000)

	predict(t, p, t0,
		actor(t, "a", t0, limits, quantities(t, 400, 100)),
		actor(t, "b", t0, limits, quantities(t, 100, 100)),
		actor(t, "c", t0, limits, quantities(t, 900, 900)))
	got := predict(t, p, t0.Add(time.Second),
		actor(t, "a", t0, limits, quantities(t, 100, 100)),
		actor(t, "b", t0, limits, quantities(t, 300, 100)))
	// Per poll a+b is 500m then 400m; the sum of peaks would be 700m.
	wantPrediction(t, "a and b", got, 500, 200)

	if len(p.history) != 2 {
		t.Fatalf("history has %d polls, want 2", len(p.history))
	}
	for _, point := range p.history {
		if _, ok := point.usage["c"]; ok {
			t.Errorf("history at %v still holds the departed actor", point.timestamp)
		}
	}
}

// An actor past the maturation window with no CPU rate, as after its CPU
// counter reset, is predicted at its limits again.
func TestWindowedMaxPredictorTreatsActorWithoutCPURateAsImmature(t *testing.T) {
	p := NewWindowedMaxPredictor(time.Second, time.Minute)
	t0 := time.Now()
	limits := quantities(t, 1000, 1000)
	predict(t, p, t0.Add(10*time.Second), actor(t, "a", t0, limits, quantities(t, 100, 100)))
	got := predict(t, p, t0.Add(20*time.Second), actor(t, "a", t0, limits, nil))
	wantPrediction(t, "no CPU rate", got, 1000, 1000)
}

func TestWindowedMaxPredictorEmptyGroup(t *testing.T) {
	p := NewWindowedMaxPredictor(time.Second, time.Minute)
	wantPrediction(t, "no actors", predict(t, p, time.Now()), 0, 0)
	if len(p.history) != 0 {
		t.Errorf("history has %d polls, want 0", len(p.history))
	}
}
