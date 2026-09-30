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

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// actorCount reads locust_actors{user_class, state} through the default
// gatherer, as a scrape would see it. An absent series reads as 0.
func actorCount(t *testing.T, userClass string, state ActorState) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != "locust_actors" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["user_class"] == userClass && labels["state"] == string(state) {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

func TestMoveActor(t *testing.T) {
	const class = "TestMoveActorUser"
	steps := []struct {
		from, to ActorState
		want     map[ActorState]float64
	}{
		{ActorStateNone, ActorStateHibernated, map[ActorState]float64{ActorStateHibernated: 1}},
		{ActorStateHibernated, ActorStateRunning, map[ActorState]float64{ActorStateRunning: 1}},
		{ActorStateRunning, ActorStateRunning, map[ActorState]float64{ActorStateRunning: 1}},
		{ActorStateRunning, ActorStateHibernatePending, map[ActorState]float64{ActorStateHibernatePending: 1}},
		{ActorStateHibernatePending, ActorStateHibernated, map[ActorState]float64{ActorStateHibernated: 1}},
		{ActorStateHibernated, ActorStateNone, map[ActorState]float64{}},
	}
	states := []ActorState{ActorStateHibernated, ActorStateRunning, ActorStateHibernatePending, ActorStateCrashed}
	for i, s := range steps {
		MoveActor(class, s.from, s.to)
		for _, state := range states {
			if got := actorCount(t, class, state); got != s.want[state] {
				t.Errorf("step %d (%q -> %q): %s = %v, want %v", i, s.from, s.to, state, got, s.want[state])
			}
		}
	}
}
