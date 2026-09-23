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

package actorevent

import (
	"os"
	"slices"
	"testing"

	"sigs.k8s.io/yaml"
)

const registryPath = "../../docs/metrics/registry/events.yaml"

// registry is the part of a Weaver event group this test reads. Weaver has no
// field for a body or a severity, so those two are pinned in Go alone.
type registry struct {
	Groups []struct {
		Type       string `json:"type"`
		Name       string `json:"name"`
		Attributes []struct {
			Ref              string `json:"ref"`
			RequirementLevel string `json:"requirement_level"`
		} `json:"attributes"`
	} `json:"groups"`
}

// TestEventsMatchTheRegistry holds the vocabulary and events.yaml in step both
// ways, so a new event has to be declared before it can ship.
//
// It covers every event group in the file. A component outside this package that
// starts emitting events needs a check of its own, and this one has to learn to
// skip what it does not own.
func TestEventsMatchTheRegistry(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(registryPath)
	if err != nil {
		t.Fatalf("read %s: %v", registryPath, err)
	}
	var reg registry
	if err := yaml.Unmarshal(raw, &reg); err != nil {
		t.Fatalf("parse %s: %v", registryPath, err)
	}

	declared := map[string][]string{}
	for _, g := range reg.Groups {
		if g.Type != "event" {
			continue
		}
		keys := make([]string, 0, len(g.Attributes))
		for _, a := range g.Attributes {
			if a.RequirementLevel != "required" {
				t.Errorf("%s declares %s as %q; an event name promises a fixed shape, so every attribute is required",
					g.Name, a.Ref, a.RequirementLevel)
			}
			keys = append(keys, a.Ref)
		}
		declared[g.Name] = keys
	}

	for _, ev := range events {
		keys, ok := declared[ev.Name]
		if !ok {
			t.Errorf("%s has no group in %s; add one", ev.Name, registryPath)
			continue
		}
		delete(declared, ev.Name)

		got, want := slices.Sorted(slices.Values(keys)), slices.Sorted(slices.Values(ev.Keys))
		if !slices.Equal(got, want) {
			t.Errorf("%s: %s declares %v, the Event declares %v", ev.Name, registryPath, got, want)
		}
	}

	for name := range declared {
		t.Errorf("%s declares %s, which no Event in this package emits", registryPath, name)
	}
}
