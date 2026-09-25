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
			Ref string `json:"ref"`
			// "required", or {conditionally_required: <condition>}.
			RequirementLevel any `json:"requirement_level"`
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

	type shape struct{ required, conditional []string }
	declared := map[string]shape{}
	for _, g := range reg.Groups {
		if g.Type != "event" {
			continue
		}
		var sh shape
		for _, a := range g.Attributes {
			switch lvl := a.RequirementLevel.(type) {
			case string:
				if lvl != "required" {
					t.Errorf("%s declares %s as %q; an event attribute is required or conditionally required with a stated condition",
						g.Name, a.Ref, lvl)
				}
				sh.required = append(sh.required, a.Ref)
			case map[string]any:
				if c, ok := lvl["conditionally_required"].(string); !ok || c == "" {
					t.Errorf("%s declares %s as %v; an event attribute is required or conditionally required with a stated condition",
						g.Name, a.Ref, lvl)
				}
				sh.conditional = append(sh.conditional, a.Ref)
			default:
				t.Errorf("%s declares %s with an unreadable requirement level %v", g.Name, a.Ref, lvl)
			}
		}
		declared[g.Name] = sh
	}

	for _, ev := range events {
		sh, ok := declared[ev.Name]
		if !ok {
			t.Errorf("%s has no group in %s; add one", ev.Name, registryPath)
			continue
		}
		delete(declared, ev.Name)

		got, want := slices.Sorted(slices.Values(sh.required)), slices.Sorted(slices.Values(ev.Keys))
		if !slices.Equal(got, want) {
			t.Errorf("%s: %s requires %v, the Event declares %v", ev.Name, registryPath, got, want)
		}
		got, want = slices.Sorted(slices.Values(sh.conditional)), slices.Sorted(slices.Values(ev.Conditional))
		if !slices.Equal(got, want) {
			t.Errorf("%s: %s conditionally requires %v, the Event declares %v", ev.Name, registryPath, got, want)
		}
	}

	for name := range declared {
		t.Errorf("%s declares %s, which no Event in this package emits", registryPath, name)
	}
}
