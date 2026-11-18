// Copyright 2026 The Agent Substrate Authors
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

// Package preview manages preview feature gates enabled via the --preview flag.
//
// A binary calls New with its --preview values and passes the returned Gates
// to the code that checks them.
package preview

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"
)

// Gate identifies a gate-controlled preview feature.  Preview gates are
// disabled by default, except:
//   - when a gated feature becomes "stable" (aka GA)
//   - when a gated feature is removed
//
// In both cases, the gate is retained for a while (for compatibility with
// existing uses of the --preview flag), but the feature is either always
// enabled or totally disabled, respectively. Such gates are "vestigial".
type Gate string

// Gates should always be named in UpperCamelCase.  Please keep this list
// sorted alphabetically by value.
const (
	// GateExternalVolumes controls whether external volumes are supported.
	GateExternalVolumes Gate = "ExternalVolumes"

	// GateGoldenEgressPolicy controls configuring a golden actor's egress policy on its template.
	GateGoldenEgressPolicy Gate = "GoldenEgressPolicy"

	// GatePerTemplateSnapshotLocation controls whether an ActorTemplate may
	// set snapshotConfig.storage_location. When disabled, the API rejects the
	// field and snapshots are stored under ate-api-server's
	// --snapshot-location.
	GatePerTemplateSnapshotLocation Gate = "PerTemplateSnapshotLocation"
)

// All is the pseudo-gate name that enables every known gate.
const All = "ALL"

// realKnownGates lists all declared preview gates. Preview gates are disabled by
// default and enabled via the --preview flag.
var realKnownGates = []Gate{
	GateExternalVolumes,
	GateGoldenEgressPolicy,
	GatePerTemplateSnapshotLocation,
}

// Gates holds the state of each known preview gate. Build one with New. It is
// immutable, so one value may be shared freely.
type Gates struct {
	known    []Gate
	enabled  map[string]bool // gate-name -> enabled?
	disabled map[string]bool // gate-name -> false (all disabled)
}

// New returns the preview gates set by the --preview flag values, and logs
// their state. The special value All enables all known gates. With no values,
// all gates are disabled.
func New(values ...string) (*Gates, error) {
	g, err := newGates(realKnownGates, values...)
	if err != nil {
		return nil, err
	}
	slog.Info("initializing preview gates", slog.String("state", g.String()))
	return g, nil
}

// newGates is New with the known gates as a parameter, so tests can use fake
// gates.
func newGates(knownGates []Gate, values ...string) (*Gates, error) {
	knownGates = slices.Sorted(slices.Values(knownGates))
	m, err := newGateMap(knownGates, values...)
	if err != nil {
		return nil, err
	}
	return &Gates{known: knownGates, enabled: m, disabled: newDisabledMap(knownGates)}, nil
}

// String returns the state of every known gate, like "[A=on, B=off]".
func (g *Gates) String() string {
	en := make([]string, 0, len(g.known))
	for _, k := range g.known {
		v := "off"
		if g.enabled[string(k)] {
			v = "on"
		}
		en = append(en, fmt.Sprintf("%s=%s", k, v))
	}
	return fmt.Sprintf("[%s]", strings.Join(en, ", "))
}

// newGateMap creates a map of the gate states, or returns an error if any
// value is not a known gate.
func newGateMap(knownGates []Gate, values ...string) (map[string]bool, error) {
	tmpMap := make(map[string]bool, len(knownGates))
	all := false
	// add all the specified gates
	for _, v := range values {
		if v == All {
			all = true
		} else if slices.Contains(knownGates, Gate(v)) {
			tmpMap[string(v)] = true
		} else {
			return nil, fmt.Errorf("unknown preview gate %q: supported gates: %q", v, append([]Gate{All}, knownGates...))
		}
	}
	// add the rest
	for _, g := range knownGates {
		s := string(g)
		tmpMap[s] = tmpMap[s] || all
	}
	return tmpMap, nil
}

func newDisabledMap(knownGates []Gate) map[string]bool {
	m := make(map[string]bool, len(knownGates))
	for _, g := range knownGates {
		m[string(g)] = false
	}
	return m
}

// Verify checks that the given values are all valid preview gates.
func Verify(values ...string) error {
	return verify(realKnownGates, values...)
}

// verify is Verify with the known gates as a parameter, so tests can use fake
// gates.
func verify(knownGates []Gate, values ...string) error {
	_, err := newGateMap(knownGates, values...)
	return err
}

// IsEnabled reports whether the given preview gate is enabled. Unknown gates
// are always treated as disabled.
func (g *Gates) IsEnabled(gate Gate) bool {
	return g.enabled[string(gate)]
}

// Enabled returns the names of the enabled gates, sorted.
func (g *Gates) Enabled() []string {
	var ret []string
	for _, k := range g.known {
		if g.enabled[string(k)] {
			ret = append(ret, string(k))
		}
	}
	return ret
}

// AsMap returns the map of known gate names to their enabled state.
// The returned map MUST NOT be modified.
//
// TODO: Fix this to use an interface when DV supports it.
func (g *Gates) AsMap() map[string]bool {
	return g.enabled
}

// DisabledMap returns the map of known gate names, all disabled.
// The returned map MUST NOT be modified.
//
// TODO: Fix this to use an interface when DV supports it.
func (g *Gates) DisabledMap() map[string]bool {
	return g.disabled
}
