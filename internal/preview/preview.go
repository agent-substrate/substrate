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

// Package preview manages preview feature gates enabled via the --preview flag.
//
// The gates are process-wide. A binary calls Init exactly once, before
// anything calls IsEnabled or AsMap.
package preview

import (
	"fmt"
	"slices"
	"sync"
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

const (
	// GateExternalVolumes controls whether external volumes are supported.
	GateExternalVolumes Gate = "ExternalVolumes"
)

// realKnownGates lists all declared preview gates. Preview gates are disabled by
// default and enabled via the --preview flag.
var realKnownGates = []Gate{
	GateExternalVolumes,
}

var globalKnownGates []Gate        // all known gates
var globalStateMap map[string]bool // gate-name -> enabled
var globalInitialized bool         // true if Init has been called, false otherwise
var globalInitLock sync.Mutex

// init sets the globalStateMap to the default state of all known gates disabled. It
// is called automatically on package initialization, but can be overridden by
// calling Init.
func init() {
	globalInitLock.Lock()
	defer globalInitLock.Unlock()
	if globalStateMap != nil {
		return
	}

	slices.Sort(realKnownGates)
	if m, err := newGateMap(realKnownGates); err != nil {
		panic(fmt.Sprintf("preview.init failed: %v", err))
	} else {
		globalKnownGates = realKnownGates
		globalStateMap = m
	}
}

// Init sets the process-wide preview gates from the --preview flag values.
// The special value "*" enables all known gates. With no values, all gates are
// disabled. It panics if called agasin after the first successful call.
func Init(values ...string) error {
	return doInit(globalKnownGates, values...)
}

// This allows tests to use fake gates.
func doInit(knownGates []Gate, values ...string) error {
	globalInitLock.Lock()
	defer globalInitLock.Unlock()
	if globalInitialized {
		panic("preview.Init called more than once")
	}

	if m, err := newGateMap(knownGates, values...); err != nil {
		return err
	} else {
		globalKnownGates = knownGates
		globalStateMap = m
		globalInitialized = true
	}
	return nil
}

func newGateMap(knownGates []Gate, values ...string) (map[string]bool, error) {
	tmpMap := make(map[string]bool, len(knownGates))
	all := false
	// add all the specified gates
	for _, v := range values {
		if v == "*" {
			all = true
		} else if slices.Contains(knownGates, Gate(v)) {
			tmpMap[string(v)] = true
		} else {
			return nil, fmt.Errorf("unknown preview gate %q: supported gates: %q", v, append([]Gate{"*"}, knownGates...))
		}
	}
	// add the rest
	for _, g := range knownGates {
		s := string(g)
		tmpMap[s] = tmpMap[s] || all
	}
	return tmpMap, nil
}

// Verify checks that the given values are all valid preview gates.
func Verify(values ...string) error {
	return doVerify(globalKnownGates, values...)
}

func doVerify(knownGates []Gate, values ...string) error {
	_, err := newGateMap(knownGates, values...)
	return err
}

// IsEnabled reports whether the given preview gate is enabled. Unknown gates
// are always treated as disabled.
func IsEnabled(gate Gate) bool {
	return globalStateMap[string(gate)]
}

// AsMap returns the map of known gate names to their enabled state.
// The returned map MUST NOT be modified.
//
// TODO: Fix this to use an interface when DV supports it.
func AsMap() map[string]bool {
	return globalStateMap
}
