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

// Gate identifies a preview feature gate.
type Gate string

const (
	// GatePreview is the preview gate for the preview system itself.
	GatePreview Gate = "Preview"
)

// knownGates lists all declared preview gates. Preview gates are disabled by
// default and enabled via the --preview flag.
var knownGates = []Gate{
	GatePreview,
}

var stateMap map[string]bool // gate-name -> enabled
var initialized bool         // true if Init has been called, false otherwise
var initLock sync.Mutex

// init sets the stateMap to the default state of all known gates disabled. It
// is called automatically on package initialization, but can be overridden by
// calling Init before any other functions in this package.
func init() {
	initLock.Lock()
	defer initLock.Unlock()
	if stateMap != nil {
		return
	}

	if m, err := newGates(); err != nil {
		panic(fmt.Sprintf("preview.init failed: %v", err))
	} else {
		stateMap = m
	}
}

// Init sets the process-wide preview gates from the --preview flag values.
// The only accepted value is "*", which enables all known gates; with no
// values, all gates are disabled. It panics if called more than once.
func Init(values ...string) error {
	initLock.Lock()
	defer initLock.Unlock()
	if initialized {
		panic("preview.Init called more than once")
	}
	initialized = true

	slices.Sort(knownGates)
	if m, err := newGates(values...); err != nil {
		return err
	} else {
		stateMap = m
	}
	return nil
}

func newGates(values ...string) (map[string]bool, error) {
	// for fast lookup of known gates
	allMap := func() map[string]bool {
		m := make(map[string]bool, len(knownGates))
		for _, g := range knownGates {
			m[string(g)] = true
		}
		return m
	}()

	tmpMap := make(map[string]bool, len(knownGates))
	all := false
	// add all the specified gates
	for _, v := range values {
		if v == "*" {
			all = true
		} else if allMap[string(v)] {
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

// IsEnabled reports whether the given preview gate is enabled. Unknown gates
// are always treated as disabled.
func IsEnabled(gate Gate) bool {
	return stateMap[string(gate)]
}

// AsMap returns the map of known gate names to their enabled state.
// The returned map MUST NOT be modified.
//
// TODO: Fix this to use an interface when DV supports it.
func AsMap() map[string]bool {
	return stateMap
}
