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

package userclass

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// Entry declares one user class: the flag value that selects it, the Locust
// file and Python class it pairs with, and the func that builds its task.
type Entry struct {
	// Name is the --user-class flag value that selects this class.
	Name string
	// LocustFile is the tests/<file> the Locust master loads for it.
	LocustFile string
	// UserClass is the Python class name. It must equal boomer.Task.Name or
	// the master's spawn messages never match and no users start.
	UserClass string
	// Init builds the boomer task func and its shutdown hook.
	Init func(*Config) (task func(), shutdown func(context.Context))
}

var (
	mu       sync.RWMutex
	registry = make(map[string]Entry)
)

// Add registers a user class Entry. It panics if an entry with the same Name is already registered.
func Add(e Entry) {
	mu.Lock()
	defer mu.Unlock()
	if _, exists := registry[e.Name]; exists {
		panic(fmt.Sprintf("userclass: duplicate registration for %q", e.Name))
	}
	registry[e.Name] = e
}

// Lookup returns the Entry registered under name, or false if not found.
func Lookup(name string) (Entry, bool) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := registry[name]
	return e, ok
}

// Names returns a sorted slice of all registered user class names.
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	names := make([]string, 0, len(registry))
	for k := range registry {
		names = append(names, k)
	}
	slices.Sort(names)
	return names
}
