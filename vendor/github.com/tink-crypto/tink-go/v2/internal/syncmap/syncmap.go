// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package syncmap provides a wrapper of [sync.Map] that provides better type safety.
package syncmap

import "sync"

// Map is a wrapper of [sync.Map] that provides better type safety.
type Map[K any, V any] struct {
	sm sync.Map
}

// New creates a new [Map].
func New[K any, V any]() *Map[K, V] { return &Map[K, V]{} }

// Load returns the value associated with the given key, or the zero value of V if the key is not
// found.
func (m *Map[K, V]) Load(key K) (V, bool) {
	val, ok := m.sm.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return val.(V), true
}

// Delete deletes the value associated with the given key.
func (m *Map[K, V]) Delete(key K) { m.sm.Delete(key) }

// LoadOrStore returns the value associated with the given key, or the zero value of V if the key
// is not found. If the key is not found, the value is stored in the map and the function returns
// the value and false.
func (m *Map[K, V]) LoadOrStore(key K, value V) (V, bool) {
	val, loaded := m.sm.LoadOrStore(key, value)
	return val.(V), loaded
}

// Clear clears the map.
func (m *Map[K, V]) Clear() { m.sm.Clear() }
