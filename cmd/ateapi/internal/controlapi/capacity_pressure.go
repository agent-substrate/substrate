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

package controlapi

import "sync"

// poolKey identifies a worker pool by namespace and name.
type poolKey struct {
	namespace string
	name      string
}

// CapacityPressureHub fans out capacity-pressure notifications — a pool had no
// free worker for a resume — to any number of subscribers (the WatchCapacity-
// Pressure RPC handlers). Publish is called on the resume hot path and must
// never block: a subscriber whose buffer is full simply misses the event, and
// the autoscaler's periodic reconcile is the backstop.
type CapacityPressureHub struct {
	mu     sync.Mutex
	nextID int
	subs   map[int]chan poolKey
}

// NewCapacityPressureHub returns an empty hub.
func NewCapacityPressureHub() *CapacityPressureHub {
	return &CapacityPressureHub{subs: make(map[int]chan poolKey)}
}

// Subscribe registers a subscriber and returns its event channel plus a cancel
// func that unregisters and closes the channel. cancel is idempotent. The
// channel is buffered so short bursts aren't dropped, and lossy beyond that by
// design.
func (h *CapacityPressureHub) Subscribe() (<-chan poolKey, func()) {
	ch := make(chan poolKey, 64)

	h.mu.Lock()
	id := h.nextID
	h.nextID++
	h.subs[id] = ch
	h.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			delete(h.subs, id)
			close(ch)
		})
	}
	return ch, cancel
}

// Publish notifies every subscriber that the named pool had no free worker. It
// never blocks: a full subscriber buffer drops the event.
func (h *CapacityPressureHub) Publish(namespace, name string) {
	key := poolKey{namespace: namespace, name: name}

	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.subs {
		select {
		case ch <- key:
		default:
		}
	}
}
