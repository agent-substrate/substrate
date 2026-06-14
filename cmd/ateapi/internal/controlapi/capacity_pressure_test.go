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

import (
	"testing"
	"time"
)

func TestCapacityPressureHubFanOut(t *testing.T) {
	h := NewCapacityPressureHub()
	a, cancelA := h.Subscribe()
	defer cancelA()
	b, cancelB := h.Subscribe()
	defer cancelB()

	h.Publish("ns", "pool")

	for i, ch := range []<-chan poolKey{a, b} {
		select {
		case got := <-ch:
			if got.namespace != "ns" || got.name != "pool" {
				t.Fatalf("subscriber %d: got %+v, want {ns pool}", i, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d: no event delivered", i)
		}
	}
}

func TestCapacityPressureHubUnsubscribe(t *testing.T) {
	h := NewCapacityPressureHub()
	ch, cancel := h.Subscribe()

	cancel()
	// Publishing after unsubscribe must not panic, and the channel is closed.
	h.Publish("ns", "pool")
	if _, ok := <-ch; ok {
		t.Fatal("channel should be closed after cancel")
	}
	// cancel is idempotent.
	cancel()
}

func TestCapacityPressureHubPublishNeverBlocks(t *testing.T) {
	h := NewCapacityPressureHub()
	_, cancel := h.Subscribe() // a subscriber that never drains
	defer cancel()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 10_000; i++ {
			h.Publish("ns", "pool") // must drop, not block, once the buffer fills
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a full subscriber buffer")
	}
}
