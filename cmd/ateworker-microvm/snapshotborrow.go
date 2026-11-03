//go:build linux

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

package main

// unbookedMemory is the worker's reported memory capacity less what its
// actors booked, overhead included.
func (s *AteWorkerService) unbookedMemory(podLimit int64) int64 {
	unbooked := podLimit - memoryReservedBytes
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	for _, h := range s.actors {
		unbooked -= actorBooking(h.memoryBytes)
	}
	return unbooked
}

// actorBooking is what the control plane books for an actor with this memory
// limit: the limit plus actorOverhead.
func actorBooking(memoryBytes int64) int64 {
	const gib = 1 << 30
	perGiB := actorOverhead.GetMemoryBytesPerGib()
	return memoryBytes + actorOverhead.GetMemoryBytes() + memoryBytes/gib*perGiB + memoryBytes%gib*perGiB/gib
}
