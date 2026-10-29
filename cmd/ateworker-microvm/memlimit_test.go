//go:build linux

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

package main

import "testing"

func TestLeafMemoryHigh(t *testing.T) {
	throttled := &AteWorkerService{throttleMemory: true}
	if got, want := throttled.leafMemoryHigh(1<<30), actorBooking(1<<30); got != want {
		t.Errorf("leafMemoryHigh(1GiB) = %d, want the booking %d", got, want)
	}
	if got := throttled.leafMemoryHigh(0); got != 0 {
		t.Errorf("leafMemoryHigh(0) = %d, want 0", got)
	}
	if got := (&AteWorkerService{}).leafMemoryHigh(1 << 30); got != 0 {
		t.Errorf("leafMemoryHigh without PSI = %d, want 0", got)
	}
}
