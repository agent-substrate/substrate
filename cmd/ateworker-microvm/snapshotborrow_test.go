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

import "testing"

func TestUnbookedMemory(t *testing.T) {
	const limit = 4 << 30
	s := &AteWorkerService{actors: map[string]*hostedActor{
		"a": {memoryBytes: 1 << 30},
		"b": {memoryBytes: 512 << 20},
	}}
	// 4MiB each, plus 8MiB per GiB
	want := int64(limit - memoryReservedBytes - (1<<30 + 4<<20 + 8<<20) - (512<<20 + 4<<20 + 4<<20))
	if got := s.unbookedMemory(limit); got != want {
		t.Errorf("unbookedMemory() = %d, want %d", got, want)
	}
}
