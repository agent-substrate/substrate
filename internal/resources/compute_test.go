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

package resources

import (
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
)

func TestCovers(t *testing.T) {
	tests := []struct {
		name string
		have *ateapi.Resources
		want *ateapi.Resources
		ok   bool
	}{
		{
			name: "enough of every dimension",
			have: CPUMemory(4000, 8<<30),
			want: CPUMemory(2000, 4<<30),
			ok:   true,
		},
		{
			name: "exactly enough",
			have: CPUMemory(2000, 0),
			want: CPUMemory(2000, 0),
			ok:   true,
		},
		{
			name: "short in one dimension",
			have: CPUMemory(4000, 1<<30),
			want: CPUMemory(2000, 4<<30),
			ok:   false,
		},
		{
			// A Worker reports everything it has, so a dimension it left at
			// zero is one it has none of.
			name: "a dimension the worker never reported is none of it",
			have: CPUMemory(4000, 0),
			want: CPUMemory(2000, 1<<20),
			ok:   false,
		},
		{
			name: "an actor asking for nothing fits anywhere",
			have: nil,
			want: nil,
			ok:   true,
		},
		{
			// What is left goes negative once a Worker is overcommitted, and
			// nothing more may be placed on it.
			name: "an overcommitted dimension covers nothing",
			have: CPUMemory(-500, 0),
			want: CPUMemory(1000, 0),
			ok:   false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Covers(tc.have, tc.want); got != tc.ok {
				t.Errorf("Covers() = %v, want %v", got, tc.ok)
			}
		})
	}
}
