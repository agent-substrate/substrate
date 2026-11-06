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

package pathelem

import "testing"

func TestIsPlain(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"uuid", "123e4567-e89b-12d3-a456-426614174000", true},
		{"file name", "runsc", true},
		{"dots inside", "a..b", true},
		{"empty", "", false},
		{"dot", ".", false},
		{"dot dot", "..", false},
		{"separator", "a/b", false},
		{"trailing separator", "a/", false},
		{"absolute", "/a", false},
		{"parent prefix", "../a", false},
		{"nul", "a\x00b", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPlain(tt.in); got != tt.want {
				t.Errorf("IsPlain(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
