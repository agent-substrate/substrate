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

func TestParseCapacity(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"8Gi", 8 << 30},
		{"512Mi", 512 << 20},
		{"1000", 1000},
	} {
		got, err := parseCapacity(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("parseCapacity(%q) = %d, %v; want %d", tc.in, got, err, tc.want)
		}
	}
	for _, in := range []string{"", "0", "-1Gi", "0.5", "1m", "lots"} {
		if got, err := parseCapacity(in); err == nil {
			t.Errorf("parseCapacity(%q) = %d, want an error", in, got)
		}
	}
}
