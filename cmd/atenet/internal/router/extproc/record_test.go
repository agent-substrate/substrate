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

package extproc

import (
	"slices"
	"testing"
)

// The ring returns newest first, whether or not it has wrapped.
func TestQueryRecorderNewestFirst(t *testing.T) {
	tests := []struct {
		name string
		size int
		add  int
		want []string
	}{
		{name: "empty", size: 3, add: 0},
		{name: "partly filled", size: 3, add: 2, want: []string{"2", "1"}},
		{name: "exactly full", size: 3, add: 3, want: []string{"3", "2", "1"}},
		{name: "wrapped", size: 3, add: 5, want: []string{"5", "4", "3"}},
		{name: "wrapped twice", size: 2, add: 5, want: []string{"5", "4"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			qr := NewQueryRecorder(tc.size)
			for i := 1; i <= tc.add; i++ {
				qr.Add(RecordedQuery{Target: string(rune('0' + i))})
			}
			var got []string
			for _, q := range qr.Get() {
				got = append(got, q.Target)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("Get() targets = %v, want %v", got, tc.want)
			}
		})
	}
}
