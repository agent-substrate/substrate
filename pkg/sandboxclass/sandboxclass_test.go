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

package sandboxclass

import "testing"

func TestValid(t *testing.T) {
	tests := []struct {
		name  string
		class string
		want  bool
	}{
		{
			name:  "gvisor",
			class: GVisor,
			want:  true,
		},
		{
			name:  "microvm",
			class: MicroVM,
			want:  true,
		},
		{
			name:  "empty",
			class: "",
			want:  false,
		},
		{
			name:  "unknown",
			class: "kata",
			want:  false,
		},
		{
			name:  "case-sensitive",
			class: "GVISOR",
			want:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Valid(tt.class); got != tt.want {
				t.Errorf("Valid(%q) = %v, want %v", tt.class, got, tt.want)
			}
		})
	}
}

func TestAllReturnsCopy(t *testing.T) {
	All()[0] = "mutated"
	if !Valid(GVisor) {
		t.Errorf("mutating the result of All changed Valid(%q)", GVisor)
	}
}
