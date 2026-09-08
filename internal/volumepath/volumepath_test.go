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

package volumepath

import (
	"fmt"
	"strings"
	"testing"
)

func TestValidateProjected(t *testing.T) {
	tests := []struct {
		path string
		ok   bool
	}{
		{"ca.pem", true},
		{"certs/ca.pem", true},
		{"méta/имя", true},
		{"with space", true},
		{".hidden", true},
		{"..dots", true},
		{strings.Repeat("d/", MaxSegments-1) + "f", true},
		{"", false},
		{"/etc/passwd", false},
		{"a/", false},
		{"a//b", false},
		{".", false},
		{"..", false},
		{"./a", false},
		{"a/../b", false},
		{"a/./b", false},
		{"a\x00b", false},
		{strings.Repeat("d/", MaxSegments) + "f", false},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%q", tt.path), func(t *testing.T) {
			err := ValidateProjected(tt.path)
			if (err == nil) != tt.ok {
				t.Errorf("ValidateProjected(%q) = %v, want ok=%v", tt.path, err, tt.ok)
			}
		})
	}
}
