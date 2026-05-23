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

package ateompath

import (
	"strings"
	"testing"
)

func TestValidateAteomSocketPath(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		podName   string
		wantErr   bool
	}{
		{
			name:      "short names well under the limit",
			namespace: "ate-demo-counter",
			podName:   "counter-deployment-abcd1234-xyzw1",
			wantErr:   false,
		},
		{
			name:      "exactly at the limit",
			namespace: strings.Repeat("a", 25),
			podName:   strings.Repeat("b", 45),
			wantErr:   false,
		},
		{
			name:      "one byte over the limit",
			namespace: strings.Repeat("a", 25),
			podName:   strings.Repeat("b", 46),
			wantErr:   true,
		},
		{
			name:      "the reproducer from the original bug report",
			namespace: "ate-demo-lovable-sandbox",
			podName:   "lovable-sandbox-pool-deployment-5797879cd7-2n7wb",
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAteomSocketPath(tt.namespace, tt.podName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateAteomSocketPath(%q, %q) err=%v, wantErr=%v",
					tt.namespace, tt.podName, err, tt.wantErr)
			}
			if tt.wantErr && err != nil {
				// Error message should mention the limit so an operator can
				// figure out by how much to shorten.
				msg := err.Error()
				if !strings.Contains(msg, "107") {
					t.Errorf("error message %q does not reference the limit (107)", msg)
				}
			}
		})
	}
}
