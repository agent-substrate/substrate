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
	"strings"
	"testing"
)

func TestValidateActorID(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{"valid lowercase", "my-actor-1", false},
		{"valid single char", "a", false},
		{"missing id", "", true},
		{"invalid uppercase", "My-Actor", true},
		{"invalid start hyphen", "-actor", true},
		{"valid start number", "1actor", false},
		{"invalid end hyphen", "actor-", true},
		{"invalid special chars", "actor@1", true},
		{"invalid length", strings.Repeat("a", 64), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateActorID(tt.id); (err != nil) != tt.wantErr {
				t.Errorf("ValidateActorID() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
