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

package atenet

import "testing"

func TestParseTargetActor(t *testing.T) {
	tests := []struct {
		name          string
		value         string
		wantAtespace  string
		wantActorName string
		wantErr       bool
	}{
		{name: "valid", value: "team-a/actor-1", wantAtespace: "team-a", wantActorName: "actor-1"},
		{name: "missing separator", value: "team-a", wantErr: true},
		{name: "extra separator", value: "team-a/actor-1/extra", wantErr: true},
		{name: "empty atespace", value: "/actor-1", wantErr: true},
		{name: "empty actor", value: "team-a/", wantErr: true},
		{name: "invalid atespace", value: "TEAM-A/actor-1", wantErr: true},
		{name: "invalid actor", value: "team-a/ACTOR-1", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTargetActor(tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseTargetActor(%q) error = %v, wantErr %v", tt.value, err, tt.wantErr)
			}
			if got.Atespace != tt.wantAtespace || got.Name != tt.wantActorName {
				t.Errorf("ParseTargetActor(%q) = %q, want %q/%q", tt.value, got.String(), tt.wantAtespace, tt.wantActorName)
			}
		})
	}
}
