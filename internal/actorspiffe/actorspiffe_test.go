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

package actorspiffe

import "testing"

func TestParse(t *testing.T) {
	tests := []struct {
		name         string
		uri          string
		wantAtespace string
		wantActor    string
		wantErr      bool
	}{
		{
			name:         "valid",
			uri:          "spiffe://substrate-actor.local/atespace/team-a/actor/my-actor",
			wantAtespace: "team-a",
			wantActor:    "my-actor",
		},
		{name: "wrong scheme", uri: "https://substrate-actor.local/atespace/team-a/actor/my-actor", wantErr: true},
		{name: "wrong trust domain", uri: "spiffe://other.local/atespace/team-a/actor/my-actor", wantErr: true},
		{name: "wrong structure", uri: "spiffe://substrate-actor.local/ns/team-a/actor/my-actor", wantErr: true},
		{name: "short path", uri: "spiffe://substrate-actor.local/atespace/team-a", wantErr: true},
		{name: "empty", uri: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			atespace, actor, err := Parse(tc.uri)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Parse(%q) = (%q, %q), want error", tc.uri, atespace, actor)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) unexpected error: %v", tc.uri, err)
			}
			if atespace != tc.wantAtespace || actor != tc.wantActor {
				t.Errorf("Parse(%q) = (%q, %q), want (%q, %q)", tc.uri, atespace, actor, tc.wantAtespace, tc.wantActor)
			}
		})
	}
}
