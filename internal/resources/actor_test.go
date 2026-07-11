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
)

func TestActorDNSName(t *testing.T) {
	got := ActorDNSName("team-a", "act-1")
	want := "act-1.team-a." + ActorDNSSuffix
	if got != want {
		t.Errorf("ActorDNSName() = %q, want %q", got, want)
	}

	// Round-trips through ParseActorDNSName.
	atespace, actorName, err := ParseActorDNSName(got)
	if err != nil || atespace != "team-a" || actorName != "act-1" {
		t.Errorf("round-trip = (%q, %q, %v), want (team-a, act-1, <nil>)", atespace, actorName, err)
	}
}

func TestParseActorDNSName(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		wantAtespace  string
		wantActorName string
		wantErr       bool
	}{
		{"valid", "act-1.team-a." + ActorDNSSuffix, "team-a", "act-1", false},
		{"valid trailing dot", "act-1.team-a." + ActorDNSSuffix + ".", "team-a", "act-1", false},
		{"wrong suffix", "act-1.team-a.example.com", "", "", true},
		{"missing atespace", "act-1." + ActorDNSSuffix, "", "", true},
		{"invalid actor name", "ACT-1.team-a." + ActorDNSSuffix, "", "", true},
		{"invalid atespace", "act-1.TEAM." + ActorDNSSuffix, "", "", true},
		{"host:port not accepted", "act-1.team-a." + ActorDNSSuffix + ":8080", "", "", true},
		{"empty", "", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atespace, actorName, err := ParseActorDNSName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseActorDNSName(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if atespace != tt.wantAtespace || actorName != tt.wantActorName {
				t.Errorf("ParseActorDNSName(%q) = (%q, %q), want (%q, %q)", tt.input, atespace, actorName, tt.wantAtespace, tt.wantActorName)
			}
		})
	}
}
