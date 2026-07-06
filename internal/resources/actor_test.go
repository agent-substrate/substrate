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

func TestActorDNSName(t *testing.T) {
	got := ActorDNSName("team-a", "act-1")
	want := "act-1.team-a." + ActorDNSSuffix
	if got != want {
		t.Errorf("ActorDNSName() = %q, want %q", got, want)
	}

	// Round-trips through ParseActorDNSName.
	atespace, actorID, err := ParseActorDNSName(got)
	if err != nil || atespace != "team-a" || actorID != "act-1" {
		t.Errorf("round-trip = (%q, %q, %v), want (team-a, act-1, <nil>)", atespace, actorID, err)
	}
}

func TestParseActorDNSName(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantAtespace string
		wantActorID  string
		wantErr      bool
	}{
		{"valid", "act-1.team-a." + ActorDNSSuffix, "team-a", "act-1", false},
		{"valid trailing dot", "act-1.team-a." + ActorDNSSuffix + ".", "team-a", "act-1", false},
		{"wrong suffix", "act-1.team-a.example.com", "", "", true},
		{"missing atespace", "act-1." + ActorDNSSuffix, "", "", true},
		{"invalid actor id", "ACT-1.team-a." + ActorDNSSuffix, "", "", true},
		{"invalid atespace", "act-1.TEAM." + ActorDNSSuffix, "", "", true},
		{"host:port not accepted", "act-1.team-a." + ActorDNSSuffix + ":8080", "", "", true},
		{"empty", "", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atespace, actorID, err := ParseActorDNSName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseActorDNSName(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if atespace != tt.wantAtespace || actorID != tt.wantActorID {
				t.Errorf("ParseActorDNSName(%q) = (%q, %q), want (%q, %q)", tt.input, atespace, actorID, tt.wantAtespace, tt.wantActorID)
			}
		})
	}
}
