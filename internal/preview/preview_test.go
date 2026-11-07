// Copyright 2026 The Agent Substrate Authors
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

package preview

import (
	"regexp"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		values  []string
		wantErr bool
		want    map[string]bool
	}{
		{name: "no values", want: map[string]bool{"Fake": false, "Phony": false}},
		{name: "ALL", values: []string{"ALL"}, want: map[string]bool{"Fake": true, "Phony": true}},
		{name: "Fake", values: []string{"Fake"}, want: map[string]bool{"Fake": true, "Phony": false}},
		{name: "Phony", values: []string{"Phony"}, want: map[string]bool{"Fake": false, "Phony": true}},
		{name: "Fake and Phony", values: []string{"Phony", "Fake"}, want: map[string]bool{"Fake": true, "Phony": true}},
		{name: "repeated ALL", values: []string{"ALL", "ALL"}, want: map[string]bool{"Fake": true, "Phony": true}},
		{name: "unknown", values: []string{"Bogus"}, wantErr: true},
		{name: "empty", values: []string{""}, wantErr: true},
		{name: "star is not a wildcard", values: []string{"*"}, wantErr: true},
		{name: "lowercase all", values: []string{"all"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g, err := newGates([]Gate{"Fake", "Phony"}, tc.values...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("newGates(%q) error = %v, wantErr %v", tc.values, err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if diff := cmp.Diff(tc.want, g.AsMap()); diff != "" {
				t.Errorf("AsMap() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

var pascalCase = regexp.MustCompile(`^[A-Z][a-z0-9]+(?:[A-Z][a-z0-9]*)*$`)

func TestRealGatesArePascalCase(t *testing.T) {
	// Sanity-check the pattern itself.
	for _, s := range []string{"ExternalVolumes", "Foo", "FooV2", "FooBar2Baz"} {
		if !pascalCase.MatchString(s) {
			t.Errorf("pattern rejected valid name %q", s)
		}
	}
	for _, s := range []string{"", "externalVolumes", "External_Volumes", "External-Volumes", "EXTERNAL", All, "F", "Foo Bar"} {
		if pascalCase.MatchString(s) {
			t.Errorf("pattern accepted invalid name %q", s)
		}
	}

	if len(realKnownGates) == 0 {
		t.Fatal("no real gates defined")
	}
	for i, g := range realKnownGates {
		if !pascalCase.MatchString(string(g)) {
			t.Errorf("gate %q is not PascalCase", g)
		}
		if i > 0 && realKnownGates[i-1] >= g {
			t.Errorf("realKnownGates not sorted: %q before %q", realKnownGates[i-1], g)
		}
	}
}

func TestIsEnabledKnownGate(t *testing.T) {
	g := NewForTestFake(t, []Gate{"Fake"}, "ALL")
	if !g.IsEnabled(Gate("Fake")) {
		t.Errorf("IsEnabled(Fake) = false, want true")
	}
}

func TestIsEnabledUnknownGate(t *testing.T) {
	g := NewForTestFake(t, []Gate{"Fake"}, "ALL")
	if g.IsEnabled(Gate("Bogus")) {
		t.Errorf("IsEnabled(unknown) = true, want false")
	}
}

func TestVerify(t *testing.T) {
	fakeGates := []Gate{"Fake", "Other"}

	for _, tc := range []struct {
		values  []string
		wantErr bool
	}{
		{values: nil},
		{values: []string{"ALL"}},
		{values: []string{"Fake"}},
		{values: []string{"Fake", "Other"}},
		{values: []string{"Bogus"}, wantErr: true},
		{values: []string{"ALL", "Bogus"}, wantErr: true},
	} {
		if err := verify(fakeGates, tc.values...); (err != nil) != tc.wantErr {
			t.Errorf("Verify(%q) = %v, want error %v", tc.values, err, tc.wantErr)
		}
	}
}

func TestEnabled(t *testing.T) {
	g := NewForTestFake(t, []Gate{"C", "A", "B"}, "C", "A")
	if diff := cmp.Diff([]string{"A", "C"}, g.Enabled()); diff != "" {
		t.Errorf("Enabled() mismatch (-want +got):\n%s", diff)
	}
}
