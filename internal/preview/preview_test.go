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

package preview

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// resetGlobalsForTest clears the global state.
func resetGlobalsForTest() {
	globalKnownGates = nil
	globalStateMap = nil
	globalInitialized = false
}

func wantPanic(t *testing.T, substr string, fn func()) {
	t.Helper()
	defer func() {
		t.Helper()
		r := recover()
		if r == nil {
			t.Fatalf("got no panic, want one containing %q", substr)
		}
		if msg, _ := r.(string); !strings.Contains(msg, substr) {
			t.Fatalf("panic = %v, want one containing %q", r, substr)
		}
	}()
	fn()
}

func TestInit(t *testing.T) {
	tests := []struct {
		name    string
		values  []string
		wantErr bool
		want    map[string]bool
	}{
		{name: "no values", want: map[string]bool{"Fake": false, "Phony": false}},
		{name: "wildcard", values: []string{"*"}, want: map[string]bool{"Fake": true, "Phony": true}},
		{name: "Fake", values: []string{"Fake"}, want: map[string]bool{"Fake": true, "Phony": false}},
		{name: "Phony", values: []string{"Phony"}, want: map[string]bool{"Fake": false, "Phony": true}},
		{name: "Fake and Phony", values: []string{"Phony", "Fake"}, want: map[string]bool{"Fake": true, "Phony": true}},
		{name: "repeated wildcard", values: []string{"*", "*"}, want: map[string]bool{"Fake": true, "Phony": true}},
		{name: "unknown", values: []string{"Bogus"}, wantErr: true},
		{name: "empty", values: []string{""}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetGlobalsForTest()
			err := doInit([]Gate{"Fake", "Phony"}, tc.values...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Init(%q) error = %v, wantErr %v", tc.values, err, tc.wantErr)
			}
			if tc.wantErr {
				if globalStateMap != nil {
					t.Errorf("Init(%q) failed but set the gates", tc.values)
				}
				return
			}
			if diff := cmp.Diff(tc.want, AsMap()); diff != "" {
				t.Errorf("AsMap() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestInitTwicePanics(t *testing.T) {
	resetGlobalsForTest()
	if err := Init(); err != nil {
		t.Fatalf("Init(): %v", err)
	}
	wantPanic(t, "more than once", func() { _ = Init("*") })
}

func TestIsEnabledKnownGate(t *testing.T) {
	InitForTestFake(t, []Gate{"Fake"}, "*")
	if !IsEnabled(Gate("Fake")) {
		t.Errorf("IsEnabled(unknown) = false, want true")
	}
}

func TestIsEnabledUnknownGate(t *testing.T) {
	InitForTestFake(t, []Gate{"Fake"}, "*")
	if IsEnabled(Gate("Bogus")) {
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
		{values: []string{"*"}},
		{values: []string{"Fake"}},
		{values: []string{"Fake", "Other"}},
		{values: []string{"Bogus"}, wantErr: true},
		{values: []string{"*", "Bogus"}, wantErr: true},
	} {
		if err := doVerify(fakeGates, tc.values...); (err != nil) != tc.wantErr {
			t.Errorf("Verify(%q) = %v, want error %v", tc.values, err, tc.wantErr)
		}
	}
}

func TestSetForTest(t *testing.T) {
	resetGlobalsForTest()
	t.Run("enabled", func(t *testing.T) {
		InitForTestFake(t, []Gate{"Fake"}, "*")
		if !IsEnabled("Fake") {
			t.Errorf("IsEnabled(%q) = false, want true", "Fake")
		}
		t.Run("nested disabled", func(t *testing.T) {
			InitForTest(t)
			if IsEnabled("Fake") {
				t.Errorf("IsEnabled(%q) = true, want false", "Fake")
			}
		})
		if !IsEnabled("Fake") {
			t.Errorf("IsEnabled(%q) after nested test = false, want true", "Fake")
		}
	})
	if globalStateMap != nil {
		t.Errorf("InitForTest did not restore the uninitialized state")
	}
	// Init still works once InitForTest has been undone.
	if err := Init(); err != nil {
		t.Errorf("Init() after InitForTest: %v", err)
	}
}

func TestSetForTestFakeRestoresKnownGates(t *testing.T) {
	resetGlobalsForTest()
	t.Run("fake", func(t *testing.T) {
		InitForTestFake(t, []Gate{"Fake"}, "Fake")
		if err := Verify("Fake"); err != nil {
			t.Errorf("Verify(Fake) with the fake installed = %v", err)
		}
	})
	if err := Verify("Fake"); err == nil {
		t.Errorf("Verify(Fake) after the fake's test ended = nil, want an error: %v", globalStateMap)
	}
}
