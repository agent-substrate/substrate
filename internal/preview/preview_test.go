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

// resetForTest clears the gates so Init can be called, restoring them after.
func resetForTest(t *testing.T) {
	t.Helper()
	prev := stateMap
	stateMap = nil
	t.Cleanup(func() { stateMap = prev })
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
		{name: "no values", want: map[string]bool{"Preview": false}},
		{name: "wildcard", values: []string{"*"}, want: map[string]bool{"Preview": true}},
		{name: "Preview", values: []string{"*"}, want: map[string]bool{"Preview": true}},
		{name: "repeated wildcard", values: []string{"*", "*"}, want: map[string]bool{"Preview": true}},
		{name: "unknown", values: []string{"Bogus"}, wantErr: true},
		{name: "empty", values: []string{""}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetForTest(t)
			err := Init(tc.values...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Init(%q) error = %v, wantErr %v", tc.values, err, tc.wantErr)
			}
			if tc.wantErr {
				if stateMap != nil {
					t.Errorf("Init(%q) failed but set the gates", tc.values)
				}
				return
			}
			if diff := cmp.Diff(tc.want, AsMap()); diff != "" {
				t.Errorf("AsMap() mismatch (-want +got):\n%s", diff)
			}
			if got, want := IsEnabled(GatePreview), tc.want["Preview"]; got != want {
				t.Errorf("IsEnabled(%q) = %v, want %v", GatePreview, got, want)
			}
		})
	}
}

func TestInitTwicePanics(t *testing.T) {
	resetForTest(t)
	if err := Init(); err != nil {
		t.Fatalf("Init(): %v", err)
	}
	wantPanic(t, "more than once", func() { _ = Init("*") })
	if IsEnabled(GatePreview) {
		t.Errorf("second Init changed the gates")
	}
}

func TestIsEnabledKnownGate(t *testing.T) {
	SetForTest(t, "*")
	if !IsEnabled(Gate("Preview")) {
		t.Errorf("IsEnabled(unknown) = false, want true")
	}
}

func TestIsEnabledUnknownGate(t *testing.T) {
	SetForTest(t, "*")
	if IsEnabled(Gate("Bogus")) {
		t.Errorf("IsEnabled(unknown) = true, want false")
	}
}

func TestSetForTest(t *testing.T) {
	resetForTest(t)
	t.Run("enabled", func(t *testing.T) {
		SetForTest(t, "*")
		if !IsEnabled(GatePreview) {
			t.Errorf("IsEnabled(%q) = false, want true", GatePreview)
		}
		t.Run("nested disabled", func(t *testing.T) {
			SetForTest(t)
			if IsEnabled(GatePreview) {
				t.Errorf("IsEnabled(%q) = true, want false", GatePreview)
			}
		})
		if !IsEnabled(GatePreview) {
			t.Errorf("IsEnabled(%q) after nested test = false, want true", GatePreview)
		}
	})
	if stateMap != nil {
		t.Errorf("SetForTest did not restore the uninitialized state")
	}
	// Init still works once SetForTest has been undone.
	if err := Init(); err != nil {
		t.Errorf("Init() after SetForTest: %v", err)
	}
}
