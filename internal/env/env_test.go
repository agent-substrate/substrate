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

package env

import (
	"os"
	"testing"
)

func TestString(t *testing.T) {
	v := Var[string]{Name: "SUBSTRATE_ENV_TEST", Default: "fallback"}
	t.Setenv(v.Name, "")
	if err := os.Unsetenv(v.Name); err != nil {
		t.Fatal(err)
	}
	if got, present := v.Lookup(); got != "fallback" || present {
		t.Fatalf("unset Lookup = (%q, %v)", got, present)
	}
	for _, raw := range []string{"", "value", "  untrimmed  "} {
		t.Setenv(v.Name, raw)
		if got, present := v.Lookup(); got != raw || !present {
			t.Fatalf("Lookup = (%q, %v), want (%q, true)", got, present, raw)
		}
		if got := v.Get(); got != raw {
			t.Fatalf("Get = %q, want %q", got, raw)
		}
	}
}

func TestBool(t *testing.T) {
	v := Var[bool]{Name: "SUBSTRATE_ENV_TEST", Default: true}
	for _, tc := range []struct {
		raw  string
		want bool
	}{
		{"true", true}, {"1", true}, {"TRUE", true}, {"false", false}, {"0", false}, {"invalid", true}, {"", true},
	} {
		t.Setenv(v.Name, tc.raw)
		if got, present := v.Lookup(); got != tc.want || !present {
			t.Fatalf("Lookup(%q) = (%v, %v), want (%v, true)", tc.raw, got, present, tc.want)
		}
	}
}
