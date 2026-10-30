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

package e2fsprogsenv

import "testing"

func TestRequired(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ci      string
		require string
		want    bool
	}{
		{name: "unset", want: false},
		{name: "CI true", ci: "true", want: true},
		{name: "REQUIRE_E2FSPROGS true", require: "true", want: true},
		{name: "CI 1 is not true", ci: "1", want: false},
		{name: "CI false", ci: "false", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CI", tc.ci)
			t.Setenv("REQUIRE_E2FSPROGS", tc.require)
			if got := Required(); got != tc.want {
				t.Errorf("Required() = %v, want %v (CI=%q REQUIRE_E2FSPROGS=%q)", got, tc.want, tc.ci, tc.require)
			}
		})
	}
}
