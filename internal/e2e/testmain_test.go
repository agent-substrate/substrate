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

package e2e

import "testing"

func TestExternalVolumeClassRequired(t *testing.T) {
	for _, tc := range []struct {
		name, ci, require string
		want              bool
	}{
		{name: "neither set", want: false},
		{name: "CI", ci: "true", want: true},
		{name: "REQUIRE_EXTERNAL_VOLUME_CLASS", require: "true", want: true},
		{name: "both", ci: "true", require: "true", want: true},
		{name: "not true", ci: "1", require: "yes", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CI", tc.ci)
			t.Setenv("REQUIRE_EXTERNAL_VOLUME_CLASS", tc.require)
			if got := ExternalVolumeClassRequired(); got != tc.want {
				t.Errorf("ExternalVolumeClassRequired() = %v, want %v", got, tc.want)
			}
		})
	}
}
