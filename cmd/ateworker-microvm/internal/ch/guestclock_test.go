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

package ch

import "testing"

func TestAdvancesGuestClockOnRestore(t *testing.T) {
	for _, tc := range []struct {
		name string
		info VMMInfo
		want bool
	}{
		// Real payloads, as reported by the binaries we ship. Measured: with chronyd
		// masked and 150s of downtime, a v53 guest resumes reading the correct wall
		// clock and a v52 guest is 160s behind.
		{"v52 does not", VMMInfo{Version: "52.0.0", BuildVersion: "v52.0"}, false},
		{"v53 does", VMMInfo{Version: "53.0.0", BuildVersion: "v53.0"}, true},
		{"later releases keep the fix", VMMInfo{Version: "60.1.2"}, true},
		{"much older", VMMInfo{Version: "41.0.0"}, false},

		// The semver field is preferred, but the release tag is enough on its own.
		{"tag only", VMMInfo{BuildVersion: "v52.0"}, false},
		{"tag only, fixed", VMMInfo{BuildVersion: "v53.0"}, true},
		{"suffixed", VMMInfo{Version: "53.0.0-dirty"}, true},

		// Unknown means "assume it does not": believing a VMM corrects the clock when
		// it does not leaves every resumed guest reading a stale time, silently.
		{"empty", VMMInfo{}, false},
		{"garbage", VMMInfo{Version: "not-a-version"}, false},
		{"partial garbage", VMMInfo{Version: "53.x.0"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.info.AdvancesGuestClockOnRestore(); got != tc.want {
				t.Errorf("AdvancesGuestClockOnRestore() = %v, want %v", got, tc.want)
			}
		})
	}
}
