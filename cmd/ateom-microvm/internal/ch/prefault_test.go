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

func TestPrefaultsUnconditionally(t *testing.T) {
	for _, tc := range []struct {
		name string
		info VMMInfo
		want bool
	}{
		// Real payloads, as reported by the binaries we ship.
		{"v52 unaffected", VMMInfo{Version: "52.0.0", BuildVersion: "v52.0"}, false},
		{"v53 affected", VMMInfo{Version: "53.0.0", BuildVersion: "v53.0"}, true},
		{"a later release stays affected until a fix is known", VMMInfo{Version: "60.1.2"}, true},
		{"much older", VMMInfo{Version: "41.0.0"}, false},

		// The semver field is preferred, but the release tag is enough on its own.
		{"tag only", VMMInfo{BuildVersion: "v52.0"}, false},
		{"tag only, affected", VMMInfo{BuildVersion: "v53.0"}, true},
		{"suffixed", VMMInfo{Version: "53.0.0-dirty"}, true},

		// Unknown means eager: a wrong guess toward eager costs memory, a wrong guess
		// toward OnDemand leaves the guest unable to pass its readiness probe.
		{"empty", VMMInfo{}, true},
		{"garbage", VMMInfo{Version: "not-a-version"}, true},
		{"partial garbage", VMMInfo{Version: "53.x.0"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.info.PrefaultsUnconditionally(); got != tc.want {
				t.Errorf("PrefaultsUnconditionally() = %v, want %v (info %+v)", got, tc.want, tc.info)
			}
		})
	}
}

// TestPrefaultingUpperBound documents how to retire the workaround: setting the
// exclusive upper bound restores OnDemand on releases that carry the fix.
func TestPrefaultingUpperBound(t *testing.T) {
	orig := prefaultingUntil
	prefaultingUntil = [3]int{55, 0, 0}
	t.Cleanup(func() { prefaultingUntil = orig })

	if !(VMMInfo{Version: "54.0.0"}).PrefaultsUnconditionally() {
		t.Error("54.0.0 is inside the affected range, want prefaulting")
	}
	if (VMMInfo{Version: "55.0.0"}).PrefaultsUnconditionally() {
		t.Error("55.0.0 is at the fix, want OnDemand back")
	}
}

func TestParseVersion(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want [3]int
		ok   bool
	}{
		{"53.0.0", [3]int{53, 0, 0}, true},
		{"v53.0", [3]int{53, 0, 0}, true},
		{"53", [3]int{53, 0, 0}, true},
		{" 52.1.3 ", [3]int{52, 1, 3}, true},
		{"53.0.0+build7", [3]int{53, 0, 0}, true},
		{"", [3]int{}, false},
		{"v", [3]int{}, false},
		{"abc", [3]int{}, false},
		{"53.abc", [3]int{}, false},
	} {
		got, ok := parseVersion(tc.in)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("parseVersion(%q) = %v,%v want %v,%v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}
