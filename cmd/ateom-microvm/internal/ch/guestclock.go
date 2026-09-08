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

import (
	"strconv"
	"strings"
)

// clockAdvanceSince is the first cloud-hypervisor release that catches the guest
// clock up to wall-clock time when restoring a snapshot: on aarch64 it advances
// CNTVCT_EL0 by the downtime, and on x86 it keeps KVM_CLOCK_REALTIME set so the
// kernel adjusts kvmclock at KVM_SET_CLOCK. Before it, a restored guest resumes
// frozen at the instant it was snapshotted and something in the guest has to notice.
var clockAdvanceSince = [3]int{53, 0, 0}

// AdvancesGuestClockOnRestore reports whether this VMM repairs the guest clock
// across a restore by itself.
//
// It reports false when the version cannot be read or parsed, which is the safe
// direction: a caller that wrongly believes the VMM corrects the clock leaves the
// guest reading a stale time after every resume, with no error to show for it.
func (i VMMInfo) AdvancesGuestClockOnRestore() bool {
	v, ok := i.semver()
	if !ok {
		return false
	}
	return compareVersions(v, clockAdvanceSince) >= 0
}

// semver parses the reported version, preferring the semver field ("53.0.0") and
// falling back to the release tag ("v53.0").
func (i VMMInfo) semver() ([3]int, bool) {
	if v, ok := parseVersion(i.Version); ok {
		return v, true
	}
	return parseVersion(i.BuildVersion)
}

// parseVersion reads a dotted version, tolerating a leading "v", a missing patch
// component, and trailing build metadata ("53.0.0-dirty").
func parseVersion(s string) ([3]int, bool) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "v")
	if s == "" {
		return [3]int{}, false
	}
	// Drop any pre-release or build suffix.
	if i := strings.IndexAny(s, "-+"); i >= 0 {
		s = s[:i]
	}

	var out [3]int
	for idx, part := range strings.SplitN(s, ".", 3) {
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			return [3]int{}, false
		}
		out[idx] = n
	}
	return out, true
}

func compareVersions(a, b [3]int) int {
	for i := range a {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}
	return 0
}
