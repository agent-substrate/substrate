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

// Guest RAM restore modes accepted by vm.restore.
const (
	// MemRestoreOnDemand faults pages in as the guest touches them, so an idle
	// restored guest holds its working set rather than its whole snapshot.
	MemRestoreOnDemand = "OnDemand"
	// MemRestoreEager reads the snapshot's populated extents up front. It registers
	// no userfaultfd, so nothing prefaults and nothing gates a later snapshot.
	MemRestoreEager = "Copy"
)

// prefaultingSince is the first cloud-hypervisor release whose userfaultfd restore
// handler background-prefaults every registered page (PR #8150) and refuses
// vm.snapshot until that finishes (PR #8556, the fix for issue #8525).
var prefaultingSince = [3]int{53, 0, 0}

// prefaultingUntil bounds the affected range, exclusive. It is deliberately open
// ({0,0,0} means "no known fix yet") rather than absent: OnDemand is the mode we
// actually want, so when a release stops prefaulting unconditionally — or lets a
// caller decline it — set this to that version and the smaller idle footprint comes
// back on its own, instead of every future release inheriting the workaround.
var prefaultingUntil = [3]int{0, 0, 0}

// PrefaultsUnconditionally reports whether this VMM prefaults an OnDemand restore.
//
// It reports true when the version cannot be read or parsed. The two ways to be
// wrong are not equal: choosing OnDemand on an affected version leaves the guest
// unable to pass its readiness probe, because the prefault storm starves it, while
// choosing eager on an unaffected one merely costs memory. Callers should log when
// they fall back on an unknown version, since that cost is otherwise invisible.
func (i VMMInfo) PrefaultsUnconditionally() bool {
	v, ok := i.semver()
	if !ok {
		return true
	}
	if compareVersions(v, prefaultingSince) < 0 {
		return false
	}
	if prefaultingUntil != [3]int{0, 0, 0} && compareVersions(v, prefaultingUntil) >= 0 {
		return false
	}
	return true
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
