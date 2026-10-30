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

package upgradepolicy

import (
	"strconv"
	"strings"
	"testing"
)

func TestCheckUpgrade(t *testing.T) {
	maxMinor := strconv.FormatUint(uint64(^uint(0)), 10)
	tests := []struct {
		name, from, to, wantError string
	}{
		{name: "later patch", from: "v1.2.1", to: "v1.2.4"},
		{name: "skipped patches", from: "v1.2.1", to: "v1.2.9"},
		{name: "next minor", from: "v1.2.4", to: "v1.3.0"},
		{name: "next minor later patch", from: "v1.2.4", to: "v1.3.7"},
		{name: "maximum minor patch", from: "v1." + maxMinor + ".0", to: "v1." + maxMinor + ".1"},
		{name: "empty source", to: "v1.2.3", wantError: "source release is empty"},
		{name: "empty target", from: "v1.2.3", wantError: "target release is empty"},
		{name: "invalid source", from: "dev", to: "v1.2.3", wantError: "vMAJOR.MINOR.PATCH"},
		{name: "invalid target", from: "v1.2.3", to: "latest", wantError: "vMAJOR.MINOR.PATCH"},
		{name: "missing v", from: "1.2.3", to: "v1.2.4", wantError: "vMAJOR.MINOR.PATCH"},
		{name: "prerelease", from: "v1.2.3", to: "v1.3.0-rc.1", wantError: "final release"},
		{name: "build metadata", from: "v1.2.3", to: "v1.2.4+build.1", wantError: "final release"},
		{name: "image digest", from: "v1.2.3", to: "v1.2.4@sha256:1234", wantError: "vMAJOR.MINOR.PATCH"},
		{name: "before GA", from: "v0.9.1", to: "v1.0.0", wantError: "applies from v1.0.0"},
		{name: "same release", from: "v1.2.3", to: "v1.2.3", wantError: "must be newer"},
		{name: "older patch", from: "v1.2.3", to: "v1.2.2", wantError: "must be newer"},
		{name: "older minor", from: "v1.2.3", to: "v1.1.9", wantError: "must be newer"},
		{name: "skipped minor", from: "v1.2.3", to: "v1.4.0", wantError: "skips a minor release"},
		{name: "new major", from: "v1.9.0", to: "v2.0.0", wantError: "changes the major version"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := CheckUpgrade(test.from, test.to)
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("CheckUpgrade(%q, %q) error = %v", test.from, test.to, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("CheckUpgrade(%q, %q) error = %v, want %q", test.from, test.to, err, test.wantError)
			}
		})
	}
}

func TestCheckRollback(t *testing.T) {
	tests := []struct {
		name, from, to, upgradeFrom, wantError string
	}{
		{name: "previous patch", from: "v1.2.4", to: "v1.2.1", upgradeFrom: "v1.2.1"},
		{name: "previous minor", from: "v1.3.0", to: "v1.2.4", upgradeFrom: "v1.2.4"},
		{name: "different previous minor", from: "v1.3.0", to: "v1.2.9", upgradeFrom: "v1.2.4", wantError: "does not match recorded source"},
		{name: "skipped minor", from: "v1.4.0", to: "v1.2.4", upgradeFrom: "v1.2.4", wantError: "skips a minor release"},
		{name: "newer target", from: "v1.2.3", to: "v1.2.4", upgradeFrom: "v1.2.4", wantError: "must be newer"},
		{name: "before GA", from: "v1.0.0", to: "v0.9.1", upgradeFrom: "v0.9.1", wantError: "applies from v1.0.0"},
		{name: "missing record", from: "v1.2.4", to: "v1.2.1", wantError: "recorded source release is empty"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := CheckRollback(test.from, test.to, test.upgradeFrom)
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("CheckRollback(%q, %q, %q) error = %v", test.from, test.to, test.upgradeFrom, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("CheckRollback(%q, %q, %q) error = %v, want %q", test.from, test.to, test.upgradeFrom, err, test.wantError)
			}
		})
	}
}
