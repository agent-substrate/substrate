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

// Package e2fsprogsenv lets tests that check ext4 images with e2fsprogs skip
// on a workstation without it and fail in CI.
package e2fsprogsenv

import (
	"os"
	"os/exec"
	"testing"
)

// Required reports whether missing e2fsprogs tools must fail the test rather
// than skip it. REQUIRE_E2FSPROGS opts a local run into the CI behavior.
func Required() bool {
	return os.Getenv("CI") == "true" || os.Getenv("REQUIRE_E2FSPROGS") == "true"
}

// Need skips t unless every tool is on PATH, or fails it when Required.
func Need(t testing.TB, tools ...string) {
	t.Helper()
	for _, tool := range tools {
		if _, err := exec.LookPath(tool); err != nil {
			if Required() {
				t.Fatalf("%s not found and required (CI or REQUIRE_E2FSPROGS is set), install e2fsprogs: %v", tool, err)
			}
			t.Skipf("%s not found, install e2fsprogs or set REQUIRE_E2FSPROGS=true to fail: %v", tool, err)
		}
	}
}
