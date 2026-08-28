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

package reaper

import (
	"os/exec"
	"testing"
	"time"
)

// TestRunSurvivesAConcurrentReaper verifies the package wiring.
func TestRunSurvivesAConcurrentReaper(t *testing.T) {
	Start()

	// Keep the reaper active throughout the test.
	for range 8 {
		go func() { _ = exec.Command("sh", "-c", "sleep 0.05 & exit 0").Run() }()
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if out, err := RunCombined(exec.Command("sh", "-c", "echo ok")); err != nil {
			t.Fatalf("guarded command failed while the reaper ran: %v", err)
		} else if string(out) != "ok\n" {
			t.Fatalf("guarded command output = %q, want %q", out, "ok\n")
		}
	}
}
