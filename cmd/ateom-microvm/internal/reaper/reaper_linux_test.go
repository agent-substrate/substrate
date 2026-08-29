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
	"sync"
	"testing"
	"time"
)

// TestRunSurvivesAConcurrentReaper verifies the package wiring.
func TestRunSurvivesAConcurrentReaper(t *testing.T) {
	Start()

	deadline := time.Now().Add(5 * time.Second)

	// Orphans for the whole run, not just its first moments: each `sh` exits
	// immediately and leaves its `sleep` behind, so the reaper has something to
	// collect the entire time the guarded commands below are running. Paced,
	// because reaping only happens in the gaps between guarded commands -- an
	// unthrottled generator outruns it and exhausts the pid table with the
	// zombies it has not reached yet.
	var orphans sync.WaitGroup
	for range 4 {
		orphans.Go(func() {
			for time.Now().Before(deadline) {
				_ = exec.Command("sh", "-c", "sleep 0.05 & exit 0").Run()
				time.Sleep(25 * time.Millisecond)
			}
		})
	}
	defer orphans.Wait()

	for time.Now().Before(deadline) {
		if out, err := RunCombined(exec.Command("sh", "-c", "echo ok")); err != nil {
			t.Fatalf("guarded command failed while the reaper ran: %v", err)
		} else if string(out) != "ok\n" {
			t.Fatalf("guarded command output = %q, want %q", out, "ok\n")
		}
		// A moment with nothing in flight, which is when the reaper collects.
		// Without one it would only ever be held off, and the orphans above
		// would go uncollected.
		time.Sleep(5 * time.Millisecond)
	}
}
