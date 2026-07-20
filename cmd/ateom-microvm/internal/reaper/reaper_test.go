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
	"io"
	"os/exec"
	"testing"
	"time"
)

func TestRunReturnsCommandResult(t *testing.T) {
	if err := Run(exec.Command("true")); err != nil {
		t.Fatalf("Run(true) = %v, want nil", err)
	}
	if err := Run(exec.Command("false")); err == nil {
		t.Fatal("Run(false) = nil, want a non-nil exit error")
	}
}

func TestRunCombinedReturnsOutput(t *testing.T) {
	out, err := RunCombined(exec.Command("sh", "-c", "echo hello"))
	if err != nil {
		t.Fatalf("RunCombined = %v, want nil", err)
	}
	if string(out) != "hello\n" {
		t.Fatalf("RunCombined output = %q, want %q", out, "hello\n")
	}
}

// TestRunHoldsReapLock verifies the invariant that makes Run safe against the child
// reaper (#418): while a guarded command is executing, the exclusive lock the reaper
// takes before wait4(-1) cannot be acquired, so the reaper cannot collect the child
// out from under cmd.Wait (which would surface as "waitid: no child processes").
func TestRunHoldsReapLock(t *testing.T) {
	// cat blocks until its stdin closes, so we control exactly when the guarded
	// command exits — no sleeps, no timing guesses.
	pr, pw := io.Pipe()
	cmd := exec.Command("cat")
	cmd.Stdin = pr

	done := make(chan error, 1)
	go func() { done <- Run(cmd) }()

	// Spin until Run has taken the shared lock (and thus forked the child): while it
	// holds RLock, the exclusive TryLock the reaper would use must fail.
	deadline := time.Now().Add(5 * time.Second)
	for lock.TryLock() {
		lock.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("Run never acquired the reap lock")
		}
		time.Sleep(time.Millisecond)
	}

	// Release the child and confirm Run succeeds and frees the lock afterward.
	_ = pw.Close()
	if err := <-done; err != nil {
		t.Fatalf("guarded command failed: %v", err)
	}
	if !lock.TryLock() {
		t.Fatal("reap lock still held after the guarded command returned")
	}
	lock.Unlock()
	_ = pr.Close()
}
