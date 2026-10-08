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
