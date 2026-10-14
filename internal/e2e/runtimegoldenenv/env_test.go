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

package runtimegoldenenv

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestRequiredAndEnabled(t *testing.T) {
	for _, tc := range []struct {
		name, ci, sandbox, opt string
		required, enabled      bool
	}{
		{"local", "", "microvm", "", false, false},
		{"opt in", "", "microvm", "true", false, true},
		{"microvm CI", "true", "microvm", "", true, true},
		{"gvisor CI", "true", "gvisor", "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CI", tc.ci)
			t.Setenv("E2E_SANDBOX_CLASS", tc.sandbox)
			t.Setenv("E2E_RUNTIME_GOLDEN", tc.opt)
			if Required() != tc.required || Enabled() != tc.enabled {
				t.Fatal("incorrect requirement")
			}
		})
	}
}
func TestRequiredMissingEvidenceFails(t *testing.T) {
	if os.Getenv("RUNTIME_GOLDEN_GUARD_CHILD") == "true" {
		if err := Check(); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Setenv("CI", "true")
	t.Setenv("E2E_SANDBOX_CLASS", "microvm")
	t.Setenv("E2E_RUNTIME_GOLDEN", "")
	t.Setenv("E2E_RUNTIME_LOG_DIR", "")
	t.Setenv("RUNTIME_GOLDEN_GUARD_CHILD", "true")
	cmd := exec.Command(os.Args[0], "-test.run=^TestRequiredMissingEvidenceFails$")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "require E2E_RUNTIME_LOG_DIR") {
		t.Fatalf("strict child should fail: %v %s", err, out)
	}
}
func TestCheckConfigured(t *testing.T) {
	t.Setenv("CI", "true")
	t.Setenv("E2E_SANDBOX_CLASS", "microvm")
	t.Setenv("E2E_RUNTIME_LOG_DIR", t.TempDir())
	if err := Check(); err != nil {
		t.Fatal(err)
	}
}
