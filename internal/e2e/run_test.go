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

package e2e

import "testing"

func TestRunCmdOutput(t *testing.T) {
	got := RunCmdOutput(t, []string{"RUN_CMD_TEST=from-env"}, "sh", "-c", `printf '%s' "$RUN_CMD_TEST"`)
	if string(got) != "from-env" {
		t.Errorf("RunCmdOutput = %q, want %q", got, "from-env")
	}
}

func TestRunCmd(t *testing.T) {
	RunCmd(t, "true")
	RunCmdWithEnv(t, []string{"RUN_CMD_TEST=x"}, "sh", "-c", `test "$RUN_CMD_TEST" = x`)
}
