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

// hack/run-e2e.sh builds a `go test` command line from its own arguments.
// These tests put a stub `go` on PATH, run the script, and check that command
// line. Nothing is built and no cluster is touched.

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// stubGo records its arguments, one per line, and succeeds.
const stubGo = `#!/usr/bin/env bash
printf '%s\n' "$@" > "${RUN_E2E_TEST_LOG}"
`

// runE2EScript runs hack/run-e2e.sh with args. It returns the arguments the
// script passed to `go`, the script's exit status and its combined output.
func runE2EScript(t *testing.T, args ...string) (goArgs []string, exitCode int, output string) {
	t.Helper()

	root, err := FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(stubGo), 0o755); err != nil {
		t.Fatalf("writing the stub go: %v", err)
	}
	log := filepath.Join(t.TempDir(), "go-args")

	script := exec.Command("bash", append([]string{filepath.Join(root, "hack", "run-e2e.sh")}, args...)...)
	script.Dir = root
	// A fixed environment, so a developer's E2E_* or CI settings cannot change
	// the expected command line. git needs PATH, and the stub go must be first.
	script.Env = []string{
		"PATH=" + bin + ":" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"NO_DEV_ENV=1",
		"RUN_E2E_TEST_LOG=" + log,
	}
	out, err := script.CombinedOutput()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
		exitCode = exitErr.ExitCode()
	default:
		t.Fatalf("running run-e2e.sh %v: %v\n%s", args, err, out)
	}

	recorded, err := os.ReadFile(log)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("reading the go argument log: %v", err)
	}
	if len(recorded) > 0 {
		goArgs = strings.Split(strings.TrimSuffix(string(recorded), "\n"), "\n")
	}
	return goArgs, exitCode, string(out)
}

func TestRunE2EScriptArgs(t *testing.T) {
	const (
		networking = "./internal/e2e/suites/networking"
		identity   = "internal/e2e/suites/identity"
		// The script's own defaults for -p and -timeout.
		defaults = "-p 4 -timeout 30m"
	)
	tests := []struct {
		name string
		args []string
		// want is the `go` command line, split on spaces.
		want string
	}{
		{
			name: "default target",
			args: []string{"-args", "--no-color"},
			want: "test -v ./internal/e2e/suites/... " + defaults + " -args --e2e --no-color",
		},
		{
			name: "target before the flags",
			args: []string{networking, "-v", "-args", "--no-color"},
			want: "test -v " + networking + " " + defaults + " -v -args --e2e --no-color",
		},
		{
			name: "target after a flag",
			args: []string{"-v", networking, "-args", "--no-color"},
			want: "test -v " + networking + " " + defaults + " -v -args --e2e --no-color",
		},
		{
			name: "target after a flag and its value",
			args: []string{"-run", "TestX", networking},
			want: "test -v " + networking + " " + defaults + " -run TestX -args --e2e",
		},
		{
			name: "several targets around the flags",
			args: []string{networking, "-count", "2", identity},
			want: "test -v " + networking + " " + identity + " " + defaults + " -count 2 -args --e2e",
		},
		{
			name: "explicit -p and -timeout replace the defaults",
			args: []string{"-timeout", "5m", networking, "-p", "2"},
			want: "test -v " + networking + " -timeout 5m -p 2 -args --e2e",
		},
		{
			name: "a path after -args is an e2e flag value",
			args: []string{networking, "-args", "--storage-class", "./internal/e2e/x"},
			want: "test -v " + networking + " " + defaults + " -args --e2e --storage-class ./internal/e2e/x",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, exitCode, out := runE2EScript(t, test.args...)
			if exitCode != 0 {
				t.Fatalf("run-e2e.sh %v exited %d:\n%s", test.args, exitCode, out)
			}
			if diff := cmp.Diff(strings.Fields(test.want), got); diff != "" {
				t.Errorf("go arguments mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// A flag may come before or after the target, and the command line must not
// change with the order.
func TestRunE2EScriptOrderDoesNotMatter(t *testing.T) {
	const target = "./internal/e2e/suites/networking"
	targetFirst, _, _ := runE2EScript(t, target, "-v", "-args", "--no-color")
	flagFirst, _, _ := runE2EScript(t, "-v", target, "-args", "--no-color")
	if diff := cmp.Diff(targetFirst, flagFirst); diff != "" {
		t.Errorf("target-first and flag-first runs differ (-target-first +flag-first):\n%s", diff)
	}
}

func TestRunE2EScriptRejectsBadFirstArgument(t *testing.T) {
	got, exitCode, out := runE2EScript(t, "networking", "-v")
	if exitCode != 1 {
		t.Errorf("exit status = %d, want 1", exitCode)
	}
	if !strings.Contains(out, "Invalid target path 'networking'") {
		t.Errorf("output = %q, want it to name the invalid target path", out)
	}
	if len(got) != 0 {
		t.Errorf("go ran with %v, want it not to run", got)
	}
}
