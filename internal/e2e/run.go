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

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// FindRepoRoot traverses directories upward starting from the current working
// directory to locate the repository root containing the go.mod file.
func FindRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get working directory: %w", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("repository root containing go.mod not found")
		}
		dir = parent
	}
}

// RunCmd executes the given command with arguments, piping stdout and stderr
// to standard outputs, and fails the test if the command returns an error.
func RunCmd(t *testing.T, name string, args ...string) {
	t.Helper()
	t.Logf("Running command: %s %s", name, strings.Join(args, " "))
	cmd := exec.Command(name, args...)
	stdoutColor := &ColorWriter{W: os.Stdout, ANSI: ansiCyan}
	cmd.Stdout = NewIndentWriter(stdoutColor, "        ")
	stderrColor := &ColorWriter{W: os.Stderr, ANSI: ansiRed}
	cmd.Stderr = NewIndentWriter(stderrColor, "        ")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Command failed: %s %s: %v", name, strings.Join(args, " "), err)
	}
}

// RunCmdWithEnv executes the given command with custom environment variables
// appended to the current process environment, and fails the test if it returns an error.
func RunCmdWithEnv(t *testing.T, env []string, name string, args ...string) {
	t.Helper()
	t.Logf("Running command with custom env: %s %s", name, strings.Join(args, " "))
	cmd := exec.Command(name, args...)
	cmd.Env = append(os.Environ(), env...)
	stdoutColor := &ColorWriter{W: os.Stdout, ANSI: ansiCyan}
	cmd.Stdout = NewIndentWriter(stdoutColor, "        ")
	stderrColor := &ColorWriter{W: os.Stderr, ANSI: ansiRed}
	cmd.Stderr = NewIndentWriter(stderrColor, "        ")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Command failed: %s %s: %v", name, strings.Join(args, " "), err)
	}
}
