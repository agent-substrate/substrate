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

package config

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// sourceEnvScript sources path in a bash subshell and prints every exported
// variable NUL-separated. compgen -e is used rather than `env -0` because the
// latter's -0 flag is not portable across the BSD env on macOS.
const sourceEnvScript = `
set -o errexit
source "$1"
for __ate_name in $(compgen -e); do
  printf '%s=%s\0' "${__ate_name}" "${!__ate_name}"
done
`

// sourceShellEnv sources a shell script in a bash subshell and returns the
// exported variables it leaves behind.
//
// This keeps .ate-dev-env.sh working verbatim: developers' files call out to
// gcloud, interpolate ${USER}, and otherwise behave like shell, so
// reimplementing a parser would silently diverge from what the shell installer did.
func sourceShellEnv(path, workdir string) (map[string]string, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("bash", "-c", sourceEnvScript, "bash", path)
	cmd.Dir = workdir
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}

	env := make(map[string]string)
	for _, entry := range strings.Split(stdout.String(), "\x00") {
		if entry == "" {
			continue
		}
		name, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		env[name] = value
	}
	return env, nil
}

// RepoRoot walks up from the working directory to the module root. This
// replaces `git rev-parse --show-toplevel` and works in a source tarball with
// no .git directory.
func RepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("while resolving the working directory: %w", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod found above %s: run ate-setup from inside the repository", dir)
		}
		dir = parent
	}
}
