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

// Package envtestbins resolves the shared envtest (kubebuilder) binary assets
package envtestbins

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

// BinaryAssetsDir resolves the envtest (kubebuilder) binary assets directory,
// downloading it on first use via `setup-envtest`, and returns its path.
//
// The setup is guarded by a cross-process file lock. `go test ./...` runs the
// envtest-backed packages (cmd/ateapi/internal/controlapi,
// cmd/atecontroller/internal/controllers, pkg/api/v1alpha1) as separate test
// binaries in parallel, and each one calls this during TestMain. On a cold
// cache, concurrent `setup-envtest use` invocations would otherwise race to
// download, extract, and exec the *shared* kubebuilder binaries — intermittently
// failing with "fork/exec .../etcd: text file busy" or "unable to create file
// ... from archive". Serializing the first download makes later callers hit a
// warm cache (a no-op path lookup).
func BinaryAssetsDir() (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", err
	}

	unlock, err := lockEnvtestSetup()
	if err != nil {
		return "", err
	}
	defer unlock()

	cmd := exec.Command("bash", filepath.Join(root, "hack", "run-tool.sh"), "setup-envtest", "use", "--print", "path")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("setup-envtest: %w (stderr: %s)", err, stderr.String())
	}
	return strings.TrimSpace(string(out)), nil
}

// lockEnvtestSetup takes an exclusive cross-process lock on a well-known file,
// returning a function that releases it.
//
// The lock lives at a fixed name under os.TempDir(). The concurrent callers are
// the per-package test binaries spawned by a single `go test ./...`; they
// inherit the same TMPDIR (or all fall back to the same OS default), so
// os.TempDir() resolves to the identical directory in every process and they all
// lock the same file. A machine/user-global location is the right scope here:
// the resource being guarded — the kubebuilder binary cache — is itself
// user-global (shared even across repo checkouts).
func lockEnvtestSetup() (func(), error) {
	lockPath := filepath.Join(os.TempDir(), "substrate-setup-envtest.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening envtest setup lock %s: %w", lockPath, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, fmt.Errorf("acquiring envtest setup lock: %w", err)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

func repoRoot() (string, error) {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("finding repo root for envtest setup: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}
