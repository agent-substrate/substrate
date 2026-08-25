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

// Package testenv starts the envtest (kubebuilder) control plane shared by
// the repo's apiserver-backed test packages.
package testenv

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// kubeVersion pins the envtest control plane to the newest *installed* 1.36
// patch, downloading one only on a cold cache. Keep the minor in
// step with k8s.io/* in go.mod.
const kubeVersion = "1.36.x"

// Start launches a kube-apiserver+etcd pair with the repo CRDs installed and
// returns a client config plus a stop function. Call it from TestMain.
//
// Under -short it exits the test binary with success instead: a cold binary
// cache needs a download, so `go test -short ./...` always works offline.
func Start() (*rest.Config, func()) {
	if !flag.Parsed() {
		flag.Parse()
	}
	if testing.Short() {
		fmt.Fprintln(os.Stderr, "skipping envtest-backed package in -short mode")
		os.Exit(0)
	}

	root, err := repoRoot()
	if err != nil {
		fatal(err)
	}
	binDir, err := binaryAssetsDir(root)
	if err != nil {
		fatal(err)
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(root, "manifests", "ate-install", "generated")},
		BinaryAssetsDirectory: binDir,
	}
	cfg, err := env.Start()
	if err != nil {
		fatal(fmt.Errorf("envtest start: %w", err))
	}
	return cfg, func() {
		if err := env.Stop(); err != nil {
			fmt.Fprintf(os.Stderr, "envtest stop: %v\n", err)
		}
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "%v\n", err)
	os.Exit(1)
}

// binaryAssetsDir resolves the envtest (kubebuilder) binary assets directory,
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
func binaryAssetsDir(root string) (string, error) {
	unlock, err := lockEnvtestSetup()
	if err != nil {
		return "", err
	}
	defer unlock()

	cmd := exec.Command("bash", filepath.Join(root, "hack", "run-tool.sh"),
		"setup-envtest", "use", kubeVersion, "--print", "path")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("setup-envtest: %w (stderr: %s) — offline with no cached binaries? `go test -short ./...` skips envtest-backed packages", err, stderr.String())
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

// repoRoot walks up from the CWD (the package directory, under `go test`) to
// the directory holding go.mod.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("finding repo root: %w", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("finding repo root: no go.mod above %s", dir)
		}
		dir = parent
	}
}
