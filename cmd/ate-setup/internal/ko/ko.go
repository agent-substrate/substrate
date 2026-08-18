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

// Package ko builds and publishes images referenced by manifests, replacing
// the run_ko helper in the shell installer.
//
// ko stays an external binary. It is pinned as a Go tool in hack/tools/ko and
// located the same way hack/run-tool.sh locates it, so ate-setup and the
// remaining shell scripts build with identical ko versions. Only `ko resolve`
// is ever invoked: the resolved manifest is applied through client-go, which
// removes the need for ko's kubectl delegation and the `-- --context=` special
// case that run_ko carried.
package ko

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// versionPkg is the package whose Version variable receives the build stamp.
// It matches VERSION_PKG in the Makefile.
const versionPkg = "github.com/agent-substrate/substrate/internal/version"

// Runner invokes ko with a fixed repository root and environment.
type Runner struct {
	// Root is the repository root; ko runs with this as its working directory.
	Root string
	// Env holds extra environment entries such as KO_DOCKER_REPO.
	Env []string
	// Stderr receives ko's build progress output.
	Stderr *os.File

	binary string
}

// New returns a Runner, resolving the ko binary once up front so a missing
// tool fails before any cluster mutation happens.
func New(root string, env []string) (*Runner, error) {
	binary, err := findBinary(root)
	if err != nil {
		return nil, err
	}
	return &Runner{Root: root, Env: env, Stderr: os.Stderr, binary: binary}, nil
}

// findBinary resolves the pinned ko tool binary, mirroring hack/run-tool.sh.
func findBinary(root string) (string, error) {
	toolDir := filepath.Join(root, "hack", "tools", "ko")
	cmd := exec.Command("go", "tool", "-n", "ko")
	cmd.Dir = toolDir

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("while locating the ko tool in %s: %w: %s",
			toolDir, err, strings.TrimSpace(stderr.String()))
	}

	binary := strings.TrimSpace(stdout.String())
	if binary == "" {
		return "", fmt.Errorf("go tool -n ko produced no path in %s", toolDir)
	}
	return binary, nil
}

// Resolve builds and publishes the images referenced by the manifest read from
// path (or from stdinManifest when path is "-") and returns the manifest with
// image references replaced by digests.
func (r *Runner) Resolve(ctx context.Context, path string, stdinManifest []byte) ([]byte, error) {
	args := []string{"resolve", "-f", path}
	for _, flag := range r.ldflags() {
		args = append(args, "--ldflags="+flag)
	}

	cmd := exec.CommandContext(ctx, r.binary, args...)
	cmd.Dir = r.Root
	cmd.Env = append(os.Environ(), r.Env...)
	if path == "-" {
		cmd.Stdin = bytes.NewReader(stdinManifest)
	}

	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	// ko writes build progress to stderr; pass it through so a slow first
	// build does not look like a hang.
	cmd.Stderr = r.Stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("while running ko resolve -f %s: %w", path, err)
	}
	return stdout.Bytes(), nil
}

// ResolvePath resolves a manifest file or directory.
func (r *Runner) ResolvePath(ctx context.Context, path string) ([]byte, error) {
	return r.Resolve(ctx, path, nil)
}

// ResolveBytes resolves an in-memory manifest, such as kustomize output.
func (r *Runner) ResolveBytes(ctx context.Context, manifest []byte) ([]byte, error) {
	return r.Resolve(ctx, "-", manifest)
}

// ldflags returns the version stamp ko should bake into binaries. The shell
// scripts shelled out to `make ldflags` for this; computing it here keeps the
// value identical without depending on make.
func (r *Runner) ldflags() []string {
	return []string{fmt.Sprintf("-X=%s.Version=%s", versionPkg, r.version())}
}

// version mirrors the Makefile's VERSION: `git describe`, or "dev" when git
// has nothing to say.
func (r *Runner) version() string {
	if v := os.Getenv("VERSION"); v != "" {
		return v
	}
	cmd := exec.Command("git", "describe", "--tags", "--always", "--dirty")
	cmd.Dir = r.Root
	out, err := cmd.Output()
	if err != nil {
		return "dev"
	}
	if v := strings.TrimSpace(string(out)); v != "" {
		return v
	}
	return "dev"
}
