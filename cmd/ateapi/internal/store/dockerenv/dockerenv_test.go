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

package dockerenv

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestConfigureFromContext(t *testing.T) {
	t.Setenv("DOCKER_HOST", "")
	t.Setenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "")
	dir := t.TempDir()
	docker := filepath.Join(dir, "docker")
	if err := os.WriteFile(docker, []byte("#!/bin/sh\nprintf 'unix:///custom/docker.sock\\n'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)

	if err := Configure(context.Background()); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	if got, want := os.Getenv("DOCKER_HOST"), "unix:///custom/docker.sock"; got != want {
		t.Errorf("DOCKER_HOST = %q, want %q", got, want)
	}
	wantSocket := "unix:///custom/docker.sock"
	if runtime.GOOS == "darwin" {
		wantSocket = "/var/run/docker.sock"
	}
	if got := os.Getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"); got != wantSocket {
		t.Errorf("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE = %q, want %q", got, wantSocket)
	}
}

func TestConfigurePreservesEnvironment(t *testing.T) {
	t.Setenv("DOCKER_HOST", "tcp://docker.example:2376")
	t.Setenv("PATH", t.TempDir())

	if err := Configure(context.Background()); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	if got, want := os.Getenv("DOCKER_HOST"), "tcp://docker.example:2376"; got != want {
		t.Errorf("DOCKER_HOST = %q, want %q", got, want)
	}
}
