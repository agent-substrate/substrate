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

// Package dockerenv points testcontainers at the active Docker context.
//
// It lives apart from storetest because storetest imports atepg, so atepg
// cannot import storetest back, and both need this before starting a
// container.
package dockerenv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

// Configure sets the environment testcontainers reads to find Docker, unless
// it is already set. testcontainers-go resolves the daemon from DOCKER_HOST
// and its own probe list; it never consults the Docker CLI context, so a
// working `docker` command is not enough on its own.
func Configure(ctx context.Context) error {
	if os.Getenv("DOCKER_HOST") != "" {
		return nil
	}
	output, err := exec.CommandContext(ctx, "docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}").Output()
	if err != nil {
		return fmt.Errorf("inspecting Docker context: %w", err)
	}
	host := strings.TrimSpace(string(output))
	if host == "" {
		return errors.New("active Docker context has no endpoint")
	}
	if err := os.Setenv("DOCKER_HOST", host); err != nil {
		return fmt.Errorf("setting DOCKER_HOST: %w", err)
	}
	if os.Getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE") == "" {
		socket := host
		if runtime.GOOS == "darwin" {
			// The reaper runs inside the Docker VM and must mount its socket path.
			socket = "/var/run/docker.sock"
		}
		if err := os.Setenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", socket); err != nil {
			return fmt.Errorf("setting TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE: %w", err)
		}
	}
	return nil
}
