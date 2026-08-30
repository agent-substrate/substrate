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
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// dialTimeout bounds a single endpoint probe. The probe only has to learn
// whether something is listening, so it stays short.
const dialTimeout = 2 * time.Second

// testingTB is the part of [testing.TB] that [FailOrSkip] uses, so the policy
// can be unit tested without a real test binary.
type testingTB interface {
	Helper()
	Fatalf(format string, args ...any)
	Skipf(format string, args ...any)
}

// FailOrSkip ends the test after a testcontainer failed to start.
//
// It fails rather than skips unless Docker is provably absent, so that a
// container problem that is not a missing Docker (a reaper timeout, an image
// pull failure, a daemon hiccup) is reported instead of silently removing the
// tests that need a container from the run. In CI it always fails: the
// runners have Docker, so any container error there is a real failure.
//
// Callers name their container in err, because the messages below are shared
// by every fixture that routes through this policy.
func FailOrSkip(t testing.TB, err error) {
	t.Helper()
	failOrSkip(t, err, inCI(), Unavailable)
}

// inCI reports whether the tests are running in continuous integration.
// GitHub Actions sets both variables on every runner; accepting either keeps
// the strict policy in place in a job that forwards only one of them.
func inCI() bool {
	return os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != ""
}

func failOrSkip(t testingTB, err error, ci bool, unavailable func() error) {
	t.Helper()
	// Each branch returns because the fake testing.TB the unit tests use
	// records the call and keeps going, where a real *testing.T never returns
	// from Fatalf or Skipf.
	if ci {
		t.Fatalf("container startup failed in CI, where Docker is expected: %v", err)
		return
	}
	if reason := unavailable(); reason != nil {
		t.Skipf("container startup failed and Docker is not reachable (%v): %v", reason, err)
		return
	}
	t.Fatalf("container startup failed even though Docker is reachable: %v", err)
}

var (
	unavailableOnce sync.Once
	unavailableErr  error
)

// Unavailable reports why Docker is provably absent, or nil when it is not.
//
// It probes the endpoint [Configure] resolved into DOCKER_HOST and then the
// default and rootless socket paths, because testcontainers does not stop at
// DOCKER_HOST either: it walks its own list and uses the first endpoint that
// answers, so an unset or dead DOCKER_HOST is not on its own proof that
// Docker is missing. Probing means dialing rather than making an API call, so
// it stays cheap, and anything it cannot decide (an endpoint scheme it cannot
// dial, a daemon that accepts the connection but misbehaves) counts as
// available, because only proven absence justifies skipping a test.
//
// The verdict is computed once: a Docker daemon does not appear or disappear
// during a test binary's run, and the fixtures call this from every test that
// hit the shared container error.
func Unavailable() error {
	unavailableOnce.Do(func() {
		unavailableErr = unavailable(os.Getenv("DOCKER_HOST"), defaultSocketPaths(), dial)
	})
	return unavailableErr
}

func unavailable(host string, socketPaths []string, dial func(network, address string) error) error {
	reason := errors.New("DOCKER_HOST is unset and no Docker context resolved one")
	if host != "" {
		reason = unreachable(host, dial)
		if reason == nil {
			return nil
		}
	}
	for _, path := range socketPaths {
		if unreachable("unix://"+path, dial) == nil {
			return nil
		}
	}
	return fmt.Errorf("%w, and no Docker socket answers at %s", reason, strings.Join(socketPaths, ", "))
}

// unreachable reports why nothing answers at a Docker endpoint, or nil when
// something does or when this probe cannot tell.
func unreachable(host string, dial func(network, address string) error) error {
	scheme, address, ok := strings.Cut(host, "://")
	if !ok {
		return nil
	}
	var network string
	switch scheme {
	case "unix":
		if _, err := os.Stat(address); err != nil {
			return fmt.Errorf("no Docker socket at %s: %w", address, err)
		}
		network = "unix"
	case "tcp", "http", "https":
		network = "tcp"
	default:
		// npipe and anything else this probe cannot dial: not proven absent.
		return nil
	}
	if err := dial(network, address); err != nil {
		return fmt.Errorf("dialing Docker endpoint %s: %w", host, err)
	}
	return nil
}

// defaultSocketPaths returns the socket paths testcontainers falls back to
// once DOCKER_HOST and the Docker context have not produced a working
// endpoint, in its order: the default path, then the rootless ones (see
// dockerSocketPath and rootlessDockerSocketPath in
// testcontainers-go/internal/core).
func defaultSocketPaths() []string {
	paths := []string{"/var/run/docker.sock"}
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		paths = append(paths, filepath.Join(dir, "docker.sock"))
	}
	if home, err := os.UserHomeDir(); err == nil {
		paths = append(paths,
			filepath.Join(home, ".docker", "run", "docker.sock"),
			filepath.Join(home, ".docker", "desktop", "docker.sock"),
		)
	}
	return append(paths, filepath.Join("/run", "user", strconv.Itoa(os.Getuid()), "docker.sock"))
}

func dial(network, address string) error {
	conn, err := net.DialTimeout(network, address, dialTimeout)
	if err != nil {
		return err
	}
	return conn.Close()
}
