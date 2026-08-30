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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// fakeTB records which of Fatalf and Skipf a policy decision reached.
type fakeTB struct {
	fatal   string
	skipped string
}

func (f *fakeTB) Helper() {}

func (f *fakeTB) Fatalf(format string, args ...any) {
	f.fatal = fmt.Sprintf(format, args...)
}

func (f *fakeTB) Skipf(format string, args ...any) {
	f.skipped = fmt.Sprintf(format, args...)
}

func TestFailOrSkip(t *testing.T) {
	containerErr := errors.New("wait for reaper 7b77f493: context deadline exceeded")
	dockerGone := errors.New("DOCKER_HOST is unset and no Docker context resolved one")

	for _, tc := range []struct {
		name        string
		ci          bool
		unavailable error
		wantFatal   bool
	}{{
		name:        "CI set fails even without Docker",
		ci:          true,
		unavailable: dockerGone,
		wantFatal:   true,
	}, {
		name:      "CI set fails with Docker present",
		ci:        true,
		wantFatal: true,
	}, {
		name:        "Docker absent locally skips",
		unavailable: dockerGone,
	}, {
		name:      "any other error with Docker present fails",
		wantFatal: true,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeTB{}
			failOrSkip(fake, containerErr, tc.ci, func() error { return tc.unavailable })

			if tc.wantFatal {
				if fake.fatal == "" {
					t.Fatalf("failOrSkip() did not fail; skip = %q", fake.skipped)
				}
				if fake.skipped != "" {
					t.Errorf("failOrSkip() also skipped: %q", fake.skipped)
				}
			} else {
				if fake.skipped == "" {
					t.Fatalf("failOrSkip() did not skip; fatal = %q", fake.fatal)
				}
				if fake.fatal != "" {
					t.Errorf("failOrSkip() also failed: %q", fake.fatal)
				}
			}
			message := fake.fatal + fake.skipped
			if !strings.Contains(message, containerErr.Error()) {
				t.Errorf("message %q does not report the underlying error", message)
			}
		})
	}
}

func TestUnavailable(t *testing.T) {
	listening := func(t *testing.T) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "docker.sock")
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	socket := listening(t)
	fallback := listening(t)
	absent := filepath.Join(t.TempDir(), "absent.sock")
	dialErr := errors.New("connection refused")

	for _, tc := range []struct {
		name string
		host string
		// socketPaths stands in for the default and rootless paths
		// testcontainers probes after DOCKER_HOST.
		socketPaths []string
		// refuse names the endpoints whose dial fails; anything dialed and
		// not named here answers.
		refuse        []string
		wantDialed    []string
		wantAvailable bool
	}{{
		name: "no endpoint anywhere",
	}, {
		name:          "listening unix socket",
		host:          "unix://" + socket,
		wantDialed:    []string{"unix " + socket},
		wantAvailable: true,
	}, {
		name:       "unix socket that refuses and no fallback",
		host:       "unix://" + socket,
		refuse:     []string{socket},
		wantDialed: []string{"unix " + socket},
	}, {
		name:          "unix socket that refuses falls back to a default path",
		host:          "unix://" + socket,
		socketPaths:   []string{absent, fallback},
		refuse:        []string{socket},
		wantDialed:    []string{"unix " + socket, "unix " + fallback},
		wantAvailable: true,
	}, {
		name:       "missing unix socket",
		host:       "unix://" + absent,
		wantDialed: nil,
	}, {
		name:          "listening tcp endpoint",
		host:          "tcp://127.0.0.1:2375",
		wantDialed:    []string{"tcp 127.0.0.1:2375"},
		wantAvailable: true,
	}, {
		name:       "tcp endpoint that refuses and no fallback",
		host:       "tcp://127.0.0.1:1",
		refuse:     []string{"127.0.0.1:1"},
		wantDialed: []string{"tcp 127.0.0.1:1"},
	}, {
		name:          "unreachable tcp endpoint falls back to a default path",
		host:          "tcp://127.0.0.1:1",
		socketPaths:   []string{fallback},
		refuse:        []string{"127.0.0.1:1"},
		wantDialed:    []string{"tcp 127.0.0.1:1", "unix " + fallback},
		wantAvailable: true,
	}, {
		name:          "no DOCKER_HOST but a default path answers",
		socketPaths:   []string{absent, fallback},
		wantDialed:    []string{"unix " + fallback},
		wantAvailable: true,
	}, {
		name:        "no DOCKER_HOST and every default path refuses",
		socketPaths: []string{absent, fallback},
		refuse:      []string{fallback},
		wantDialed:  []string{"unix " + fallback},
	}, {
		name:          "scheme this probe cannot dial",
		host:          "npipe:////./pipe/docker_engine",
		wantAvailable: true,
	}, {
		name:          "endpoint without a scheme",
		host:          "/var/run/docker.sock",
		wantAvailable: true,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			var dialed []string
			err := unavailable(tc.host, tc.socketPaths, func(network, address string) error {
				dialed = append(dialed, network+" "+address)
				if slices.Contains(tc.refuse, address) {
					return dialErr
				}
				return nil
			})
			if got := err == nil; got != tc.wantAvailable {
				t.Errorf("unavailable(%q) = %v, want available = %v", tc.host, err, tc.wantAvailable)
			}
			if !slices.Equal(dialed, tc.wantDialed) {
				t.Errorf("dialed %q, want %q", dialed, tc.wantDialed)
			}
		})
	}
}

// TestDefaultSocketPaths checks that the fallback list names the default
// socket, so an unset or dead DOCKER_HOST alone never counts as proof that
// Docker is absent.
func TestDefaultSocketPaths(t *testing.T) {
	paths := defaultSocketPaths()
	if !slices.Contains(paths, "/var/run/docker.sock") {
		t.Errorf("defaultSocketPaths() = %q, want it to include the default socket", paths)
	}
}

// TestUnavailableMemoizes checks that the exported verdict is computed once,
// so a DOCKER_HOST that swallows connections costs one dial timeout for the
// whole run rather than one per failing test.
func TestUnavailableMemoizes(t *testing.T) {
	first := Unavailable()
	if second := Unavailable(); !errors.Is(second, first) && second != first {
		t.Errorf("Unavailable() = %v then %v, want the same verdict", first, second)
	}
}
