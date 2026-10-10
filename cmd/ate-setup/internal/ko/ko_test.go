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

package ko

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Without --base-import-paths ko publishes cmd/atelet as "atelet-<md5>", and
// images.ImageName -- which is how an --image-repo install addresses the same
// component -- would be naming something that was never pushed. Nothing in an
// install from source fails when the flag goes missing, because ko writes the
// digests it just published straight into the manifest.
func TestArgsAlwaysRequestBaseImportPaths(t *testing.T) {
	// Keep the version stamp off git, so the flags are comparable.
	t.Setenv("VERSION", "v0.0.0-test")
	r := &Runner{Root: t.TempDir()}

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"resolve", r.args("resolve", "-f", "manifests/ate-install")},
		{"resolve from stdin", r.args("resolve", "-f", "-")},
		{"build", r.args("build", "github.com/agent-substrate/substrate/cmd/ateom-gvisor")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !slices.Contains(tc.args, "--base-import-paths") {
				t.Errorf("args = %v, want --base-import-paths", tc.args)
			}
			if !slices.Contains(tc.args, "--ldflags=-X=github.com/agent-substrate/substrate/internal/version.Version=v0.0.0-test") {
				t.Errorf("args = %v, want the version stamp", tc.args)
			}
		})
	}
}

// The subcommand has to stay first, and its target has to stay with it: ko
// takes the import path as a positional argument.
func TestArgsKeepTheSubcommandAndTargetInFront(t *testing.T) {
	r := &Runner{Root: t.TempDir()}

	args := r.args("resolve", "-f", "-")
	if got := args[:3]; !slices.Equal(got, []string{"resolve", "-f", "-"}) {
		t.Errorf("args[:3] = %v, want [resolve -f -]", got)
	}
}

func TestBuildVersionPrefersTheEnvironment(t *testing.T) {
	t.Setenv("VERSION", "v1.2.3")
	if got := BuildVersion(t.TempDir()); got != "v1.2.3" {
		t.Errorf("BuildVersion() = %q, want v1.2.3", got)
	}
}

// A source tarball has no git metadata, so BuildVersion falls back to "dev"
// there rather than stamping an empty version.
func TestBuildVersionFallsBackToDev(t *testing.T) {
	t.Setenv("VERSION", "")
	if got := BuildVersion(t.TempDir()); got != "dev" {
		t.Errorf("BuildVersion() = %q, want dev", got)
	}
}

// fakeKo writes a ko stand-in that records its arguments, one per line, and
// prints stdout.
func fakeKo(t *testing.T, stdout string) (binary, argsFile string) {
	t.Helper()
	dir := t.TempDir()
	argsFile = filepath.Join(dir, "args")
	binary = filepath.Join(dir, "ko")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > " + argsFile + "\nprintf '" + stdout + "'\n"
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return binary, argsFile
}

// A release is only installable if every image carries the tag and the
// --base-import-paths name that --image-repo looks it up by.
func TestBuildTagged(t *testing.T) {
	t.Setenv("VERSION", "v1.2.3")
	binary, argsFile := fakeKo(t, "repo/ateapi:v1.2.3@sha256:a\\nrepo/atelet:v1.2.3@sha256:b\\n")
	r := &Runner{Root: t.TempDir(), Stderr: os.Stderr, binary: binary}

	refs, err := r.BuildTagged(t.Context(), "v1.2.3", "./cmd/ateapi", "./cmd/atelet")
	if err != nil {
		t.Fatalf("BuildTagged() error = %v", err)
	}
	if want := []string{"repo/ateapi:v1.2.3@sha256:a", "repo/atelet:v1.2.3@sha256:b"}; !slices.Equal(refs, want) {
		t.Errorf("BuildTagged() = %q, want %q", refs, want)
	}
	raw, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Fields(string(raw))
	if got := args[:3]; !slices.Equal(got, []string{"build", "./cmd/ateapi", "./cmd/atelet"}) {
		t.Errorf("args[:3] = %v, want [build ./cmd/ateapi ./cmd/atelet]", got)
	}
	for _, want := range []string{"--tags=v1.2.3", "--base-import-paths"} {
		if !slices.Contains(args, want) {
			t.Errorf("args = %v, want %s", args, want)
		}
	}
}

// A ref missing from ko's output would leave a release short an image, so it
// fails the build rather than printing an incomplete list.
func TestBuildTaggedRejectsMissingRefs(t *testing.T) {
	binary, _ := fakeKo(t, "repo/ateapi:v1@sha256:a\\n")
	r := &Runner{Root: t.TempDir(), Stderr: os.Stderr, binary: binary}

	if _, err := r.BuildTagged(t.Context(), "v1", "./cmd/ateapi", "./cmd/atelet"); err == nil {
		t.Error("BuildTagged() error = nil, want one for 1 ref from 2 packages")
	}
}
