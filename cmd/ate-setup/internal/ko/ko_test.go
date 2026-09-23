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
	"slices"
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

// A source tarball has no git metadata, and the Makefile falls back to "dev"
// there rather than stamping an empty version.
func TestBuildVersionFallsBackToDev(t *testing.T) {
	t.Setenv("VERSION", "")
	if got := BuildVersion(t.TempDir()); got != "dev" {
		t.Errorf("BuildVersion() = %q, want dev", got)
	}
}
