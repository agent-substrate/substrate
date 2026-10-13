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
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
)

// devEnvMarker is exported by the developer environment file the tests write.
// It is a name loadEnv does not blank: mergeDevEnv fills only keys absent from
// the environment, so a variable blanked for isolation would never show the
// file had been read.
const devEnvMarker = "ATE_TEST_DEV_ENV_MARKER"

// fakeRepo makes a directory RepoRoot will stop at, holding a developer
// environment file. A Kind install must not source it, whichever channel
// selected Kind.
//
// It also clears NO_DEV_ENV, which loadEnv sets: these tests are about the
// file being read, so the switch that skips it unconditionally has to be off.
func fakeRepo(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	for name, body := range map[string]string{
		"go.mod":   "module fake\n",
		devEnvFile: "export " + devEnvMarker + "=sourced\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
	}
	unsetEnv(t, "NO_DEV_ENV")
	t.Chdir(dir)
}

// Selecting Kind has to suppress the developer environment from every channel
// that can select it. Answering the question from the flag and the environment
// alone left a document that set kindCluster.enabled sourcing the file anyway,
// pointing a local install at the project it names.
func TestKindSuppressesTheDevEnvironmentFromEveryChannel(t *testing.T) {
	for _, tc := range []struct {
		name     string
		env      map[string]string
		file     string
		opts     Options
		wantKind bool
	}{
		{name: "not selected", wantKind: false},
		{name: "flag", opts: Options{Kind: true}, wantKind: true},
		{name: "environment", env: map[string]string{"ATE_INSTALL_KIND": "true"}, wantKind: true},
		{
			name:     "file",
			file:     "kindCluster:\n  enabled: true\n",
			wantKind: true,
		},
		{
			// "1" is a bool the registry accepts, so the channel that answers
			// the Kind question has to accept it too.
			name:     "environment spelled 1",
			env:      map[string]string{"ATE_INSTALL_KIND": "1"},
			wantKind: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loadEnv(t)
			fakeRepo(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if tc.file != "" {
				t.Setenv(ConfigPathEnv, writeConfig(t, validHeader+tc.file))
			}

			cfg, err := Load(tc.opts)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.Kind != tc.wantKind {
				t.Fatalf("Kind = %v, want %v", cfg.Kind, tc.wantKind)
			}

			sourced := cfg.shellEnv[devEnvMarker] == "sourced"
			if tc.wantKind && sourced {
				t.Error("the developer environment was sourced for a Kind install")
			}
			if !tc.wantKind && !sourced {
				t.Error("the developer environment was not sourced for a non-Kind install")
			}
		})
	}
}

// An explicit --kind=false is a deliberate override of an inherited variable.
// Treating only a true flag as an answer let the environment win.
func TestKindFalseFlagOverridesTheEnvironment(t *testing.T) {
	loadEnv(t)
	fakeRepo(t)
	t.Setenv("ATE_INSTALL_KIND", "true")

	fs := optionsFlagSetForTest(t)
	if err := fs.Set("kind", "false"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	cfg, err := LoadFlags(fs, LoadOptions{})
	if err != nil {
		t.Fatalf("LoadFlags() error = %v", err)
	}
	if cfg.Kind {
		t.Error("Kind = true; an explicit --kind=false must outrank ATE_INSTALL_KIND")
	}
	if cfg.shellEnv[devEnvMarker] != "sourced" {
		t.Error("the developer environment was skipped even though Kind was turned off")
	}
}

func optionsFlagSetForTest(t *testing.T) *pflag.FlagSet {
	t.Helper()
	fs, err := optionsFlagSet(Options{})
	if err != nil {
		t.Fatalf("optionsFlagSet: %v", err)
	}
	return fs
}
