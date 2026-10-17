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
	"runtime"
	"strings"
	"testing"
)

// Under --kind the ambient environment is ignored, but a value the caller named
// on this run is kept. Before the resolver both were discarded alike, because
// nothing recorded which channel a value came from.
func TestKindKeepsExplicitValuesButNotAmbientOnes(t *testing.T) {
	loadEnv(t)
	t.Setenv("BUCKET_NAME", "ambient-cloud-bucket")
	t.Setenv("KO_DEFAULTPLATFORMS", "linux/ambient")

	t.Run("ambient environment is discarded", func(t *testing.T) {
		cfg, err := Load(Options{Kind: true})
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.BucketName != "ate-snapshots" {
			t.Errorf("BucketName = %q, want ate-snapshots", cfg.BucketName)
		}
		if want := "linux/" + runtime.GOARCH; cfg.KODefaultPlatforms != want {
			t.Errorf("KODefaultPlatforms = %q, want %q", cfg.KODefaultPlatforms, want)
		}
	})

	t.Run("a configuration file is kept", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "install.yaml")
		body := "apiVersion: " + FileAPIVersion + "\nkind: " + FileKind + "\n" +
			"storage:\n  bucketName: from-file\n" +
			"ko:\n  defaultPlatforms: linux/from-file\n"
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		fs, err := optionsFlagSet(Options{Kind: true})
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadFlags(fs, LoadOptions{ConfigPath: path})
		if err != nil {
			t.Fatalf("LoadFlags() error = %v", err)
		}
		if cfg.BucketName != "from-file" {
			t.Errorf("BucketName = %q, want from-file", cfg.BucketName)
		}
		if cfg.KODefaultPlatforms != "linux/from-file" {
			t.Errorf("KODefaultPlatforms = %q, want linux/from-file", cfg.KODefaultPlatforms)
		}
	})
}

// The file layer outranks the environment, which the dotfile does not.
func TestLoadFlagsFileOutranksEnvironment(t *testing.T) {
	loadEnv(t)
	t.Setenv("ATE_ATENET_DATAPLANE", RouterAgentgateway)

	path := filepath.Join(t.TempDir(), "install.yaml")
	body := "apiVersion: " + FileAPIVersion + "\nkind: " + FileKind + "\n" +
		"atenet:\n  dataplane: " + RouterEnvoy + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	fs, err := optionsFlagSet(Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFlags(fs, LoadOptions{ConfigPath: path})
	if err != nil {
		t.Fatalf("LoadFlags() error = %v", err)
	}
	if cfg.Router != RouterEnvoy {
		t.Errorf("Router = %q, want %q from the file", cfg.Router, RouterEnvoy)
	}
}

// A validation failure names the channel the offending value came from.
func TestLoadFlagsErrorsNameTheSupplyingChannel(t *testing.T) {
	loadEnv(t)
	t.Setenv("ATE_ATENET_DATAPLANE", "nginx")

	fs, err := optionsFlagSet(Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = LoadFlags(fs, LoadOptions{})
	if err == nil {
		t.Fatal("LoadFlags() = nil error, want an invalid-dataplane error")
	}
	if !strings.Contains(err.Error(), "ATE_ATENET_DATAPLANE") {
		t.Errorf("error %q does not name the environment variable that supplied it", err)
	}
}

func TestLoadFlagsRejectsBadConfigPath(t *testing.T) {
	loadEnv(t)
	fs, err := optionsFlagSet(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFlags(fs, LoadOptions{ConfigPath: filepath.Join(t.TempDir(), "absent.yaml")}); err == nil {
		t.Fatal("LoadFlags() = nil error, want a missing-file error")
	}
}

// The file channel must cross hack/install-ate.sh, which forwards only a fixed
// set of flags but passes the whole environment through. ATE_CONFIG is how it
// gets there without a shim change.
func TestConfigPathFromEnvironment(t *testing.T) {
	loadEnv(t)
	path := filepath.Join(t.TempDir(), "install.yaml")
	body := "apiVersion: " + FileAPIVersion + "\nkind: " + FileKind + "\n" +
		"atenet:\n  dataplane: " + RouterAgentgateway + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Run("environment supplies the document", func(t *testing.T) {
		t.Setenv(ConfigPathEnv, path)
		fs, err := optionsFlagSet(Options{})
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadFlags(fs, LoadOptions{})
		if err != nil {
			t.Fatalf("LoadFlags() error = %v", err)
		}
		if cfg.Router != RouterAgentgateway {
			t.Errorf("Router = %q, want %q from $%s", cfg.Router, RouterAgentgateway, ConfigPathEnv)
		}
	})

	t.Run("the flag outranks the environment", func(t *testing.T) {
		other := filepath.Join(t.TempDir(), "other.yaml")
		otherBody := "apiVersion: " + FileAPIVersion + "\nkind: " + FileKind + "\n" +
			"atenet:\n  dataplane: " + RouterEnvoy + "\n"
		if err := os.WriteFile(other, []byte(otherBody), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Setenv(ConfigPathEnv, path)
		fs, err := optionsFlagSet(Options{})
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadFlags(fs, LoadOptions{ConfigPath: other})
		if err != nil {
			t.Fatalf("LoadFlags() error = %v", err)
		}
		if cfg.Router != RouterEnvoy {
			t.Errorf("Router = %q, want %q from --config", cfg.Router, RouterEnvoy)
		}
	})

	t.Run("unset means no file layer", func(t *testing.T) {
		fs, err := optionsFlagSet(Options{})
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadFlags(fs, LoadOptions{})
		if err != nil {
			t.Fatalf("LoadFlags() error = %v", err)
		}
		if cfg.Router != RouterEnvoy {
			t.Errorf("Router = %q, want the default", cfg.Router)
		}
	})
}

// A relative path means different files from different working directories,
// and the shell installer changes to the repository root. The error names the
// absolute path so that is visible rather than silent.
func TestMissingConfigErrorNamesAbsolutePath(t *testing.T) {
	loadEnv(t)
	t.Setenv(ConfigPathEnv, "no-such-config.yaml")
	fs, err := optionsFlagSet(Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = LoadFlags(fs, LoadOptions{})
	if err == nil {
		t.Fatal("LoadFlags() = nil error, want a missing-file error")
	}
	if !strings.HasPrefix(errPath(err), "/") {
		t.Errorf("error %q does not name an absolute path", err)
	}
}

// errPath pulls the path out of "reading config <path>: ...".
func errPath(err error) string {
	const prefix = "reading config "
	s := err.Error()
	i := strings.Index(s, prefix)
	if i < 0 {
		return ""
	}
	rest := s[i+len(prefix):]
	if j := strings.Index(rest, ":"); j >= 0 {
		return rest[:j]
	}
	return rest
}
