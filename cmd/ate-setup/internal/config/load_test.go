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
	"time"
)

// Under --kind the ambient environment is ignored, but a value the caller named
// on this run is kept. Before the resolver both were discarded alike, because
// nothing recorded which channel a value came from.
func TestKindKeepsExplicitValuesButNotAmbientOnes(t *testing.T) {
	loadEnv(t)
	t.Setenv("BUCKET_NAME", "ambient-cloud-bucket")
	t.Setenv("KO_DEFAULTPLATFORMS", "linux/ambient")
	t.Setenv("SNAPSHOT_LOCATION", "gs://ambient-cloud-bucket/")

	t.Run("ambient environment is discarded", func(t *testing.T) {
		cfg, err := Load(Options{Kind: true})
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.BucketName != "ate-snapshots" {
			t.Errorf("BucketName = %q, want ate-snapshots", cfg.BucketName)
		}
		if cfg.SnapshotLocation != "gs://ate-snapshots/" {
			t.Errorf("SnapshotLocation = %q, want gs://ate-snapshots/", cfg.SnapshotLocation)
		}
		if want := "linux/" + runtime.GOARCH; cfg.KODefaultPlatforms != want {
			t.Errorf("KODefaultPlatforms = %q, want %q", cfg.KODefaultPlatforms, want)
		}
	})

	t.Run("a configuration file is kept", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "install.yaml")
		body := "apiVersion: " + FileAPIVersion + "\nkind: " + FileKind + "\n" +
			"storage:\n  bucketName: from-file\n  snapshotLocation: gs://from-file/prefix/\n" +
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
		if cfg.SnapshotLocation != "gs://from-file/prefix/" {
			t.Errorf("SnapshotLocation = %q, want gs://from-file/prefix/", cfg.SnapshotLocation)
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

// ATE_CONFIG names the document when --config is absent, and the flag
// outranks it.
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

// A relative path means different files from different working directories.
// The error names the absolute path so that is visible rather than silent.
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

func TestLoadFlagsRouterSettings(t *testing.T) {
	loadEnv(t)
	t.Setenv("ATE_ATENET_ROUTE_TIMEOUT", "30m")
	t.Setenv("ATE_ATENET_PARKED_REQUEST_BUDGET", "2m")
	t.Setenv("ATE_ATENET_EXTPROC_MAX_REQUESTS", "4096")

	fs, err := optionsFlagSet(Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFlags(fs, LoadOptions{})
	if err != nil {
		t.Fatalf("LoadFlags() error = %v", err)
	}
	if cfg.RouterRouteTimeout != 30*time.Minute {
		t.Errorf("RouterRouteTimeout = %s, want 30m", cfg.RouterRouteTimeout)
	}
	if cfg.RouterParkedRequestBudget != 2*time.Minute {
		t.Errorf("RouterParkedRequestBudget = %s, want 2m", cfg.RouterParkedRequestBudget)
	}
	if cfg.RouterExtProcMaxRequests != 4096 {
		t.Errorf("RouterExtProcMaxRequests = %d, want 4096", cfg.RouterExtProcMaxRequests)
	}
}

func TestLoadFlagsRouterSettingsNeedEnvoy(t *testing.T) {
	for env, value := range map[string]string{
		"ATE_ATENET_PARKED_REQUEST_BUDGET": "2m",
		"ATE_ATENET_EXTPROC_MAX_REQUESTS":  "4096",
	} {
		t.Run(env, func(t *testing.T) {
			loadEnv(t)
			t.Setenv("ATE_ATENET_DATAPLANE", RouterAgentgateway)
			t.Setenv(env, value)

			fs, err := optionsFlagSet(Options{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = LoadFlags(fs, LoadOptions{})
			if err == nil {
				t.Fatal("LoadFlags() = nil error, want a dataplane conflict")
			}
			for _, want := range []string{env, "ATE_ATENET_DATAPLANE"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %s", err, want)
				}
			}
		})
	}
}

func TestLoadFlagsRouterSettingsMustBePositive(t *testing.T) {
	for _, tc := range []struct{ env, value, want string }{
		{"ATE_ATENET_ROUTE_TIMEOUT", "0s", "positive duration"},
		{"ATE_ATENET_EXTPROC_MAX_REQUESTS", "0", "positive integer"},
		{"ATE_ATENET_EXTPROC_MAX_REQUESTS", "-1", "positive integer"},
	} {
		t.Run(tc.env+"="+tc.value, func(t *testing.T) {
			loadEnv(t)
			t.Setenv(tc.env, tc.value)

			fs, err := optionsFlagSet(Options{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := LoadFlags(fs, LoadOptions{}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("LoadFlags() error = %v, want a %s error", err, tc.want)
			}
		})
	}
}
