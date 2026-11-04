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
	"slices"
	"strings"
	"testing"

	"github.com/spf13/pflag"
)

// writeComponent creates dir holding a kustomization of the given kind.
func writeComponent(t *testing.T, dir, kind string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: " + kind + "\n"
	if err := os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// loadComponents runs LoadFlags with args on the root flag set.
func loadComponents(t *testing.T, args []string, configPath string) (*Config, error) {
	t.Helper()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	BindFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("Parse(%v) = %v", args, err)
	}
	return LoadFlags(fs, LoadOptions{ConfigPath: configPath, NoDevEnv: true})
}

// Each channel yields absolute paths in the order given. Relative paths from
// the flag and the environment resolve against the working directory, and
// from a file against the file's directory.
func TestLoadComponents(t *testing.T) {
	loadEnv(t)
	unsetEnv(t, "ATE_INSTALL_COMPONENTS")
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	a := writeComponent(t, filepath.Join(root, "a"), "Component")
	b := writeComponent(t, filepath.Join(root, "b"), "Component")
	fileDir := filepath.Join(root, "conf")
	c := writeComponent(t, filepath.Join(fileDir, "c"), "Component")

	t.Run("flag", func(t *testing.T) {
		cfg, err := loadComponents(t, []string{"--component", "b", "--component", a}, "")
		if err != nil {
			t.Fatalf("LoadFlags() error = %v", err)
		}
		if want := []string{b, a}; !slices.Equal(cfg.Components, want) {
			t.Errorf("Components = %v, want %v", cfg.Components, want)
		}
	})

	t.Run("environment", func(t *testing.T) {
		t.Setenv("ATE_INSTALL_COMPONENTS", "a"+string(os.PathListSeparator)+b)
		cfg, err := loadComponents(t, nil, "")
		if err != nil {
			t.Fatalf("LoadFlags() error = %v", err)
		}
		if want := []string{a, b}; !slices.Equal(cfg.Components, want) {
			t.Errorf("Components = %v, want %v", cfg.Components, want)
		}
	})

	t.Run("file", func(t *testing.T) {
		path := filepath.Join(fileDir, "install.yaml")
		body := validHeader + "components:\n  - c\n  - " + a + "\n"
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		cfg, err := loadComponents(t, nil, path)
		if err != nil {
			t.Fatalf("LoadFlags() error = %v", err)
		}
		if want := []string{c, a}; !slices.Equal(cfg.Components, want) {
			t.Errorf("Components = %v, want %v", cfg.Components, want)
		}
	})

	t.Run("record replays the absolute paths", func(t *testing.T) {
		cfg, err := loadComponents(t, []string{"--component", "a", "--component", "b"}, "")
		if err != nil {
			t.Fatalf("LoadFlags() error = %v", err)
		}
		path, _, err := RecordSuccess(t.TempDir(), "kind", "", cfg.Resolved())
		if err != nil {
			t.Fatalf("RecordSuccess() error = %v", err)
		}
		replayed, err := loadComponents(t, nil, path)
		if err != nil {
			t.Fatalf("LoadFlags(record) error = %v", err)
		}
		if want := []string{a, b}; !slices.Equal(replayed.Components, want) {
			t.Errorf("replayed Components = %v, want %v", replayed.Components, want)
		}
	})
}

// A component that kustomize would reject fails the load, naming the channel.
func TestLoadComponentsRejectsBadDirectories(t *testing.T) {
	loadEnv(t)
	unsetEnv(t, "ATE_INSTALL_COMPONENTS")
	dir := t.TempDir()
	notDir := filepath.Join(dir, "file")
	if err := os.WriteFile(notDir, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty")
	if err := os.Mkdir(empty, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path, want string
	}{
		{name: "missing", path: filepath.Join(dir, "absent"), want: "no such file or directory"},
		{name: "not a directory", path: notDir, want: "is not a directory"},
		{name: "no kustomization", path: empty, want: "has no kustomization.yaml"},
		{
			name: "not a component",
			path: writeComponent(t, filepath.Join(dir, "kustomization"), "Kustomization"),
			want: "must declare kind: Component",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadComponents(t, []string{"--component", tc.path}, "")
			if err == nil {
				t.Fatal("LoadFlags() = nil error, want an error")
			}
			for _, want := range []string{tc.want, "--component"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err, want)
				}
			}
		})
	}
}

func TestComponentFlagRejectsBadValues(t *testing.T) {
	for _, v := range []string{"", "a" + string(os.PathListSeparator) + "b"} {
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		BindFlags(fs)
		if err := fs.Parse([]string{"--component=" + v}); err == nil {
			t.Errorf("--component=%q parsed, want an error", v)
		}
	}
}

func TestParseFileComponentsMustBePaths(t *testing.T) {
	for _, body := range []string{
		"components:\n  - \"\"\n",
		"components:\n  - [a]\n",
		"components:\n  - a" + string(os.PathListSeparator) + "b\n",
	} {
		if _, err := ParseFile(writeConfig(t, validHeader+body)); err == nil {
			t.Errorf("ParseFile(%q) = nil error, want an error", body)
		}
	}
}

func TestLoadStorageBackend(t *testing.T) {
	component := writeComponent(t, t.TempDir(), "Component")
	for _, tc := range []struct {
		name         string
		opts         Options
		env          map[string]string
		want         string
		wantFrom     Origin
		wantLocation string
		wantErr      []string
	}{
		{name: "default", want: StorageBackendGCS, wantFrom: OriginDefault},
		{name: "kind profile", opts: Options{Kind: true}, want: StorageBackendS3, wantFrom: OriginKindProfile, wantLocation: "gs://ate-snapshots/"},
		{
			name: "external", env: map[string]string{"STORAGE_BACKEND": "external", "ATE_INSTALL_COMPONENTS": component},
			want: StorageBackendExternal, wantFrom: OriginEnv,
		},
		{
			name: "external on kind derives no location", opts: Options{Kind: true},
			env:  map[string]string{"STORAGE_BACKEND": "external", "ATE_INSTALL_COMPONENTS": component},
			want: StorageBackendExternal, wantFrom: OriginEnv,
		},
		{
			name: "external with a microVM asset location",
			env: map[string]string{"STORAGE_BACKEND": "external", "ATE_INSTALL_COMPONENTS": component,
				"MICROVM_ASSET_LOCATION": "az://acct/assets"},
			want: StorageBackendExternal, wantFrom: OriginEnv,
		},
		{
			name: "microVM asset location on gcs", env: map[string]string{"MICROVM_ASSET_LOCATION": "gs://b/assets"},
			wantErr: []string{"storage.microVMAssetLocation", "stages the microVM assets"},
		},
		{
			name: "microVM asset location on kind", opts: Options{Kind: true},
			env:     map[string]string{"MICROVM_ASSET_LOCATION": "s3://b/assets"},
			wantErr: []string{"storage.microVMAssetLocation", "stages the microVM assets"},
		},
		{name: "unknown", env: map[string]string{"STORAGE_BACKEND": "azure"}, wantErr: []string{"gcs, s3, external", "STORAGE_BACKEND"}},
		{name: "s3 off kind", env: map[string]string{"STORAGE_BACKEND": "s3"}, wantErr: []string{"only on kind"}},
		{name: "gcs on kind", opts: Options{Kind: true}, env: map[string]string{"STORAGE_BACKEND": "gcs"}, wantErr: []string{"rustfs"}},
		{name: "external without a component", env: map[string]string{"STORAGE_BACKEND": "external"}, wantErr: []string{"--component"}},
		{
			name: "external with the fake data plane", opts: Options{FakeDataPlane: true},
			env:     map[string]string{"STORAGE_BACKEND": "external", "ATE_INSTALL_COMPONENTS": component},
			wantErr: []string{"fakeDataPlane.enabled"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loadEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := Load(tc.opts)
			if len(tc.wantErr) > 0 {
				if err == nil {
					t.Fatal("Load() = nil error, want an error")
				}
				for _, want := range tc.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not contain %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.StorageBackend != tc.want {
				t.Errorf("StorageBackend = %q, want %q", cfg.StorageBackend, tc.want)
			}
			if v, _ := cfg.Resolved().Value("storage.backend"); v.From != tc.wantFrom {
				t.Errorf("storage.backend from %v, want %v", v.From, tc.wantFrom)
			}
			if cfg.SnapshotLocation != tc.wantLocation {
				t.Errorf("SnapshotLocation = %q, want %q", cfg.SnapshotLocation, tc.wantLocation)
			}
		})
	}
}
