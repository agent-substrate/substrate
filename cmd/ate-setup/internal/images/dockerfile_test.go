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

package images

import (
	"cmp"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestDockerBuildArgs(t *testing.T) {
	got := dockerBuildArgs("linux/amd64", []string{"--cache-from", "type=gha"}, "repo/img:build-1", "ctx")
	want := []string{"buildx", "build", "--platform=linux/amd64", "--push", "--cache-from", "type=gha", "-t", "repo/img:build-1", "ctx"}
	if !slices.Equal(got, want) {
		t.Errorf("dockerBuildArgs() = %q, want %q", got, want)
	}
	got = dockerBuildArgs("linux/amd64", nil, "repo/img:build-1", "ctx")
	want = []string{"buildx", "build", "--platform=linux/amd64", "--push", "-t", "repo/img:build-1", "ctx"}
	if !slices.Equal(got, want) {
		t.Errorf("dockerBuildArgs() without flags = %q, want %q", got, want)
	}
}

// ko builds for .ko.yaml's defaultPlatforms unless KO_DEFAULTPLATFORMS says
// otherwise, and a Dockerfile image installed beside the ko images has to
// cover the same platforms or it cannot run on some of their nodes.
func TestDockerfilePlatforms(t *testing.T) {
	const twoPlatforms = "defaultPlatforms:\n  - linux/amd64\n  - linux/arm64\n"
	for _, tc := range []struct {
		name         string
		koYAML       string // empty: no ko config
		koYAMLFile   string // where it is written, relative to the root; default .ko.yaml
		koConfigPath string
		koPlatforms  string
		want         string
	}{
		{name: "explicit platforms win", koYAML: twoPlatforms, koPlatforms: "linux/s390x", want: "linux/s390x"},
		{name: "ko config", koYAML: twoPlatforms, want: "linux/amd64,linux/arm64"},
		{name: "ko config without platforms", koYAML: "defaultBaseImage: example.com/base\n", want: "linux/amd64"},
		{name: "no ko config", want: "linux/amd64"},
		{name: "KO_CONFIG_PATH file", koYAML: twoPlatforms, koYAMLFile: "conf/ko.yaml", koConfigPath: "conf/ko.yaml", want: "linux/amd64,linux/arm64"},
		{name: "KO_CONFIG_PATH directory", koYAML: twoPlatforms, koYAMLFile: "conf/.ko.yaml", koConfigPath: "conf", want: "linux/amd64,linux/arm64"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			t.Setenv("KO_CONFIG_PATH", tc.koConfigPath)
			if tc.koYAML != "" {
				file := filepath.Join(root, cmp.Or(tc.koYAMLFile, ".ko.yaml"))
				if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(file, []byte(tc.koYAML), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			got, err := dockerfilePlatforms(root, tc.koPlatforms)
			if err != nil {
				t.Fatalf("dockerfilePlatforms() error = %v", err)
			}
			if got != tc.want {
				t.Errorf("dockerfilePlatforms() = %q, want %q", got, tc.want)
			}
		})
	}
}
