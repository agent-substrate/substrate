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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// fakeDocker puts a docker on PATH that records each invocation's arguments.
// `docker buildx version` exits with versionStatus and every other command
// with buildStatus; non-zero values print what a docker CLI without the buildx
// plugin prints. It returns the path of the invocation log.
func fakeDocker(t *testing.T, versionStatus, buildStatus int) string {
	t.Helper()
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	script := fmt.Sprintf(`#!/bin/sh
echo "$*" >> %q
if [ "$1 $2" = "buildx version" ]; then
  if [ %d -eq 0 ]; then echo "github.com/docker/buildx v0.0.0"; exit 0; fi
  echo "docker: unknown command: docker buildx" >&2
  exit %d
fi
if [ %d -ne 0 ]; then echo "unknown flag: --platform" >&2; fi
exit %d
`, calls, versionStatus, versionStatus, buildStatus, buildStatus)
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return calls
}

func readCalls(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}

func TestCheckBuildx(t *testing.T) {
	t.Run("available", func(t *testing.T) {
		calls := fakeDocker(t, 0, 0)
		if err := CheckBuildx(t.Context()); err != nil {
			t.Fatalf("CheckBuildx() = %v, want nil", err)
		}
		if got, want := readCalls(t, calls), []string{"buildx version"}; !slices.Equal(got, want) {
			t.Errorf("docker invocations = %q, want %q", got, want)
		}
	})
	t.Run("missing", func(t *testing.T) {
		fakeDocker(t, 1, 125)
		err := CheckBuildx(t.Context())
		if err == nil {
			t.Fatal("CheckBuildx() = nil, want an error")
		}
		// The message has to name the missing plugin, carry docker's own
		// output, and offer the install that needs no docker build.
		for _, want := range []string{"docker buildx plugin", "unknown command: docker buildx", "--image-repo"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("CheckBuildx() error = %q, want it to contain %q", err, want)
			}
		}
	})
}

// Without the plugin docker reports "unknown flag: --platform", so a failed
// build names the missing plugin instead.
func TestBuildDockerfileImageNamesMissingBuildx(t *testing.T) {
	fakeDocker(t, 1, 125)
	_, err := BuildDockerfileImage(t.Context(), t.TempDir(), "example.invalid/repo", "envoy-dataplane", t.TempDir(), "linux/amd64", nil)
	if err == nil {
		t.Fatal("BuildDockerfileImage() = nil, want an error")
	}
	for _, want := range []string{"while building the envoy-dataplane image", "docker buildx plugin"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("BuildDockerfileImage() error = %q, want it to contain %q", err, want)
		}
	}
}

// A build that fails with buildx present keeps docker's own error.
func TestBuildDockerfileImageKeepsBuildErrorWithBuildx(t *testing.T) {
	fakeDocker(t, 0, 1)
	_, err := BuildDockerfileImage(t.Context(), t.TempDir(), "example.invalid/repo", "envoy-dataplane", t.TempDir(), "linux/amd64", nil)
	if err == nil {
		t.Fatal("BuildDockerfileImage() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "exit status 1") || strings.Contains(err.Error(), "buildx plugin") {
		t.Errorf("BuildDockerfileImage() error = %q, want docker's exit status and no buildx diagnosis", err)
	}
}

func TestDockerBuildArgs(t *testing.T) {
	got := dockerBuildArgs("linux/amd64", []string{"--cache-from", "type=gha"}, "repo/img:build-1", "meta.json", "ctx")
	want := []string{"buildx", "build", "--platform=linux/amd64", "--push", "--metadata-file", "meta.json", "--cache-from", "type=gha", "-t", "repo/img:build-1", "ctx"}
	if !slices.Equal(got, want) {
		t.Errorf("dockerBuildArgs() = %q, want %q", got, want)
	}
	got = dockerBuildArgs("linux/amd64", nil, "repo/img:build-1", "meta.json", "ctx")
	want = []string{"buildx", "build", "--platform=linux/amd64", "--push", "--metadata-file", "meta.json", "-t", "repo/img:build-1", "ctx"}
	if !slices.Equal(got, want) {
		t.Errorf("dockerBuildArgs() without flags = %q, want %q", got, want)
	}
}

// The digest comes from what buildx pushed, so a tag that names another push
// cannot stand in for it.
func TestPushedDigest(t *testing.T) {
	const digest = "sha256:bab239626dc054856e882a2ef022dfa845834113d213697e1dccd27dfbfac1a5"
	for _, tc := range []struct {
		name     string
		metadata string
		want     string
		wantErr  bool
	}{
		{
			name:     "multi-platform push",
			metadata: `{"buildx.build.ref":"ate-release/ate-release0/x","containerimage.digest":"` + digest + `","image.name":"localhost:5000/substrate/envoy-dataplane:v1"}`,
			want:     digest,
		},
		{name: "no digest", metadata: `{"buildx.build.ref":"ate-release/ate-release0/x"}`, wantErr: true},
		{name: "not JSON", metadata: ``, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pushedDigest([]byte(tc.metadata))
			if (err != nil) != tc.wantErr {
				t.Fatalf("pushedDigest() error = %v, want error %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("pushedDigest() = %q, want %q", got, tc.want)
			}
		})
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
