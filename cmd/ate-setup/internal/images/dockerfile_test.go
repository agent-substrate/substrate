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
