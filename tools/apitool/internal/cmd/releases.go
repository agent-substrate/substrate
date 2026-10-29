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

package cmd

import (
	"bytes"
	"fmt"
	"os/exec"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// releaseTag matches full release tags, as tools/site-release-data does.
// Pre-releases such as v0.3.0-rc.1 do not match.
var releaseTag = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

// examplesDir is where a release keeps its resource examples, if it has any.
const examplesDir = "tools/apitool/internal/examples"

// release is one release's API, read from git.
type release struct {
	tag    string
	source string // its ateapi.proto
	// examples holds its resource examples keyed by message name, as
	// examples.ByResource does. Releases before the examples have none.
	examples map[string]string
}

// releaseTags returns the full release tags in tags at or after since,
// newest first.
func releaseTags(tags []string, since []int) []string {
	type version struct {
		tag string
		v   []int
	}
	var vs []version
	for _, tag := range tags {
		if v, ok := parseVersion(tag); ok && slices.Compare(v, since) >= 0 {
			vs = append(vs, version{tag, v})
		}
	}
	slices.SortFunc(vs, func(a, b version) int { return slices.Compare(b.v, a.v) })
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = v.tag
	}
	return out
}

// parseVersion returns the numeric parts of a full release tag.
func parseVersion(tag string) ([]int, bool) {
	m := releaseTag.FindStringSubmatch(tag)
	if m == nil {
		return nil, false
	}
	v := make([]int, 3)
	for i := range v {
		n, err := strconv.Atoi(m[i+1])
		if err != nil {
			return nil, false
		}
		v[i] = n
	}
	return v, true
}

// readReleases reads every release tagged at or after since from the git
// repository holding the working directory, newest first.
func readReleases(since string) ([]release, error) {
	sinceV, ok := parseVersion(since)
	if !ok {
		return nil, fmt.Errorf("%q is not a release tag like v1.2.3", since)
	}
	top, err := git(".", "rev-parse", "--show-toplevel")
	if err != nil {
		return nil, err
	}
	top = strings.TrimSpace(top)
	tags, err := git(top, "tag", "--list", "v*")
	if err != nil {
		return nil, err
	}

	var releases []release
	for _, tag := range releaseTags(strings.Fields(tags), sinceV) {
		r, err := readRelease(top, tag)
		if err != nil {
			return nil, fmt.Errorf("while reading %s: %w", tag, err)
		}
		releases = append(releases, r)
	}
	return releases, nil
}

// readRelease reads tag's ateapi.proto, wherever that release keeps it, and
// its examples.
func readRelease(top, tag string) (release, error) {
	files, err := git(top, "ls-tree", "-r", "--name-only", tag)
	if err != nil {
		return release{}, err
	}
	var protos, exampleFiles []string
	for f := range strings.FieldsSeq(files) {
		switch {
		case path.Base(f) == "ateapi.proto":
			protos = append(protos, f)
		case path.Dir(f) == examplesDir && path.Ext(f) == ".json":
			exampleFiles = append(exampleFiles, f)
		}
	}
	if len(protos) != 1 {
		return release{}, fmt.Errorf("found %d ateapi.proto files, want 1: %v", len(protos), protos)
	}

	source, err := git(top, "show", tag+":"+protos[0])
	if err != nil {
		return release{}, err
	}
	r := release{tag: tag, source: source, examples: map[string]string{}}
	for _, f := range exampleFiles {
		text, err := git(top, "show", tag+":"+f)
		if err != nil {
			return release{}, err
		}
		r.examples[strings.TrimSuffix(path.Base(f), ".json")] = text
	}
	return r, nil
}

func git(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}
