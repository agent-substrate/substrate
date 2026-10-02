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

// Command site-release-data writes the release metadata the website's
// templates read: the version being published, the previous full release,
// and the Markdown files added between the two.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// releaseData is the shape of site/data/release.json.
type releaseData struct {
	Version  string   `json:"version"`
	Previous string   `json:"previous"`
	Added    []string `json:"added"`
}

// releaseTag matches full release tags. Pre-releases such as v0.3.0-rc.1 do
// not match.
var releaseTag = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func main() {
	ref := flag.String("ref", "HEAD", "Git ref being published.")
	out := flag.String("out", "", "Path of the JSON file to write.")
	flag.Parse()
	if *out == "" {
		fmt.Fprintln(os.Stderr, "site-release-data: --out is required")
		os.Exit(2)
	}
	if err := run(".", *ref, *out); err != nil {
		fmt.Fprintf(os.Stderr, "site-release-data: %v\n", err)
		os.Exit(1)
	}
}

func run(dir, ref, out string) error {
	data, err := compute(dir, ref)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	return os.WriteFile(out, append(b, '\n'), 0o644)
}

// compute finds the previous full release and the Markdown files added since
// it. If ref is itself a full release tag, the previous release is the
// highest one below it; otherwise it is the highest one overall.
func compute(dir, ref string) (releaseData, error) {
	data := releaseData{Added: []string{}}
	if _, err := git(dir, "rev-parse", "--verify", "--quiet", ref+"^{commit}"); err != nil {
		return data, fmt.Errorf("resolving %q: %w", ref, err)
	}

	name := strings.TrimPrefix(ref, "refs/tags/")
	var current []int
	if v, ok := parseVersion(name); ok {
		if _, err := git(dir, "rev-parse", "--verify", "--quiet", "refs/tags/"+name); err == nil {
			data.Version, current = name, v
		}
	}

	tags, err := git(dir, "tag", "--list", "v*")
	if err != nil {
		return data, err
	}
	var best []int
	for tag := range strings.FieldsSeq(tags) {
		v, ok := parseVersion(tag)
		if !ok || (current != nil && slices.Compare(v, current) >= 0) {
			continue
		}
		if best == nil || slices.Compare(v, best) > 0 {
			best, data.Previous = v, tag
		}
	}
	if data.Previous == "" {
		return data, nil
	}

	// A release that predates the site has no pages to compare against, and
	// counting from it would mark every page new.
	if _, err := git(dir, "cat-file", "-e", data.Previous+":site"); err != nil {
		return data, nil
	}

	diff, err := git(dir, "diff", "--name-only", "-z", "--diff-filter=A", "--find-renames", data.Previous, ref)
	if err != nil {
		return data, err
	}
	for p := range strings.SplitSeq(diff, "\x00") {
		if strings.HasSuffix(p, ".md") {
			data.Added = append(data.Added, p)
		}
	}
	slices.Sort(data.Added)
	return data, nil
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
