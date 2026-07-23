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

// mountinfo parsing lives in a portable file (the reader comes in as an
// io.Reader) so the logic is unit-testable off-Linux; only the caller that
// opens /proc/self/mountinfo is linux-tagged.

package imagecache

import (
	"bufio"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

// mountPointsIn returns the mount points at or below dir found in r, which
// must be in /proc/[pid]/mountinfo format:
//
//	ID parentID major:minor root MOUNTPOINT options...
func mountPointsIn(r io.Reader, dir string) ([]string, error) {
	dir = filepath.Clean(dir)
	var points []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}
		mp := unescapeMountPath(fields[4])
		if mp == dir || strings.HasPrefix(mp, dir+"/") {
			points = append(points, mp)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("while reading mountinfo: %w", err)
	}
	return points, nil
}

// unescapeMountPath decodes the octal escapes mountinfo uses for space, tab,
// newline, and backslash.
func unescapeMountPath(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	r := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return r.Replace(s)
}
