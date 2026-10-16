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
	"bytes"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"strings"
)

// mountinfo octal-escapes space, tab, newline and backslash in paths (\040,
// \011, \012, \134); these decode and encode that form.
var (
	mountPathUnescaper = strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	mountPathEscaper   = strings.NewReplacer(" ", `\040`, "\t", `\011`, "\n", `\012`, `\`, `\134`)
)

// mountPointsIn returns the mount points at or below dir found in r, which
// must be in /proc/[pid]/mountinfo format:
//
//	ID parentID major:minor root MOUNTPOINT options...
//
// mountinfo can run to several MB on a dense worker and is read on every
// teardown, so the loop allocates only for matching lines.
func mountPointsIn(r io.Reader, dir string) ([]string, error) {
	// Compare in mountinfo's escaped form, so only matches are decoded. The
	// escaping is per character and leaves '/' alone, so a prefix match on
	// the escaped path is a prefix match on the decoded one.
	esc := []byte(mountPathEscaper.Replace(filepath.Clean(dir)))
	var points []string
	scanner := bufio.NewScanner(r)
	// No line-length cap: an overlay's record lists every lowerdir path, and
	// a long chain outgrows the 64 KiB default token size.
	scanner.Buffer(nil, math.MaxInt)
	for scanner.Scan() {
		mp, ok := mountinfoField(scanner.Bytes(), 4)
		if !ok || !bytes.HasPrefix(mp, esc) {
			continue
		}
		if rest := mp[len(esc):]; len(rest) == 0 || rest[0] == '/' {
			points = append(points, unescapeMountPath(string(mp)))
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("while reading mountinfo: %w", err)
	}
	return points, nil
}

// mountinfoField returns the nth (0-based) field of line. The kernel
// separates fields with a single space.
func mountinfoField(line []byte, n int) ([]byte, bool) {
	for ; n > 0; n-- {
		i := bytes.IndexByte(line, ' ')
		if i < 0 {
			return nil, false
		}
		line = line[i+1:]
	}
	if i := bytes.IndexByte(line, ' '); i >= 0 {
		line = line[:i]
	}
	return line, len(line) > 0
}

// unescapeMountPath decodes the octal escapes mountinfo uses for space, tab,
// newline, and backslash.
func unescapeMountPath(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	return mountPathUnescaper.Replace(s)
}
