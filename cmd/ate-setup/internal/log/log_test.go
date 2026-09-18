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

package log

import (
	"bytes"
	"os"
	"regexp"
	"testing"
	"time"
)

// The timing line keeps the shell installer's shape, "(label took N.NNNs)",
// so the same log scrape attributes a slow install under either installer.
func TestElapsed(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	t.Cleanup(func() { SetOutput(os.Stdout) })

	Elapsed(time.Now().Add(-1500*time.Millisecond), "ko resolve")

	want := regexp.MustCompile(`^  \(ko resolve took 1\.5\d\ds\)\n$`)
	if !want.MatchString(buf.String()) {
		t.Errorf("Elapsed() printed %q, want a match for %s", buf.String(), want)
	}
}
