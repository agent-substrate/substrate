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

// Package log prints install progress, preserving the "[step]: name" output
// the shell scripts produced so existing CI log scrapers keep working.
package log

import (
	"fmt"
	"io"
	"os"
)

const (
	colorCyan  = "\033[1;36m"
	colorReset = "\033[0m"
)

// out is the destination for progress output. Steps go to stdout, matching the
// shell scripts.
var out io.Writer = os.Stdout

// SetOutput redirects progress output, for tests.
func SetOutput(w io.Writer) { out = w }

// Step announces a named install step. It takes a plain string rather than a
// format because most step names are computed (a demo name plus a suffix, a
// namespace/name pair), and a stray % in one of those should not be read as a
// verb.
func Step(msg string) {
	fmt.Fprintf(out, "%s[step]: %s%s\n", colorCyan, msg, colorReset)
}

// Stepf announces a formatted install step.
func Stepf(format string, args ...any) {
	Step(fmt.Sprintf(format, args...))
}

// Infof prints an unadorned progress line.
func Infof(format string, args ...any) {
	fmt.Fprintf(out, format+"\n", args...)
}

// Warnf prints a warning to stderr.
func Warnf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "Warning: "+format+"\n", args...)
}
