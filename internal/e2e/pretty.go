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

package e2e

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
)

// noColor() disables colors for tests running in environments without colored output.
var (
	NoColor bool
	// envNoColor (see https://no-color.org/)
	envNoColor bool
)

const (
	ansiReset  = "\033[0m"
	ansiDim    = "\033[2m"
	ansiRed    = "\033[31m"
	ansiCyan   = "\033[36m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiBold   = "\033[1m"
)

type colorEntry struct {
	name string
	ansi string
	re   *regexp.Regexp
}

var (
	colors []colorEntry
)

func init() {
	colors = append(colors, colorEntry{name: "cyan", ansi: ansiCyan})
	colors = append(colors, colorEntry{name: "green", ansi: ansiGreen})
	colors = append(colors, colorEntry{name: "red", ansi: ansiRed})
	colors = append(colors, colorEntry{name: "yellow", ansi: ansiYellow})
	colors = append(colors, colorEntry{name: "bold", ansi: ansiBold})
	colors = append(colors, colorEntry{name: "dim", ansi: ansiDim})
	colors = append(colors, colorEntry{name: "reset", ansi: ansiReset})

	for i := range colors {
		colors[i].re = regexp.MustCompile(fmt.Sprintf("(?s)<%s>(.*?)</%s>", colors[i].name, colors[i].name))
	}

	// """
	// Command-line software which adds ANSI color to its output by default
	// should check for a NO_COLOR environment variable that, when present
	// and not an empty string (regardless of its value), prevents the addition
	// of ANSI color.
	// """ -- https://no-color.org/
	envNoColor = os.Getenv("NO_COLOR") != ""
}

func noColor() bool {
	return NoColor || envNoColor
}

// Colorf renders a formatted string with color directives:
//
// "Example text: <green>this is green</green>".
//
// Unterminated directives will be treated as literals.
func Colorf(format string, a ...any) string {
	str := fmt.Sprintf(format, a...)
	for _, c := range colors {
		if noColor() {
			str = c.re.ReplaceAllString(str, "$1")
		} else {
			str = c.re.ReplaceAllString(str, c.ansi+"$1"+ansiReset)
		}
	}

	return str
}

// FColorf writes a formatted string with color directives to the given writer.
func FColorf(f io.Writer, format string, a ...any) error {
	_, err := fmt.Fprint(f, Colorf(format, a...))
	return err
}

// FColorfln writes a formatted string with color directives to the given writer, followed by a newline.
func FColorfln(f io.Writer, format string, a ...any) error {
	return FColorf(f, format+"\n", a...)
}

// ColorWriter wraps an io.Writer and forces all writes to be colored with the given ANSI code.
// It respects noColor().
type ColorWriter struct {
	W    io.Writer
	ANSI string
}

// Write writes p to the underlying writer, wrapped in the ANSI color code and reset code.
func (cw *ColorWriter) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if noColor() {
		return cw.W.Write(p)
	}
	// Write color code
	_, err = cw.W.Write([]byte(cw.ANSI))
	if err != nil {
		return 0, err
	}
	// Write content
	n, err = cw.W.Write(p)
	if err != nil {
		return n, err
	}
	// Write reset code
	_, err = cw.W.Write([]byte(ansiReset))
	if err != nil {
		return n, err
	}
	return n, nil
}

// IndentWriter wraps an io.Writer and indents each line of output.
type IndentWriter struct {
	W           io.Writer
	Val         string
	atLineStart bool
}

// NewIndentWriter creates a new IndentWriter.
func NewIndentWriter(w io.Writer, indent string) *IndentWriter {
	return &IndentWriter{
		W:           w,
		Val:         indent,
		atLineStart: true,
	}
}

// Write writes p to the underlying writer, inserting the indent string at the start of each line.
func (iw *IndentWriter) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	var buf bytes.Buffer
	for _, b := range p {
		if iw.atLineStart {
			buf.WriteString(iw.Val)
			iw.atLineStart = false
		}
		buf.WriteByte(b)
		if b == '\n' {
			iw.atLineStart = true
		}
	}

	_, err = iw.W.Write(buf.Bytes())
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
