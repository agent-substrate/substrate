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
	"testing"
)

func TestColorf(t *testing.T) {
	tests := []struct {
		name    string
		noColor bool
		format  string
		args    []interface{}
		want    string
	}{
		{
			name:    "single green tag",
			noColor: false,
			format:  "This is <green>green</green> text",
			want:    "This is \033[32mgreen\033[0m text",
		},
		{
			name:    "multiple green tags",
			noColor: false,
			format:  "<green>one</green> and <green>two</green>",
			want:    "\033[32mone\033[0m and \033[32mtwo\033[0m",
		},
		{
			name:    "different tags",
			noColor: false,
			format:  "<green>green</green> and <red>red</red> and <yellow>yellow</yellow>",
			want:    "\033[32mgreen\033[0m and \033[31mred\033[0m and \033[33myellow\033[0m",
		},
		{
			name:    "unterminated tag",
			noColor: false,
			format:  "This is <green>unterminated",
			want:    "This is <green>unterminated",
		},
		{
			name:    "formatting with tags",
			noColor: false,
			format:  "User <green>%s</green> logged in",
			args:    []interface{}{"bowei"},
			want:    "User \033[32mbowei\033[0m logged in",
		},
		{
			name:    "multiline tag",
			noColor: false,
			format:  "This is <green>green\ntext</green>",
			want:    "This is \033[32mgreen\ntext\033[0m",
		},
		{
			name:    "color disabled - multiline tags stripped",
			noColor: true,
			format:  "This is <green>green\ntext</green>",
			want:    "This is green\ntext",
		},
		{
			name:    "color disabled - tags stripped",
			noColor: true,
			format:  "This is <green>green</green> and <red>red</red>",
			want:    "This is green and red",
		},
		{
			name:    "color disabled - unterminated remains",
			noColor: true,
			format:  "This is <green>unterminated",
			want:    "This is <green>unterminated",
		},
		{
			name:    "dim tag",
			noColor: false,
			format:  "This is <dim>dim</dim> text",
			want:    "This is \033[2mdim\033[0m text",
		},
		{
			name:    "color disabled - dim tag stripped",
			noColor: true,
			format:  "This is <dim>dim</dim> text",
			want:    "This is dim text",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			NoColor = tc.noColor
			defer func() { NoColor = false }()

			got := Colorf(tc.format, tc.args...)
			if got != tc.want {
				t.Errorf("Colorf() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFColorfAndFColorfln(t *testing.T) {
	t.Run("FColorf", func(t *testing.T) {
		var buf bytes.Buffer
		NoColor = false
		err := FColorf(&buf, "Hello <green>%s</green>", "world")
		if err != nil {
			t.Fatalf("FColorf failed: %v", err)
		}
		want := "Hello \033[32mworld\033[0m"
		if buf.String() != want {
			t.Errorf("FColorf output = %q, want %q", buf.String(), want)
		}
	})

	t.Run("FColorfln", func(t *testing.T) {
		var buf bytes.Buffer
		NoColor = false
		err := FColorfln(&buf, "Hello <green>%s</green>", "world")
		if err != nil {
			t.Fatalf("FColorfln failed: %v", err)
		}
		want := "Hello \033[32mworld\033[0m\n"
		if buf.String() != want {
			t.Errorf("FColorfln output = %q, want %q", buf.String(), want)
		}
	})

	t.Run("FColorf no color", func(t *testing.T) {
		var buf bytes.Buffer
		NoColor = true
		defer func() { NoColor = false }()
		err := FColorf(&buf, "Hello <green>%s</green>", "world")
		if err != nil {
			t.Fatalf("FColorf failed: %v", err)
		}
		want := "Hello world"
		if buf.String() != want {
			t.Errorf("FColorf output = %q, want %q", buf.String(), want)
		}
	})
}

func TestColorWriter(t *testing.T) {
	tests := []struct {
		name    string
		noColor bool
		ansi    string
		input   string
		want    string
	}{
		{
			name:    "color enabled - red",
			noColor: false,
			ansi:    "\033[31m",
			input:   "hello",
			want:    "\033[31mhello\033[0m",
		},
		{
			name:    "color enabled - dim",
			noColor: false,
			ansi:    "\033[2m",
			input:   "world",
			want:    "\033[2mworld\033[0m",
		},
		{
			name:    "color disabled",
			noColor: true,
			ansi:    "\033[31m",
			input:   "hello",
			want:    "hello",
		},
		{
			name:    "empty input",
			noColor: false,
			ansi:    "\033[31m",
			input:   "",
			want:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			NoColor = tc.noColor
			defer func() { NoColor = false }()

			var buf bytes.Buffer
			cw := &ColorWriter{W: &buf, ANSI: tc.ansi}
			n, err := cw.Write([]byte(tc.input))
			if err != nil {
				t.Fatalf("Write failed: %v", err)
			}
			if n != len(tc.input) {
				t.Errorf("Write returned n = %d, want %d", n, len(tc.input))
			}
			if buf.String() != tc.want {
				t.Errorf("ColorWriter output = %q, want %q", buf.String(), tc.want)
			}
		})
	}
}

func TestIndentWriter(t *testing.T) {
	tests := []struct {
		name   string
		indent string
		inputs []string
		want   string
	}{
		{
			name:   "single line",
			indent: "  ",
			inputs: []string{"hello"},
			want:   "  hello",
		},
		{
			name:   "multiple lines in one write",
			indent: "  ",
			inputs: []string{"hello\nworld"},
			want:   "  hello\n  world",
		},
		{
			name:   "multiple writes",
			indent: "  ",
			inputs: []string{"hello\n", "world"},
			want:   "  hello\n  world",
		},
		{
			name:   "newline at end",
			indent: "  ",
			inputs: []string{"hello\n"},
			want:   "  hello\n",
		},
		{
			name:   "empty writes",
			indent: "  ",
			inputs: []string{"", "hello"},
			want:   "  hello",
		},
		{
			name:   "write without newline then write with newline",
			indent: "  ",
			inputs: []string{"hello ", "world\n"},
			want:   "  hello world\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			iw := NewIndentWriter(&buf, tc.indent)
			for _, input := range tc.inputs {
				n, err := iw.Write([]byte(input))
				if err != nil {
					t.Fatalf("Write failed: %v", err)
				}
				if n != len(input) {
					t.Errorf("Write returned n = %d, want %d", n, len(input))
				}
			}
			if buf.String() != tc.want {
				t.Errorf("IndentWriter output = %q, want %q", buf.String(), tc.want)
			}
		})
	}
}

func TestIndentAndColorChaining(t *testing.T) {
	var buf bytes.Buffer
	cyanWriter := &ColorWriter{W: &buf, ANSI: "\033[36m"}
	iw := NewIndentWriter(cyanWriter, "        ")

	NoColor = false
	defer func() { NoColor = false }()

	_, err := iw.Write([]byte("hello\nworld\n"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	want := "\033[36m        hello\n        world\n\033[0m"
	if buf.String() != want {
		t.Errorf("Chained output = %q, want %q", buf.String(), want)
	}
}

func TestEnvNoColor(t *testing.T) {
	origEnvNoColor := envNoColor
	origNoColor := NoColor
	defer func() {
		envNoColor = origEnvNoColor
		NoColor = origNoColor
	}()

	t.Run("envNoColor true disables color", func(t *testing.T) {
		envNoColor = true
		NoColor = false

		if !noColor() {
			t.Errorf("noColor() = false, want true when envNoColor is true")
		}

		got := Colorf("<green>hello</green>")
		want := "hello"
		if got != want {
			t.Errorf("Colorf() = %q, want %q", got, want)
		}
	})

	t.Run("envNoColor false allows color", func(t *testing.T) {
		envNoColor = false
		NoColor = false

		if noColor() {
			t.Errorf("noColor() = true, want false when both are false")
		}

		got := Colorf("<green>hello</green>")
		want := "\033[32mhello\033[0m"
		if got != want {
			t.Errorf("Colorf() = %q, want %q", got, want)
		}
	})
}
