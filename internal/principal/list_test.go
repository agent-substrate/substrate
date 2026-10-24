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

package principal

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/spf13/pflag"
)

func TestParseList(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want []string
	}{
		{name: "empty", in: ""},
		{name: "only separators", in: " , ,"},
		{name: "one", in: "alice", want: []string{"alice"}},
		{name: "several", in: "alice,bob", want: []string{"alice", "bob"}},
		{name: "whitespace around entries", in: "  alice ,\tbob  ", want: []string{"alice", "bob"}},
		{name: "empty entries dropped", in: "alice,,bob,", want: []string{"alice", "bob"}},
		{name: "comma in quotes", in: `"Doe, Jane",bob`, want: []string{"Doe, Jane", "bob"}},
		{name: "whitespace before a quoted entry", in: `alice, "Doe, Jane"`, want: []string{"alice", "Doe, Jane"}},
		{name: "doubled quote", in: `"say ""hi""",bob`, want: []string{`say "hi"`, "bob"}},
		{name: "quoted without a comma", in: `"alice",bob`, want: []string{"alice", "bob"}},
		{name: "service account", in: "system:serviceaccount:ate-system:ate-client", want: []string{"system:serviceaccount:ate-system:ate-client"}},
		{name: "newline separates", in: "alice\nbob", want: []string{"alice", "bob"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseList(tc.in)
			if err != nil {
				t.Fatalf("ParseList(%q) error = %v", tc.in, err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("ParseList(%q) (-want +got):\n%s", tc.in, diff)
			}
		})
	}
}

func TestParseListInvalid(t *testing.T) {
	for _, in := range []string{
		`"alice`,   // unterminated quote
		`ali"ce`,   // quote inside an unquoted entry
		`"alice"x`, // text after the closing quote
	} {
		if got, err := ParseList(in); err == nil {
			t.Errorf("ParseList(%q) = %q, want an error", in, got)
		}
	}
}

// A list ate-setup renders must reach ate-api-server's StringSlice flag as the
// principals it was built from.
func TestFormatList(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []string
		want string
	}{
		{name: "none"},
		{name: "plain", in: []string{"alice", "bob"}, want: "alice,bob"},
		{name: "comma", in: []string{"Doe, Jane", "bob"}, want: `"Doe, Jane",bob`},
		{name: "quote", in: []string{`say "hi"`, "bob"}, want: `"say ""hi""",bob`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := FormatList(tc.in)
			if got != tc.want {
				t.Errorf("FormatList(%q) = %q, want %q", tc.in, got, tc.want)
			}

			parsed, err := ParseList(got)
			if err != nil {
				t.Fatalf("ParseList(%q) error = %v", got, err)
			}
			if diff := cmp.Diff(tc.in, parsed); diff != "" {
				t.Errorf("ParseList(FormatList(%q)) (-want +got):\n%s", tc.in, diff)
			}

			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			owners := fs.StringSlice("owners", nil, "")
			if err := fs.Parse([]string{"--owners=" + got}); err != nil {
				t.Fatalf("flag parse error = %v", err)
			}
			if diff := cmp.Diff(tc.in, *owners, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("StringSlice of FormatList(%q) (-want +got):\n%s", tc.in, diff)
			}
		})
	}
}
