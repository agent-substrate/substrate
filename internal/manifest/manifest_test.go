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

package manifest

import (
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func meta(name string) *ateapipb.ResourceMetadata {
	return &ateapipb.ResourceMetadata{Atespace: "a", Name: name}
}

func TestParse(t *testing.T) {
	const (
		one = "atespace: a\nname: one\n"
		two = "atespace: a\nname: two\n"
	)
	tests := []struct {
		name            string
		manifest        string
		want            []*ateapipb.ResourceMetadata
		wantErrContains string
	}{
		{name: "one document", manifest: one, want: []*ateapipb.ResourceMetadata{meta("one")}},
		{name: "json", manifest: `{"atespace": "a", "name": "one"}`, want: []*ateapipb.ResourceMetadata{meta("one")}},
		{name: "leading separator", manifest: "---\n" + one, want: []*ateapipb.ResourceMetadata{meta("one")}},
		{name: "document end marker", manifest: one + "...\n", want: []*ateapipb.ResourceMetadata{meta("one")}},
		{name: "two documents", manifest: one + "---\n" + two, want: []*ateapipb.ResourceMetadata{meta("one"), meta("two")}},
		{
			name:     "two json documents",
			manifest: `{"atespace": "a", "name": "one"}` + "\n---\n" + `{"atespace": "a", "name": "two"}`,
			want:     []*ateapipb.ResourceMetadata{meta("one"), meta("two")},
		},
		{name: "empty documents skipped", manifest: "---\n# nothing\n---\n" + one + "---\n---\n" + two + "---\n", want: []*ateapipb.ResourceMetadata{meta("one"), meta("two")}},
		{name: "alias", manifest: "atespace: &x a\nname: *x\n", want: []*ateapipb.ResourceMetadata{{Atespace: "a", Name: "a"}}},
		{name: "empty", manifest: "", wantErrContains: "manifest is empty"},
		{name: "only separators", manifest: "---\n---\n", wantErrContains: "manifest is empty"},
		{name: "comment only", manifest: "# nothing\n", wantErrContains: "manifest is empty"},
		{name: "not yaml", manifest: "\t{", wantErrContains: "invalid YAML"},
		{name: "bad second document", manifest: one + "---\n\t{", wantErrContains: "invalid YAML"},
		{name: "unknown field", manifest: "atespace: a\nnmae: one\n", wantErrContains: "invalid ResourceMetadata"},
		{name: "unknown field in second document", manifest: one + "---\nnmae: two\n", wantErrContains: "document 2: invalid ResourceMetadata"},
		{name: "not an object", manifest: "- a\n", wantErrContains: "invalid ResourceMetadata"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Parse[ateapipb.ResourceMetadata]([]byte(test.manifest))
			if test.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErrContains) {
					t.Fatalf("Parse() error = %v, want it to contain %q", err, test.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if diff := cmp.Diff(test.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("Parse() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseOne(t *testing.T) {
	const one = "atespace: a\nname: one\n"
	tests := []struct {
		name            string
		manifest        string
		want            *ateapipb.ResourceMetadata
		wantErrContains string
	}{
		{name: "one document", manifest: one, want: meta("one")},
		{name: "json", manifest: `{"atespace": "a", "name": "one"}`, want: meta("one")},
		{name: "leading separator", manifest: "---\n" + one, want: meta("one")},
		{name: "document end marker", manifest: one + "...\n", want: meta("one")},
		{name: "empty", manifest: "", wantErrContains: "manifest is empty"},
		{name: "only a separator", manifest: "---\n", wantErrContains: "manifest is empty"},
		{name: "comment only", manifest: "# nothing\n", wantErrContains: "manifest is empty"},
		{name: "not yaml", manifest: "\t{", wantErrContains: "invalid YAML"},
		{name: "unknown field", manifest: "nmae: one\n", wantErrContains: "invalid ResourceMetadata"},
		{name: "second document", manifest: one + "---\n" + one, wantErrContains: "more than one document"},
		{name: "trailing separator", manifest: one + "---\n", wantErrContains: "more than one document"},
		{name: "empty first document", manifest: "---\n# nothing\n---\n" + one, wantErrContains: "more than one document"},
		{name: "two leading separators", manifest: "---\n---\n" + one, wantErrContains: "more than one document"},
		{name: "end marker then separator", manifest: one + "...\n---\n", wantErrContains: "more than one document"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseOne[ateapipb.ResourceMetadata]([]byte(test.manifest))
			if test.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErrContains) {
					t.Fatalf("ParseOne() error = %v, want it to contain %q", err, test.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseOne() error = %v", err)
			}
			if diff := cmp.Diff(test.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("ParseOne() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
