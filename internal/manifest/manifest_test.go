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

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/testing/protocmp"
)

// The tests parse into ateapi.Atespace, whose only field is a nested message,
// so that flow-style YAML has something to nest.
func TestParse(t *testing.T) {
	tests := []struct {
		name            string
		manifest        string
		want            []*ateapi.Atespace
		wantErrContains string
	}{
		// Supported.
		{
			name: "single block-style YAML",
			manifest: `metadata:
  atespace: a
  name: one
`,
			want: []*ateapi.Atespace{{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "one"}}},
		},
		{
			name:     "single flow-style YAML",
			manifest: "metadata: { atespace: a, name: one, }\n",
			want:     []*ateapi.Atespace{{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "one"}}},
		},
		{
			name:     "single JSON",
			manifest: `{"metadata": {"atespace": "a", "name": "one"}}`,
			want:     []*ateapi.Atespace{{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "one"}}},
		},
		{
			name: "YAML: leading document separator",
			manifest: `---
metadata:
  atespace: a
  name: one
`,
			want: []*ateapi.Atespace{{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "one"}}},
		},
		{
			name: "YAML: document end marker (...)",
			manifest: `metadata:
  atespace: a
  name: one
...
`,
			want: []*ateapi.Atespace{{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "one"}}},
		},
		{
			name: "YAML: trailing document separator",
			manifest: `metadata:
  atespace: a
  name: one
---
`,
			want: []*ateapi.Atespace{{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "one"}}},
		},
		{
			name: "YAML multi-document in block style",
			manifest: `metadata:
  atespace: a
  name: one
---
metadata:
  atespace: a
  name: two
`,
			want: []*ateapi.Atespace{
				{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "one"}},
				{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "two"}},
			},
		},
		{
			name: "YAML multi-document in flow style",
			manifest: `metadata: { atespace: a, name: one, }
---
metadata: { atespace: a, name: two, }
`,
			want: []*ateapi.Atespace{
				{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "one"}},
				{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "two"}},
			},
		},
		{
			name: "YAML multi-document in JSON style",
			manifest: `{"metadata": {"atespace": "a", "name": "one"}}
---
{"metadata": {"atespace": "a", "name": "two"}}
`,
			want: []*ateapi.Atespace{
				{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "one"}},
				{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "two"}},
			},
		},
		{
			name: "YAML: empty documents are skipped",
			manifest: `---
# nothing
---
metadata: { atespace: a, name: one, }
---
---
metadata: { atespace: a, name: two, }
---
`,
			want: []*ateapi.Atespace{
				{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "one"}},
				{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "two"}},
			},
		},
		{
			name:     "YAML: alias",
			manifest: "metadata: { atespace: &x a, name: *x, }\n",
			want:     []*ateapi.Atespace{{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "a"}}},
		},
		// Rejected.
		{name: "empty input", manifest: "", wantErrContains: "manifest is empty"},
		{name: "YAML: only document separators", manifest: "---\n---\n", wantErrContains: "manifest is empty"},
		{name: "YAML: comment only", manifest: "# nothing\n", wantErrContains: "manifest is empty"},
		{name: "not YAML: stray tab", manifest: "\t{", wantErrContains: "invalid YAML"},
		{
			name:            "YAML: invalid second document",
			manifest:        "metadata: { atespace: a, name: one, }\n---\n\t{\n",
			wantErrContains: "invalid YAML",
		},
		{
			name: "JSON objects with no separator",
			manifest: `{"metadata": {"atespace": "a", "name": "one"}}
{"metadata": {"atespace": "a", "name": "two"}}
`,
			wantErrContains: "invalid YAML",
		},
		{
			name:            "unknown field",
			manifest:        `{"metadata": {"atespace": "a", "nmae": "one"}}`,
			wantErrContains: "invalid Atespace",
		},
		{
			name: "YAML: unknown field in second document",
			manifest: `metadata: { atespace: a, name: one, }
---
metadata: { atespace: a, nmae: two, }
`,
			wantErrContains: "document 2: invalid Atespace",
		},
		{name: "YAML: top-level sequence", manifest: "- a\n", wantErrContains: "invalid Atespace"},
		{name: "JSON: top-level array", manifest: `[{"metadata": {"atespace": "a", "name": "one"}}]`, wantErrContains: "invalid Atespace"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Parse[ateapi.Atespace]([]byte(test.manifest))
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

func TestOne(t *testing.T) {
	one := &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "one"}}
	two := &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Atespace: "a", Name: "two"}}
	tests := []struct {
		name            string
		msgs            []*ateapi.Atespace
		want            *ateapi.Atespace
		wantErrContains string
	}{
		{name: "one record", msgs: []*ateapi.Atespace{one}, want: one},
		{name: "no records", wantErrContains: "manifest is empty"},
		{name: "two records", msgs: []*ateapi.Atespace{one, two}, wantErrContains: "manifest holds 2 Atespace records, expected one"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := One(test.msgs)
			if test.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErrContains) {
					t.Fatalf("One() error = %v, want it to contain %q", err, test.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("One() error = %v", err)
			}
			if diff := cmp.Diff(test.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("One() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
