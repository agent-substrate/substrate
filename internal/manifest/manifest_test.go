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

// The tests parse into ateapipb.Atespace, whose only field is a nested message,
// so that flow-style YAML has something to nest.
func TestParse(t *testing.T) {
	tests := []struct {
		name            string
		manifest        string
		want            []*ateapipb.Atespace
		wantErrContains string
	}{
		{
			name: "single block-style YAML",
			manifest: `metadata:
  atespace: a
  name: one
`,
			want: []*ateapipb.Atespace{{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "one"}}},
		},
		{
			name:     "single flow-style YAML",
			manifest: "metadata: { atespace: a, name: one, }\n",
			want:     []*ateapipb.Atespace{{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "one"}}},
		},
		{
			name:     "single JSON",
			manifest: `{"metadata": {"atespace": "a", "name": "one"}}`,
			want:     []*ateapipb.Atespace{{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "one"}}},
		},
		{
			name: "YAML: leading document separator",
			manifest: `---
metadata:
  atespace: a
  name: one
`,
			want: []*ateapipb.Atespace{{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "one"}}},
		},
		{
			name: "YAML: document end marker (...)",
			manifest: `metadata:
  atespace: a
  name: one
...
`,
			want: []*ateapipb.Atespace{{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "one"}}},
		},
		{
			name: "YAML: trailing document separator",
			manifest: `metadata:
  atespace: a
  name: one
---
`,
			want: []*ateapipb.Atespace{{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "one"}}},
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
			want: []*ateapipb.Atespace{
				{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "one"}},
				{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "two"}},
			},
		},
		{
			name: "YAML multi-document in flow style",
			manifest: `metadata: { atespace: a, name: one, }
---
metadata: { atespace: a, name: two, }
`,
			want: []*ateapipb.Atespace{
				{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "one"}},
				{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "two"}},
			},
		},
		{
			name: "YAML multi-document in JSON style",
			manifest: `{"metadata": {"atespace": "a", "name": "one"}}
---
{"metadata": {"atespace": "a", "name": "two"}}
`,
			want: []*ateapipb.Atespace{
				{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "one"}},
				{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "two"}},
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
			want: []*ateapipb.Atespace{
				{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "one"}},
				{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "two"}},
			},
		},
		{
			name:     "YAML: alias",
			manifest: "metadata: { atespace: &x a, name: *x, }\n",
			want:     []*ateapipb.Atespace{{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "a"}}},
		},
		{name: "empty", manifest: "", wantErrContains: "manifest is empty"},
		{name: "YAML: only document separators", manifest: "---\n---\n", wantErrContains: "manifest is empty"},
		{name: "YAML: comment only", manifest: "# nothing\n", wantErrContains: "manifest is empty"},
		{name: "not YAML", manifest: "\t{", wantErrContains: "invalid YAML"},
		{
			name:            "YAML: invalid second document",
			manifest:        "metadata: { atespace: a, name: one, }\n---\n\t{\n",
			wantErrContains: "invalid YAML",
		},
		{
			name: "JSON documents with no separator are not supported",
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
		{name: "not an object", manifest: "- a\n", wantErrContains: "invalid Atespace"},
		{name: "JSON array of objects is not supported", manifest: `[{"metadata": {"atespace": "a", "name": "one"}}]`, wantErrContains: "invalid Atespace"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Parse[ateapipb.Atespace]([]byte(test.manifest))
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
	one := &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "one"}}
	two := &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "two"}}
	tests := []struct {
		name            string
		msgs            []*ateapipb.Atespace
		want            *ateapipb.Atespace
		wantErrContains string
	}{
		{name: "one record", msgs: []*ateapipb.Atespace{one}, want: one},
		{name: "no records", wantErrContains: "manifest is empty"},
		{name: "two records", msgs: []*ateapipb.Atespace{one, two}, wantErrContains: "manifest holds 2 Atespace records, expected one"},
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
