// Copyright 2026 The Agent Substrate Authors
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

package cmd

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestManifestMessages(t *testing.T) {
	tests := []struct {
		name string
		data string
		// wantNames is the metadata.name of each parsed document, in order.
		wantNames []string
		// wantIndexes is the document position of each parsed document.
		wantIndexes []int
		wantErr     []string
		wantNoErr   []string
	}{
		{
			name:        "single YAML document",
			data:        "metadata:\n  name: one\n",
			wantNames:   []string{"one"},
			wantIndexes: []int{1},
		},
		{
			name:        "single JSON object",
			data:        `{"metadata":{"name":"one"}}`,
			wantNames:   []string{"one"},
			wantIndexes: []int{1},
		},
		{
			name:        "block-style YAML documents",
			data:        "metadata: {name: one}\n---\nmetadata: {name: two}\n",
			wantNames:   []string{"one", "two"},
			wantIndexes: []int{1, 2},
		},
		{
			name:        "JSON-style YAML documents",
			data:        "--- {\"metadata\":{\"name\":\"one\"}}\n--- {\"metadata\":{\"name\":\"two\"}}\n",
			wantNames:   []string{"one", "two"},
			wantIndexes: []int{1, 2},
		},
		{
			name:        "empty documents are skipped but still counted",
			data:        "metadata: {name: one}\n---\n---\nmetadata: {name: two}\n---\n",
			wantNames:   []string{"one", "two"},
			wantIndexes: []int{1, 3},
		},
		{
			name: "two JSON objects without a separator",
			data: "{\"metadata\":{\"name\":\"one\"}}\n{\"metadata\":{\"name\":\"two\"}}\n",
			wantErr: []string{
				"invalid YAML",
			},
		},
		{
			name:      "unknown field in the second document names the document",
			data:      "metadata: {name: one}\n---\nmetadata: {name: two}\nsandboxClass: gvisor\n",
			wantErr:   []string{"document 2: invalid ActorTemplate", "unknown field"},
			wantNoErr: []string{"document 1:"},
		},
		{
			name:      "unknown field in a single document with a trailing separator has no document prefix",
			data:      "metadata: {name: one}\nsandboxClass: gvisor\n---\n",
			wantErr:   []string{"invalid ActorTemplate", "unknown field"},
			wantNoErr: []string{"document 1:"},
		},
		{
			name:      "unknown field in a single document has no document prefix",
			data:      "metadata: {name: one}\nsandboxClass: gvisor\n",
			wantErr:   []string{"invalid ActorTemplate", "unknown field"},
			wantNoErr: []string{"document 1:"},
		},
		{
			name:    "top-level sequence",
			data:    "- a\n- b\n",
			wantErr: []string{"expected a YAML mapping or a JSON object, got a sequence"},
		},
		{
			name:    "top-level scalar",
			data:    "just-a-string\n",
			wantErr: []string{"expected a YAML mapping or a JSON object, got a scalar"},
		},
		{
			name:    "sequence in the second document names the document",
			data:    "metadata: {name: one}\n---\n- a\n- b\n",
			wantErr: []string{"document 2: expected a YAML mapping or a JSON object, got a sequence"},
		},
		{
			name:        "YAML aliases resolve",
			data:        "containers:\n- &c {name: c, image: i}\n- *c\n",
			wantNames:   []string{""},
			wantIndexes: []int{1},
		},
		{
			name:    "empty manifest",
			data:    "",
			wantErr: []string{"manifest is empty"},
		},
		{
			name:    "only document separators",
			data:    "---\n",
			wantErr: []string{"manifest is empty"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			docs, err := manifestMessages([]byte(tt.data), &ateapi.ActorTemplate{})
			if len(tt.wantErr) > 0 {
				if err == nil {
					t.Fatalf("manifestMessages succeeded with %d documents, want an error", len(docs))
				}
				for _, want := range tt.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error = %q, want it to contain %q", err, want)
					}
				}
				for _, no := range tt.wantNoErr {
					if strings.Contains(err.Error(), no) {
						t.Errorf("error = %q, want it not to contain %q", err, no)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("manifestMessages: %v", err)
			}
			var names []string
			var indexes []int
			for _, doc := range docs {
				names = append(names, doc.Message.(*ateapi.ActorTemplate).GetMetadata().GetName())
				indexes = append(indexes, doc.Index)
			}
			if diff := cmp.Diff(tt.wantNames, names); diff != "" {
				t.Errorf("document names mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantIndexes, indexes); diff != "" {
				t.Errorf("document indexes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestManifestMessagesAliasContent checks that an aliased node is expanded
// rather than dropped.
func TestManifestMessagesAliasContent(t *testing.T) {
	data := "containers:\n- &c {name: c, image: i}\n- *c\n"
	docs, err := manifestMessages([]byte(data), &ateapi.ActorTemplate{})
	if err != nil {
		t.Fatalf("manifestMessages: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("manifestMessages returned %d documents, want 1", len(docs))
	}
	template := docs[0].Message.(*ateapi.ActorTemplate)
	want := []*ateapi.Container{{Name: "c", Image: "i"}, {Name: "c", Image: "i"}}
	if diff := cmp.Diff(want, template.GetContainers(), protocmp.Transform()); diff != "" {
		t.Errorf("containers mismatch (-want +got):\n%s", diff)
	}
}
