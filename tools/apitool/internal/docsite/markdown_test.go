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

package docsite

import "testing"

func TestFullDoc(t *testing.T) {
	tests := []struct {
		name, comment, want string
	}{
		{
			name:    "strips k8s directives",
			comment: "atespace is the namespace.\n\n+k8s:optional\n+k8s:immutable",
			want:    "<p>atespace is the namespace.</p>\n",
		},
		{
			name:    "placeholder in a code span escaped once",
			comment: "sub: `actor/<atespace>/<name>`.",
			want:    "<p>sub: <code>actor/&lt;atespace&gt;/&lt;name&gt;</code>.</p>\n",
		},
		{
			name:    "script tag dropped",
			comment: "<script>alert(1)</script>",
			want:    "<!-- raw HTML omitted -->\n",
		},
		{
			name:    "link with a title",
			comment: `see [the docs](https://example.com/?a=1&b=2 "title")`,
			want:    `<p>see <a href="https://example.com/?a=1&amp;b=2" title="title">the docs</a></p>` + "\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := fullDoc(tc.comment)
			if err != nil {
				t.Fatalf("fullDoc() error = %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("fullDoc(%q) = %q, want %q", tc.comment, got, tc.want)
			}
		})
	}
}

func TestSummaryDoc(t *testing.T) {
	got, err := summaryDoc("Get an Actor.\n\nThe Actor must exist.\n\n+k8s:required")
	if err != nil {
		t.Fatalf("summaryDoc() error = %v", err)
	}
	if want := "<p>Get an Actor.</p>\n"; string(got) != want {
		t.Errorf("summaryDoc() = %q, want %q", got, want)
	}
}
