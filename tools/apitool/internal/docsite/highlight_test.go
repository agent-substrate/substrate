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

package docsite

import (
	"html/template"
	"testing"
)

func TestCodeLines(t *testing.T) {
	in := template.HTML("<span class=\"tok-punct\">{</span>\n\n    <span class=\"tok-type\">a</span>\n}")
	want := `<span class="line" style="--indent: 0"><span class="tok-punct">{</span></span>` +
		`<span class="line" style="--indent: 0"></span>` +
		`<span class="line" style="--indent: 4">    <span class="tok-type">a</span></span>` +
		`<span class="line" style="--indent: 0">}</span>`
	if got := codeLines(in); got != template.HTML(want) {
		t.Errorf("codeLines() =\n%s\nwant:\n%s", got, want)
	}
}

func TestHighlightYAML(t *testing.T) {
	for _, tc := range []struct {
		name, in, want string
	}{
		{
			name: "mappings and lists",
			in: `spec:
  count: 2
  ok: true
  quoted: "true"
  url: gs://a/b # note
  tag: a <b>
  items:
  - name: a`,
			want: `<span class="tok-type">spec</span><span class="tok-punct">:</span>
  <span class="tok-type">count</span><span class="tok-punct">:</span> <span class="tok-number">2</span>
  <span class="tok-type">ok</span><span class="tok-punct">:</span> <span class="tok-keyword">true</span>
  <span class="tok-type">quoted</span><span class="tok-punct">:</span> <span class="tok-string">&#34;true&#34;</span>
  <span class="tok-type">url</span><span class="tok-punct">:</span> <span class="tok-string">gs://a/b</span> # note
  <span class="tok-type">tag</span><span class="tok-punct">:</span> <span class="tok-string">a &lt;b&gt;</span>
  <span class="tok-type">items</span><span class="tok-punct">:</span>
  - <span class="tok-type">name</span><span class="tok-punct">:</span> <span class="tok-string">a</span>`,
		},
		{
			name: "multiline token",
			in: `note: |
  one
  two`,
			want: `<span class="tok-type">note</span><span class="tok-punct">:</span> <span class="tok-punct">|</span>
  <span class="tok-string">one</span>
  <span class="tok-string">two</span>`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := highlightYAML(tc.in)
			if err != nil {
				t.Fatalf("highlightYAML() error = %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("highlightYAML() =\n%s\nwant:\n%s", got, tc.want)
			}
		})
	}
}
