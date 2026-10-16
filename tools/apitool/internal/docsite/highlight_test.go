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

func TestHighlightJSON(t *testing.T) {
	in := `{
  "name": "a \"b\" <c>",
  "count": -1.5e3,
  "ok": true,
  "none": null,
  "list": []
}`
	want := `<span class="tok-punct">{</span>
  <span class="tok-type">&#34;name&#34;</span><span class="tok-punct">:</span> <span class="tok-string">&#34;a \&#34;b\&#34; &lt;c&gt;&#34;</span><span class="tok-punct">,</span>
  <span class="tok-type">&#34;count&#34;</span><span class="tok-punct">:</span> <span class="tok-number">-1.5e3</span><span class="tok-punct">,</span>
  <span class="tok-type">&#34;ok&#34;</span><span class="tok-punct">:</span> <span class="tok-keyword">true</span><span class="tok-punct">,</span>
  <span class="tok-type">&#34;none&#34;</span><span class="tok-punct">:</span> <span class="tok-keyword">null</span><span class="tok-punct">,</span>
  <span class="tok-type">&#34;list&#34;</span><span class="tok-punct">:</span> <span class="tok-punct">[</span><span class="tok-punct">]</span>
<span class="tok-punct">}</span>`
	got, err := highlightJSON(in)
	if err != nil {
		t.Fatalf("highlightJSON() error = %v", err)
	}
	if string(got) != want {
		t.Errorf("highlightJSON() =\n%s\nwant:\n%s", got, want)
	}
	if _, err := highlightJSON(`{"name": }`); err == nil {
		t.Error("highlightJSON() of invalid JSON: got nil error, want one")
	}
}
