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
	"bytes"
	"html/template"

	"github.com/yuin/goldmark"

	"go.ate.dev/substrate/tools/apitool/internal/model"
)

// markdownRenderer renders doc comments as plain CommonMark, with no
// extensions and raw HTML dropped (see the API style guide, section #13.2).
var markdownRenderer = goldmark.New()

// fullDoc renders comment's public doc as HTML.
func fullDoc(comment string) (template.HTML, error) {
	return renderMarkdown(model.PublicDoc(comment))
}

// summaryDoc renders comment's summary, its first paragraph, as HTML.
func summaryDoc(comment string) (template.HTML, error) {
	return renderMarkdown(model.Summary(comment))
}

func renderMarkdown(text string) (template.HTML, error) {
	var buf bytes.Buffer
	if err := markdownRenderer.Convert([]byte(text), &buf); err != nil {
		return "", err
	}
	return template.HTML(buf.String()), nil
}
