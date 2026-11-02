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
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/renderer"
	"github.com/yuin/goldmark/util"

	"go.ate.dev/substrate/tools/apitool/internal/model"
)

// markdownRenderer renders doc comments as plain CommonMark, with no
// extensions (see the API style guide, section #13.2). Raw HTML shows as
// text: the doc-format rule keeps it out of today's comments, but a release
// from before the rule may have a placeholder such as <name> outside a code
// span.
var markdownRenderer = goldmark.New(goldmark.WithRendererOptions(
	renderer.WithNodeRenderers(util.Prioritized(rawHTMLAsText{}, 500)),
))

// rawHTMLAsText renders raw HTML, inline or as a block, as escaped text.
type rawHTMLAsText struct{}

func (rawHTMLAsText) RegisterFuncs(r renderer.NodeRendererFuncRegisterer) {
	r.Register(ast.KindRawHTML, renderRawHTML)
	r.Register(ast.KindHTMLBlock, renderHTMLBlock)
}

func renderRawHTML(w util.BufWriter, source []byte, n ast.Node, entering bool) (ast.WalkStatus, error) {
	if entering {
		segs := n.(*ast.RawHTML).Segments
		for i := range segs.Len() {
			seg := segs.At(i)
			template.HTMLEscape(w, seg.Value(source))
		}
	}
	return ast.WalkSkipChildren, nil
}

func renderHTMLBlock(w util.BufWriter, source []byte, n ast.Node, entering bool) (ast.WalkStatus, error) {
	if !entering {
		return ast.WalkContinue, nil
	}
	b := n.(*ast.HTMLBlock)
	if _, err := w.WriteString("<p>"); err != nil {
		return ast.WalkStop, err
	}
	lines := b.Lines()
	for i := range lines.Len() {
		line := lines.At(i)
		template.HTMLEscape(w, line.Value(source))
	}
	if b.HasClosure() {
		template.HTMLEscape(w, b.ClosureLine.Value(source))
	}
	if _, err := w.WriteString("</p>\n"); err != nil {
		return ast.WalkStop, err
	}
	return ast.WalkContinue, nil
}

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
