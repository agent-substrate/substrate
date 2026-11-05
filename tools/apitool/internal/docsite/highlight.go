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
	"fmt"
	"html/template"
	"strings"

	"github.com/alecthomas/chroma/v2"
	"github.com/alecthomas/chroma/v2/lexers"
)

// yamlLexer tokenizes the examples.
var yamlLexer = lexers.Get("yaml")

// highlightYAML renders code, a YAML document, as HTML, coloring each token
// and keeping the code's own formatting.
func highlightYAML(code string) (template.HTML, error) {
	tokens, err := yamlLexer.Tokenise(nil, code)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for tok := tokens(); tok != chroma.EOF; tok = tokens() {
		class := tokenClass(tok.Type)
		// Keep line breaks and leading spaces outside the spans, so codeLines
		// can split lines and measure their indentation.
		for i, part := range strings.Split(tok.Value, "\n") {
			if i > 0 {
				b.WriteByte('\n')
			}
			body := strings.TrimLeft(part, " ")
			b.WriteString(part[:len(part)-len(body)])
			writeToken(&b, class, body)
		}
	}
	return template.HTML(b.String()), nil
}

// tokenClass is the class highlightYAML colors a token of type t with, or "" to
// leave it uncolored.
func tokenClass(t chroma.TokenType) string {
	switch {
	case t == chroma.NameTag:
		return "tok-type"
	case t.InSubCategory(chroma.LiteralNumber):
		return "tok-number"
	case t.InCategory(chroma.Literal):
		return "tok-string"
	case t.InCategory(chroma.Keyword):
		return "tok-keyword"
	case t.InCategory(chroma.Punctuation), t.InCategory(chroma.Operator):
		return "tok-punct"
	default:
		return ""
	}
}

// codeLines wraps each line of code, as highlightYAML renders it, in a block
// whose --indent is the line's indentation. The stylesheet hangs a wrapped
// line's continuation under its own indentation. highlightYAML only breaks
// lines and writes indentation outside any tag.
func codeLines(code template.HTML) template.HTML {
	var b strings.Builder
	for line := range strings.SplitSeq(string(code), "\n") {
		indent := len(line) - len(strings.TrimLeft(line, " "))
		fmt.Fprintf(&b, `<span class="line" style="--indent: %d">%s</span>`, indent, line)
	}
	return template.HTML(b.String())
}

// writeToken writes text, escaped, in a span of class, or bare if class is "".
func writeToken(b *strings.Builder, class, text string) {
	if text == "" {
		return
	}
	if class == "" {
		template.HTMLEscape(b, []byte(text))
		return
	}
	b.WriteString(`<span class="`)
	b.WriteString(class)
	b.WriteString(`">`)
	template.HTMLEscape(b, []byte(text))
	b.WriteString(`</span>`)
}
