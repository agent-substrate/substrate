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
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"html/template"
	"io"
	"strings"
)

// highlightJSON renders text, a JSON value, as HTML, coloring each token and
// keeping the text's own formatting.
func highlightJSON(text string) (template.HTML, error) {
	var b strings.Builder
	dec := jsontext.NewDecoder(strings.NewReader(text))
	for {
		start := dec.InputOffset()
		isName := nextIsName(dec)
		tok, err := dec.ReadToken()
		if errors.Is(err, io.EOF) {
			b.WriteString(text[start:])
			return template.HTML(b.String()), nil
		}
		if err != nil {
			return "", err
		}
		// ReadToken also reads what precedes the token: whitespace and at
		// most one "," or ":".
		read := text[start:dec.InputOffset()]
		raw := strings.TrimLeft(read, " \t\r\n,:")
		writeSeparators(&b, read[:len(read)-len(raw)])
		writeToken(&b, jsonClass(tok.Kind(), isName), raw)
	}
}

// nextIsName reports whether dec's next token is the name of an object
// member. An object counts its names and values separately, so its next token
// is a name when the count so far is even.
func nextIsName(dec *jsontext.Decoder) bool {
	kind, n := dec.StackIndex(dec.StackDepth())
	return kind == jsontext.KindBeginObject && n%2 == 0
}

// jsonClass is the class highlightJSON colors a token of the given kind with.
func jsonClass(kind jsontext.Kind, isName bool) string {
	switch kind {
	case jsontext.KindString:
		if isName {
			return "tok-type"
		}
		return "tok-string"
	case jsontext.KindNumber:
		return "tok-number"
	case jsontext.KindNull, jsontext.KindFalse, jsontext.KindTrue:
		return "tok-keyword"
	default:
		return "tok-punct"
	}
}

// writeSeparators writes the text between two tokens, coloring its "," or
// ":".
func writeSeparators(b *strings.Builder, text string) {
	for _, c := range text {
		if c == ',' || c == ':' {
			writeToken(b, "tok-punct", string(c))
		} else {
			b.WriteRune(c)
		}
	}
}

// codeLines wraps each line of code, as highlightJSON renders it, in a
// block whose --indent is the line's indentation. The stylesheet hangs a
// wrapped line's continuation under its own indentation. highlightJSON only
// breaks lines between tokens, outside any tag.
func codeLines(code template.HTML) template.HTML {
	var b strings.Builder
	for line := range strings.SplitSeq(string(code), "\n") {
		indent := len(line) - len(strings.TrimLeft(line, " "))
		fmt.Fprintf(&b, `<span class="line" style="--indent: %d">%s</span>`, indent, line)
	}
	return template.HTML(b.String())
}

func writeToken(b *strings.Builder, class, text string) {
	b.WriteString(`<span class="`)
	b.WriteString(class)
	b.WriteString(`">`)
	template.HTMLEscape(b, []byte(text))
	b.WriteString(`</span>`)
}
