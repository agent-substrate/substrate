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

package lint

import (
	"regexp"
	"strings"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.ate.dev/substrate/tools/apitool/internal/model"
)

var (
	ifDisabledDirective = regexp.MustCompile(`^\+k8s:ifDisabled\(([^)]+)\)=`)
	gateNamePattern     = regexp.MustCompile(`^[A-Z][a-z0-9]+(?:[A-Z][a-z0-9]*)*$`)
	internalNoteSpan    = regexp.MustCompile(`(?s)\(--.*?--\)`)
)

var DocFormat = Rule{
	Name:        "doc-format",
	Description: "Every doc comment closes each \"(--\" internal note with \"--)\", ends with its +k8s: and +ate: directives, with nothing after them, uses valid +ate:preview directives, and has no raw HTML.",
	Check: allOf(
		forEachMessage(func(m model.Message) []Finding { return checkDocFormat(m.FullName, m.Comment, false) }),
		forEachField(func(m model.Message, f model.Field) []Finding {
			return checkDocFormat(m.FullName.Append(f.Name), f.Comment, true)
		}),
		forEachEnum(checkEnumDocFormat),
		forEachMethod(func(rpc model.Method) []Finding { return checkDocFormat(subjectOf(rpc), rpc.Comment, false) }),
	),
}

func checkEnumDocFormat(e model.Enum) []Finding {
	findings := checkDocFormat(e.FullName, e.Comment, false)
	for _, v := range e.Values {
		findings = append(findings, checkDocFormat(e.FullName.Append(v.Name), v.Comment, false)...)
	}
	return findings
}

func checkDocFormat(subject protoreflect.FullName, comment string, allowPreview bool) []Finding {
	var findings []Finding
	stripped := comment
	if !internalNotesBalanced(comment) {
		findings = append(findings, findingf(subject, `doc comment has an internal note that isn't opened with "(--" and closed with "--)"`))
	} else {
		stripped = internalNoteSpan.ReplaceAllString(comment, "")
	}
	if line, ok := textAfterDirectives(stripped); ok {
		findings = append(findings, findingf(subject, "doc comment has text after its directives: %q", line))
	}
	findings = append(findings, checkPreviewDirectives(subject, stripped, allowPreview)...)
	if html, ok := rawHTML(model.PublicDoc(comment)); ok {
		findings = append(findings, findingf(subject, "doc comment has raw HTML, which shows as literal text; put a placeholder in a code span: %q", html))
	}
	return findings
}

func checkPreviewDirectives(subject protoreflect.FullName, comment string, allowPreview bool) []Finding {
	var findings []Finding
	var previewGate string
	var ifDisabledGates []string
	for line := range strings.SplitSeq(comment, "\n") {
		trimmed := strings.TrimSpace(line)
		if i := strings.IndexByte(trimmed, '#'); i >= 0 {
			trimmed = strings.TrimSpace(trimmed[:i])
		}
		if strings.HasPrefix(trimmed, "+ate:") {
			if !allowPreview {
				findings = append(findings, findingf(subject, "+ate: directive is only supported on fields: %q", trimmed))
				continue
			}
			gate, ok := strings.CutPrefix(trimmed, "+ate:preview=")
			if !ok || !gateNamePattern.MatchString(gate) {
				findings = append(findings, findingf(subject, `invalid +ate: directive %q; want "+ate:preview=<UpperCamelCaseGate>"`, trimmed))
				continue
			}
			if previewGate != "" {
				findings = append(findings, findingf(subject, "field has multiple +ate:preview directives"))
				continue
			}
			previewGate = gate
		}
		if m := ifDisabledDirective.FindStringSubmatch(trimmed); m != nil {
			ifDisabledGates = append(ifDisabledGates, strings.Trim(m[1], `"`))
		}
	}
	if allowPreview {
		for _, g := range ifDisabledGates {
			if previewGate != g {
				findings = append(findings, findingf(subject, "field has +k8s:ifDisabled(%s) without matching +ate:preview=%s", g, g))
			}
		}
	}
	return findings
}

// rawHTML returns the first raw HTML that doc parses to as CommonMark, such
// as a "<name>" placeholder outside a code span.
func rawHTML(doc string) (string, bool) {
	source := []byte(doc)
	var found string
	_ = ast.Walk(goldmark.DefaultParser().Parse(text.NewReader(source)), func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		switch n := n.(type) {
		case *ast.RawHTML:
			found = string(n.Segments.Value(source))
		case *ast.HTMLBlock:
			found = strings.TrimSpace(string(n.Lines().Value(source)))
		default:
			return ast.WalkContinue, nil
		}
		return ast.WalkStop, nil
	})
	return found, found != ""
}

// internalNotesBalanced reports whether every "(--" in comment is closed by
// a "--)" before the next "(--", and every "--)" closes one.
func internalNotesBalanced(comment string) bool {
	open := false
	for rest := comment; ; {
		i, j := strings.Index(rest, "(--"), strings.Index(rest, "--)")
		switch {
		case i < 0 && j < 0:
			return !open
		case i >= 0 && (j < 0 || i < j):
			if open {
				return false
			}
			open, rest = true, rest[i+len("(--"):]
		default:
			if !open {
				return false
			}
			open, rest = false, rest[j+len("--)"):]
		}
	}
}

// textAfterDirectives returns the first non-blank line after comment's
// first +k8s: or +ate: directive that isn't a directive itself.
func textAfterDirectives(comment string) (string, bool) {
	inDirectives := false
	for line := range strings.SplitSeq(comment, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "+k8s:"), strings.HasPrefix(trimmed, "+ate:"):
			inDirectives = true
		case inDirectives && trimmed != "":
			return trimmed, true
		}
	}
	return "", false
}
