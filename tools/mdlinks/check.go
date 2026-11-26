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

package main

import (
	"bytes"
	"errors"
	"fmt"
	stdhtml "html"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"
	xnethtml "golang.org/x/net/html"
)

type problem struct {
	file   string
	line   int
	target string
	reason string
}

func (p problem) String() string {
	return fmt.Sprintf("%s:%d: %s: %s", p.file, p.line, p.target, p.reason)
}

type link struct {
	line   int
	target string
}

type checker struct {
	root    string
	md      goldmark.Markdown
	anchors map[string]map[string]bool
}

func newChecker(root string) (*checker, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root %q: %w", root, err)
	}
	info, err := os.Stat(absRoot)
	if err != nil {
		return nil, fmt.Errorf("stat root %q: %w", root, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("root %q is not a directory", root)
	}
	return &checker{
		root:    absRoot,
		md:      goldmark.New(goldmark.WithExtensions(extension.GFM)),
		anchors: make(map[string]map[string]bool),
	}, nil
}

func (c *checker) checkFile(file string) ([]problem, error) {
	absPath := file
	if !filepath.IsAbs(absPath) {
		absPath = filepath.Join(c.root, file)
	}
	absPath = filepath.Clean(absPath)

	displayPath := file
	if rel, err := filepath.Rel(c.root, absPath); err == nil && filepath.IsLocal(rel) {
		displayPath = filepath.ToSlash(rel)
	}

	src, err := os.ReadFile(absPath)
	if err != nil {
		return nil, err
	}
	src = bytes.ReplaceAll(src, []byte("\r\n"), []byte("\n"))

	body, offset := stripFrontMatter(src)
	var problems []problem
	for _, l := range c.extractLinks(body, offset) {
		if p := c.checkLink(displayPath, absPath, l); p != nil {
			problems = append(problems, *p)
		}
	}
	return problems, nil
}

func (c *checker) checkLink(displayFile, srcAbs string, l link) *problem {
	dest := strings.TrimSpace(l.target)
	fail := func(reason string) *problem {
		return &problem{file: displayFile, line: l.line, target: dest, reason: reason}
	}

	if dest == "" {
		return fail("empty link target")
	}
	if scheme := uriScheme(dest); scheme != "" {
		if strings.EqualFold(scheme, "file") {
			return fail("local file:// URL")
		}
		return nil
	}
	if strings.HasPrefix(dest, "//") {
		return nil
	}

	rawPath, rawFrag, hasFrag := strings.Cut(dest, "#")
	rawPath, _, _ = strings.Cut(rawPath, "?")

	pathPart, err := url.PathUnescape(rawPath)
	if err != nil {
		return fail(fmt.Sprintf("invalid path escape: %v", err))
	}
	frag, err := url.PathUnescape(rawFrag)
	if err != nil {
		return fail(fmt.Sprintf("invalid fragment escape: %v", err))
	}

	var targetAbs string
	switch {
	case pathPart == "":
		targetAbs = srcAbs
	case strings.HasPrefix(pathPart, "/"):
		targetAbs = filepath.Join(c.root, filepath.FromSlash(strings.TrimPrefix(pathPart, "/")))
	default:
		targetAbs = filepath.Join(filepath.Dir(srcAbs), filepath.FromSlash(pathPart))
	}
	targetAbs = filepath.Clean(targetAbs)

	if rel, err := filepath.Rel(c.root, targetAbs); err != nil || !filepath.IsLocal(rel) {
		return fail("path escapes repository root")
	}

	info, err := os.Stat(targetAbs)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fail("file not found")
		}
		return fail(err.Error())
	}
	if strings.HasSuffix(pathPart, "/") && !info.IsDir() {
		return fail("not a directory")
	}

	if !hasFrag {
		return nil
	}
	if frag == "" {
		if pathPart == "" {
			return nil
		}
		return fail("empty anchor fragment")
	}

	anchorTarget := targetAbs
	if info.IsDir() {
		anchorTarget = filepath.Join(targetAbs, "README.md")
		if _, err := os.Stat(anchorTarget); err != nil {
			return fail(fmt.Sprintf("anchor #%s not found (directory has no README.md)", frag))
		}
	} else if !strings.EqualFold(filepath.Ext(anchorTarget), ".md") {
		return nil
	}

	anchors, err := c.fileAnchors(anchorTarget)
	if err != nil {
		return fail(fmt.Sprintf("read target anchors: %v", err))
	}
	if !anchors[frag] {
		return fail(fmt.Sprintf("anchor #%s not found", frag))
	}
	return nil
}

func (c *checker) fileAnchors(absPath string) (map[string]bool, error) {
	if cached, ok := c.anchors[absPath]; ok {
		return cached, nil
	}
	src, err := os.ReadFile(absPath)
	if err != nil {
		return nil, err
	}
	src = bytes.ReplaceAll(src, []byte("\r\n"), []byte("\n"))
	anchors := c.extractAnchors(src)
	c.anchors[absPath] = anchors
	return anchors, nil
}

// decodeDestination unescapes backslash-escaped punctuation and HTML character
// references in a Goldmark link or image destination, matching Goldmark's HTML
// renderer.
func decodeDestination(raw []byte) string {
	v := util.UnescapePunctuations(raw)
	v = util.ResolveNumericReferences(v)
	v = util.ResolveEntityNames(v)
	return string(v)
}

func (c *checker) extractLinks(src []byte, lineOffset int) []link {
	doc := c.md.Parser().Parse(text.NewReader(src))
	lines := newLineIndex(src, lineOffset)
	var out []link

	_ = ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		switch node := n.(type) {
		case *ast.Link:
			out = append(out, link{
				line:   lines.lineAt(node.Pos()),
				target: decodeDestination(node.Destination),
			})
		case *ast.Image:
			out = append(out, link{
				line:   lines.lineAt(node.Pos()),
				target: decodeDestination(node.Destination),
			})
		case *ast.RawHTML:
			if buf, start := joinSegments(src, node.Segments, text.Segment{}, false); len(buf) > 0 {
				out = append(out, extractHTMLLinks(buf, lines.lineAt(start))...)
			}
		case *ast.HTMLBlock:
			if buf, start := joinSegments(src, node.Lines(), node.ClosureLine, node.HasClosure()); len(buf) > 0 {
				out = append(out, extractHTMLLinks(buf, lines.lineAt(start))...)
			}
		}
		return ast.WalkContinue, nil
	})
	return out
}

func (c *checker) extractAnchors(src []byte) map[string]bool {
	body, _ := stripFrontMatter(src)
	doc := c.md.Parser().Parse(text.NewReader(body))
	anchors := make(map[string]bool)

	_ = ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		switch node := n.(type) {
		case *ast.Heading:
			if base := githubSlug(headingText(node, body)); base != "" {
				anchors[uniqueSlug(base, anchors)] = true
			}
		case *ast.RawHTML:
			if buf, _ := joinSegments(body, node.Segments, text.Segment{}, false); len(buf) > 0 {
				extractHTMLAnchors(buf, anchors)
			}
		case *ast.HTMLBlock:
			if buf, _ := joinSegments(body, node.Lines(), node.ClosureLine, node.HasClosure()); len(buf) > 0 {
				extractHTMLAnchors(buf, anchors)
			}
		}
		return ast.WalkContinue, nil
	})
	return anchors
}

// uniqueSlug reproduces GitHub's duplicate-heading suffixing: the first
// occurrence uses base, and subsequent collisions append -1, -2, and so on.
func uniqueSlug(base string, used map[string]bool) string {
	if !used[base] {
		return base
	}
	for i := 1; ; i++ {
		candidate := fmt.Sprintf("%s-%d", base, i)
		if !used[candidate] {
			return candidate
		}
	}
}

func headingText(heading *ast.Heading, src []byte) string {
	var b strings.Builder
	inCode := 0
	_ = ast.Walk(heading, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		switch node := n.(type) {
		case *ast.CodeSpan:
			if entering {
				inCode++
			} else {
				inCode--
			}
		case *ast.Image, *ast.RawHTML:
			return ast.WalkSkipChildren, nil
		case *ast.AutoLink:
			if entering {
				b.Write(node.Label(src))
			}
		case *ast.Text:
			if !entering {
				return ast.WalkContinue, nil
			}
			val := string(node.Segment.Value(src))
			if inCode == 0 {
				val = stdhtml.UnescapeString(val)
			}
			b.WriteString(val)
			if node.SoftLineBreak() || node.HardLineBreak() {
				b.WriteByte(' ')
			}
		case *ast.String:
			if entering {
				val := string(node.Value)
				if inCode == 0 {
					val = stdhtml.UnescapeString(val)
				}
				b.WriteString(val)
			}
		}
		return ast.WalkContinue, nil
	})
	return b.String()
}

// githubSlug matches GitHub's and Hugo's autoHeadingIDType=github anchor IDs:
// lowercase text, keep letters, digits, hyphens, and underscores, convert
// whitespace to hyphens without collapsing consecutive hyphens, and drop other
// punctuation.
func githubSlug(s string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(strings.ToLower(s)) {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r), r == '-', r == '_':
			b.WriteRune(r)
		case unicode.IsSpace(r):
			b.WriteByte('-')
		}
	}
	return b.String()
}

// stripFrontMatter removes a leading YAML front matter block delimited by ---
// lines so Goldmark does not parse it as a setext heading. Expects LF line
// endings and returns the body plus the number of stripped lines.
func stripFrontMatter(src []byte) ([]byte, int) {
	if !bytes.HasPrefix(src, []byte("---\n")) {
		return src, 0
	}
	pos := 4
	lines := 1
	for pos < len(src) {
		nl := bytes.IndexByte(src[pos:], '\n')
		var line []byte
		var next int
		if nl < 0 {
			line = src[pos:]
			next = len(src)
		} else {
			line = src[pos : pos+nl]
			next = pos + nl + 1
		}
		lines++
		if bytes.Equal(line, []byte("---")) {
			return src[next:], lines
		}
		pos = next
	}
	return src, 0
}

// uriScheme returns the RFC 3986 URI scheme of dest, or "" if dest has none.
func uriScheme(dest string) string {
	scheme, _, ok := strings.Cut(dest, ":")
	if !ok || scheme == "" {
		return ""
	}
	for i := 0; i < len(scheme); i++ {
		ch := scheme[i]
		isAlpha := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
		if i == 0 && !isAlpha {
			return ""
		}
		if !isAlpha && (ch < '0' || ch > '9') && ch != '+' && ch != '-' && ch != '.' {
			return ""
		}
	}
	return scheme
}

type lineIndex struct {
	starts []int
	offset int
}

func newLineIndex(src []byte, lineOffset int) lineIndex {
	starts := []int{0}
	for i, b := range src {
		if b == '\n' {
			starts = append(starts, i+1)
		}
	}
	return lineIndex{starts: starts, offset: lineOffset}
}

func (li lineIndex) lineAt(byteOffset int) int {
	if byteOffset < 0 {
		return li.offset + 1
	}
	return sort.Search(len(li.starts), func(i int) bool {
		return li.starts[i] > byteOffset
	}) + li.offset
}

func joinSegments(src []byte, segs *text.Segments, closure text.Segment, hasClosure bool) ([]byte, int) {
	var buf []byte
	firstStart := -1
	appendSeg := func(seg text.Segment) {
		val := seg.Value(src)
		if len(val) == 0 {
			return
		}
		if firstStart < 0 {
			firstStart = seg.Start
		}
		buf = append(buf, val...)
	}
	if segs != nil {
		for i := range segs.Len() {
			appendSeg(segs.At(i))
		}
	}
	if hasClosure {
		appendSeg(closure)
	}
	return buf, firstStart
}

func extractHTMLLinks(buf []byte, firstLine int) []link {
	var out []link
	z := xnethtml.NewTokenizer(bytes.NewReader(buf))
	offset := 0
	for {
		tt := z.Next()
		raw := z.Raw()
		tokStart := offset
		offset += len(raw)
		if tt == xnethtml.ErrorToken {
			return out
		}
		if tt != xnethtml.StartTagToken && tt != xnethtml.SelfClosingTagToken {
			continue
		}
		tok := z.Token()
		line := firstLine + bytes.Count(buf[:tokStart], []byte("\n"))
		for _, a := range tok.Attr {
			switch {
			case tok.Data == "a" && a.Key == "href":
				out = append(out, link{line: line, target: a.Val})
			case (tok.Data == "img" || tok.Data == "source") && a.Key == "src":
				out = append(out, link{line: line, target: a.Val})
			case (tok.Data == "img" || tok.Data == "source") && a.Key == "srcset":
				for part := range strings.SplitSeq(a.Val, ",") {
					if f := strings.Fields(part); len(f) > 0 {
						out = append(out, link{line: line, target: f[0]})
					}
				}
			}
		}
	}
}

func extractHTMLAnchors(buf []byte, anchors map[string]bool) {
	z := xnethtml.NewTokenizer(bytes.NewReader(buf))
	for {
		tt := z.Next()
		if tt == xnethtml.ErrorToken {
			return
		}
		if tt != xnethtml.StartTagToken && tt != xnethtml.SelfClosingTagToken {
			continue
		}
		tok := z.Token()
		for _, a := range tok.Attr {
			if a.Key == "id" || (tok.Data == "a" && a.Key == "name") {
				if val := strings.TrimSpace(a.Val); val != "" {
					anchors[val] = true
				}
			}
		}
	}
}
