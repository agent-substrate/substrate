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

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	maxMessageLines = 20
	maxContextLines = 60
	maxStackLines   = 200
	maxLineLen      = 1000
	// GitHub rejects a step summary over 1MiB
	summaryBudget = 900 << 10
	// GitHub drops error annotations past 10 per step
	maxAnnotations = 10
)

// event is one line of `go test -json`, see `go doc cmd/test2json`
type event struct {
	Action      string
	Package     string
	Test        string
	Elapsed     float64
	Output      string
	OutputType  string
	ImportPath  string
	FailedBuild string
}

type testResult struct {
	action  string // "" if the test never finished
	elapsed float64
	output  []string // everything but framing
	errors  []string // from t.Error and t.Fatal
}

type pkgResult struct {
	action      string
	failedBuild string
	output      []string // not attributed to a test
	all         []string // all output in order, test or not
	tests       map[string]*testResult
}

// failure is a failed test, or a package that failed with no failed test to
// blame (build failure, timeout, crash)
type failure struct {
	pkg     string
	test    string
	label   string // in place of test for a package failure
	elapsed float64
	message []string
	context []string
}

func (f failure) title() string {
	if f.test != "" {
		return f.test
	}
	return f.label
}

// run is the result of one `go test -json` stream
type run struct {
	name                    string
	passed, failed, skipped int
	failures                []failure
	err                     error
}

func parseRun(path string) run {
	r := run{name: strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))}
	f, err := os.Open(path)
	if err != nil {
		r.err = err
		return r
	}
	defer f.Close()

	pkgs := map[string]*pkgResult{}
	build := map[string][]string{}
	getPkg := func(name string) *pkgResult {
		p, ok := pkgs[name]
		if !ok {
			p = &pkgResult{tests: map[string]*testResult{}}
			pkgs[name] = p
		}
		return p
	}

	br := bufio.NewReader(f)
	for {
		raw, readErr := br.ReadBytes('\n')
		var e event
		// gotestsum only writes events, but don't let a stray line hide the rest
		if len(raw) > 0 && json.Unmarshal(raw, &e) == nil {
			line := strings.TrimSuffix(e.Output, "\n")
			switch {
			case e.Action == "build-output":
				build[e.ImportPath] = append(build[e.ImportPath], line)
			case e.Test == "":
				p := getPkg(e.Package)
				switch e.Action {
				case "output":
					if e.OutputType != "frame" {
						p.output = append(p.output, line)
						p.all = append(p.all, line)
					}
				case "pass", "fail", "skip":
					p.action = e.Action
					p.failedBuild = e.FailedBuild
				}
			default:
				p := getPkg(e.Package)
				t, ok := p.tests[e.Test]
				if !ok {
					t = &testResult{}
					p.tests[e.Test] = t
				}
				switch e.Action {
				case "output":
					if e.OutputType == "frame" {
						break
					}
					t.output = append(t.output, line)
					p.all = append(p.all, line)
					if e.OutputType == "error" || e.OutputType == "error-continue" {
						t.errors = append(t.errors, line)
					}
				case "pass", "fail", "skip":
					r.count(e.Action)
					// a failure sticks if the test runs again, e.g. with -count
					if t.action != "fail" {
						t.action = e.Action
					}
					t.elapsed = e.Elapsed
				}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			r.err = readErr
			return r
		}
	}

	for _, name := range sortedKeys(pkgs) {
		r.addPackage(name, pkgs[name], build)
	}
	return r
}

func (r *run) count(action string) {
	switch action {
	case "pass":
		r.passed++
	case "fail":
		r.failed++
	case "skip":
		r.skipped++
	}
}

func (r *run) addPackage(name string, p *pkgResult, build map[string][]string) {
	var failed, unfinished []string
	for _, tn := range sortedKeys(p.tests) {
		switch p.tests[tn].action {
		case "fail":
			failed = append(failed, tn)
		case "":
			unfinished = append(unfinished, tn)
		}
	}
	// a panic kills the whole test binary, and test2json files it under
	// whichever test printed a framing line last: the parent of a panicking
	// subtest, or a test that already passed before a timeout
	var crashed []string
	if i := crashIndex(p.all); i >= 0 {
		crashed = p.all[i:]
	}
	for _, tn := range failed {
		t := p.tests[tn]
		// the failed subtests already say why
		if len(t.errors) == 0 && hasSubtest(failed, tn) {
			continue
		}
		f := failure{pkg: name, test: tn, elapsed: t.elapsed}
		// a panic or race report says more than the error testing adds for it
		if f.message = crash(t.output, maxMessageLines); f.message == nil {
			f.message = head(t.errors, maxMessageLines)
		}
		if len(f.message) == 0 {
			f.message = tail(t.output, maxMessageLines)
		}
		if len(f.message) == 0 {
			f.message = head(crashed, maxMessageLines)
		}
		if ctx := tail(t.output, maxContextLines); !slices.Equal(ctx, f.message) {
			f.context = ctx
		}
		r.failures = append(r.failures, f)
	}
	if p.action != "fail" {
		return
	}

	f := failure{pkg: name}
	switch {
	case p.failedBuild != "":
		r.failed++
		f.label = "build failed"
		f.message = head(build[p.failedBuild], maxMessageLines)
		if len(f.message) == 0 {
			f.message = excerpt(p.output, maxMessageLines)
		}
	case len(unfinished) > 0:
		// on a timeout the running tests never report
		r.failed += len(unfinished)
		var leaves, rest []string
		for _, tn := range unfinished {
			rest = append(rest, p.tests[tn].output...)
			if !hasSubtest(unfinished, tn) {
				leaves = append(leaves, tn)
			}
		}
		f.label = "did not finish: " + strings.Join(head(leaves, 5), ", ")
		if len(leaves) > 5 {
			f.label += fmt.Sprintf(" and %d more", len(leaves)-5)
		}
		if crashed != nil {
			f.message = head(crashed, maxMessageLines)
			f.context = head(stacksOf(crashed, leaves), maxStackLines)
		} else {
			f.message = tail(slices.Concat(p.output, rest), maxMessageLines)
		}
	case len(failed) == 0:
		r.failed++
		f.label = "failed outside of a test"
		f.message = excerpt(p.output, maxMessageLines)
	default:
		return
	}
	r.failures = append(r.failures, f)
}

func hasSubtest(tests []string, test string) bool {
	for _, tn := range tests {
		if strings.HasPrefix(tn, test+"/") {
			return true
		}
	}
	return false
}

// excerpt picks up to n lines likely to explain a failure
func excerpt(lines []string, n int) []string {
	if c := crash(lines, n); c != nil {
		return c
	}
	return tail(lines, n)
}

// crash returns up to n lines from the first panic or race report, nil if
// there is none
func crash(lines []string, n int) []string {
	if i := crashIndex(lines); i >= 0 {
		return head(lines[i:], n)
	}
	return nil
}

// crashIndex finds the first panic or race report. The runtime prints these
// unindented, unlike test logs, which may quote a panic from somewhere else.
func crashIndex(lines []string) int {
	for i, l := range lines {
		if strings.HasPrefix(l, "panic: ") || strings.HasPrefix(l, "fatal error: ") ||
			strings.HasPrefix(l, "WARNING: DATA RACE") {
			return i
		}
	}
	return -1
}

// stacksOf picks the goroutines running one of tests out of a stack dump
func stacksOf(lines, tests []string) []string {
	var out, block []string
	keep := false
	flush := func() {
		if keep {
			out = append(out, block...)
		}
		block, keep = nil, false
	}
	for _, l := range lines {
		if strings.HasPrefix(l, "goroutine ") {
			flush()
			block = []string{}
		}
		if block == nil {
			continue
		}
		block = append(block, l)
		for _, tn := range tests {
			// subtests and their closures run as TestX.funcN
			top, _, _ := strings.Cut(tn, "/")
			if strings.Contains(l, "."+top+"(") || strings.Contains(l, "."+top+".func") {
				keep = true
			}
		}
	}
	flush()
	return out
}

func head(lines []string, n int) []string {
	if len(lines) > n {
		return lines[:n]
	}
	return lines
}

func tail(lines []string, n int) []string {
	if len(lines) > n {
		return lines[len(lines)-n:]
	}
	return lines
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// dedent strips the indent go test puts on every line of test output
func dedent(lines []string) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		l = strings.TrimPrefix(l, "    ")
		if len(l) > maxLineLen {
			n := maxLineLen
			for n > 0 && !utf8.RuneStart(l[n]) {
				n--
			}
			l = l[:n] + " ..."
		}
		out[i] = l
	}
	return out
}

func (r run) writeText(w io.Writer) {
	if r.err != nil {
		fmt.Fprintf(w, "%s: no results: %v\n", r.name, r.err)
		return
	}
	fmt.Fprintf(w, "%s: %d passed, %d failed, %d skipped\n", r.name, r.passed, r.failed, r.skipped)
	for _, f := range r.failures {
		fmt.Fprintf(w, "--- FAIL: %s %s\n", f.pkg, f.title())
		for _, l := range dedent(f.message) {
			fmt.Fprintf(w, "    %s\n", l)
		}
	}
}

// writeMarkdown renders runs for the GitHub job summary, leaving out failures
// once the output would pass budget bytes
func writeMarkdown(w io.Writer, runs []run, budget int) error {
	var b strings.Builder
	b.WriteString("| run | passed | failed | skipped |\n|:--|--:|--:|--:|\n")
	for _, r := range runs {
		if r.err != nil {
			fmt.Fprintf(&b, "| %s | - | - | - |\n", r.name)
			continue
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %d |\n", r.name, r.passed, r.failed, r.skipped)
	}
	omitted := 0
	for _, r := range runs {
		if len(r.failures) == 0 {
			continue
		}
		fmt.Fprintf(&b, "\n#### %s\n", html.EscapeString(r.name))
		for _, f := range r.failures {
			entry := f.markdown()
			if b.Len()+len(entry) > budget {
				omitted++
				continue
			}
			b.WriteString(entry)
		}
	}
	if omitted > 0 {
		fmt.Fprintf(&b, "\n%d more failures did not fit, see the step logs.\n", omitted)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func (f failure) markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n<code>%s</code> in <code>%s</code>", html.EscapeString(f.title()), html.EscapeString(f.pkg))
	if f.test != "" {
		fmt.Fprintf(&b, " (%.2fs)", f.elapsed)
	}
	b.WriteString("\n\n")
	writeCodeBlock(&b, dedent(f.message))
	if len(f.context) > 0 {
		b.WriteString("\n<details><summary>output</summary>\n\n")
		writeCodeBlock(&b, dedent(f.context))
		b.WriteString("\n</details>\n")
	}
	return b.String()
}

var backticks = regexp.MustCompile("`+")

// writeCodeBlock fences lines with more backticks than any run inside them,
// so test output can't end the block early
func writeCodeBlock(b *strings.Builder, lines []string) {
	n := 3
	for _, l := range lines {
		for _, m := range backticks.FindAllString(l, -1) {
			n = max(n, len(m)+1)
		}
	}
	fence := strings.Repeat("`", n)
	fmt.Fprintf(b, "%stext\n", fence)
	for _, l := range lines {
		b.WriteString(l)
		b.WriteByte('\n')
	}
	fmt.Fprintf(b, "%s\n", fence)
}

var (
	errorLine = regexp.MustCompile(`^\s*([^\s:]+\.go):(\d+): `)
	stackLine = regexp.MustCompile(`^\s*(/\S+\.go):(\d+)`)
)

// writeAnnotations prints a GitHub error annotation per failure, pointing at
// the line that failed when it is in the test's own package under root
func writeAnnotations(w io.Writer, runs []run, root string) {
	module := modulePath(root)
	total := 0
	for _, r := range runs {
		total += len(r.failures)
	}
	n := 0
	for _, r := range runs {
		for _, f := range r.failures {
			if n == maxAnnotations-1 && total > maxAnnotations {
				fmt.Fprintf(w, "::error::%d more test failures, see the job summary\n", total-n)
				return
			}
			n++
			title := fmt.Sprintf("%s: %s.%s", r.name, path.Base(f.pkg), f.test)
			if f.test == "" {
				title = fmt.Sprintf("%s: %s %s", r.name, path.Base(f.pkg), f.label)
			}
			props := "title=" + escapeProperty(title)
			if file, line := location(root, module, f); file != "" {
				props = fmt.Sprintf("file=%s,line=%s,%s", escapeProperty(file), line, props)
			}
			fmt.Fprintf(w, "::error %s::%s\n", props, escapeData(strings.Join(dedent(f.message), "\n")))
		}
	}
}

// location finds where f failed under root: the file of the first error line
// if it is in the test's own package, else the first stack frame under root,
// which for a timeout is in the context
func location(root, module string, f failure) (file, line string) {
	if root == "" || len(f.message) == 0 {
		return "", ""
	}
	if m := errorLine.FindStringSubmatch(f.message[0]); m != nil {
		if file := sourcePath(root, module, f.pkg, m[1]); file != "" {
			return file, m[2]
		}
		return "", ""
	}
	for _, l := range slices.Concat(f.message, f.context) {
		m := stackLine.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		if rel, err := filepath.Rel(root, m[1]); err == nil && !strings.HasPrefix(rel, "..") {
			return filepath.ToSlash(rel), m[2]
		}
	}
	return "", ""
}

func modulePath(root string) string {
	if root == "" {
		return ""
	}
	raw, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return ""
	}
	for l := range strings.SplitSeq(string(raw), "\n") {
		if m, ok := strings.CutPrefix(strings.TrimSpace(l), "module "); ok {
			if u, err := strconv.Unquote(m); err == nil {
				return u
			}
			return strings.TrimSpace(m)
		}
	}
	return ""
}

// sourcePath returns file's path relative to root if it is in pkg's directory.
// Nested modules here have paths under the root module, so the import path
// maps to a directory either way.
func sourcePath(root, module, pkg, file string) string {
	if module == "" {
		return ""
	}
	rel, ok := strings.CutPrefix(pkg, module)
	if !ok || (rel != "" && !strings.HasPrefix(rel, "/")) {
		return ""
	}
	p := path.Join(strings.TrimPrefix(rel, "/"), file)
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(p))); err != nil {
		return ""
	}
	return p
}

// https://github.com/actions/toolkit/blob/7e25f67e8fd17369aa21e6e816ac99654106e0dc/packages/core/src/command.ts#L103-L117
func escapeData(s string) string {
	return strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A").Replace(s)
}

func escapeProperty(s string) string {
	return strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A", ":", "%3A", ",", "%2C").Replace(s)
}
