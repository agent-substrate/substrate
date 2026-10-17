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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const mod = "example.com/m"

// stream renders events as go test -json output.
func stream(t *testing.T, events ...event) string {
	t.Helper()
	var b strings.Builder
	for _, e := range events {
		raw, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(raw)
		b.WriteByte('\n')
	}
	return b.String()
}

func out(pkg, test, typ, line string) event {
	return event{Action: "output", Package: pkg, Test: test, OutputType: typ, Output: line + "\n"}
}

func end(action, pkg, test string) event {
	return event{Action: action, Package: pkg, Test: test}
}

func TestParseRun(t *testing.T) {
	a, b, c, d, e := mod+"/a", mod+"/b", mod+"/c", mod+"/d", mod+"/e"
	content := stream(t,
		// passing test, its logs are not reported
		out(a, "TestPass", "frame", "=== RUN   TestPass"),
		out(a, "TestPass", "", "    a_test.go:5: fine"),
		end("pass", a, "TestPass"),
		end("skip", a, "TestSkip"),
		// failed subtests; the parent only has logs
		out(a, "TestParent", "", "    a_test.go:10: setting up"),
		out(a, "TestParent/sub", "frame", "=== RUN   TestParent/sub"),
		out(a, "TestParent/sub", "", "    a_test.go:12: trying"),
		out(a, "TestParent/sub", "error", "    a_test.go:13: expected 1"),
		out(a, "TestParent/sub", "error-continue", "        but got 2"),
		out(a, "TestParent/sub", "frame", "    --- FAIL: TestParent/sub (0.00s)"),
		end("fail", a, "TestParent/sub"),
		end("fail", a, "TestParent"),
		// a failed parent with an error of its own is still reported
		out(a, "TestOwn/sub", "error", "    a_test.go:20: sub broke"),
		end("fail", a, "TestOwn/sub"),
		out(a, "TestOwn", "error", "    a_test.go:22: cleanup broke"),
		end("fail", a, "TestOwn"),
		// a panic has no error lines
		out(a, "TestPanic", "", "    a_test.go:30: before"),
		out(a, "TestPanic", "frame", "--- FAIL: TestPanic (0.00s)"),
		out(a, "TestPanic", "", "panic: boom [recovered]"),
		out(a, "TestPanic", "", ""),
		out(a, "TestPanic", "", "a.TestPanic(...)"),
		end("fail", a, "TestPanic"),
		// a logged panic is not a crash
		out(a, "TestQuotesPanic", "", "    a_test.go:41: pod logs:"),
		out(a, "TestQuotesPanic", "", "        panic: not ours"),
		out(a, "TestQuotesPanic", "error", "    a_test.go:42: pod crashed"),
		end("fail", a, "TestQuotesPanic"),
		// the race report says more than the error testing adds for it
		out(a, "TestRace", "", "WARNING: DATA RACE"),
		out(a, "TestRace", "", "Write at 0x1 by goroutine 7:"),
		out(a, "TestRace", "error", "    testing.go:1865: race detected during execution of test"),
		end("fail", a, "TestRace"),
		end("fail", a, ""),
		// a timeout leaves tests unfinished, the panic lands in whichever test
		// printed a framing line last
		event{Action: "run", Package: b, Test: "TestHang"},
		event{Action: "run", Package: b, Test: "TestHang2"},
		event{Action: "run", Package: b, Test: "TestHang2/sub"},
		out(b, "TestQuick", "frame", "--- PASS: TestQuick (0.00s)"),
		out(b, "TestQuick", "", "panic: test timed out after 2s"),
		out(b, "TestQuick", "", "\trunning tests:"),
		out(b, "TestQuick", "", ""),
		out(b, "TestQuick", "", "goroutine 1 [chan receive]:"),
		out(b, "TestQuick", "", "testing.(*M).Run(0x1)"),
		out(b, "TestQuick", "", ""),
		out(b, "TestQuick", "", "goroutine 7 [sleep]:"),
		out(b, "TestQuick", "", "b.TestHang2.func1(0x2)"),
		out(b, "TestQuick", "", "\t/src/b/b_test.go:9 +0x1"),
		end("pass", b, "TestQuick"),
		end("fail", b, ""),
		// build failure
		event{Action: "build-output", ImportPath: c + " [" + c + ".test]", Output: "c/c_test.go:5:28: undefined: x\n"},
		event{Action: "fail", Package: c, FailedBuild: c + " [" + c + ".test]"},
		// TestMain exits non-zero
		out(d, "", "", "TestMain setup failed"),
		end("fail", d, ""),
		// a panicking subtest's panic is filed under its parent
		out(e, "TestParent", "", "    e_test.go:13: parent setup"),
		out(e, "TestParent/sub", "frame", "--- FAIL: TestParent/sub (0.00s)"),
		end("fail", e, "TestParent/sub"),
		out(e, "TestParent", "frame", "--- FAIL: TestParent (0.00s)"),
		out(e, "TestParent", "", "panic: boom [recovered, repanicked]"),
		end("fail", e, "TestParent"),
		end("fail", e, ""),
	) + "not json\n"
	path := write(t, t.TempDir(), "unit.json", content)

	got := parseRun(path)
	if got.err != nil {
		t.Fatalf("parseRun() error = %v", got.err)
	}
	if got.name != "unit" || got.passed != 2 || got.failed != 14 || got.skipped != 1 {
		t.Errorf("parseRun() = %s %d passed, %d failed, %d skipped; want unit 2, 14, 1",
			got.name, got.passed, got.failed, got.skipped)
	}

	want := []failure{
		{pkg: a, test: "TestOwn", message: []string{"    a_test.go:22: cleanup broke"}},
		{pkg: a, test: "TestOwn/sub", message: []string{"    a_test.go:20: sub broke"}},
		{pkg: a, test: "TestPanic",
			message: []string{"panic: boom [recovered]", "", "a.TestPanic(...)"},
			context: []string{"    a_test.go:30: before", "panic: boom [recovered]", "", "a.TestPanic(...)"}},
		{pkg: a, test: "TestParent/sub",
			message: []string{"    a_test.go:13: expected 1", "        but got 2"},
			context: []string{"    a_test.go:12: trying", "    a_test.go:13: expected 1", "        but got 2"}},
		{pkg: a, test: "TestQuotesPanic",
			message: []string{"    a_test.go:42: pod crashed"},
			context: []string{"    a_test.go:41: pod logs:", "        panic: not ours", "    a_test.go:42: pod crashed"}},
		{pkg: a, test: "TestRace", message: []string{"WARNING: DATA RACE", "Write at 0x1 by goroutine 7:",
			"    testing.go:1865: race detected during execution of test"}},
		{pkg: b, label: "did not finish: TestHang, TestHang2/sub",
			message: []string{"panic: test timed out after 2s", "\trunning tests:", "",
				"goroutine 1 [chan receive]:", "testing.(*M).Run(0x1)", "",
				"goroutine 7 [sleep]:", "b.TestHang2.func1(0x2)", "\t/src/b/b_test.go:9 +0x1"},
			context: []string{"goroutine 7 [sleep]:", "b.TestHang2.func1(0x2)", "\t/src/b/b_test.go:9 +0x1"}},
		{pkg: c, label: "build failed", message: []string{"c/c_test.go:5:28: undefined: x"}},
		{pkg: d, label: "failed outside of a test", message: []string{"TestMain setup failed"}},
		{pkg: e, test: "TestParent/sub", message: []string{"panic: boom [recovered, repanicked]"}},
	}
	if !reflect.DeepEqual(got.failures, want) {
		t.Errorf("failures:\n%s\nwant:\n%s", dump(got.failures), dump(want))
	}
}

func dump(fs []failure) string {
	var b strings.Builder
	for _, f := range fs {
		fmt.Fprintf(&b, "  %+v\n", f)
	}
	return b.String()
}

func TestParseRunMissing(t *testing.T) {
	got := parseRun(filepath.Join(t.TempDir(), "e2e-gvisor.json"))
	if got.err == nil || got.name != "e2e-gvisor" {
		t.Errorf("parseRun(missing) = %+v, want an error for e2e-gvisor", got)
	}
}

func TestExcerpt(t *testing.T) {
	lines := []string{"1", "2", "3", "4"}
	if got := excerpt(lines, 2); !reflect.DeepEqual(got, []string{"3", "4"}) {
		t.Errorf("excerpt() = %v, want the last 2 lines", got)
	}
	race := []string{"log", "WARNING: DATA RACE", "Write at", "by goroutine 7", "more"}
	if got := excerpt(race, 3); !reflect.DeepEqual(got, race[1:4]) {
		t.Errorf("excerpt() = %v, want the race report", got)
	}
}

func TestWriteAnnotations(t *testing.T) {
	root := t.TempDir()
	write(t, root, "go.mod", "module "+mod+"\n\ngo 1.27.0\n")
	if err := os.MkdirAll(filepath.Join(root, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(root, "a"), "a_test.go", "package a\n")

	runs := []run{{name: "unit", failures: []failure{
		{pkg: mod + "/a", test: "TestX", message: []string{"    a_test.go:13: 50% off, a: b", "        next"}},
		// the file is in another package, so no location
		{pkg: mod + "/a", test: "TestY", message: []string{"    helper.go:7: bad"}},
		{pkg: mod + "/a", test: "TestZ", message: []string{"panic: boom",
			"\t/usr/lib/go/src/testing/testing.go:2123 +0x2b8",
			"\t" + filepath.Join(root, "a", "a_test.go") + ":31 +0x3c"}},
		// a timeout's hung test is in the context
		{pkg: mod + "/a", label: "did not finish: TestW", message: []string{"panic: test timed out after 2s"},
			context: []string{"a.TestW(0x1)", "\t" + filepath.Join(root, "a", "a_test.go") + ":40 +0x3c"}},
	}}}
	var b strings.Builder
	writeAnnotations(&b, runs, root)
	want := "::error file=a/a_test.go,line=13,title=unit%3A a.TestX::a_test.go:13: 50%25 off, a: b%0A    next\n" +
		"::error title=unit%3A a.TestY::helper.go:7: bad\n" +
		"::error file=a/a_test.go,line=31,title=unit%3A a.TestZ::panic: boom%0A\t/usr/lib/go/src/testing/testing.go:2123 +0x2b8%0A\t" +
		filepath.Join(root, "a", "a_test.go") + ":31 +0x3c\n" +
		"::error file=a/a_test.go,line=40,title=unit%3A a did not finish%3A TestW::panic: test timed out after 2s\n"
	if b.String() != want {
		t.Errorf("writeAnnotations() =\n%s\nwant:\n%s", b.String(), want)
	}

	t.Run("capped", func(t *testing.T) {
		r := run{name: "e2e"}
		for i := range 12 {
			r.failures = append(r.failures, failure{pkg: mod + "/a", test: fmt.Sprintf("Test%d", i)})
		}
		var b strings.Builder
		writeAnnotations(&b, []run{r}, "")
		lines := strings.Split(strings.TrimSpace(b.String()), "\n")
		if len(lines) != maxAnnotations {
			t.Fatalf("writeAnnotations() printed %d lines, want %d", len(lines), maxAnnotations)
		}
		if last := lines[len(lines)-1]; last != "::error::3 more test failures, see the job summary" {
			t.Errorf("last annotation = %q", last)
		}
	})
}

func TestWriteMarkdown(t *testing.T) {
	runs := []run{
		{name: "unit", passed: 3, failed: 1, failures: []failure{
			{pkg: mod + "/a", test: "TestX<y>", elapsed: 1.5, message: []string{"    a_test.go:1: has ``` in it"}},
		}},
		{name: "root", err: os.ErrNotExist},
	}
	var b strings.Builder
	if err := writeMarkdown(&b, runs, summaryBudget); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"| unit | 3 | 1 | 0 |",
		"| root | - | - | - |",
		"<code>TestX&lt;y&gt;</code> in <code>" + mod + "/a</code> (1.50s)",
		"````text\na_test.go:1: has ``` in it\n````\n",
	} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("writeMarkdown() missing %q in:\n%s", want, b.String())
		}
	}

	t.Run("over budget", func(t *testing.T) {
		var b strings.Builder
		if err := writeMarkdown(&b, runs, 200); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(b.String(), "TestX") || !strings.Contains(b.String(), "1 more failures did not fit") {
			t.Errorf("writeMarkdown() over budget =\n%s", b.String())
		}
	})
}
