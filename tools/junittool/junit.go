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
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"
)

// testSuites matches the JUnit gotestsum writes; only counts are read. Summed
// from the testsuite children, not the root attribute, so a root disagreeing
// with its children cannot inflate the total.
type testSuites struct {
	Suites []struct {
		Name     string `xml:"name,attr"`
		Tests    int    `xml:"tests,attr"`
		Failures int    `xml:"failures,attr"`
		Errors   int    `xml:"errors,attr"`
		Skipped  int    `xml:"skipped,attr"`
	} `xml:"testsuite"`
}

type totals struct {
	tests, failures, errors, skipped int
}

func (t *totals) add(o totals) {
	t.tests += o.tests
	t.failures += o.failures
	t.errors += o.errors
	t.skipped += o.skipped
}

func parse(path string) (totals, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return totals{}, err
	}
	var ts testSuites
	if err := xml.Unmarshal(raw, &ts); err != nil {
		return totals{}, fmt.Errorf("parsing %s: %w", path, err)
	}
	var out totals
	for _, s := range ts.Suites {
		out.add(totals{tests: s.Tests, failures: s.Failures, errors: s.Errors, skipped: s.Skipped})
	}
	return out, nil
}

// expand resolves each argument to JUnit files. A directory contributes the
// *.xml inside it; anything else passes through literally, so a missing file
// reaches parse() and is reported.
func expand(args []string) ([]string, error) {
	var out []string
	for _, a := range args {
		info, err := os.Stat(a)
		if err != nil || !info.IsDir() {
			out = append(out, a)
			continue
		}
		matches, err := filepath.Glob(filepath.Join(a, "*.xml"))
		if err != nil {
			return nil, fmt.Errorf("expanding %s: %w", a, err)
		}
		sort.Strings(matches)
		out = append(out, matches...)
	}
	return out, nil
}

// row is one file's contribution. err set means the counts are meaningless.
type row struct {
	path string
	t    totals
	err  error
}

// result is the verify decision, separate from reporting so it can be tested
// without capturing output.
type result struct {
	rows       []row
	grand      totals
	empty      []string
	unreadable []string
}

// ok reports whether every file was readable and met the minimum.
func (r result) ok() bool { return len(r.empty) == 0 && len(r.unreadable) == 0 }

// evaluate reads and classifies every path. min applies per file, never to the
// total, so one empty file fails even when others are full. Order matches the
// manifest.
func evaluate(paths []string, min int) result {
	var res result
	for _, p := range paths {
		t, err := parse(p)
		res.rows = append(res.rows, row{path: p, t: t, err: err})
		if err != nil {
			res.unreadable = append(res.unreadable, p)
			continue
		}
		res.grand.add(t)
		if t.tests < min {
			res.empty = append(res.empty, p)
		}
	}
	return res
}

// write renders the per-file table to out and the reasons for failure to errOut.
func (r result) write(out, errOut io.Writer, min int) {
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "FILE\tTESTS\tFAILURES\tERRORS\tSKIPPED")
	for _, row := range r.rows {
		if row.err != nil {
			fmt.Fprintf(w, "%s\t-\t-\t-\t-\n", row.path)
			continue
		}
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\n", row.path, row.t.tests, row.t.failures, row.t.errors, row.t.skipped)
	}
	fmt.Fprintf(w, "TOTAL\t%d\t%d\t%d\t%d\n", r.grand.tests, r.grand.failures, r.grand.errors, r.grand.skipped)
	w.Flush()

	for _, row := range r.rows {
		if row.err != nil {
			fmt.Fprintf(errOut, "junittool: %v\n", row.err)
		}
	}
	for _, p := range r.empty {
		fmt.Fprintf(errOut,
			"junittool: %s reported %d test(s), want at least %d. "+
				"Check the package list and -run filter of the run that wrote it.\n",
			p, r.testsFor(p), min)
	}
}

// testsFor returns the count recorded for path, for error messages.
func (r result) testsFor(path string) int {
	for _, row := range r.rows {
		if row.path == path {
			return row.t.tests
		}
	}
	return 0
}
