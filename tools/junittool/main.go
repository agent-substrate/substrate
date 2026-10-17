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

// Command junittool records the JUnit files a CI run will write, then checks
// they reported tests.
//
//	junittool register <path>          record a file this run will write
//	junittool verify -manifest <file>  require every recorded file to hold tests
//	junittool report <dir>             summarize failures from the go test -json files
//
// An exit code cannot distinguish "all tests passed" from "no test ran".
// `go test ./...` passes the e2e suites with no cluster, and a package filter
// that stops matching reports success having run nothing.
//
// Registering before the run has two effects: the expected set comes from the
// runs themselves, so adding a run needs no change to verify; and a run that
// died part-way is distinguishable from one that never existed.
//
// No dependencies — it runs on the critical path of every CI job.
package main

import (
	"flag"
	"fmt"
	"os"
)

const usage = `usage:
  junittool register [-manifest FILE] [-reject-duplicate] <junit.xml>
  junittool verify (-manifest FILE | <junit.xml|dir>...) [-min N]
  junittool report [-summary FILE] [-annotate] [-root DIR] <test.json|dir>...
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "register":
		os.Exit(runRegister(os.Args[2:]))
	case "verify":
		os.Exit(runVerify(os.Args[2:]))
	case "report":
		os.Exit(runReport(os.Args[2:]))
	case "-h", "--help", "help":
		fmt.Print(usage)
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "junittool: unknown subcommand %q\n%s", os.Args[1], usage)
		os.Exit(2)
	}
}

// inCI follows cmd/ateapi/internal/store/dockerenv.Required: strict in CI,
// permissive locally, where re-running into the same file is ordinary.
// See docs/dev/best-practices/ci-fail-closed.md.
func inCI() bool { return os.Getenv("CI") == "true" }

func runRegister(args []string) int {
	fs := flag.NewFlagSet("register", flag.ExitOnError)
	manifest := fs.String("manifest", "",
		"manifest to append to (default: expected-junit.txt beside the entry)")
	reject := fs.Bool("reject-duplicate", inCI(),
		"treat a repeat registration as an error (defaults true when CI=true)")
	_ = fs.Parse(args)

	if fs.NArg() != 1 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	entry := fs.Arg(0)
	path := *manifest
	if path == "" {
		path = defaultManifest(entry)
	}
	if err := register(path, entry, *reject); err != nil {
		fmt.Fprintf(os.Stderr, "junittool register: %v\n", err)
		return 1
	}
	return 0
}

func runVerify(args []string) int {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	min := fs.Int("min", 1, "minimum number of test cases EACH file must report")
	manifest := fs.String("manifest", "", "manifest listing the JUnit paths that were registered")
	_ = fs.Parse(args)

	var paths []string
	switch {
	case *manifest != "":
		if fs.NArg() > 0 {
			fmt.Fprintln(os.Stderr, "junittool verify: -manifest takes no positional arguments")
			return 2
		}
		var err error
		if paths, err = readManifest(*manifest); err != nil {
			fmt.Fprintf(os.Stderr, "junittool verify: %v\n", err)
			return 1
		}
		if len(paths) == 0 {
			fmt.Fprintf(os.Stderr,
				"junittool verify: %s is empty. No test run registered a JUnit file, "+
					"so nothing ran.\n", *manifest)
			return 1
		}
	case fs.NArg() > 0:
		var err error
		if paths, err = expand(fs.Args(), ".xml"); err != nil {
			fmt.Fprintf(os.Stderr, "junittool verify: %v\n", err)
			return 1
		}
	default:
		fmt.Fprint(os.Stderr, usage)
		return 2
	}

	res := evaluate(paths, *min)
	res.write(os.Stdout, os.Stderr, *min)
	if !res.ok() {
		return 1
	}
	return 0
}

// runReport lists each failed test with its t.Error and t.Fatal output. It
// only reports, the test steps and verify decide whether the job fails.
//
// NOTE: the go test -json files must come from runs without -v, which
// replaces the -test.v=test2json that marks error output.
func runReport(args []string) int {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	summary := fs.String("summary", os.Getenv("GITHUB_STEP_SUMMARY"),
		"file to append a markdown summary to, defaults to $GITHUB_STEP_SUMMARY")
	annotate := fs.Bool("annotate", os.Getenv("GITHUB_ACTIONS") == "true",
		"print GitHub error annotations for failures (defaults true in GitHub Actions)")
	root := fs.String("root", os.Getenv("GITHUB_WORKSPACE"),
		"repository root, for annotation file paths, defaults to $GITHUB_WORKSPACE")
	_ = fs.Parse(args)

	if fs.NArg() == 0 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	paths, err := expand(fs.Args(), ".json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "junittool report: %v\n", err)
		return 1
	}
	var runs []run
	for _, p := range paths {
		r := parseRun(p)
		r.writeText(os.Stdout)
		runs = append(runs, r)
	}
	if *annotate {
		writeAnnotations(os.Stdout, runs, *root)
	}
	if *summary != "" {
		f, err := os.OpenFile(*summary, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "junittool report: %v\n", err)
			return 1
		}
		defer f.Close()
		if err := writeMarkdown(f, runs, summaryBudget); err != nil {
			fmt.Fprintf(os.Stderr, "junittool report: failed to write summary: %v\n", err)
			return 1
		}
	}
	return 0
}
