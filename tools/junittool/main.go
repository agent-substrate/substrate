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
		if paths, err = expand(fs.Args()); err != nil {
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
