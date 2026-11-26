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

// Command mdlinks verifies internal links and heading anchors in Markdown
// files without making network requests.
//
//	mdlinks [--root DIR] <file.md>...
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	root := flag.String("root", ".", "Repository root used to resolve relative file arguments, root-relative links, and path bounds.")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: mdlinks [--root DIR] <file.md>...")
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "Error: at least one Markdown file is required")
		flag.Usage()
		os.Exit(2)
	}

	c, err := newChecker(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	var problems []problem
	for _, file := range flag.Args() {
		found, err := c.checkFile(file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %s: %v\n", file, err)
			os.Exit(1)
		}
		problems = append(problems, found...)
	}

	if len(problems) > 0 {
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, p.String())
		}
		os.Exit(1)
	}
}
