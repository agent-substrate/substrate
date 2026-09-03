// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command cmd prints "<label value> <object-name suffix>" for a build
// version, so shell installers and upgrade tooling use the same derivation
// instead of mirroring it. Versions the label sanitizer would rewrite are
// rejected: the cluster labels must match the version string stamped into
// the binaries byte for byte.
package main

import (
	"fmt"
	"os"

	"github.com/agent-substrate/substrate/internal/versionlabel"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: versionlabel <build version>")
		os.Exit(2)
	}
	raw := os.Args[1]
	if v := versionlabel.Value(raw); v != raw {
		fmt.Fprintf(os.Stderr, "error: build version %q is not a valid label value (it would sanitize to %q); pin a label-safe one with VERSION=...\n", raw, v)
		os.Exit(1)
	}
	fmt.Printf("%s %s\n", raw, versionlabel.NameSuffix(raw))
}
