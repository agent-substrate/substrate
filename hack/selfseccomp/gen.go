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

// Command selfseccomp generates internal/selfseccomp's per-architecture
// syscall tables from manifests/seccomp and the vendored golang.org/x/sys/unix
// syscall tables. Profile names without a native 64-bit syscall (32-bit/x32
// compat names) are skipped.
//
//go:generate go run . -out ../../internal/selfseccomp
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
)

type profileArg struct {
	Index int    `json:"index"`
	Value uint64 `json:"value"`
	Op    string `json:"op"`
}

type profileSyscall struct {
	Names  []string     `json:"names"`
	Action string       `json:"action"`
	Args   []profileArg `json:"args"`
}

type profile struct {
	Syscalls []profileSyscall `json:"syscalls"`
}

var supportedOps = map[string]bool{
	"SCMP_CMP_LT": true,
	"SCMP_CMP_EQ": true,
}

type conditionalRule struct {
	Op    string
	Value uint32
}

type conditionalSyscall struct {
	Name  string
	Nr    uint32
	Rules []conditionalRule
}

var sysnumLine = regexp.MustCompile(`SYS_([A-Z0-9_]+)\s*=\s*([0-9]+)`)

func archSyscallNumbers(path string) (map[string]uint32, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	numbers := map[string]uint32{}
	for _, m := range sysnumLine.FindAllStringSubmatch(string(data), -1) {
		n, err := strconv.ParseUint(m[2], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%s: parsing %s: %w", path, m[0], err)
		}
		numbers[strings.ToLower(m[1])] = uint32(n)
	}
	if len(numbers) == 0 {
		return nil, fmt.Errorf("%s: found no SYS_* constants", path)
	}
	return numbers, nil
}

// readProfile parses a generated seccomp profile into unconditional syscall
// names and args[0]-conditioned rules. Any action or argument shape that
// internal/selfseccomp cannot represent fails generation rather than silently
// widening the filter.
func readProfile(path string) (unconditional []string, conditional map[string][]conditionalRule, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var p profile
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	unconditionalSeen := map[string]bool{}
	conditional = map[string][]conditionalRule{}
	for _, s := range p.Syscalls {
		if s.Action != "SCMP_ACT_ALLOW" {
			return nil, nil, fmt.Errorf("%s: %v has unsupported action %q", path, s.Names, s.Action)
		}
		if len(s.Args) == 0 {
			for _, name := range s.Names {
				if len(conditional[name]) > 0 {
					return nil, nil, fmt.Errorf("%s: %q appears in both unconditional and conditional rules", path, name)
				}
				unconditionalSeen[name] = true
			}
			continue
		}
		// Multiple entries in s.Args are ANDed in the OCI spec; the BPF
		// compiler only supports single-condition rules ORed across entries.
		if len(s.Args) != 1 {
			return nil, nil, fmt.Errorf("%s: %v has %d ANDed args in one entry, only 1 is supported", path, s.Names, len(s.Args))
		}
		a := s.Args[0]
		if a.Index != 0 {
			return nil, nil, fmt.Errorf("%s: %v filters args[%d], only args[0] is supported", path, s.Names, a.Index)
		}
		if !supportedOps[a.Op] {
			return nil, nil, fmt.Errorf("%s: %v uses unsupported op %q", path, s.Names, a.Op)
		}
		if a.Value > 0xFFFFFFFF {
			return nil, nil, fmt.Errorf("%s: %v value %d exceeds uint32", path, s.Names, a.Value)
		}
		for _, name := range s.Names {
			if unconditionalSeen[name] {
				return nil, nil, fmt.Errorf("%s: %q appears in both unconditional and conditional rules", path, name)
			}
			conditional[name] = append(conditional[name], conditionalRule{Op: a.Op, Value: uint32(a.Value)})
		}
	}
	for name := range unconditionalSeen {
		unconditional = append(unconditional, name)
	}
	sort.Strings(unconditional)
	return unconditional, conditional, nil
}

func opConst(op string) string {
	switch op {
	case "SCMP_CMP_LT":
		return "opLessThan"
	case "SCMP_CMP_EQ":
		return "opEqual"
	default:
		panic("unsupported op " + op)
	}
}

func main() {
	out := flag.String("out", ".", "Directory to write generated files into")
	manifestsDir := flag.String("manifests", "../../manifests/seccomp", "Directory holding substrate-worker-<arch>.json profiles")
	vendorDir := flag.String("vendor", "../../vendor/golang.org/x/sys/unix", "Vendored golang.org/x/sys/unix directory")
	flag.Parse()

	for _, goarch := range []string{"amd64", "arm64"} {
		unconditionalNames, conditionalByName, err := readProfile(filepath.Join(*manifestsDir, "substrate-worker-"+goarch+".json"))
		if err != nil {
			log.Fatal(err)
		}
		numbers, err := archSyscallNumbers(filepath.Join(*vendorDir, "zsysnum_linux_"+goarch+".go"))
		if err != nil {
			log.Fatal(err)
		}

		archUpper := strings.ToUpper(goarch)
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "//go:build linux\n\n")
		fmt.Fprintf(&buf, "// Copyright 2026 The Agent Substrate Authors\n//\n")
		fmt.Fprintf(&buf, "// Licensed under the Apache License, Version 2.0 (the \"License\");\n")
		fmt.Fprintf(&buf, "// you may not use this file except in compliance with the License.\n")
		fmt.Fprintf(&buf, "// You may obtain a copy of the License at\n//\n")
		fmt.Fprintf(&buf, "//     http://www.apache.org/licenses/LICENSE-2.0\n//\n")
		fmt.Fprintf(&buf, "// Unless required by applicable law or agreed to in writing, software\n")
		fmt.Fprintf(&buf, "// distributed under the License is distributed on an \"AS IS\" BASIS,\n")
		fmt.Fprintf(&buf, "// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n")
		fmt.Fprintf(&buf, "// See the License for the specific language governing permissions and\n")
		fmt.Fprintf(&buf, "// limitations under the License.\n\n")
		fmt.Fprintf(&buf, "// Code generated by hack/selfseccomp. DO NOT EDIT.\n\n")
		fmt.Fprintf(&buf, "package selfseccomp\n\n")

		var resolved []uint32
		var skipped []string
		for _, name := range unconditionalNames {
			if n, ok := numbers[name]; ok {
				resolved = append(resolved, n)
			} else {
				skipped = append(skipped, name)
			}
		}
		slices.Sort(resolved)
		resolved = slices.Compact(resolved)

		fmt.Fprintf(&buf, "// allowedSyscalls%s lists the native %s syscall numbers allowed unconditionally.\n", archUpper, goarch)
		fmt.Fprintf(&buf, "var allowedSyscalls%s = []uint32{\n", archUpper)
		for _, n := range resolved {
			fmt.Fprintf(&buf, "\t%d,\n", n)
		}
		fmt.Fprintf(&buf, "}\n")
		if len(skipped) > 0 {
			fmt.Fprintf(&buf, "\n// Skipped (no native %s syscall): %s\n", goarch, strings.Join(skipped, ", "))
		}

		var condNames []string
		for name := range conditionalByName {
			condNames = append(condNames, name)
		}
		sort.Strings(condNames)

		var condSkipped []string
		var emittedCond []conditionalSyscall
		for _, name := range condNames {
			n, ok := numbers[name]
			if !ok {
				condSkipped = append(condSkipped, name)
				continue
			}
			emittedCond = append(emittedCond, conditionalSyscall{Name: name, Nr: n, Rules: conditionalByName[name]})
		}

		fmt.Fprintf(&buf, "\n// conditionalSyscalls%s lists native %s syscalls allowed only for matching args[0] values.\n", archUpper, goarch)
		fmt.Fprintf(&buf, "var conditionalSyscalls%s = []conditionalSyscall{\n", archUpper)
		for _, c := range emittedCond {
			fmt.Fprintf(&buf, "\t{Name: %q, Nr: %d, Rules: []conditionalRule{\n", c.Name, c.Nr)
			for _, r := range c.Rules {
				fmt.Fprintf(&buf, "\t\t{Op: %s, Value: %d},\n", opConst(r.Op), r.Value)
			}
			fmt.Fprintf(&buf, "\t}},\n")
		}
		fmt.Fprintf(&buf, "}\n")
		if len(condSkipped) > 0 {
			fmt.Fprintf(&buf, "\n// Skipped conditional (no native %s syscall): %s\n", goarch, strings.Join(condSkipped, ", "))
		}

		formatted, err := format.Source(buf.Bytes())
		if err != nil {
			log.Fatalf("formatting %s table: %v", goarch, err)
		}
		// Prefix with <arch>_ rather than _<arch>.go so both tables compile on
		// every Linux architecture for cross-arch unit tests.
		outPath := filepath.Join(*out, goarch+"_syscallnum.go")
		if err := os.WriteFile(outPath, formatted, 0o644); err != nil {
			log.Fatal(err)
		}
	}
}
