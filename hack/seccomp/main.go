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

// Command seccomp writes the worker seccomp profiles in manifests/seccomp,
// one per architecture, from the syscall lists in syscalls.go.
package main

//go:generate go run . -out ../../manifests/seccomp

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// architectures maps each profile's architecture to the seccomp architectures
// it covers, which are moby's for that architecture: the native one and its
// 32-bit compat modes.
var architectures = map[string][]specs.Arch{
	"amd64": {specs.ArchX86_64, specs.ArchX86, specs.ArchX32},
	"arm64": {specs.ArchAARCH64, specs.ArchARM},
}

func main() {
	out := flag.String("out", "", "directory to write the profiles to")
	flag.Parse()
	if *out == "" {
		log.Fatal("-out is required")
	}
	for arch := range architectures {
		data, err := render(arch)
		if err != nil {
			log.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(*out, profileName(arch)), data, 0o644); err != nil {
			log.Fatal(err)
		}
	}
}

func profileName(arch string) string {
	return "substrate-worker-" + arch + ".json"
}

// profile returns the worker profile for arch: deny with EPERM unless a rule
// allows the syscall.
func profile(arch string) (*specs.LinuxSeccomp, error) {
	archs, ok := architectures[arch]
	if !ok {
		return nil, fmt.Errorf("unknown architecture %q", arch)
	}
	allowed := slices.Concat(mobyDefault, workerSyscalls, archSyscalls[arch])
	allowed = slices.DeleteFunc(allowed, func(name string) bool {
		return slices.Contains(notOnArch[arch], name)
	})
	slices.Sort(allowed)
	for i := 1; i < len(allowed); i++ {
		if allowed[i] == allowed[i-1] {
			return nil, fmt.Errorf("syscall %s is listed twice", allowed[i])
		}
	}
	eperm := uint(1)
	p := &specs.LinuxSeccomp{
		DefaultAction:   specs.ActErrno,
		DefaultErrnoRet: &eperm,
		Architectures:   archs,
		Syscalls:        []specs.LinuxSyscall{{Names: allowed, Action: specs.ActAllow}},
	}
	p.Syscalls = append(p.Syscalls, specs.LinuxSyscall{
		Names:  []string{"socket"},
		Action: specs.ActAllow,
		Args:   []specs.LinuxSeccompArg{{Index: 0, Value: socketFamilies.below, Op: specs.OpLessThan}},
	})
	for _, family := range socketFamilies.equal {
		p.Syscalls = append(p.Syscalls, specs.LinuxSyscall{
			Names:  []string{"socket"},
			Action: specs.ActAllow,
			Args:   []specs.LinuxSeccompArg{{Index: 0, Value: family, Op: specs.OpEqualTo}},
		})
	}
	for _, persona := range personalities {
		p.Syscalls = append(p.Syscalls, specs.LinuxSyscall{
			Names:  []string{"personality"},
			Action: specs.ActAllow,
			Args:   []specs.LinuxSeccompArg{{Index: 0, Value: persona, Op: specs.OpEqualTo}},
		})
	}
	return p, nil
}

// render returns arch's profile as indented JSON.
func render(arch string) ([]byte, error) {
	p, err := profile(arch)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(p); err != nil {
		return nil, fmt.Errorf("encoding %s profile: %w", arch, err)
	}
	return buf.Bytes(), nil
}
