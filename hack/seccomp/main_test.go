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
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const profilesDir = "../../manifests/seccomp"

// deniedSyscalls are syscalls manifests/seccomp/README.md says the profiles deny.
var deniedSyscalls = []string{
	"add_key", "bpf", "delete_module", "finit_module", "init_module", "io_uring_enter",
	"io_uring_register", "io_uring_setup", "kexec_file_load", "kexec_load", "keyctl",
	"open_by_handle_at", "perf_event_open", "request_key", "userfaultfd",
}

func TestProfilesUpToDate(t *testing.T) {
	for arch := range architectures {
		want, err := render(arch)
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(profilesDir, profileName(arch)))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s is out of date: run go generate ./hack/seccomp", profileName(arch))
		}
	}
}

// TestProfilesAreValid decodes each committed profile as containerd reads a
// Localhost profile, and checks what it allows.
func TestProfilesAreValid(t *testing.T) {
	for arch := range architectures {
		t.Run(arch, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(profilesDir, profileName(arch)))
			if err != nil {
				t.Fatal(err)
			}
			dec := json.NewDecoder(bytes.NewReader(data))
			dec.DisallowUnknownFields()
			var p specs.LinuxSeccomp
			if err := dec.Decode(&p); err != nil {
				t.Fatalf("decoding %s: %v", profileName(arch), err)
			}
			if dec.More() {
				t.Fatalf("%s has data after the profile", profileName(arch))
			}
			if p.DefaultAction != specs.ActErrno || p.DefaultErrnoRet == nil || *p.DefaultErrnoRet != 1 {
				t.Errorf("default action = %s/%v, want %s with EPERM", p.DefaultAction, p.DefaultErrnoRet, specs.ActErrno)
			}
			if !slices.Equal(p.Architectures, architectures[arch]) {
				t.Errorf("architectures = %v, want %v", p.Architectures, architectures[arch])
			}

			always := map[string]bool{}
			for _, rule := range p.Syscalls {
				if rule.Action != specs.ActAllow {
					t.Errorf("rule for %v has action %s, want %s", rule.Names, rule.Action, specs.ActAllow)
				}
				if len(rule.Args) > 0 {
					continue
				}
				for _, name := range rule.Names {
					if always[name] {
						t.Errorf("%s is allowed twice", name)
					}
					always[name] = true
				}
			}
			for _, rule := range p.Syscalls {
				for _, name := range rule.Names {
					if len(rule.Args) > 0 && always[name] {
						t.Errorf("%s is allowed both with and without argument checks", name)
					}
				}
			}

			for other, names := range archSyscalls {
				if other == arch {
					continue
				}
				for _, name := range names {
					if always[name] {
						t.Errorf("%s profile allows %s, which only %s has", arch, name, other)
					}
				}
			}
			for _, name := range workerSyscalls {
				if !always[name] {
					t.Errorf("%s is not allowed, but the worker needs it", name)
				}
			}
			for _, name := range deniedSyscalls {
				if always[name] {
					t.Errorf("%s is allowed", name)
				}
			}
			const afVsock = 40
			for _, rule := range p.Syscalls {
				if !slices.Equal(rule.Names, []string{"socket"}) {
					continue
				}
				for _, arg := range rule.Args {
					if arg.Index == 0 && ((arg.Op == specs.OpEqualTo && arg.Value == afVsock) || (arg.Op == specs.OpLessThan && arg.Value > afVsock)) {
						t.Errorf("socket rule %+v allows AF_VSOCK", arg)
					}
				}
			}
		})
	}
}
