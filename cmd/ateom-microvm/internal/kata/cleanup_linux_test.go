//go:build linux

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

package kata

import (
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/nodepath"
)

func TestIsSandboxProcess(t *testing.T) {
	const id = "0e16c0fa-5c06-4b48-8742-4068635c08c1"
	staged := nodepath.StaticFilesDir + "/runsc-ed81e9b2"
	argv := func(args ...string) string { return strings.Join(args, "\x00") + "\x00" }
	for _, tc := range []struct {
		name    string
		cmdline string
		exe     string
		want    bool
	}{
		{"staged VMM", argv(staged, "--api-socket", "/run/vc/vm/"+id+"/clh-api.sock"), staged, true},
		{"staged virtiofsd", argv(staged, "--socket-path=/run/vc/vm/"+id+"/virtiofsd.sock"), staged, true},
		{"VMM by name", argv("cloud-hypervisor", "--api-socket", "/run/vc/vm/"+id+"/clh-api.sock"), "/usr/bin/cloud-hypervisor", true},
		{"another sandbox", argv(staged, "--api-socket", "/run/vc/vm/other/clh-api.sock"), staged, false},
		{"unrelated process naming the id", argv("/bin/sh", "-c", "echo "+id), "/bin/busybox", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSandboxProcess(id, tc.cmdline, tc.exe); got != tc.want {
				t.Errorf("isSandboxProcess() = %v, want %v", got, tc.want)
			}
		})
	}
}
