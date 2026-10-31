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
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"go.ate.dev/substrate/internal/e2e/procstatus"
)

// workerStatus is a microVM worker's status: its twelve capabilities in the
// permitted, effective and bounding sets, and no seccomp filter.
const workerStatus = `Name:	ateworker-micro
Umask:	0022
State:	S (sleeping)
Tgid:	1
Pid:	1
PPid:	0
NSpid:	1
Threads:	24
CapInh:	0000000000000000
CapPrm:	00000000882011df
CapEff:	00000000882011df
CapBnd:	00000000882011df
CapAmb:	0000000000000000
NoNewPrivs:	1
Seccomp:	0
Seccomp_filters:	0
`

// virtiofsdChildStatus is virtiofsd's serving child, seen from the worker's
// PID namespace: its nine child capabilities, an empty bounding set and its
// own seccomp filter.
const virtiofsdChildStatus = `Name:	virtiofsd
Umask:	0022
State:	S (sleeping)
Tgid:	126
Pid:	126
PPid:	125
NSpid:	126	1
Threads:	2
CapInh:	0000000000000000
CapPrm:	00000000880000df
CapEff:	00000000880000df
CapBnd:	0000000000000000
CapAmb:	0000000000000000
NoNewPrivs:	1
Seccomp:	2
Seccomp_filters:	1
`

func TestParseProcessStatus(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  string
		want    procstatus.Process
		wantErr bool
	}{
		{
			name:   "worker",
			status: workerStatus,
			want:   procstatus.Process{CapPrm: 0x882011df, CapEff: 0x882011df, CapBnd: 0x882011df, NoNewPrivs: 1},
		},
		{
			name:   "virtiofsd child",
			status: virtiofsdChildStatus,
			want:   procstatus.Process{PPID: 125, CapPrm: 0x880000df, CapEff: 0x880000df, NoNewPrivs: 1, Seccomp: 2},
		},
		{
			name:    "missing field",
			status:  "Name:\tx\nPPid:\t1\nCapPrm:\t0\n",
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseProcessStatus(tc.status)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseProcessStatus() = %+v, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseProcessStatus() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func threadStatus(name string, seccomp int) string {
	return fmt.Sprintf("Name:\t%s\nState:\tS (sleeping)\nSeccomp:\t%d\n", name, seccomp)
}

func TestReadProcesses(t *testing.T) {
	proc := t.TempDir()
	write := func(path, contents string) {
		t.Helper()
		path = filepath.Join(proc, path)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("1/cmdline", "/ko-app/ateworker-microvm\x00--pod-uid=abc\x00")
	write("1/status", workerStatus)
	write("1/task/1/status", threadStatus("ateworker-micro", 0))
	// runsc sets the gofer's argv[0]; its comm is "exe"
	write("20/cmdline", "runsc-gofer\x00gofer\x00--bundle\x00/run/bundle\x00")
	write("20/status", workerStatus)
	write("20/task/20/status", threadStatus("exe", 2))
	// names virtiofsd only in an argument
	write("21/cmdline", "/bin/sh\x00-c\x00virtiofsd --shared-dir=/run/shared\x00")
	write("21/status", workerStatus)
	write("21/task/21/status", threadStatus("sh", 0))
	write("22/cmdline", "testserver\x00procstatus\x00")
	// listed, then exited
	if err := os.Mkdir(filepath.Join(proc, "23"), 0o755); err != nil {
		t.Fatal(err)
	}
	const assets = "/var/lib/ate/actors/abc/sandbox-assets/"
	write("126/cmdline", assets+"virtiofsd\x00--shared-dir=/run/shared\x00")
	write("126/status", virtiofsdChildStatus)
	write("126/task/126/status", threadStatus("virtiofsd", 2))
	write("126/task/127/status", threadStatus("virtiofsd", 2))
	write("130/cmdline", assets+"cloud-hypervisor\x00--api-socket\x00/run/vc/vm/abc/clh-api.sock\x00")
	write("130/status", workerStatus)
	write("130/task/130/status", threadStatus("cloud-hyperviso", 0))
	write("130/task/131/status", threadStatus("vmm", 2))
	write("130/task/132/status", threadStatus("vcpu0", 2))
	// a thread that exited
	if err := os.MkdirAll(filepath.Join(proc, "130/task/133"), 0o755); err != nil {
		t.Fatal(err)
	}

	procs, err := readProcesses(proc, []string{"runsc-gofer", "virtiofsd", "cloud-hypervisor"}, 22)
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		PID     int
		Name    string
		Threads []procstatus.Thread
	}
	var got []row
	for _, p := range procs {
		got = append(got, row{p.PID, p.Name, p.Threads})
	}
	want := []row{
		{1, "ateworker-microvm", []procstatus.Thread{{TID: 1, Name: "ateworker-micro"}}},
		{126, "virtiofsd", []procstatus.Thread{{TID: 126, Name: "virtiofsd", Seccomp: 2}, {TID: 127, Name: "virtiofsd", Seccomp: 2}}},
		{130, "cloud-hypervisor", []procstatus.Thread{{TID: 130, Name: "cloud-hyperviso"}, {TID: 131, Name: "vmm", Seccomp: 2}, {TID: 132, Name: "vcpu0", Seccomp: 2}}},
		{20, "runsc-gofer", []procstatus.Thread{{TID: 20, Name: "exe", Seccomp: 2}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("readProcesses() = %+v\nwant %+v", got, want)
	}
}
